################ server-msg-googlesheet-include.R
# 
# Das Include, das die immer gleichen Aufgaben erledigt: 
# - Die msg-Funktion vorbereiten
# - Den API-Key für Googlesheets laden
# - Die RKI-Lese-Funktion definieren
# - Die üblichen Bibiotheken laden
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 21.11.2020

# Sollte im WD liegen - so baut man's in ein Skript ein: 

# msgTarget <- "B8:C8" # oder irgendwo - Zielzellen im CSemaphore-GSheet
# 
# if (file.exists("./server-msg-googlesheet-include.R")) {
#   source("./server-msg-googlesheet-include.R")
# } else {
#   source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
# }
# 

# Erwartet eine Variable namens msgTarget, die die Zellen angibt, in die die msg-Funktion schreibt
# Default-Wert: irgendwo unten. 

if (!exists("msgTarget")) {
  msgTarget <- "B21:C21"
}

# Init-Werte fürs Logging, das Arbeitsverzeichnis und die Keyfile-Auswahl
server <- FALSE

# Logging und Update der Semaphore-Seite
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

# ---- Bibliotheken  ----
library(dplyr)
library(stringr)
library(readr)
library(tidyr)
library(openxlsx)
library(googlesheets4) #v0.2.0 - viele Änderungen
library(lubridate)
library(DatawRappr)
library(jsonlite)

# ---- Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----

if (!is.null(msgTarget)) {
  msg <- function(x,...) {
    print(paste0(x,...))
    # Zeitstempel in msgTarget
    d <- data.frame(b = now(tzone= "CEST"), c = paste0(x,...))
    tryCatch(range_write(id_msg,d,sheet="Tabellenblatt1",
                         range=msgTarget,col_names = FALSE,reformat=FALSE))
    if (server) Sys.sleep(1)     # Skript ein wenig runterbremsen wegen Quota
    if (logfile != "") {
      cat(x,...,file = logfile, append = TRUE)
    }
  }
} else {
  msg <- function(x,...) {
    print(paste0(x,...))
    if (logfile != "") {
      cat(x,...,file = logfile, append = TRUE)
    }
  }
}

# Argumente werden in einem String-Vektor namens args übergeben,
# wenn ein Argument übergeben wurde, dort suchen, sonst Unterverzeichnis "src"

args = commandArgs(trailingOnly = TRUE)
if (length(args)!=0) { 
  server <- args[1] %in% c("server","logfile")
  if(args[1] == "logfile") logfile <- "./logs/hessen.log"
} 

sheets_email <- "corona-rki-hmsi-googlesheets@corona-rki-hmsi-2727248702.iam.gserviceaccount.com"
sheets_filename <- "corona-rki-hmsi-2727248702-d35af1e1baab.json"

# VERSION FÜR DEN SERVER 
if (dir.exists("/home/jan_eggers_hr_de/rscripts/")) {
  # Arbeitsverzeichnis, Logdatei beschreiben
  setwd("/home/jan_eggers_hr_de/rscripts/")
  # Authentifizierung Google-Docs umbiegen
  sheets_keypath <- "/home/jan_eggers_hr_de/key/"
  server <- TRUE
} else {
  # Etwas umständlich: Die Pfade zu den beiden Entwicklungs-Rechnern...
  
  # ...mein privater Laptop: 
  if (dir.exists("D:/Nextcloud/hr-DDJ/projekte/covid-19")) {
    setwd("D:/Nextcloud/hr-DDJ/projekte/covid-19")
    sheets_keypath <- "D:/key/"
  }  
  
  # ...mein Datenteam-Laptop im Sender: 
  if (dir.exists("F:/projekte/covid-19")) {
    setwd("F:/projekte/covid-19")
    sheets_keypath <- "F:/creds/"
  }  
  
  # Gibt es die Datei? 
  if (!file.exists(paste0(sheets_keypath,sheets_filename))) { 
    simpleError("Kein Keyfile!")
  }
}


gs4_deauth() # Authentifizierung löschen
gs4_auth(email=sheets_email,path=paste0(sheets_keypath,sheets_filename))

msg("Google Credentials erfolgreich gesetzt\n")

# ---- Start, RKI-Daten lesen, Hessen-Fälle filtern, Kopie schreiben ----

read_rki_data <- function(use_json = TRUE) {
  if (use_json) {
    
    # JSON-Abfrage-Code von Till (danke!)
    # Dokumentation: https://github.com/br-data/corona-deutschland-api/blob/master/RKI-API.md
    rki_json_link <- paste0("https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/",
      "rest/services/RKI_COVID19/FeatureServer/0/query?",
      "where=1%3D1",            # where 1=1
      "&outFields=*",           # alle Ausgabefelder
      "&returnGeometry=false",
      "&cacheHint=true",
      "&f=json",
      "&resultRecordCount=2000")  # 2000 Fälle pro Abfrage (Maximum sind 5k)
    
    
    # ---- GRABSTEIN für rekursiver_offset() ----
    
    # An dieser Stelle, liebe Gemeinde, halten wir inne und gedenken einen
    # Moment der Stille lang unserer Freundin, der alten rekursiven Funktion.
    # Von ihrem Vater Till Hafermann mit unnachahmlicher Eleganz versehen, 
    # erledigte sie ihre Arbeit schnell und diskret wie niemand sonst.
    # Allein, es gelüstete sie nach immer neuen Speicherseiten, und mit 
    # dieser Gier riss sie nicht nur sich, sondern auch die ganze R-Umgebung,
    # in der sie lebte, in den Hungertod - denn ihre Gefräßigkeit beschwor
    # den unbarmherzigen OOM-kill-daemon herauf. 
    # Möge sie in Frieden ruhen. 
    
    # Muss in Stückchen gelesen werden, weil per JSON nicht mehr als 5000 Datensätze zurückgegeben werden
    rki_ <- NULL
    offset <- 0
    
    rki_json_offset <- function(offset = 0){
      neuer_rki_link = str_c(rki_json_link, "&resultOffset=", offset)
      neue_liste = read_json(neuer_rki_link, simplifyVector = TRUE)
      neue_faelle = neue_liste$features$attributes
      return(neue_faelle)
    }
      
    while(!is.null(neue_faelle <- rki_json_offset(offset))) {
      if(is.null(rki_)){
        rki_ = neue_faelle
      } else{
        rki_ = bind_rows(rki_, neue_faelle) 
      }
      offset <- as.integer(offset + nrow(neue_faelle)) 
      # as.integer() vermeidet einen Bug, bei dem die Zahl 100000 
      # als "1e+05" übergeben wird.
    }
    
    # Datumsspalten in solche umwandeln
    # Die Spalten "Meldedatum" und "Refdatum" sind in UTC-Millisekunden(!) angegeben.
    
    rki_ <- rki_ %>%
      mutate(Meldedatum = as_datetime(Meldedatum/1000, origin = lubridate::origin, tz = "UTC"),
             Refdatum = as_datetime(Refdatum/1000, origin = lubridate::origin, tz = "UTC"))
    
    rki_$Datenstand <- as_date(dmy_hm(rki_$Datenstand))
    
  } else {
    # use_json == FALSE
    rki_url <- "https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv"  
    ndr_url <- "https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/rki_api.current.csv"
    rki_ = read.csv(url(rki_url)) 
    if (ncol(rki_)> 17 & nrow(rki_) > 100000) {
      msg("Daten erfolgreich vom RKI-CSV gelesen")
    } else {
      # Rückfall: Aus dem NDR-Data-Warehouse holen
      rki_ = read.csv(url(ndr_url))
      msg("Daten erfolgreich aus dem NDR Data Warehouse gelesen")
    }
  }
  # Sollte die Spalte mit der Landkreis kein String sein, umwandeln und mit führender 0 versehen
  if(class(rki_$IdLandkreis) != "character") {
    rki_$IdLandkreis <- paste0("0",rki_$IdLandkreis)
  }
  # Wenn 'Datenstand' ein String ist, in ein Datum umwandeln. Sonst das Datum nutzen. 
  if (class(rki_$Datenstand) == "character") {
    rki_$Datenstand <- parse_date(rki_$Datenstand[1],format = "%d.%m.%y%H, %M:%S Uhr")
  }
  return(rki_)
}

