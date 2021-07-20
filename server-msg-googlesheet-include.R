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
aaa_id = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"

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

  # ...mein privates Macbook: 
  if (dir.exists("~/Nextcloud2/hr-DDJ/projekte/covid-19")) {
    setwd("~/Nextcloud2/hr-DDJ/projekte/covid-19/scrape-covid19")
    sheets_keypath <- "~/key/"
  }  
  
  # ...mein privater Windows-Laptop: 
  if (dir.exists("D:/Nextcloud2/hr-DDJ/projekte/covid-19")) {
    setwd("D:/Nextcloud2/hr-DDJ/projekte/covid-19/scrape-covid19")
    sheets_keypath <- "D:/key/"
  }  

    
  # ...mein Datenteam-Laptop im Sender: 
  if (dir.exists("D:/Nextcloud/hr-DDJ/projekte/covid-19")) {
    setwd("D:/Nextcloud/hr-DDJ/projekte/covid-19/scrape-covid19")
    sheets_keypath <- "D:/key/"
  }  

  if (dir.exists("F:/projekte/covid-19")) {
    setwd("F:/projekte/covid-19/scrape-covid19")
    sheets_keypath <- "E:/creds/"
  }  
  
  # Gibt es die Datei? 
  if (!file.exists(paste0(sheets_keypath,sheets_filename))) { 
    simpleError("Kein Keyfile!")
  }
}




gs4_deauth() # Authentifizierung löschen
gs4_auth(email=sheets_email,path=paste0(sheets_keypath,sheets_filename))

msg("Google Credentials erfolgreich gesetzt\n")

