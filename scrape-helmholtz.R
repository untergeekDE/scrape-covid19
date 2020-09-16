################ scrape-helmholtz.R
# 
# Einfaches Kopier-Skript. Schaut nach einem neuen CSV auf dem 
# SECIR-Repository der Helmholtz-System-Immunologoen und kopiert es in ein
# Blatt des fallzahl-id-Google-Sheets.  
#
# Kontakt bei Helmholtz: Saham Khailaie, khailaie.sahamoddin@gmail.com
# Infoseite SECIR: https://gitlab.com/simm/covid19/secir/-/tree/master
#
# CSVs werden am frühen Nachmittag aktualisiert; Skript startet 15 Uhr. 
# Wenn keine neuen Daten da, versuch es zwei Stunden lang, dann gib auf. 
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 16.9.2020 vormittags

library("rvest")
library("tidyverse")
library(openxlsx)
library(readxl)
library(googlesheets4)
library(lubridate)
library(openssl)
library(httr)
library(DatawRappr)

# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Init-Werte fürs Logging, das Arbeitsverzeichnis und die Keyfile-Auswahl
server <- FALSE

# ---- Logging und Update der Semaphore-Seite ----
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B10, Statuszeile in C10
  d <- data.frame(b = now(tzone= "CEST"), c = paste0(x,...))
  range_write(id_msg,d,sheet="Tabellenblatt1",
              range="B10:C10",col_names = FALSE,reformat=FALSE)
  if (server) Sys.sleep(5)     # Skript ein wenig runterbremsen wegen Quota
  if (logfile != "") {
    cat(x,...,file = logfile, append = TRUE)
  }
}

# Argumente werden in einem String-Vektor namens args übergeben,
# wenn ein Argument übergeben wurde, dort suchen, sonst Unterverzeichnis "src"

args = commandArgs(trailingOnly = TRUE)
if (length(args)!=0) { 
  server <- args[1] %in% c("server","logfile")
  if(args[1] == "logfile") logfile <- "./logs/scrape-hsm.log"
} 

sheets_email <- "corona-rki-hmsi-googlesheets@corona-rki-hmsi-2727248702.iam.gserviceaccount.com"
sheets_filename <- "corona-rki-hmsi-2727248702-d35af1e1baab.json"

# VERSION FÜR DEN SERVER 
if (server) {
  # Arbeitsverzeichnis, Logdatei beschreiben
  setwd("/home/jan_eggers_hr_de/rscripts/")
  # Authentifizierung Google-Docs umbiegen
  sheets_keypath <- "/home/jan_eggers_hr_de/key/"
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

msg(as.character(now()),"-- scrape-helmholtz --")

# Tabelle: "Corona-Fallzahlen-Hessen"
#id_fallzahl = "1OhMGQJXe2rbKg-kCccVNpAMc3yT2i3ubmCndf-zX0JU"
id_fallzahl = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"

# Vergleichsdaten vom Google Sheet: letztes gelesenes Datum
fallzahl_df <- read_sheet(id_fallzahl,sheet ="rt-helmholtz")
lastdate <- max(as.Date(fallzahl_df$date))



# ---- Lies Helmholtz-Daten Rt und schreibe in Hilfsdokument id_fallzahl ----
brics_url <- "https://gitlab.com/simm/covid19/secir/-/raw/master/img/dynamic/Rt_rawData/Hessen_Rt.csv?inline=false"
this_date <- lastdate
starttime <- now()

msg("Lies CSV vom SECIR-Gitlab")
while(lastdate == this_date) {
  brics_df <- read.csv(brics_url)
  # Manchmal enthält das Dokument Kontrollzeilen, die man daran erkennt, dass in Spalte date
  # kein gültiges Datum liegt. 
  brics_df <- brics_df %>% filter(str_detect(date,"\\d{4}-\\d\\d-\\d\\d"))
  this_date <- max(as.Date(brics_df$date))
  msg("CSV gelesen vom ",this_date," (gestern: ",lastdate,")")
  # Neues Datum?
  if (this_date == lastdate){
    # Falls Startzeit schon mehr als 2 Stunden zurück: 
    if (now() > starttime+7200)
      { 
        simpleWarning("Timeout")
        msg("Alte SECIR-Daten vom ",lastdate)
        lastdate <- this_date-1               # wir tun als ob
    }
    Sys.sleep(300)
  }
}


# Maximum, Minimum, Median in Spalten schreiben. Willenlos abgeschrieben. 

rt_df <- brics_df %>% 
  rowwise() %>% 
  do(as.data.frame(.) %>% { 
    subs <- select(., 2:105)
    mutate(., Min = as.numeric(subs) %>% min,
           Max = as.numeric(subs) %>% max,
           Med = as.numeric(subs) %>% median()) 
  } ) %>%
  ungroup() %>%
  select(date,Min,Med,Max) %>%
  mutate(date = as.Date(date)) %>%
  filter(date > as.Date("2020-03-08"))

msg("Schreibe Daten ins Google Sheet")
sheet_write(rt_df,ss = id_fallzahl, sheet = "rt-helmholtz")

# ---- Jetzt noch die RKI-Tabelle für R dazuholen ----

rki_r_url <- "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Projekte_RKI/Nowcasting_Zahlen.xlsx?__blob=publicationFile"
tryCatch(rki_r_df <- read.xlsx(rki_r_url,sheet = 1,detectDates = TRUE))
# Gerne kommen auf Blatt 1 mal die Erläuterungen. Dann mit Blatt 2 starten. 
if (ncol(rki_r_df < 2)) {
  tryCatch(rki_r_df <- read.xlsx(rki_r_url,sheet = 2,detectDates = TRUE))
  
}

rki_r_df <- rki_r_df %>%
  select(datum_erkrankt =1, 
         neue_punkt_og = 2, 
         neue_lo_og = 3, 
         neue_hi_og = 4, 
         neue_punkt = 5, 
         neue_lo = 6,
         neue_hi = 7, 
         r_punkt = 8, 
         r_lo = 9, 
         r_hi = 10)

# RKI-Nowcast-Sheet auf Sheet pushen
msg("Schreibe Kopie der RKI-Daten")
sheet_write(rki_r_df,ss = id_fallzahl, sheet = "rt-rki")

r_df <- rki_r_df %>%
  select(datum = datum_erkrankt, r_lo, r_hi) %>%
  full_join(rt_df, by = c("datum" = "date")) %>%
  select(datum,r_rki_lo = r_lo, r_rki_hi = r_hi,
         r_helmholtz_min = Min, 
         r_helmholtz_med = Med,
         r_helmholtz_max = Max)

# Auf letzte 14 Tage beschränken
r_df <- r_df[nrow(r_df)-(27:0),]


msg("Schreibe Arbeitskopie r_rki_helmholtz")
sheet_write(r_df, ss = id_fallzahl, sheet = "r_rki_helmholtz")

# ---- Pinge Datawrapper-Grafik R-Werte, wenn neue Zahlen ----
msg("Pinge Datawrapper-Grafik")
dw_publish_chart(chart_id = "82BUn")

if (this_date > lastdate) {
  msg("OK!")
} else {
  msg("OK (nur Ping)")
}

