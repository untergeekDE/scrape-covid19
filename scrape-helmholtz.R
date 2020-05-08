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
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 30.4.2020 vormittags

library("rvest")
library("tidyverse")
library(openxlsx)
library(readxl)
library(googlesheets4)
library(lubridate)
library(openssl)
library(httr)
library(DatawRappr)

# Init-Werte fürs Logging, das Arbeitsverzeichnis und die Keyfile-Auswahl
server <- FALSE

# ---- Logging und Update der Semaphore-Seite ----
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B10, Statuszeile in C10
  sheets_edit(id_msg,as.data.frame(now(tzone = "CEST")),sheet="Tabellenblatt1",
              range="B10",col_names = FALSE,reformat=FALSE)
  sheets_edit(id_msg,as.data.frame(paste0(x,...)),sheet="Tabellenblatt1",
              range="C10",col_names = FALSE,reformat=FALSE)
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

sheets_email <- "googlesheets4@scrapers-272317.iam.gserviceaccount.com"
sheets_keypath <- "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json"

###### VERSION FÜR DEN SERVER #####
if (server) {
  # Arbeitsverzeichnis, Logdatei beschreiben
  setwd("/home/jan_eggers_hr_de/rscripts/")
  # Authentifizierung Google-Docs umbiegen
  sheets_keypath <- "/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json"
} 


sheets_deauth() # Authentifizierung löschen
sheets_auth(email=sheets_email,path=sheets_keypath)

msg(as.character(now()),"\n\n---------------- START ",as.character(today()),"------------------\n")

# Tabelle: "Corona-Fallzahlen-Hessen"
id_fallzahl = "1OhMGQJXe2rbKg-kCccVNpAMc3yT2i3ubmCndf-zX0JU"
dw_rt ="82BUn"

# Vergleichsdaten vom Google Sheet: letztes gelesenes Datum
fallzahl_df <- read_sheet(id_fallzahl,sheet ="rt-helmholtz")
lastdate <- max(as.Date(fallzahl_df$date))


# ---- Lies Helmholtz-Daten Rt und schreibe in Hilfsdokument id_fallzahl ----

msg("Lies CSV vom SECIR-Gitlab")
brics_url <- "https://gitlab.com/simm/covid19/secir/-/raw/master/img/dynamic/Rt_rawData/Hessen_Rt.csv?inline=false"
brics_df <- read.csv(brics_url)

update <- max(as.Date(brics_df$date))

msg("CSV gelesen vom ",update," (gestern: ",lastdate,")")

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
  filter(as.Date(date) > as.Date("2020-03-08"))

msg("Schreibe Daten ins Google Sheet")
sheets_write(rt_df,ss = id_fallzahl, sheet = "rt-helmholtz")


# ---- Pinge Datawrapper-Grafik, wenn neue Zahlen ----
msg("Pinge Datawrapper-Grafik")
dw_publish_chart(chart_id = "82BUn")

if (update > lastdate) {
  msg("OK!")
} else {
  msg("OK (nur Ping)")
}

