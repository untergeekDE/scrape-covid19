# Hospitalisierungs-Daten lesen
# 


# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- NULL

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# Die R-Scraping-Library
library(rvest)

# URL der Ministeriumsseite mit CSV und Corona-Daten
hmsi_url <- "https://soziales.hessen.de/Corona/Bulletin/Tagesaktuelle-Zahlen"

csv_url <- read_html(hmsi_url) %>% 
  html_nodes(".d-inline-flex.link--download.link") %>% 
  html_attr('href') %>% 
  as_tibble(.) %>% 
  rename(url = 1) %>% 
  filter(str_detect(url,"csv")) %>% 
  pull(url)

hmsi_daten <- read_delim(csv_url, 
                         delim = ";", escape_double = FALSE, 
                         col_types = cols(Inzidenz_Datum = col_date(format = "%d.%m.%Y"), 
                                                              Inzidenz_letzte_Woche_Datum = col_date(format = "%d.%m.%Y"), 
                                                              Bettenauslastung_Datum = col_date(format = "%d.%m.%Y"), 
                                                              Impfquote_Datum = col_date(format = "%d.%m.%Y")), 
                         locale = locale(date_names = "de", decimal_mark = ",", 
                         grouping_mark = ".", encoding = "WINDOWS-1252"), 
                         trim_ws = TRUE) 

# hier erste Plausibilitäts-Checks

# Das Datenblatt aus dem Google-Sheet lesen und checken
hosp_daten_df <- read_sheet(ss=aaa_id, sheet="Krankenhauszahlen")

# Paar Probleme mit der Tabelle, weshalb ich sie jetzt nicht direkt mit 
# bind_rows() dranklatschen kann: 
# - An vielen Stellen "Impquote" statt "Impfquote".
# - Sandra hat ein Abfrage-Datum in die erste Spalte geschrieben. 
# - Könnte ja sein, die Daten sind schon auf dem neuesten Stand!

if(max(hosp_daten_df$`Normalbettenauslastung Datum`) < hmsi_daten) {
  
}