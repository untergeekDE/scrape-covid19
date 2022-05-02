################ hole-rki-testzahlen.R
# 
# Holt sich wöchentlich die Testdaten vom RKI
#
# (Quelle: https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.xlsx)
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 09.12.2020

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt

rm(list=ls())
msgTarget <- "B15:C15"

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")

# ---- RKI-Testdatei einlesen und schreiben. ---- 
# GSheet AAA
aaa_id = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"

rki_tests_url = "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.xlsx;jsessionid=809855C1F065A87B89C2E139CB57CAA9.internet051?__blob=publicationFile"

aktuell_df <- read_sheet(aaa_id, sheet="TestzahlenAutomatisch")
tests_df <- aktuell_df

ts <- now()
while (nrow(tests_df) == nrow(aktuell_df)) {
  tests_df <- read.xlsx(rki_tests_url, sheet = 2)

  # Erste Zeile ("bis KW10") und die Summen-Zeilen (mit NA) abschneiden
  # Vorläufig-Sternchen aus der ersten Spalte tilgen

  tests_df <- na.omit(tests_df[2:nrow(tests_df),]) %>%
    rename(kw = 1, tests = 2, positiv = 3, quote = 4) %>%
    mutate(jahr = as.numeric(str_extract(kw,"20[0-9][0-9]"))) %>%
    mutate(kw = as.numeric(str_replace(str_extract(kw,"[0-9]+/"),"[^0-9]",""))) %>%
    # Sonderlösung für 2020 und 2021 - weil es 2020 eine KW53 gab, 371 Tage bis KW1/2021
    mutate(stichtag = ceiling_date(make_date(jahr),
                                   unit="week",
                                   week_start=7)+7*kw) %>%
    select(kw ,stichtag, tests, positiv, quote)
  if (now() > ts+(24*3600)) {
    msg("KEINE DATEN GEFUNDEN")
    stop("Timeout")
  }
}
msg("Neue Testzahlen vom RKI gelesen")
write_sheet(tests_df, aaa_id, sheet="TestzahlenAutomatisch")
dw_publish_chart("YjiG1") # Testdatenzahlen neu publizieren

msg("OK!")
