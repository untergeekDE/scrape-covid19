################ hole-rki-testzahlen.R
# 
# Holt sich wöchentlich die Testdaten vom RKI
#
# (Quelle: https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.xlsx)
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 09.12.2020

rm(list=ls())
msgTarget <- "B15:C15"

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# ---- RKI-Testdatei einlesen und schreiben. ---- 
# GSheet AAA
aaa_id = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"

rki_tests_url = "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.xlsx;jsessionid=809855C1F065A87B89C2E139CB57CAA9.internet051?__blob=publicationFile"

aktuell_df <- read_sheet(aaa_id, sheet="TestzahlenAutomatisch")
tests_df <- aktuell_df

ts <- now()
while (nrow(tests_df) == nrow(aktuell_df)) {
  tests_df <- read.xlsx(rki_tests_url, sheet = "Testzahlen")

  # Erste Zeile ("bis KW10") und die Summen-Zeilen (mit NA) abschneiden
  # Vorläufig-Sternchen aus der ersten Spalte tilgen

  tests_df <- na.omit(tests_df[2:nrow(tests_df),]) %>%
    rename(kw = 1, tests = 2, positiv = 3, quote = 4) %>%
    mutate(kw = as.numeric(str_replace(kw,"[^0-9]",""))) %>%
    mutate(stichtag = as_date("2020-03-15")+(kw-11)*7) %>%
    select(kw ,stichtag, tests, positiv, quote)
  if (now() > ts+(24*3600)) {
    msg("KEINE DATEN GEFUNDEN")
    stop("Timeout")
  }
}
msg("Neue Testzahlen vom RKI gelesen")
write_sheet(tests_df, aaa_id, sheet="TestzahlenAutomatisch")

msg("OK!")
