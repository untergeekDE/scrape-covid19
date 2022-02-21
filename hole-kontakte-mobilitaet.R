###################### hole-kontakte-mobilitaet.R ###################
# Anzahl der Kontakte im zeitlichen Verlauf - dazu hat die HU Berlin 
# (Team Prof. Dirk Brockmann) Mobilfunk-Daten aufbereitet und bietet
# sie über ein Dashboard an. 
#
# - Lies die Kontaktdaten aus einem CSV der HU Berlin. 
# - (TODO) Nutze Mobilitätsdaten aus den experimentellen Datensätzen bei Destatis
#
# Einmal täglich abends aktualisieren - Daten kommen zwischen 17-18 Uhr. 
# Stand: 3.2.2022
#
# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

msgTarget <- NULL # Messaging zu Google abschalten

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

k_dir_url <- "https://rocs.hu-berlin.de/viz/contactindex-monitor/contactindex-data/data/"
k_de_url <- paste0(k_dir_url,"cx_nation_processed.csv")
k_laender_url <- paste0(k_dir_url, "cx_state_processed.csv")


# Zeitstempel, wenn es losgeht. 
ts <- now()

# Nicht tagesaktuell

try(
  k_he_df <- read_csv(k_laender_url) %>% 
  select(date,contains("Hessen"))  %>% 
  select(Datum = date, 
         Kontakte = k_Hessen_7davg,
         Standardabweichung = k_std_Hessen_7davg) %>% 
  mutate(lo = Kontakte - Standardabweichung,
         hi = Kontakte + Standardabweichung) %>% 
  mutate(lo = ifelse(lo < 0,0,lo)) %>% 
  filter(!is.na(Kontakte))
)

# Was ist in den Zahlen drin?
# - k ist die durchschnittliche Anzahl der Kontakte
#   (tagesaktuell und im 7-Tage-Mittel)
# - k_std ist die Standardabweichung in den Kontakten
#   (tagesaktuell und im 7-Tage-Mittel)
#   ...also die mittlere Abweichung vom Durchschnittswert
# - 95-Prozent-Konfidenzintervall für beide Werte
#   (Auswertung unklar)
# 
# Die selbst errechneten Werte - Kontaktzahl plus/minus 
# Standardabweichung - verwende ich vorerst nicht. 

# Daten ins Google Sheet ausgeben
sheet_write(k_he_df,ss = aaa_id, sheet = "KontakteHessen")
#dw_data_to_chart(k_he_df,chart_id = "chE4O")
dw_publish_chart(chart_id = "chE4O")
