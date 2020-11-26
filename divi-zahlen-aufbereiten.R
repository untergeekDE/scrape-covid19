################ divi-zahlen-aufbereiten.R
#
# Liest die täglich um 12.15h bereitgestellte DIVI-Datensatz-Datei
# mit den regionalen Daten aus und bereitet sie in die Auslastungs- 
# und 
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 24.11.2020



# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- "B8:C8"
if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

aaasheet_id <- "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"

msg("Starte DIVI-Abfrage... \n")

# ---- Kreistabelle lesen ----

divi_kreise_url <- "https://diviexchange.blob.core.windows.net/%24web/DIVI_Intensivregister_Auszug_pro_Landkreis.csv"
divi_kreise_df <- read.csv(divi_kreise_url) %>%
  mutate(AGS = paste0("0",gemeindeschluessel)) %>%
  select(-gemeindeschluessel)

ts <- now()
# Prüfen, ob das von gestern ist
OLD_DATA <- (as_date(divi_kreise_df$daten_stand[1]) < today()) 
# mit voller Absicht noch nicht ausgewertet; später: Uhrzeit vergleichen, evtl. warten, sonst Fehler
while (OLD_DATA) {
  msg("Alte Daten, warte 2 Minuten...")
  Sys.sleep(120)
  divi_kreise_df <- read.csv(divi_kreise_url) %>%
    mutate(AGS = paste0("0",gemeindeschluessel)) %>%
    select(-gemeindeschluessel)
  OLD_DATA <- (as_date(divi_kreise_df$daten_stand[1]) < today()) 
  if ((ts-now())>7200){
    msg("!!!Mache mit veralteten Daten weiter!!!")
    break
  }
}

# ...und ins Archiv: 
write.csv(divi_kreise_df, format(Sys.time(), "archiv/divi_kreise_%Y%m%d.csv"), fileEncoding = "UTF-8", row.names = F)

msg("DIVI-Kreisdaten gelesen und archiviert")

# ---- Tabelle 1: Belegungs- und Auslastungszahlen nach Kreis bzw. Versorgungsgebiet 

kreise_idx <- read.xlsx("index/kreise-divi-vg.xlsx")

divi_k_df <- divi_kreise_df %>%
  filter(bundesland == 6) %>%
  select(-anzahl_meldebereiche,-bundesland) %>%
  full_join(kreise_idx,by = "AGS") %>%
  mutate(betten = betten_frei + betten_belegt) %>%
  group_by(VG_NR) %>%
  mutate(auslastung_q = round(sum(betten_belegt)/sum(betten)*100,1)) %>%
  mutate(auslastung_covid_q = round(sum(faelle_covid_aktuell) / sum(betten)*100,1)) %>%
  ungroup() %>%
  mutate(daten_stand = as_date(daten_stand))

msg("DIVI-Kreisdaten nach hess. VG aufbereitet")

write.csv(divi_k_df, format(Sys.time(), "divi_kreise.csv"), fileEncoding = "UTF-8", row.names = F)
sheet_write(divi_k_df, ss = aaasheet_id, sheet = "DIVI Kreise")
if (server) {
  # Google-Bucket befüllen
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_kreise.csv gs://d.data.gcp.cloud.hr.de/')
}  

msg("Daten archiviert und aufs GSheet gepusht")


dw_publish_chart(chart_id = "Jat49") # die Choropleth-Karte aktualisieren

msg("Choropleth-Karte aktualisiert")

# ---- Tabelle 2: Belegungszahlen nach Bundesland ----

laender <- c("Schleswig-Holstein", "Hamburg", "Niedersachsen", "Bremen",
            "Nordrhein-Westfalen", "Hessen", "Rheinland-Pfalz", "Baden-Württemberg", 
            "Bayern", "Saarland", "Berlin", "Brandenburg",
            "Mecklenburg-Vorpommern", "Sachsen", "Sachsen-Anhalt", "Thüringen")

laender_df <- divi_kreise_df %>%
  select(-anzahl_standorte,-anzahl_meldebereiche,-AGS) %>%
  group_by(bundesland) %>%
  summarize(faelle_covid_aktuell = sum(faelle_covid_aktuell),
            faelle_covid_aktuell_beatmet = sum(faelle_covid_aktuell_beatmet),
            betten = sum(betten_frei)+sum(betten_belegt),
            betten_frei = sum(betten_frei),
            betten_belegt = sum(betten_belegt),
            daten_stand = max(as_date(daten_stand))) %>%
  mutate(auslastung = round(betten_belegt/betten*100,1)) %>%
  mutate(bundesland = laender[bundesland])

# Jetzt die Summe: 

laender_df <- rbind(laender_df,
  tibble(bundesland="Deutschland",
         faelle_covid_aktuell = sum(laender_df$faelle_covid_aktuell),
         faelle_covid_aktuell_beatmet = sum(laender_df$faelle_covid_aktuell_beatmet),
         betten=sum(laender_df$betten),
         betten_frei=sum(laender_df$betten_frei),
         betten_belegt=sum(laender_df$betten_belegt),
         daten_stand=laender_df$daten_stand[1],
         auslastung=round(sum(laender_df$betten_belegt)/sum(laender_df$betten)*100,1)))

hessen_df <- laender_df %>%
  filter(bundesland %in% c("Deutschland","Hessen")) %>%
  select(bundesland,betten,betten_frei,faelle_covid_aktuell,auslastung,faelle_covid_aktuell_beatmet)

msg("Ländertabelle errechnet")

write.csv(laender_df, "divi_laender.csv", fileEncoding = "UTF-8", row.names = F)
write.csv(hessen_df, "divi_hessen.csv", fileEncoding = "UTF-8", row.names = F)

if (server) {
  # Google-Bucket befüllen
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_laender.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_hessen.csv gs://d.data.gcp.cloud.hr.de/')
}  

sheet_write(laender_df, ss = aaasheet_id, sheet = "DIVI Bundesländer")
sheet_write(hessen_df, ss = aaasheet_id, sheet = "DIVI nur Hessen")


msg("Daten Länder und Hessen gepusht")

dw_publish_chart(chart_id = "Np5sj") # die Ländertabelle aktualisieren
dw_publish_chart(chart_id = "tYJGs") # die Choropleth-Karte aktualisieren

msg("Choropleth-Karte aktualisiert")

# ---- Tabelle 4: Historie DIVI ----

# Tabelle laender_df nutzen

nur_hessen_df <- laender_df %>% 
  filter(bundesland == "Hessen") %>%
  select(Datum = daten_stand,
         faelle_covid_aktuell,
         faelle_covid_aktuell_beatmet,
         betten,
         betten_frei,
        auslastung) %>%
  # Auslastung nochmal ohne Rundung
  mutate(auslastung = (betten-betten_frei)/betten*100)

# Tabelle vom GSheet laden

hessen_archiv_df <- read_sheet(ss = aaasheet_id, sheet = "DIVI Hessen-Archiv")
hessen_archiv_df$Datum <- as_date(hessen_archiv_df$Datum)
# Daten ergänzen

if (nur_hessen_df$Datum %in% hessen_archiv_df$Datum) {
  hessen_archiv_df[hessen_archiv_df$Datum == nur_hessen_df$Datum,] <- nur_hessen_df
} else {
  hessen_archiv_df <- rbind(hessen_archiv_df,nur_hessen_df)
}

sheet_write(hessen_archiv_df, ss = aaasheet_id, sheet = "DIVI Hessen-Archiv")
write.csv(hessen_archiv_df, "archiv/divi_hessen_archiv.csv", fileEncoding = "UTF-8", row.names = F)

msg("Archivdaten geschrieben")

# ---- Tabelle 5: ICU-Prognose updaten ----

# DIVI-freie Betten - Hypothese: maximale Kapazität entspricht
# der Anzahl der derzeit freien Betten plus der COVID-Intensivfälle

# Kapazitätsprognose: 
max_beds <- nur_hessen_df$faelle_covid_aktuell+nur_hessen_df$betten_frei

prognose_df <- read_sheet(ss="12S4ZSLR3H7cOd9ZsHNmxNnzKqZcbnzShMxaWUcB9Zj4","ICUPrognose") %>%
  select(-intensiv,-`ungefähre derzeitige Kapazität`)

icu_df <- read_sheet(ss=aaasheet_id,sheet="DIVI Hessen-Archiv") %>%
  select(datum = 1,intensiv = 2) %>%
  mutate(datum = as_date(datum))

if (nur_hessen_df$Datum %in% icu_df$datum) {
  icu_df$intensiv[icu_df$datum == nur_hessen_df$Datum] <- nur_hessen_df$faelle_covid_aktuell 
} else {
  t_df = data.frame(nur_hessen_df$Datum,nur_hessen_df$faelle_covid_aktuell)
  names(t_df) = c("datum","intensiv")
  icu_df <- rbind(icu_df,t_df)
}

icu_df <- icu_df %>%
  full_join(prognose_df,by = c("datum" = "datum")) %>%
  arrange(datum) %>%
  #letzte 4 Wochen
  filter(datum > today()-28) %>%
  # nächste 28 Tage
  filter(datum < today()+ 14) %>%
  mutate(kapazitaet = max_beds) %>%
  select(1,2,3,4,5,`ungefähre derzeitige Kapazität` = kapazitaet)


write_sheet(icu_df,ss="12S4ZSLR3H7cOd9ZsHNmxNnzKqZcbnzShMxaWUcB9Zj4","ICUPrognose")



# ---- Krankenhaus-Einzelmeldungen abrufen und archivieren ----

d_json <- read_json("https://www.intensivregister.de/api/public/intensivregister", simplifyVector = T)

d_tbl <- tibble(
  id = d_json[["data"]]$id,
  ik_nummer = d_json[["data"]][["krankenhausStandort"]]$ikNummer,
  name = d_json[["data"]][["krankenhausStandort"]]$bezeichnung,
  address = str_c(d_json[["data"]][["krankenhausStandort"]]$strasse, " ", 
                  d_json[["data"]][["krankenhausStandort"]]$hausnummer, "; ", 
                  d_json[["data"]][["krankenhausStandort"]]$plz, " ",
                  d_json[["data"]][["krankenhausStandort"]]$ort),
  city = d_json[["data"]][["krankenhausStandort"]]$ort,
  state = d_json[["data"]][["krankenhausStandort"]]$bundesland,
  lat = d_json[["data"]][["krankenhausStandort"]][["position"]]$latitude,
  long = d_json[["data"]][["krankenhausStandort"]][["position"]]$longitude,
  timestamp = d_json[["data"]]$meldezeitpunkt,
  icu_low = d_json[["data"]][["bettenStatus"]]$statusLowCare,
  icu_high = d_json[["data"]][["bettenStatus"]]$statusHighCare,
  ecmo = d_json[["data"]][["bettenStatus"]]$statusECMO,
  scraped = ts(now(tzone = "CET"))
)
# ---- Daten putzen ----

d_tbl <- d_tbl %>%
  mutate(state = str_to_title(state),
         timestamp = str_replace_all(timestamp, c("T"=" ", ":\\d\\dZ" = "")),
         icu_low = str_replace_all(icu_low, c("NICHT_VERFUEGBAR"="ausgelastet", "VERFUEGBAR"="verfügbar", "BEGRENZT"="begrenzt")),
         icu_high = str_replace_all(icu_high, c("NICHT_VERFUEGBAR"="ausgelastet", "VERFUEGBAR"="verfügbar", "BEGRENZT"="begrenzt")),
         ecmo = str_replace_all(ecmo, c("NICHT_VERFUEGBAR"="ausgelastet", "VERFUEGBAR"="verfügbar", "BEGRENZT"="begrenzt"))
  ) %>%
  mutate(icu_low = ifelse(is.na(icu_low), "k. A.", icu_low),
         icu_high = ifelse(is.na(icu_high), "k. A.", icu_high),
         ecmo = ifelse(is.na(ecmo), "k. A.", ecmo),
  )

# Postleitzahlen in AGS konvertieren, Kliniktabelle ergänzen
plz_df <- read.xlsx("index/plz-hessen-mit-ags.xlsx")

dh_tbl <- d_tbl %>%
  filter(state == "Hessen") %>%
  mutate(PLZ = as.numeric(str_extract(address,"[356][0-9][0-9][0-9][0-9]"))) %>%
  left_join(plz_df,by=c("PLZ" = "PLZ")) %>%
  group_by(AGS) 

write.csv(d_tbl, format(Sys.time(), "archiv/divi_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)



# ---- Alles OK, melde dich ab ----
if (OLD_DATA) {
  msg("Lauf mit alten Daten beendet.") 
} else {
  msg("OK!")
}
