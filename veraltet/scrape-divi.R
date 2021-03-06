# DIVI Scraper Skript
# Zieht die Daten von der DIVI-Seite - bis die ein CSV liefern
# Da die täglich zu 9 Uhr aktualisiert werden, sollte das Skript um 7:30 UTC laufen. 
# Erfahrungsgemäß dauert es ein oder zwei Stunden länger. 
# 
# 23.3. Till Hafermann, hr-Datenteam
# zuletzt bearbeitet: 19.11.je

#------------------------------------------#
#       Load required packages             #
#------------------------------------------#

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

msgTarget <- "B8:C8"

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

require(httr)
require(rvest)

sheet_id <- "15hhqkeyKkwEXsd7qlt90QOfsxlQFUdoJbpT18EMlHo8"

#format timestamp
ts <- stamp("2020-01-31 15:55")

msg("Starte DIVI-Scraper... \n")

# ---- JSON-Daten über die REST-API ziehen ----

msg("Lese intensivregister.de via JSON\n")
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

# different format for table-display in datawrapper
dw_timestamp = str_c("zuletzt abgerufen: ", day(now()), ". ", month(now(), label = T), " ", year(now()),
                     ", ", hour(now()), ":", str_pad(minute(now()), width = 2, pad = "0"), " Uhr")

# Sicherheitsabfrage
if (ncol(d_tbl) != 13) simpleError("Formatänderung!")


# ---- Kreistabelle lesen ----

divi_kreise_url <- "https://diviexchange.blob.core.windows.net/%24web/DIVI_Intensivregister_Auszug_pro_Landkreis.csv"
divi_kreise_df <- downloadData <- read.csv(divi_kreise_url,colClasses = "character")

# ---- Ländertabelle lesen ----

# Neue URL: 

# Muss nicht mehr per OCR aus dem SVG gescraped werden, weil es ein handliches CSV gibt, 
# jeden Tag neu. 

# Allerdings muss man es erst mal finden. 

divi_table_url <- "https://www.divi.de"

ts <- today()-1
starttime <- now()
while(ts < today()) {
  # Suche nach einem aktuellen Datum
  msg("Versuche CSV-Datei von heute zu lesen...")
  # Stabile URL abfragen
  d_kreise <- read.csv(url("http://www.divi.de/DIVI-Intensivregister-Tagesreport.csv"),sep=",",dec = ".")
  ts <- as.Date(d_kreise$daten_stand[1])
  if (ts < today())
  {
    if (now() > starttime+7200) {
      msg("--TIMEOUT--")
      # Letzter Versuch nach 2 Stunden: 
      # Alternativ: versuche Link auf der Tagesreport-Seite zu finden und dem zu folgen
      tryCatch(webpage <- read_html("https://www.divi.de/register/tagesreport")) # Seite einlesen. Versuchs halt. 
      nodes <- html_nodes(webpage,"a.doclink.docman_track_download.k-ui-namespace") # Alle Links von der Seite holen.
      # Gehe davon aus, dass der 2. Link zum CSV führt
      divi_table_file <- html_attr(nodes[2],"href") # Link lesen
      ts <- ymd(html_attr(nodes[2],"data-title")) # Dateinamen lesen, Datum greppen
      d_kreise <- read.csv(url(paste0(divi_table_url,divi_table_file)),sep=",",dec = ".")
      if (ts < today()) simpleError("Keine tagesaktuelle CSV auf der Seite in 2 Stunden")
      msg("Alternative Lesemethode erfolgreich")
    } else {
      Sys.sleep(120)
    }
    
  }
}

# Ab und zu: Fehler in den Daten; mehr beatmete als Fälle. Korrekturanweisung. 
# d_kreise <- d_kreise %>%
#   mutate(faelle_covid_aktuell = ifelse(faelle_covid_aktuell < faelle_covid_aktuell_beatmet,
#                                        faelle_covid_aktuell_beatmet,
#                                        faelle_covid_aktuell))

msg("Gelesen: DIVI-Tagesreport ",d_kreise$daten_stand[1],"\n")

# Um Tills Code weiter benutzen zu können, müssen die Fälle
# (a) genauso sortiert sein (Deutschland, dann Länder alphabetisch)
# (b) dieselben Spalten enthalten: 
#     state, cases, resp, resp_rel, beds_occ, beds_free, beds_total, scraped

states <- c("Schleswig-Holstein", "Hamburg", "Niedersachsen", "Bremen",
          "Nordrhein-Westfalen", "Hessen", "Rheinland-Pfalz", "Baden-Württemberg", 
          "Bayern", "Saarland", "Berlin", "Brandenburg",
          "Mecklenburg-Vorpommern", "Sachsen", "Sachsen-Anhalt", "Thüringen")


d_beds <- d_kreise %>%
  # Prozentanteile Beatmete, Betten gesamt eintragen eintragen
  mutate(beatmet_anteil = 0, betten_gesamt = 0) %>%
  mutate(bundesland = states[bundesland]) %>%
  select(bundesland, faelle_covid_aktuell, beatmet = faelle_covid_aktuell_beatmet, beatmet_anteil, betten_belegt, 
         betten_frei,daten_stand) %>%
  mutate(scraped = daten_stand) %>%
  # Nach Ländern aufsummieren
  group_by(bundesland) %>%
  summarize(faelle_covid_aktuell = sum(faelle_covid_aktuell), 
         beatmet = sum(beatmet),
         beatmet_anteil = ifelse(faelle_covid_aktuell > 0,round(sum(beatmet)/sum(faelle_covid_aktuell)*100,1),0),
         betten_belegt = sum(betten_belegt),
         betten_frei = sum(betten_frei),
         betten_gesamt = sum(betten_belegt+betten_frei),
         scraped = as_date(max(scraped)))
  
# Summe berechnen, vornedranstellen, Länder (alphabetisch sortiert) hintendran klatschen
d_beds <- rbind(
  tibble(bundesland="Deutschland",
                 faelle_covid_aktuell=sum(d_beds$faelle_covid_aktuell),
                 beatmet=sum(d_beds$beatmet),
                 beatmet_anteil = round(sum(d_beds$beatmet)/sum(d_beds$faelle_covid_aktuell)*100,1),
                 betten_belegt = sum(d_beds$betten_belegt),
                 betten_frei = sum(d_beds$betten_frei),
                 betten_gesamt = sum(d_beds$betten_gesamt),
                 scraped = as_date(max(d_beds$scraped))),
              d_beds)


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

# ---- Postleitzahlen in AGS konvertieren, Kliniktabelle ergänzen ----
plz_df <- read.xlsx("index/plz-hessen-mit-ags.xlsx")

dh_tbl <- d_tbl %>%
  filter(state == "Hessen") %>%
  mutate(PLZ = as.numeric(str_extract(address,"[356][0-9][0-9][0-9][0-9]"))) %>%
  left_join(plz_df,by=c("PLZ" = "PLZ")) %>%
  group_by(AGS) 



# ---- Ausgabetabellen vorbereiten ----

h_tbl <- d_tbl %>%
    filter(state == "Hessen")

h_beds <- d_beds %>%
    filter(bundesland == "Hessen")




# format for dw table (https://datawrapper.dwcdn.net/7VKZD/3/)
h_beds_dw <- tibble(text = c("Covid-19-Fälle in Behandlung", "davon beatmet", "betreibbare Intensivbetten", "davon belegte", "Anteil belegter Betten", dw_timestamp),
                  zahlen = c(h_beds$faelle_covid_aktuell, h_beds$beatmet, h_beds$betten_gesamt, h_beds$betten_belegt, round(h_beds$betten_belegt/h_beds$betten_gesamt * 100, 1), NA))

# format for pie or stacked bar chart (https://datawrapper.dwcdn.net/qHKhR/4/, https://datawrapper.dwcdn.net/cwnbs/1/)
h_beds_dw2 <- tibble(Betten = c("frei", "belegt"),
                     Deutschland = c(filter(d_beds, bundesland == "Deutschland")$betten_frei, filter(d_beds, bundesland == "Deutschland")$betten_belegt),
                     Hessen = c(filter(d_beds, bundesland == "Hessen")$betten_frei, filter(d_beds, bundesland == "Hessen")$betten_belegt))

d_beds_dw <- d_beds %>%
    select(Land = bundesland, Betten = betten_gesamt, `davon belegt` = betten_belegt, `CoVID-Fälle` = faelle_covid_aktuell) %>%
    mutate(`in Prozent` = round(`davon belegt` / Betten * 100, 1))
    

h_beds_dw3 <- d_beds %>%
    filter(bundesland == "Hessen" | bundesland == "Deutschland") %>%
    select(` ` = bundesland, `CoVID-Fälle` = faelle_covid_aktuell, Betten = betten_gesamt, `davon belegt` = betten_belegt)  %>%
    mutate(Auslastung = round(`davon belegt` / Betten * 100, 1)) %>%
    arrange(desc(` `))



kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  select(kreis,AGS,lat = Lat,lon = Lon,pop) %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))


h_kreise_dw4 <- d_kreise %>%
  filter(bundesland == 6) %>% 
  mutate(beatmet_anteil = 0, betten_gesamt = 0) %>%
  select(AGS = gemeindeschluessel, faelle_covid_aktuell, beatmet = faelle_covid_aktuell_beatmet, beatmet_anteil, betten_belegt,
         betten_frei, betten_gesamt) %>%
  mutate(beatmet_anteil = round(beatmet / faelle_covid_aktuell * 100,0),
         betten_gesamt = betten_frei + betten_belegt) %>%
  mutate(belegungsquote = round(betten_belegt/betten_gesamt*100,0)) %>%
  mutate(abgefragt = ymd(today())) %>%
  mutate(AGS = paste0("0",as.character(AGS))) %>%
  left_join(kreise,by = c("AGS" = "AGS"))


# ---- Daten für Prognose aktualisieren - nur die DIVI-Daten ---- 

# DIVI-freie Betten - Hypothese: maximale Kapazität entspricht
# der Anzahl der derzeit freien Betten plus der COVID-Intensivfälle

# Kapazitätsprognose: 
max_beds <- h_beds$faelle_covid_aktuell+h_beds$betten_frei

prognose_df <- read_sheet(ss="12S4ZSLR3H7cOd9ZsHNmxNnzKqZcbnzShMxaWUcB9Zj4","ICUPrognose") %>%
  select(-intensiv,-`ungefähre derzeitige Kapazität`)

icu_df <- read_sheet(ss=sheet_id,sheet="icu_hessen_archive") %>%
  select(datum = 1,intensiv = 2) %>%
  mutate(datum = as_date(datum))
  
if (h_beds$scraped %in% icu_df$datum) {
  icu_df$intensiv[icu_df$datum == h_beds$scraped] <-h_beds$faelle_covid_aktuell 
} else {
  t_df = data.frame(h_beds$scraped,h_beds$faelle_covid_aktuell)
  names(t_df) = c("datum","intensiv")
  icu_df <- rbind(icu_df,t_df)
}

write_sheet(icu_df,ss=sheet_id,sheet="icu_hessen_archive")


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

  
  
#------------------------------------------#
#               save data                  #
#------------------------------------------#


msg("Daten lokal sichern...")
write.csv(d_tbl, format(Sys.time(), "archiv/divi_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(d_beds, format(Sys.time(), "archiv/divi_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_tbl, format(Sys.time(), "archiv/divi_he_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_beds, format(Sys.time(), "archiv/divi_he_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)

# ins Arbeitsverzeichnis schreibne und auf den Google Bucket
write.csv(d_tbl, "./divi.csv", fileEncoding = "UTF-8", row.names = F)
write.csv(d_beds, "./divi_beds.csv", fileEncoding = "UTF-8", row.names = F)
write.csv(h_tbl, "./divi_he.csv", fileEncoding = "UTF-8", row.names = F)
write.csv(h_beds, "./divi_he_beds.csv", fileEncoding = "UTF-8", row.names = F)
write.csv(h_kreise_dw4,"./divi_he_kreise.csv", fileEncoding = "UTF-8", row.names = F)



msg("Daten im Google-Sheet sichern...")
sheet_write(d_tbl, ss = sheet_id, sheet = "current")
sheet_write(d_beds, ss = sheet_id, sheet = "beds_current")
sheet_write(h_tbl, ss = sheet_id, sheet = "current_hessen")
sheet_write(h_beds_dw, ss = sheet_id, sheet = "beds_dw_hessen")
sheet_write(h_beds_dw2, ss = sheet_id, sheet = "beds_dw_hessen2")
sheet_write(h_beds_dw3, ss = sheet_id, sheet = "beds_dw_hessen3")
sheet_write(d_beds_dw, ss = sheet_id, sheet = "beds_dw_alle")
sheet_write(h_kreise_dw4, ss = sheet_id, sheet = "kreise_hessen")
sheet_append(d_tbl, ss=sheet_id, sheet = "archive")
sheet_append(d_beds, ss=sheet_id, sheet = "beds_archive")
sheet_append(h_tbl, ss=sheet_id, sheet = "archive_hessen")
sheet_append(h_beds, ss=sheet_id, sheet = "beds_hessen_archive")

# ---- Pinge Datawrapper-Grafik 

dw_publish_chart(chart_id = "UI83t") # die Choropleth-Karte
dw_publish_chart(chart_id = "JmqFL") # die Symbol-Karte
dw_publish_chart(chart_id = "t1rGf") # die Tabelle Anzahl Intensivfälle (neu)
dw_publish_chart(chart_id = "tYJGs") # die Tabelle Auslastung Deutschland/Hessen mit Barchart

if (server) {
  # Google-Bucket befüllen
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_he.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_beds.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_he_beds.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_he_kreise.csv gs://d.data.gcp.cloud.hr.de/')
  }  

# ---- Alles OK, melde dich ab ----
msg("OK!")
