# DIVI Scraper Skript
# Zieht die Daten von der DIVI-Seite - bis die ein CSV liefern
# Da die täglich um 9 Uhr aktualisiert werden, sollte das Skript um 7:10 UTC laufen. 
# 
# 23.3. Till Hafermann, hr-Datenteam
# zuletzt bearbeitet: 1.5.je

#------------------------------------------#
#       Load required packages             #
#------------------------------------------#

require(tidyverse)
require(lubridate)
require(jsonlite)
require(googlesheets4)
require(httr)
require(openxlsx)
require(DatawRappr)

# ---- Logging und Update der Semaphore-Seite, Vorbereitung Google Sheet ----
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B8, Statuszeile in C8
  sheets_edit(id_msg,as.data.frame(now(tzone = "CEST")),sheet="Tabellenblatt1",
              range="B8",col_names = FALSE,reformat=FALSE)
  sheets_edit(id_msg,as.data.frame(paste0(x,...)),sheet="Tabellenblatt1",
              range="C8",col_names = FALSE,reformat=FALSE)
  if (logfile != "") {
    cat(x,...,file = logfile, append = TRUE)
  }
}

server <- FALSE
args = commandArgs(trailingOnly = TRUE)
if (length(args)!=0) { 
  server <- args[1] %in% c("server","logfile")
  if(args[1] == "logfile") logfile <- "./logs/scrape-hsm.log"
} 

sheets_deauth() # Authentifizierung löschen

###### VERSION FÜR DEN SERVER #####
if (server) {
  setwd("/home/jan_eggers_hr_de/rscripts/") 
  sheets_auth(email="googlesheets4@scrapers-272317.iam.gserviceaccount.com", 
              path = "/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json")
} else {
  sheets_auth(email="googlesheets4@scrapers-272317.iam.gserviceaccount.com", 
              path = "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json")
}
msg("Google Credentials erfolgreich gesetzt\n")

sheet_id <- "15hhqkeyKkwEXsd7qlt90QOfsxlQFUdoJbpT18EMlHo8"

#format timestamp
ts <- stamp("2020-01-31 15:55")

msg("Starte DIVI-Scraper... \n")

# ---- JSON-Daten über die REST-API ziehen ----

msg("Lese intensivregister.de via JSON\n")
d_json <- read_json("https://www.intensivregister.de/api/public/intensivregister?page=0", simplifyVector = T)

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

# Sicherheitsabfrage
if (ncol(d_tbl) != 13) simpleError("Formatänderung!")


# ---- Ländertabelle lesen ----

# Muss nicht mehr per OCR aus dem SVG gescraped werden, weil es ein handliches CSV gibt, 
# jeden Tag neu. 

divi_table_url <- "https://www.divi.de/images/Dokumente/Tagesdaten_Intensivregister_CSV/"
divi_table_file <- paste0("DIVI-Intensivregister_", as.character(ymd(today())),"_09-15.csv")


# Prüfe, ob Date schon erreichbar ist...
while(tryCatch(
  stop_for_status(GET(paste0(divi_table_url,divi_table_file))),
  http_404 = function (e) {404},
  http_403 = function (e) {403}, # Forbidden
  http_405 = function (e) {405}, 
  http_408 = function (e) {408} # Timeout
) %in% c(403,404,405,408)) {
  simpleWarning("Seite nicht erreichbar ")
  msg("Datei ",divi_table_file," nicht vorhanden, warte 60 Sekunden...\n")
  Sys.sleep(60)
} 

d_kreise <- read.csv(paste0(divi_table_url,divi_table_file),sep=",",dec = ".") %>%
  mutate(faelle_covid_aktuell = ifelse(faelle_covid_aktuell < faelle_covid_aktuell_beatmet,
                                       faelle_covid_aktuell_beatmet,
                                       faelle_covid_aktuell))

msg("Gelesen: DIVI-Tagesreport ",divi_table_file,"\n")

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
  mutate(resp_rel = 0, beds_total = 0) %>%
  mutate(state = states[bundesland]) %>%
  select(state, cases = 3, resp = 4, resp_rel, beds_occ="betten_belegt", 
         beds_free = "betten_frei", beds_total) %>%
  mutate(scraped = ts(now())) %>%
  # Nach Ländern aufsummieren
  group_by(state) %>%
  summarize(cases = sum(cases), 
         resp = sum(resp),
         resp_rel = round(sum(resp)/sum(cases)*100,1),
         beds_occ = sum(beds_occ),
         beds_free = sum(beds_free),
         beds_total = sum(beds_occ+beds_free))
  
# Summe berechnen, vornedranstellen, Länder (alphabetisch sortiert) hintendran klatschen
d_beds <- rbind(
  tibble(state="Deutschland",
                 cases=sum(d_beds$cases),
                 resp=sum(d_beds$resp),
                 resp_rel = round(sum(d_beds$resp)/sum(d_beds$cases)*100,1),
                 beds_occ = sum(d_beds$beds_occ),
                 beds_free = sum(d_beds$beds_free),
                 beds_total = sum(d_beds$beds_total)),
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
    filter(state == "Hessen")



# different format for table-display in datawrapper
dw_timestamp = str_c("zuletzt abgerufen: ", day(now()), ". ", month(now(), label = T), " ", year(now()),
                     ", ", hour(now()), ":", str_pad(minute(now()), width = 2, pad = "0"), " Uhr")

# format for dw table (https://datawrapper.dwcdn.net/7VKZD/3/)
h_beds_dw <- tibble(text = c("Covid-19-Fälle in Behandlung", "davon beatmet", "betreibbare Intensivbetten", "davon belegte", "Anteil belegter Betten", dw_timestamp),
                  zahlen = c(h_beds$cases, h_beds$resp, h_beds$beds_total, h_beds$beds_occ, round(h_beds$beds_occ/h_beds$beds_total * 100, 1), NA))

# format for pie or stacked bar chart (https://datawrapper.dwcdn.net/qHKhR/4/, https://datawrapper.dwcdn.net/cwnbs/1/)
h_beds_dw2 <- tibble(Betten = c("frei", "belegt"),
                     Deutschland = c(filter(d_beds, state == "Deutschland")$beds_free, filter(d_beds, state == "Deutschland")$beds_occ),
                     Hessen = c(filter(d_beds, state == "Hessen")$beds_free, filter(d_beds, state == "Hessen")$beds_occ))

# format for pie or stacked bar chart (https://datawrapper.dwcdn.net/qHKhR/4/, https://datawrapper.dwcdn.net/cwnbs/1/)
h_beds_dw2 <- tibble(Betten = c("frei", "belegt"),
                     Deutschland = c(filter(d_beds, state == "Deutschland")$beds_free, filter(d_beds, state == "Deutschland")$beds_occ),
                     Hessen = c(filter(d_beds, state == "Hessen")$beds_free, filter(d_beds, state == "Hessen")$beds_occ))

d_beds_dw <- d_beds %>%
    select(Land = state, Betten = beds_total, `davon belegt` = beds_occ) %>%
    mutate(`in Prozent` = round(`davon belegt` / Betten * 100, 1))

h_beds_dw3 <- d_beds %>%
    filter(state == "Hessen" | state == "Deutschland") %>%
    select(` ` = state, Betten = beds_total, `davon belegt` = beds_occ) %>%
    mutate(`in Prozent` = round(`davon belegt` / Betten * 100, 1)) %>%
    arrange(desc(` `))



kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  select(kreis,AGS,lat = Lat,lon = Lon,pop) %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))


h_kreise_dw4 <- d_kreise %>%
  filter(bundesland == 6) %>% 
  mutate(resp_rel = 0, beds_total = 0) %>%
  select(AGS = gemeindeschluessel, cases = 3, resp = 4, resp_rel, beds_occ="betten_belegt", 
         beds_free = "betten_frei", beds_total) %>%
  mutate(resp_rel = round(resp / cases * 100,0),
         beds_total = beds_occ + beds_free,
         beds_rel = round(beds_occ/beds_total*100,0)) %>%
  mutate(scraped = ts(now())) %>%
  mutate(AGS = paste0("0",as.character(AGS))) %>%
  left_join(kreise,by = c("AGS" = "AGS"))
  
  
#------------------------------------------#
#               save data                  #
#------------------------------------------#

msg("Daten lokal sichern...")
write.csv(d_tbl, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(d_beds, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_tbl, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_he_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_beds, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_he_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)

msg("Daten im Google-Sheet sichern...")
write_sheet(d_tbl, ss = sheet_id, sheet = "current")
write_sheet(d_beds, ss = sheet_id, sheet = "beds_current")
write_sheet(h_tbl, ss = sheet_id, sheet = "current_hessen")
write_sheet(h_beds_dw, ss = sheet_id, sheet = "beds_dw_hessen")
write_sheet(h_beds_dw2, ss = sheet_id, sheet = "beds_dw_hessen2")
write_sheet(h_beds_dw3, ss = sheet_id, sheet = "beds_dw_hessen3")
write_sheet(d_beds_dw, ss = sheet_id, sheet = "beds_dw_alle")
write_sheet(h_kreise_dw4, ss = sheet_id, sheet = "kreise_hessen")
sheets_append(d_tbl, ss=sheet_id, sheet = "archive")
sheets_append(d_beds, ss=sheet_id, sheet = "beds_archive")
sheets_append(h_tbl, ss=sheet_id, sheet = "archive_hessen")
sheets_append(h_beds, ss=sheet_id, sheet = "beds_hessen_archive")

# ---- Pinge Datawrapper-Grafik 

dw_publish_chart(chart_id = "UI83t") # die Choropleth-Karte
dw_publish_chart(chart_id = "JmqFL") # die Symbol-Karte



# ---- Alles OK, melde dich ab ----
msg("OK!")
