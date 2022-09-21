################ hessen-zahlen-aufbereiten.R
# 
# Greift die tagesaktuellen Corona-Zahlen-/Falldatenbank des RKI ab
# und generiert daraus die wichtigsten Zahlen: 
# - die Meldung nach Kreisen
# - absolute Fallzahl, Zuwächse, Steigerungsrate, Vergleich Vorwoche
# - Schätzzahlen Genesene/Aktive Fälle
#
# Die Ergebnisse schreibt das Skript in die Corona-Daten-Tabelle, in folgende Tabs: 
# - Basisdaten
# - KreisdatenAktuell (für die Choropleth-Karten Inzidenz nach Kreis und Dynamik)
# - FallzahlVerlauf (alle Meldedaten für Hessen seit Beginn, soweit möglich)
# - Fallzahl4Wochen (für die Neufälle und das Flächendiagramm)
# - AktiveAlter (für aktive Fälle nach Alter und Geschlecht)
# - ToteAlter (für Todesfälle nach Alter und Geschlecht)
#
# Bevölkerungsstatistik als Grundlage für die Inzidenzen: 
# 
# Basis für die Inzidenzberechnung ist die DESTATIS-Bevölkerungsstatistik nach Kreisen
# (regionalstatistik.de: 12411-0015), zuletzt aktualisiert am 21.9.2021 auf den Stand
# 31.12.2021. Im Abschnitt "Überblick erstellen" wird aus dem aktuellen Datenbank-Stand eine
# Tabelle der Inzidenzen nach Kreis erzeugt - dort wird für Meldedaten vor dem 21.9.2022 der
# Bevölkerungsstand vom Vorjahr genutzt (Stand 31.12.2019), der für den Großteil der Pandemie
# die Berechnungsgrundlage darstellte. Abweichungen für die Zeit vor dem Juli 2020, an dem das
# RKI die Zahlen erstmalig aktualisiert hatte, werden ignoriert. 
#
# Die entsprechenden Grafiken werden am Ende gepingt, um aktualisiert zu werden. 
# CSVs der hessischen Fälle, der Kreisdaten und der letzten 4 Wochen werden archiviert und 
# als Aktuell-Kopie auf den Google-Bucket geschoben. 
#
# Wenn der Zugriff auf das Github-Repository und alternative
# Datenquellen misslingt bzw. die gelesenen Daten nicht plausibel
# sind, führe das Skript lies-esri-tabelle-direkt.R aus - 
# das aufgearbeitete Daten direkt von der ESRI-Quelle zieht.
#
# Weiter NICHT von diesem Skript betreut sind:
# - die DIVI-Daten (-> divi-zahlen-aufbereiten.R)
# - die Reproduktionszahlen-Daten (->scrape-helmholtz.R)
# - die Übertragung der Prognose und der RKI-Testdaten (-> mittwochsskript.R)
# - die Impfstatistik (-> hole-impfzahlen.R)
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 21.9.2022

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- "B12:C12"

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")

pacman::p_load(teamr)
pacman::p_load(magick)

# ---- Zur Vorbereitung: Kreisdaten einlesen ----

msg("\n\n-- START ",as.character(today())," --")

# Vorbereitung: Index-Datei einlesen; enthält Kreise/AGS und Bevölkerungszahlen
msg("Lies index/kreise-index-pop.xlsx","...")
# Jeweils aktuelle Bevölkerungszahlen; zuletzt aktualisiert Juli 2020
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))
kreise2019 <- read.xlsx("index/kreise-index-pop2019.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))
kreise2020 <- read.xlsx("index/kreise-index-pop2020.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))
kreise2021 <- read.xlsx("index/kreise-index-pop2021.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))

# Tabelle, in der die Bevölkerungszahlen in der jeweiligen RKI-Altersgruppe aufgerechnet sind
# Nutzt die Daten von statistik.hessen.de, Stand 31.12.2021 (seit 21.9.2022 verbindlich)

# Bevölkerungstabelle für alle Bundesländer vorbereiten
t12411_0012_2021_df <- read_delim("./index/12411-0012.csv", 
                                  delim = ";", escape_double = FALSE, 
                                  locale = locale(date_names = "de", encoding = "ISO-8859-1"), trim_ws = TRUE, 
                                  skip = 5) %>% 
  # Spalte mit dem Stichtag raus
  select(-1) %>% 
  rename(ag = 1) %>% 
  pivot_longer(cols = -ag, names_to = "bundesland", values_to = "n") %>% 
  group_by(bundesland) %>% 
  filter(!is.na(ag)) %>% 
  filter(ag!="Insgesamt") %>% 
  # id für Bundesländer dazuholen
  left_join(read.xlsx("./index/bltabelle.xlsx"), by="bundesland")


bev_bl_df <- t12411_0012_2021_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^[Uu]nter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # Zusätzliche Variable mit der Altersgruppe
  mutate(Altersgruppe = case_when(
    ag < 5 ~ "A00-A04",
    ag < 15 ~ "A05-A14",
    ag < 35 ~ "A15-A34",
    ag < 60 ~ "A35-A59",
    ag < 80 ~ "A60-A79",
    TRUE    ~ "A80+")) %>% 
  select(id,Bundesland = bundesland,Altersgruppe,Insgesamt = n) %>% 
  # Tabelle bauen: Bevölkerung in der jeweiligen Altersgruppe nach BL,
  # Werte aus der Variable aufsummieren. 
  pivot_wider(names_from=Altersgruppe, values_from=Insgesamt,values_fn=sum) %>% 
  ungroup()

# Für Hessen!
# Ländercode hier ggf. anpassen. 
bl = "06"

altersgruppen_df <- bev_bl_df %>% 
  filter(id == bl) %>% 
  pivot_longer(cols=starts_with("A"), 
               names_to="Altersgruppe",
               values_to="pop") %>% 
  select(-id, -Bundesland)




# ---- Funktionen RKI-Daten lesen ----

# Seit Mitte 2021 publiziert das RKI die Tagestabelle
# in einem Github-Repository. 

read_github_rki_data <- function() {
  # Vor 4 Uhr 30 erwarten wir vom RKI-Github - nichts. 
  while (now()<  as_datetime("03:30 CET", format="%H:%M")) {
    msg("Noch zu früh für Github-Daten")
    Sys.sleep(300)
  }
  starttime <- now()
  # Repository auf Github
  repo <- "robert-koch-institut/SARS-CoV-2_Infektionen_in_Deutschland/"
  path <- "Archiv/"
  fname <- paste0(ymd(today()),    # Filename beginnt mit aktuellem Datum
    "_Deutschland_SarsCov2_Infektionen.csv")
  # Wann war der letzte Commit des Github-Files? Das als Prognosedatum prog_d.
  
  github_api_url <- paste0("https://api.github.com/repos/",
                           repo,
                           "commits?path=",path,
                           "&page=1&per_page=1")
  github_data <- read_json(github_api_url, simplifyVector = TRUE)
  d <- as_date(github_data$commit$committer$date)
  # Immer noch nix? 
  while (d<today()) {
    # noch kein Commit von heute?
    # Ist es nach fünf - und probieren wir schon eine Stunde?
    msg("Daten vom ",format.Date(d,"%d.%m.")," warte 60s")
    Sys.sleep(60)
    github_data <- read_json(github_api_url, simplifyVector = TRUE)
    d <- as_date(github_data$commit$committer$date)
    if (now() > starttime+3600) {
      warning("Keine aktuellen Daten im Github-Repository")
      return(NULL)
    }
  }
  msg("Aktuelle Daten vom ",d)
  # Seit 21.9.2022
  rki_csv_url <- paste0("https://github.com/",
                 repo,
                 "raw/master/",
                 path,
                 fname)
  # Am 12.8.2021 scheiterte das Programm mit einem SSL ERROR 104 - 
  # Sicherheitsfeature: wenn kein Dataframe, probiere nochmal. 
  try(rki_ <- read_csv(rki_csv_url))
  # Kein Dataframe zurückbekommen (also vermutlich Fehlermeldung)?
  while (!"data.frame" %in% class(rki_)) {
    msg(rki_," - neuer Versuch in 60s")
    Sys.sleep(60)
    try(rki_ <- read_csv(path))
    # Wenn 15min ergebnislos probiert, abbrechen
    if (now()>starttime+900){
      msg("Github-Leseversuch abgebrochen")
      return(NULL)
    } 
  }
  # Einfacher Check: Jüngste Fälle mit Meldedatum gestern?
  # (Oh, dass dieser Check eines Tages scheitern möge!)
  if (max(rki_$Meldedatum) != d-1) {
    warning("Keine Neufälle von gestern")
  }
  # Daten in der Tabelle ergänzen, um kompatibel zu bleiben
  rki_ <- rki_ %>% 
    # Datenstand (heute!)
    mutate(Datenstand = d) %>% 
    # IdLandkreis in char, Landkreis-Namen für Hessen ergänzen
    mutate(IdLandkreis =ifelse(IdLandkreis>9999,
                               as.character(IdLandkreis),
                               paste0("0",IdLandkreis))) %>% 
    left_join(kreise %>% select(AGS,Landkreis=kreis),
              by=c("IdLandkreis"="AGS")) %>% 
    # IdBundesland numerisch; Hessen ergänzen
    mutate(IdBundesland = as.numeric(str_sub(IdLandkreis,1,2))) %>% 
    mutate(Bundesland = ifelse(IdBundesland == 6,"Hessen",""))
  # Raus damit. 
  return(rki_)
}

# Datenabfrage der RKI-Daten aus dem Data Warehouse 
# des Daten-Dienstleisters ESRI,der auch das Dashboard 
# betreibt - dies war von Mai 2020-Juli 2021 die Hauptquelle

# ESRI-Status-Abfrage; danke Björn Schwendker, NDR

get_esri_status <- function() {
  # Holt und säubert Statusinformationen über die Bereitstellung des RKI Covid-19-Datensatzes durch ESRI
  # (Gemeint ist dieser Datensatz: https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0/data)
  # Beschreibung des Services: https://www.arcgis.com/home/item.html?id=cd0eda38e31d41259465f9c763de1941  esri_base_url <- "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/rki_service_status_v/FeatureServer/0/query"
  esri_base_url <- "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/rki_service_status_v/FeatureServer/0/query"
  response <- httr::GET(esri_base_url, 
                        query = list(f = "pjson", 
                                     outfields = "*",
                                     where = "Url='https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/RKI_COVID19/FeatureServer/0'") ) %>% 
    httr::content() %>% jsonlite::fromJSON()  
  attributes <- response["features"][[1]]$attributes# %>% 
  # Erfahrung: Wenn der ESRI-Server sich verschluckt, bricht das Programm
  # mit einem Fehler ab, weil die Felder Timestamp und Datum nicht existieren
  if (!is.null(attributes$Timestamp) & !is.null(attributes$Datum)) {
    attribute <- attributes %>%
      mutate(Timestamp = lubridate::as_datetime(Timestamp/1000),
    Datum = lubridate::as_datetime(Datum/1000,  tz = "UTC"))  
  }
  return(attributes)
}

# NOT RUN
# get_esri_status()$Timestamp_txt # -> "03.03.2021, 03:22 Uhr"
# get_esri_status()$Status # -> "Ok"


read_esri_rki_data <- function(use_json = TRUE) {
  
  # Wenn ESRI-Datenbank noch nicht OK, warte!
  
  if (use_json) {
  #   while(get_esri_status()$Status != "OK") {
  #     msg("ESRI-Status: ", get_esri_status()$Status)
  #     Sys.sleep(60)
  #   }
    
    # Anmerkung zur Schnittstelle (März 2021):
    # Der Service RKI_COVID19 wird zwar vom Corona-Dashboard der ESRI
    # selbst nicht mehr genutzt, dient aber weiter zur Bereitstellung der Daten.
    # Mehr hier: https://arcgis.esri.de/neue-datenstrukturen-im-dashboard/
    
    # JSON-Abfrage-Code von Till (danke!)
    # Dokumentation: https://github.com/br-data/corona-deutschland-api/blob/master/RKI-API.md
    # Demo-Abfrage: https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&objectIds=&time=&resultType=standard&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=true&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=pjson&token=
    rki_json_link <- paste0("https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/Covid19_hubv/FeatureServer/0/query?",
                            "where=1%3D1",            # where 1=1
                            "&outFields=*",           # alle Ausgabefelder
                            "&cacheHint=true",
                            "&f=json",
                            "&resultRecordCount=20000")  # 20000 Fälle pro Abfrage (Maximum sind 25k)
    
    
    # ---- GRABSTEIN für rekursiver_offset() ----
    
    # An dieser Stelle, liebe Gemeinde, halten wir inne und gedenken einen
    # Moment der Stille lang unserer Freundin, der alten rekursiven Funktion.
    # Von ihrem Vater Till Hafermann mit unnachahmlicher Eleganz versehen, 
    # erledigte sie ihre Arbeit schnell und diskret wie niemand sonst.
    # Allein, es gelüstete sie nach immer neuen Speicherseiten, und mit 
    # dieser Gier riss sie nicht nur sich, sondern auch die ganze R-Umgebung,
    # in der sie lebte, in den Hungertod - denn ihre Gefräßigkeit beschwor
    # den unbarmherzigen OOM-kill-daemon herauf. 
    # Möge sie in Frieden ruhen. 
    
    # Muss in Stückchen gelesen werden, weil per JSON nicht mehr als 25000 Datensätze zurückgegeben werden
    rki_ <- NULL
    offset <- 0
    
    rki_json_offset <- function(offset = 0){
      neuer_rki_link = str_c(rki_json_link, "&resultOffset=", offset)
      neue_liste = read_json(neuer_rki_link, simplifyVector = TRUE)
      neue_faelle = neue_liste$features$attributes
      return(neue_faelle)
    }
    
    msg("JSON-Abfrage läuft...")
    while(!is.null(neue_faelle <- rki_json_offset(offset))) {
      if(is.null(rki_)){
        rki_ = neue_faelle
      } else{
        rki_ = bind_rows(rki_, neue_faelle) 
      }
      offset <- as.integer(offset + nrow(neue_faelle)) 
      # as.integer() vermeidet einen Bug, bei dem die Zahl 100000 
      # als "1e+05" übergeben wird.
    }
    
    # Datumsspalten in solche umwandeln
    # Die Spalten "Meldedatum" und "Refdatum" sind in UTC-Millisekunden(!) angegeben.
    
    rki_ <- rki_ %>%
      mutate(Meldedatum = as_datetime(Meldedatum/1000, origin = lubridate::origin, tz = "UTC"),
             Refdatum = as_datetime(Refdatum/1000, origin = lubridate::origin, tz = "UTC"))
    
    rki_$Datenstand <- as_date(dmy_hm(rki_$Datenstand))
    
  } else {
    # use_json == FALSE
    msg("Download der CSV-Datei vom RKI-Server läuft...")
    # Datei-URL seit 2/2022 - https://arcgis.esri.de/aenderung-des-rki-covid-19-datensatzes/
    rki_url <- "https://www.arcgis.com/sharing/rest/content/items/66876b81065340a4a48710b062319336/data"  
    ndr_url <- "https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/rki_api.current.csv"
    library(data.table)
    msg("RKI_COVID19.CSV einlesen...")
    # Die Funktion in data.table scheint etwas zuverlässiger als die in readr
    rki_ = data.table::fread(rki_url) %>% 
      # AGS des Landkreises von Integer in String mit führender 0 wandeln
      mutate(IdLandkreis = ifelse(as.integer(IdLandkreis) < 10000,
             paste0("0",as.integer(IdLandkreis)),
             as.character(IdLandkreis))) %>% 
      # Datenstand ist hier: "01.03.2022, 00:00 Uhr"
      mutate(Datenstand = as_date(Datenstand,format="%d.%m.%Y")) %>% 
      # Datei pinkompatibel zu Github machen
      select(IdLandkreis,
             Altersgruppe,
             Geschlecht,
             Meldedatum,
             Refdatum,
             IstErkrankungsbeginn,
             NeuerFall,
             NeuerTodesfall,
             NeuGenesen,
             AnzahlFall,
             AnzahlTodesfall,
             AnzahlGenesen,
             Datenstand,
             Landkreis,
             IdBundesland,
             Bundesland
             )
    if (ncol(rki_)== 16 & nrow(rki_) > 100000) {
      msg("Daten erfolgreich vom RKI-CSV gelesen")
    } else {
      # Rückfall: Aus dem NDR-Data-Warehouse holen
      rki_ = fread(ndr_url)
      msg("Daten erfolgreich aus dem NDR Data Warehouse gelesen")
      # Sollte die Spalte mit der Landkreis kein String sein, umwandeln und mit führender 0 versehen
      if(class(rki_$IdLandkreis) != "character") {
        rki_$IdLandkreis <- paste0("0",rki_$IdLandkreis)
      }
      # Wenn 'Datenstand' ein String ist, in ein Datum umwandeln. Sonst das Datum nutzen. 
      if (class(rki_$Datenstand) == "character") {
        rki_$Datenstand <- parse_date(rki_$Datenstand[1],"%d.%m.%Y")
      }
    }
  }
  return(rki_)
}

# ---- RKI-Falldatensatz komplett laden ----
# RKI-Daten lesen und auf Hessen filtern

# rki_df vom Repository lesen. 
# (Wenn nicht über FORCE_CSV übersprungen werden soll)
if (!FORCE_CSV) {
  rki_df <- read_github_rki_data()
}
# Wenn noch keine Daten da sind: 
# (Oder die Daten nicht plausibel sind)
# Starte alternative Datenabfrage bei der ESRI

if (exists("rki_df")) {
  if (max(rki_df$Meldedatum) != today()-1) rki_df <- NULL
  } else {
    rki_df <- NULL
  }
   
# JSON-Schnittstelle für Datenimport nutzen? CSV geht schneller, 
# ist aber ab und zu fehleranfällig. 

use_json <- FALSE

if (is.null(rki_df)) {
  if (use_json) msg("Daten vom RKI via JSON anfordern...") else msg("RKI-CSV lesen...")
  rki_df <- read_esri_rki_data(use_json)
  # Alte Daten? 
  ts <- rki_df$Datenstand[1]
  starttime <- now()
  while (ts < today()) {
    msg("!!!RKI-Daten sind Stand ",ts)
    Sys.sleep(300)   # Warte fünf Minuten
    if (now() > starttime+36000) {
      msg("--- TIMEOUT ---")
      simpleError("Timeout, keine aktuellen RKI-Daten nach 10 Stunden")
      quit()
    }
    # alternierend versuchen, das CSV zu lesen
    use_json <- !use_json
    rki_df <- read_esri_rki_data(use_json)
    # Datum auslesen; was uns hierher gebracht hat, ist schließlich die Beobachtung, 
    # dass der Datensatz von gestern ist. 
    ts <- rki_df$Datenstand[1]
    # und nochmal in die while()-Bedingung. 
  }
}

# Daten lesen; wenn noch Daten von gestern, warten. 


# FALLBACK ----
# Der NDR pflegt ein Repository, wo er alle Briefkastenmeldungen ablegt - als TSV. (!)
# Dummerweise auch noch komprimiert. 
# https://storage.googleapis.com/public.ndrdata.de/rki_covid_19_bulk/daily/covid_19_daily_2021-04-09.tsv.gz

# Fallback: Von Hand laden
fallback <- FALSE
if (fallback) {
  # Eierlegende Wollmilchsau beim Datenimport
  heute <- today()
  fallback_url <- paste0("https://storage.googleapis.com/public.ndrdata.de/",
                         "rki_covid_19_bulk/daily/covid_19_daily_", heute,
                         ".tsv.gz")
  library(data.table)
  rki_df <- fread(fallback_url) # Entpackt und liest das TSV des NDR-Archivs
#  rki_df <- read.csv("~/Downloads/RKI_COVID19.csv")
  # Sollte die Spalte mit der Landkreis kein String sein, umwandeln und mit führender 0 versehen
  if(class(rki_df$IdLandkreis) != "character") {
    rki_df$IdLandkreis <- paste0("0",rki_df$IdLandkreis)
  }
  # Wenn 'Datenstand' ein String ist, in ein Datum umwandeln. Sonst das Datum nutzen.
  if (class(rki_df$Datenstand) == "character") {
    rki_df$Datenstand <- parse_date(rki_df$Datenstand[1],format = "%d.%m.%y%H, %M:%S Uhr")
  }
}

ts <- rki_df$Datenstand[1]

# Plausibilität: Datensätze per JSON 

msg("RKI-Daten gelesen - ",nrow(rki_df)," Zeilen ",ncol(rki_df)," Spalten - ",ts)

# Daten für Hessen filtern; später tagesaktuelle Kopie lokal ablegen

rki_he_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>% 
  group_by(Meldedatum)

# ---- Plausibilitätsprüfung und ggf. ESRI-Ersatzdaten ----

# Basisdaten aus der letzten Zeile des lokalen Archivs lesen

if (!file.exists("daten/hessen_rki_df.csv")) {
  # aus dem Google-Bucket lesen - da liegt immer was
  check_df <- read_csv2("https://d.data.gcp.cloud.hr.de/hessen_rki_df.csv")
} else {
  check_df <- read_csv2("daten/hessen_rki_df.csv")
}

heute_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1))
check_heute_df <- check_df %>%
  filter(NeuerFall %in% c(0,1))

# Neue Daten, aber merkwürdig?
# Anzahl Fälle rückläufig oder unverändert?
# 
if ((as_date(heute_df$Datenstand[1]) > 
             as_date(check_df$Datenstand[1])) &
     (sum(heute_df$AnzahlFall) <= sum(check_heute_df$AnzahlFall) |
     # Tabelle gegenüber gestern um mehr als 10% gewachsen?
    (nrow(heute_df)/nrow(check_df) > 1.1))) {
  msg("ESRI-Fallback")
  # ESRI-Datenauswertung als vorläufige Tabelle auslesen
  source("./lies-esri-tabelle-direkt.R")
  # Generiert eine Liste namens esri_he
  # Karte über Teams-Webhook absenden
  cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
  cc$text(paste0("Vorläufige RKI-Corona-Datenabfrage, Stand: ",now()))
  
  sec <- card_section$new()
  
  sec$text("<strong>Direkt-Datenabfrage ergab heute keine Neufälle in Hessen!</strong> Ersatzweise ESRI-Datentabelle gelesen und als vorläufig publiziert.")
  sec$add_fact("Fälle gesamt",format(esri_he$AnzFall,big.mark=".",
                                 decimal.mark = ","))
  sec$add_fact("Neufälle",format(esri_he$AnzFallNeu,big.mark=".",decimal.mark=","))
  sec$add_fact("Tote heute",format(esri_he$AnzTodesfall,
                                               big.mark = ".", 
                                               decimal.mark = ",", nsmall =0))
               
    # Karte vorbereiten und abschicken. 
  cc$add_section(new_section = sec)
  if(cc$send()) msg("OK, Teams-Karte abgeschickt") else msg("OK")
  # Mach erst mal Schluss hier
  stop("Keine Neufälle aus Hessen heute")
  
}


# CSV-Archivkopien von rki_he_df anlegen
heute <- as_date(ymd(today()))
write_csv2(rki_he_df,"daten/hessen_rki_df.csv")
write_csv2(rki_he_df,paste0("archiv/rki-",heute,".csv"))

# heute_df wird immer wieder überschrieben, was nicht besonders
# klar ist - ein alter Zopf. Ich behalte ihn vorerst mal. 
# Eine Verbesserung pro Schritt. 

faelle_neu <- rki_he_df %>% 
  # so zählt man laut RKI die Summe der Fälle
  filter(NeuerFall %in% c(-1,1))    %>% 
  pull(AnzahlFall) %>% 
  sum()

faelle_gesamt <- rki_he_df %>% 
  filter(NeuerFall %in% c(0,1)) %>% 
  pull(AnzahlFall) %>% 
  sum()

genesen_gesamt <- rki_he_df %>% 
  filter(NeuGenesen %in% c(0,1)) %>% 
  pull(AnzahlGenesen) %>% 
  sum()

tote_gesamt <- rki_he_df %>% 
  filter(NeuerTodesfall %in% c(0,1)) %>% 
  pull(AnzahlTodesfall) %>% 
  sum()

tote_neu <- rki_he_df %>% 
  filter(NeuerTodesfall %in% c(-1,1)) %>% 
  pull(AnzahlTodesfall) %>% 
  sum()

neu7tage <- rki_he_df %>% 
  mutate(Meldedatum = as_date(Meldedatum)) %>% 
  filter(Meldedatum > as_date(heute-8) & Meldedatum < heute) %>%
  # Auf die Summen filtern?
  filter(NeuerFall %in% c(0,1)) %>%
  pull(AnzahlFall) %>% 
  sum()

inzidenz_gemeldet <- neu7tage/sum(kreise$pop)*100000

aktiv_gesamt <- faelle_gesamt - genesen_gesamt  - tote_gesamt

datumsstring <- paste0(day(ts),".",month(ts),".",year(ts),", 00:00 Uhr")


# ---- Verlauf Fallzahl ergänzen, letzte 4 Wochen berechnen ----

msg("Berechne fallzahl und fallzahl4w...")
fallzahl_df <- read_sheet(aaa_id,sheet="FallzahlVerlauf")

fallzahl_df$datum <- as_date(fallzahl_df$datum)
fallzahl_ofs <- as.numeric(heute - fallzahl_df$datum[1]) + 1

# Brutale Korrektur: Wenn wir einen Sprung in den Daten haben, 
# dann: 
# - lege leere Datumszeilen an
while (fallzahl_ofs > nrow(fallzahl_df)) {
  # Fieses base R: neue letzte Zeile,
  # dann die mit dem heutigen Datum versehen
  n <- nrow(fallzahl_df)
  fallzahl_df[n+1,]<- NA
  # Datum um 1 Tag fortschreiben
  fallzahl_df$datum[n+1] <- fallzahl_df$datum[n]+1
  # Unveränderte Daten kopieren, derzeit nur: Fälle, Tote
  fallzahl_df$faelle[n+1] <-fallzahl_df$faelle[n]
  fallzahl_df$tote[n+1] <- fallzahl_df$tote[n]
  # Inzidenzen weglassen - Zum Loch stehen!
  # neu7Tage evtl. kopieren - wird mE gebraucht. 
  # !!!PRÜFEN!!!
  # Null bei neu und tote_steigerung (sonst klappt die
  # neu7tage-Berechnung nicht)
  fallzahl_df$tote_steigerung[n+1] <- 0
  fallzahl_df$neu[n+1] <- 0
}
# Genesen heute eintragen

fallzahl_df$gsum[fallzahl_ofs] <- genesen_gesamt
fallzahl_df$faelle[fallzahl_ofs] <- faelle_gesamt
fallzahl_df$steigerung[fallzahl_ofs] <- faelle_gesamt/(faelle_gesamt-faelle_neu)-1
fallzahl_df$tote[fallzahl_ofs] <- tote_gesamt
fallzahl_df$tote_steigerung[fallzahl_ofs] <- tote_neu
fallzahl_df$aktiv[fallzahl_ofs] <- faelle_gesamt-genesen_gesamt-tote_gesamt
fallzahl_df$neu[fallzahl_ofs] <- faelle_neu
fallzahl_df$aktiv_ohne_neu[fallzahl_ofs] <- faelle_gesamt-genesen_gesamt-tote_gesamt-faelle_neu
fallzahl_df$neu7tage[fallzahl_ofs] <- neu7tage
fallzahl_df$inzidenz_gemeldet[fallzahl_ofs] <- inzidenz_gemeldet 

# Neumeldungen letzte 4 Wochen; jeweils aktueller (also korrigierter) Stand. 
# Weicht fatalerweise von den fall4w_df-Meldungsdaten leicht ab. 
# Heutiges Datum => Meldedatum bis gestern. 
f28_df <- rki_he_df %>%
  mutate(Meldedatum = as_date(Meldedatum)) %>%
  filter(Meldedatum > heute-29) %>%
  # Auf die Summen filtern?
  filter(NeuerFall %in% c(0,1)) %>%
  select(Meldedatum,AnzahlFall) %>%
  group_by(Meldedatum) %>%
  #  pivot_wider(names_from = datum, values_from = AnzahlFall)
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  select(Meldedatum,AnzahlFall)



#7-Tage-Trend der Neuinfektionen
fall4w_df <- fallzahl_df %>% mutate(neu7tagemittel = (lag(neu)+
                                     lag(neu,n=2)+
                                     lag(neu,n=3)+
                                     lag(neu,n=4)+
                                     lag(neu,n=5)+
                                     lag(neu,n=6)+
                                     neu)/7)
# Letzte 4 Wochen isolieren
fall4w_df <- fall4w_df[(nrow(fall4w_df)-27):nrow(fall4w_df),]

msg("FallzahlVerlauf (",fallzahl_ofs," Zeilen) -> Fallzahl4Wochen von ",
    fall4w_df$datum[1]," bis ",fall4w_df$datum[28])

# Letzte 4 Wochen und Verlauf auf die Sheets
write_sheet(fallzahl_df, ss=aaa_id, sheet="FallzahlVerlauf")
range_write(fall4w_df,ss = aaa_id, sheet = "Fallzahl4Wochen",reformat=FALSE)

# ---- Basisdaten schreiben ----

msg("Basisdaten-Seite (Google) schreiben...")

#      filter(ymd(Meldedatum) == ymd(Datenstand)-1)  %>%


# Datumsstring schreiben (Zeile 2)
range_write(aaa_id,as.data.frame(datumsstring),range="Basisdaten!A2",
            col_names = FALSE, reformat=FALSE)

# Neufälle heute (Zeile 3)
# range_write braucht einen col_names=FALSE Parameter, um nicht zwei Zeilen vollzuschreiben
range_write(aaa_id,as.data.frame("Neufälle heute"),range="Basisdaten!A3", col_names=FALSE)
range_write(aaa_id,as.data.frame(faelle_neu),
            range="Basisdaten!B3", col_names = FALSE, reformat=FALSE)
# Neufälle wie vor 7 Tagen gemeldet
letzte_woche <- fall4w_df %>% filter(datum == heute-7)
# In die Zelle "Vorwoche" schreiben
range_write(aaa_id,
            as.data.frame(letzte_woche$neu),
            range="Basisdaten!C3",
            col_names = FALSE,
            reformat = FALSE)


## ACHTUNG: ##
# Seit 23.8. machen wir es wie das RKI und berechnen 7-Tage-Inzidenz und Vergleich
# zu den 7 Tagen davor aus den korrigierten Daten nach Meldedatum. 


# Neufälle letzte 7 Tage mit Vergleich (Zeile 4)
# **ACHTUNG** Berechnung nach Meldedatum, nicht aus den gemeldeten "Briefkastendaten" der Neufälle
range_write(aaa_id,as.data.frame("Neufälle 7 Tage (Diff. Vorwoche)"),range="Basisdaten!A4", col_names=FALSE)
steigerung_7t=sum(f28_df$AnzahlFall[22:28])
steigerung_7t_inzidenz <- round(steigerung_7t/sum(kreise$pop)*100000,1)

# Vergleichzahlen Vorwoche aus dem Archiv
steigerung_7t_vorwoche <- letzte_woche$neu7tage
# prozentuale Veränderung bestimmt den farbigen Marker
steigerung_prozent_vorwoche <- (steigerung_7t/steigerung_7t_vorwoche*100)-100

trend_string <- "&#9632;"
if (steigerung_prozent_vorwoche < -10) # gefallen
  trend_string <- "<b style='color:#019b72'>&#9660;</b><!--gefallen-->"
if (steigerung_prozent_vorwoche > 10) # gestiegen
  trend_string <- "<b style='color:#cc1a14'>&#9650;</b><!--gestiegen-->"
if (steigerung_prozent_vorwoche < -50) # stark gefallen
  trend_string <- "<b style='color:#019b72'>&#9660;&#9660;</b><!--stark gefallen-->"
if (steigerung_prozent_vorwoche > 50) # stark gestiegen
  trend_string <- "<b style='color:#cc1a14'>&#9650;&#9650;</b><!--stark gestiegen-->"

range_write(aaa_id,as.data.frame(
  paste0(format(steigerung_7t,big.mark = ".", decimal.mark = ","),
         " (",
         # Veränderung mit Trend-Symbolmarker
         ifelse(steigerung_7t-steigerung_7t_vorwoche > 0,"+",""),
         format(steigerung_7t - steigerung_7t_vorwoche,big.mark = ".", decimal.mark = ","),
         trend_string,")")),
            range="Basisdaten!B4", col_names = FALSE, reformat=FALSE)
# 7 Tage Vorwoche
range_write(aaa_id,
            as.data.frame(steigerung_7t_vorwoche),
            range="Basisdaten!C4",
            col_names = FALSE,
            reformat = FALSE)

# Wachstumsrate durch Inzidenz ersetzt
# Durchschnitt der letzten 7 Steigerungsraten (in fall4w_df sind die letzten 4 Wochen)
#steigerung_prozent <- round(mean(fall4w_df$steigerung[22:28]) * 100,1)
#v_zeit <- round(log(2)/log(1+mean(fall4w_df$steigerung[22:28])),1)

# Inzidenz (Zeile 5)

range_write(aaa_id,as.data.frame("7-Tage-Inzidenz"),range="Basisdaten!A5",col_names=FALSE)
range_write(aaa_id,as.data.frame(format(steigerung_7t_inzidenz,big.mark = ".",decimal.mark=",",nsmall=1)),
            range="Basisdaten!B5", col_names = FALSE, reformat=FALSE)
# Gemeldet vor 7 Tagen
range_write(aaa_id,
            as.data.frame(letzte_woche$inzidenz_gemeldet),
            range="Basisdaten!C5",
            col_names = FALSE,
            reformat = FALSE)


# Hospitalisierungsinzidenz, Intensiv-Fälle, Hessen-Warnstufe (Zeile 6-8)
#
# Wird von Sandras Python-Scraper aktualisiert - nix tun


# AUSKOMMENTIERT - lass das dem hole-impfzahlen.R-Skript: 
# # Immunisiert und geimpft (Zeile 9+10)
# 

# Gesamt und aktiv (Zeile 11)
range_write(aaa_id,as.data.frame("Fälle gesamt (aktiv)"),range="Basisdaten!A11",col_names=FALSE)
aktiv_str <- format(round((faelle_gesamt-genesen_gesamt-tote_gesamt)/100) * 100,
                    big.mark = ".", decimal.mark = ",", nsmall =0)
range_write(aaa_id,as.data.frame(paste0(format(faelle_gesamt,big.mark = ".", decimal.mark = ",", nsmall =0),
                                           " (~",aktiv_str,")")),
            range="Basisdaten!B11", col_names = FALSE, reformat=FALSE)

# Todesfälle heute (Zeile 12)
range_write(aaa_id,as.data.frame("Todesfälle gestern"),range="Basisdaten!A12",col_names=FALSE)
range_write(aaa_id,as.data.frame(tote_neu),
            range="Basisdaten!B12",col_names = FALSE, reformat=FALSE)
# vor 7 Tagen
range_write(aaa_id,
            as.data.frame(letzte_woche$tote_steigerung),
            range="Basisdaten!C12",
            col_names = FALSE,
            reformat = FALSE)

# Todesfälle gesamt (Zeile 13)
range_write(aaa_id,as.data.frame("Todesfälle gesamt"),range="Basisdaten!A13",col_names=FALSE)
range_write(aaa_id,as.data.frame(format(tote_gesamt,big.mark=".",decimal.mark = ",")),
            range="Basisdaten!B13",
            col_names = FALSE,reformat=FALSE)
# vor 7 Tagen
range_write(aaa_id,
            as.data.frame(letzte_woche$tote),
            range="Basisdaten!C13",
            col_names = FALSE,
            reformat = FALSE)


# ---- Update der Prognose-Sheets NeuPrognose und ICUPrognose ----

# NEUPrognose ersetzt seit Februar 2021 die Neufall-Grafik
# Liest die Prognose-Daten aus dem Sheet und rechnet die Neufälle dazu.
# Die eingelesenen Seiten werden einmal wöchentlich aktualisiert - 
# über das Skript "hole-covid-simulator.R"

msg("Prognosen zu Neufällen und ICU vorbereiten...")
# Google Sheet mit Krankenhausdaten
#hosp_id = "12S4ZSLR3H7cOd9ZsHNmxNnzKqZcbnzShMxaWUcB9Zj4"

neu_p_df <- read_sheet(aaa_id,sheet = "NeuPrognose")
icu_p_df <- read_sheet(aaa_id,sheet = "ICUPrognose")
# Prognosen dranhängen

#Etwas übersichtlicher
f4w_neu_df <- fall4w_df %>%
  select(datum, neu, neu7tagemittel)

neu_p_df <- neu_p_df %>%
  select(datum, min, mean, max, prognosedatum) %>%
  left_join(f4w_neu_df, by="datum") %>%
  select(datum,neu,neu7tagemittel, min, mean, max, prognosedatum) 
# In Sheet "NeuPrognose" ausgeben

write_sheet(neu_p_df,ss=aaa_id,sheet="NeuPrognose")  

# ---- Passanten in den Fußgängerzonen ----

# nur montags
# in CRON-Job verlagert
# if (wday(today()) == 2) source("./hystreet-passantenzahlen.R")

# ---- Aufbereitung nach Kreisen ----

msg("Aufbereitung nach Kreisen...")

# Änderung: Als "Notizen" evtl. Ausgangssperren vermerken
# (aus dem Sperren-Dokument)
# Das letzte Kreis-Dokument ziehen und die Notizen isolieren

sperren_id = "1zdR1rDOt0H6THUke_W6Hb-Lt3luzh0ZJtBPxvf3cZbI"

# notizen_df <- range_read(sperren_id, sheet = "Ausgangssperren") %>%
#   mutate(Infolink = ifelse(!is.na(Infolink),Infolink,Gesundheitsamt)) %>%
#   mutate(Infos = ifelse(!is.na(Infos),
#                         paste0("<a href=\'",
#                                Infolink,
#                                "\' target=\'_blank\'>",
#                                Infos,
#                                "</a>"),
#                         paste0("<a href=\'",
#                                Infolink,
#                                "\' target=\'_blank\'>",
#                                "[Kreisinfos]",
#                                "</a>"))) %>%
#   select (AGS, notizen = Infos)

# brauchen wir nicht; Sperren-Dokument einstweilen abschalten. Nur 
# GA-Link aus dem Kreise-Dokument

notizen_df <- kreise %>%
  mutate(GA_link = paste0("<a href=\'",GA_link,
                          "\' target=\'_blank\'>",
                          "[Kreisinfos]</a>")) %>% 
  select(AGS, notizen = GA_link)


# für die Inzidenz: Letzte 7 Tage filtern
f7tage_df <- rki_he_df %>%
  mutate(datum = as_date(Meldedatum)) %>%
  filter(datum > heute-8) %>%
  # Auf die Summen filtern?
  filter(NeuerFall %in% c(0,1)) %>%
  select(AGS = IdLandkreis,AnzahlFall) %>%
  # Nach Kreis sortieren
  group_by(AGS) %>%
#  pivot_wider(names_from = datum, values_from = AnzahlFall)
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  ungroup() %>%
  select(AGS,neu7tage = AnzahlFall)
  
  

kreise_summe_df <- rki_he_df %>% 
  filter(NeuerFall %in% c(0,1)) %>%
  select(AGS = IdLandkreis,AnzahlFall,AnzahlGenesen,AnzahlTodesfall) %>%
  # Nach Kreis sortieren
  group_by(AGS) %>%
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlGenesen = sum(AnzahlGenesen),
            AnzahlFall = sum(AnzahlFall),
            AnzahlTodesfall = sum(AnzahlTodesfall)) %>%
  # Aktive Fälle als übrige berechnen
  mutate(AnzahlAktiv = AnzahlFall - AnzahlTodesfall - AnzahlGenesen) %>%
  # Mit der Kreis-Tabelle zusammenlegen, um Namen und Bevölkerungszahl zu haben
  full_join(kreise, by = c ("AGS" = "AGS"))%>%
  mutate(inzidenz = AnzahlFall / pop * 100000) %>%
  mutate(TotProz = round(AnzahlTodesfall/AnzahlFall*100),
         GenesenProz = round(AnzahlGenesen/AnzahlFall*100),
         AktivProz =round(AnzahlAktiv/AnzahlFall*100),
         stand = datumsstring) %>%
  # neu7tage aus der anderen Tabelle ergänzen...
  left_join(f7tage_df, by = c("AGS" = "AGS")) %>%
  # ...und den ergänzten Wert mit Nullen auffüllen, wo nötig
  mutate(neu7tage = ifelse(is.na(neu7tage),0,neu7tage)) %>%
  # Notizen aus dem GSheet übernehmen
  left_join(notizen_df, by = c("AGS" = "AGS")) %>%
  mutate(inz7t = neu7tage /pop * 100000) %>%
  select(ags_kreis = AGS, 
         kreis,
         gesamt = AnzahlFall,
         stand,
         pop,
         inzidenz,
         tote = AnzahlTodesfall,
         neu7tage,
         inz7t,
         AnzahlGenesen,
         AnzahlAktiv,
         notizen,
         TotProz,
         GenesenProz,
         AktivProz,
         GA_link)

# CSV-Archivkopien von kreise_summe_df anlegen

write_csv2(kreise_summe_df,paste0("archiv/kreis-",heute,".csv"))


# ---- Neufälle letzte vier Wochen, SVG-Grafik ----

msg("Neufälle letzte 4 Wochen zusammenstellen...")

f28_21_df <- rki_he_df %>%
  mutate(datum = as_date(Meldedatum)) %>%
  filter((datum > heute-29) & (datum < heute-21)) %>%
  # Auf die Summen filtern?
  filter(NeuerFall %in% c(0,1)) %>%
  select(AGS = IdLandkreis,AnzahlFall) %>%
  # Nach Kreis sortieren
  group_by(AGS) %>%
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  select(AGS,f28_21 = AnzahlFall) %>%
  # NA-Werte auf 0 setzen
  mutate(f28_21 = ifelse(is.na(f28_21),0,f28_21))


f21_14_df <- rki_he_df %>%
  mutate(datum = as_date(Meldedatum)) %>%
  filter((datum > heute-22) & (datum < heute-14)) %>%
  # Auf die Summen filtern?
  filter(NeuerFall %in% c(0,1)) %>%
  select(AGS = IdLandkreis,AnzahlFall) %>%
  # Nach Kreis sortieren
  group_by(AGS) %>%
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  select(AGS,f21_14 = AnzahlFall)


f14_7_df <- rki_he_df %>%
  mutate(datum = as_date(Meldedatum)) %>%
  filter((datum > heute-15) & (datum < heute-7)) %>%
  # Auf die Summen filtern?
  filter(NeuerFall %in% c(0,1)) %>%
  select(AGS = IdLandkreis,AnzahlFall) %>%
  # Nach Kreis sortieren
  group_by(AGS) %>%
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  select(AGS,f14_7 = AnzahlFall)

# Die heute gemeldeten(!) Neufälle
# Nicht nach dem Meldedatum gefiltert, sondern nach dem Publikationsdatum - 
# mithilfe der Neufall-Filter - und das aus einem gewichtigen Grund: 
# Mitunter melden Kreise dem RKI Fälle mit dem falschen Daten; das führt dann
# zu null Fällen unter dem heutigen Meldedatum. Mit dem nächsten Tag werden
# die Fälle korrigiert - und so tauchen die nachgemeldeten Fälle wenigstens
# einmal auf. 
f_1_df <- rki_he_df %>%
  mutate(datum = as_date(Meldedatum)) %>%
#  filter(datum == heute-1) %>%
  # Auf die Summen filtern?
  # Filter auf neu gemeldete Fälle (auch nachgemeldete)
  filter(NeuerFall %in% c(-1,1)) %>%
  select(AGS = IdLandkreis,AnzahlFall) %>%
  # Nach Kreis sortieren
  group_by(AGS) %>%
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  mutate(AnzahlFall = ifelse(is.na(AnzahlFall),0,AnzahlFall)) %>%
  select(AGS, neu = AnzahlFall)
# Grafik vorbereiten: Maximalwert und Skalierung. 

skalierung <- 30 / max(f28_21_df$f28_21,f21_14_df$f21_14,f14_7_df$f14_7)

# Die Wochensummen nach Kreis mit in die große Tabelle...
# ...und die SVG-Daten erzeugen. 
kreise_summe_df <- kreise_summe_df %>%
  left_join(f28_21_df, by = c("ags_kreis" = "AGS")) %>%
  left_join(f21_14_df, by = c("ags_kreis" = "AGS")) %>%
  left_join(f14_7_df, by = c("ags_kreis" = "AGS")) %>%
  left_join(f_1_df,by = c("ags_kreis" = "AGS")) %>%
  # NA-Werte auf 0 setzen
  mutate(f14_7 = ifelse(is.na(f14_7),0,f14_7),
        f21_14 = ifelse(is.na(f21_14),0,f21_14),
        f28_21 = ifelse(is.na(f28_21),0,f28_21),
        neu = ifelse(is.na(neu),0,neu)) %>%
  mutate(w1 = floor(f28_21*skalierung),
         w2 = floor(f21_14*skalierung),
         w3 = floor(f14_7*skalierung),
         w4 = floor(neu7tage*skalierung))

# ---- Ergänzung der regionalen R-Werte aus der Prognose ----

msg("Ergänze regionales R")

kreise_summe_df <- kreise_summe_df %>%
  left_join(range_read(aaa_id,sheet="Regionales Rt neu") %>% 
                         select(kreis=Kreis,rt,vom,vzeit,rrt,Abk),by="kreis")

# Kompletten Datensatz mit alle und scharf
write_csv2(kreise_summe_df,"daten/KreisdatenAktuell.csv")
write_sheet(kreise_summe_df,ss=aaa_id,sheet="KreisdatenAktuell")

# Aktualisieren der Trend-Grafik
dw_publish_chart(chart_id ="9UVBF")


#---- Überblick erstellen: Archivdaten 7-Tage-Inzidenzen nach Kreis ----

msg("Gesamttabelle 7-Tage-Inzidenzen nach Kreis erstellen...")

# Dafür brauchen wir die Bevölkerungszahlen der Vorjahre


# Daten für Hessen nach Kreis und Datum pivotieren:
# Neufälle je Tag und Kreis
hessen_neu_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1)) %>%
  select(Meldedatum,AGS = IdLandkreis,AnzahlFall) %>%
  group_by(AGS, Meldedatum) %>%
  summarize(Neu = sum(AnzahlFall)) %>%
  pivot_wider(names_from = AGS,values_from=Neu,values_fill = list(Neu = 0)) %>%
  arrange(Meldedatum)

# Leeres Dataframe anlegen
hsum_df <- hessen_neu_df

# Spalten von Integer in Double  umwandeln
hsum_df[,2:27] <- sapply(hsum_df[,2:27],as.numeric)

ags_idx <- colnames(hsum_df)

# Für alle 26 Kreise: 
for ( k in 2:27){
  # Kopiere aus hessen_df eine gleitende 7-Tage-Summe und berechne die Inzidenz
  # Stumpfer Algorithmus ohne jede Raffinesse, dauert entsprechend lang
  # 
  # Aus der Kreisnummer k die richtigen Bevölkerungszahlen ziehen
  pops_kr <- c(kreise2019$pop[kreise2019$AGS==ags_idx[k]],
               kreise2020$pop[kreise2020$AGS==ags_idx[k]],
               kreise2021$pop[kreise2021$AGS==ags_idx[k]],
               kreise$pop[kreise$AGS==ags_idx[k]])
  for (i in 8:nrow(hessen_neu_df)){
    # Erst mal die richtige Bevölkerungszahl für den Kreis und den Tag
    # aus der Tabelle holen
    # Bevölkerung des Kreises aus der Kreis-Tabelle ziehen, via AGS
    # und für das jeweilige Jahr
    md <- hessen_neu_df$Meldedatum[i]
    pop_kr <- case_when(
      md >= as_date("2022-09-21") ~ pops_kr[4],
      md >= as_date("2021-08-25") ~ pops_kr[3],
      md >= as_date("2020-09-01") ~ pops_kr[2],
      TRUE ~ pops_kr[1]
    ) 
    hsum_df[i,k] <- round(sum(hessen_neu_df[(i-6):i,k])/
                            pop_kr * 100000,1)
  }
}

# Kreisnamen als Spaltenköpfe
kk <- kreise %>%
  arrange(AGS)
  
colnames(hsum_df) <- c("Datum",kk$kreis)

# Datum auf Datumsspalte
# Um eins erhöht, weil die Inzidenz ja die Meldedaten bis gestern berücksichtigt. 

hsum_df$Datum <- as_date(hsum_df$Datum)+1

# Als Excel-Blatt exportieren
write_sheet(hsum_df,ss=aaa_id,sheet="ArchivKreisInzidenz")
write_csv2(hsum_df,"daten/ArchivKreisInzidenz.csv")

# ---- Heutige Inzidenzen im Briefkasten-Archiv ablegen ----

# Jetzt: Heutige Inzidenzen als Zeile im Archiv der Meldedaten ablegen
msg("Archivierte Briefkasten-Inzidenzen ergänzen...")

# 
inz_wt_df <- read_sheet(aaa_id,sheet="ArchivInzidenzGemeldet") %>%
  mutate(datum = as_date(datum))

ref7tage_df <- rki_he_df %>%
  mutate(datum = as_date(Meldedatum)) %>%
  filter(datum > as_date(heute-8) & datum < as_date(heute)) %>%
  # Auf die Summen filtern?
  filter(NeuerFall %in% c(0,1)) %>%
  select(AGS = IdLandkreis,AnzahlFall) %>%
  # gruppiere nach Kreis
  group_by(AGS) %>%
  #pivot_wider(names_from = datum, values_from = AnzahlFall)
  # Summen für Fallzahl, Genesen, Todesfall bilden
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  ungroup() %>%
  # jetzt die Bevölkerungszahlen dazuladen
  full_join(kreise,by="AGS") %>%
  # Inzidenz 0? Kommt vor - erstmalig am 20.6.2021. 
  # In diesem Fall ist die sum(AnzahlFall) == NA, also
  # jetzt alle NA durch 0 ersetzen. 
  mutate(AnzahlFall = ifelse(is.na(AnzahlFall),0,AnzahlFall)) %>%  
  # Inzidenz berechnen
  mutate(inz7t = AnzahlFall/pop*100000) %>%
  # Unter dem Datum des Datenstandes (Briefkastendatum) ablegen
  mutate(datum = as_date(heute)) %>%
  # Nach AGS sortieren - nicht alphabetisch, weil 
  # die Kreise "Darmstadt (Stadt)" und "Darmstadt-Dieburg"
  # unterschiedlich sortiert werden, je nachdem, ob sie auf einer
  # Linux/Unix-Maschine mit UTF-8 sortiert werden oder auf einer Windows-
  # Maschine mit CP1252-Zeichensatz. 
  # AGS ist immer die gleiche Sortierung. 
  arrange(datum,AGS) %>%
  select(datum,kreis,inz7t) %>%
  pivot_wider(names_from = kreis, values_from = inz7t)

# Wenn das Datum schon existiert, überschreiben, 
# sonst an Tabelle anfügen
if (as_date(heute) %in% inz_wt_df$datum) {
  inz_wt_df[inz_wt_df$datum == as_date(heute),] <- ref7tage_df
} else {
  inz_wt_df <- bind_rows(inz_wt_df,ref7tage_df)
}

# Daten aufsteigend sortieren
inz_wt_df <- inz_wt_df %>% arrange(datum)

write_csv2(inz_wt_df,"./archiv/ArchivInzidenzGemeldet.xlsx")
write_sheet(inz_wt_df,ss=aaa_id,sheet="ArchivInzidenzGemeldet")
  

# ---- Archivdaten in die GSheets ArchivKreisFallzahl, (...Tote, ...Genesen) -----

msg("Archivdaten Fallzahl, Tote, Genesen nach Kreis...")

ArchivKreisFallzahl_df <- read_sheet(ss = aaa_id, sheet="ArchivKreisFallzahl") %>%
  mutate(Datum = as.Date.character(Datum))

archiv_tmp_df <- kreise_summe_df %>%
  select(Datum=stand,kreis,gesamt) %>%
  pivot_wider(id_cols=Datum,names_from=kreis,values_from=gesamt) %>%
  mutate(Datum=ts)

if (ts %in% ArchivKreisFallzahl_df$Datum) {
  ArchivKreisFallzahl_df[ArchivKreisFallzahl_df$Datum == ts,] <- archiv_tmp_df
} else {
  ArchivKreisFallzahl_df <- rbind(ArchivKreisFallzahl_df,archiv_tmp_df)
}

write_sheet(ArchivKreisFallzahl_df,ss=aaa_id, sheet="ArchivKreisFallzahl")
write_csv2(ArchivKreisFallzahl_df,"daten/ArchivKreisFallzahl.csv")

# Tote
ArchivKreisTote_df <- read_sheet(ss = aaa_id, sheet="ArchivKreisTote") %>%
  mutate(Datum = as.Date.character(Datum))

archiv_tmp_df <- kreise_summe_df %>%
  select(Datum=stand,kreis,tote) %>%
  pivot_wider(id_cols=Datum,names_from=kreis,values_from=tote) %>%
  mutate(Datum=ts)

if (ts %in% ArchivKreisTote_df$Datum) {
  ArchivKreisTote_df[ArchivKreisTote_df$Datum == ts,] <- archiv_tmp_df
} else {
  ArchivKreisTote_df <- rbind(ArchivKreisTote_df,archiv_tmp_df)
}

write_sheet(ArchivKreisTote_df,ss=aaa_id, sheet="ArchivKreisTote")
write_csv2(ArchivKreisTote_df,"daten/ArchivKreisTote.csv")

# Genesen
ArchivKreisGenesen_df <- read_sheet(ss = aaa_id, sheet="ArchivKreisGenesen") %>%
  mutate(Datum = as.Date.character(Datum))

archiv_tmp_df <- kreise_summe_df %>%
  select(Datum=stand,kreis,AnzahlGenesen) %>%
  pivot_wider(id_cols=Datum,names_from=kreis,values_from=AnzahlGenesen) %>%
  mutate(Datum=ts)

if (ts %in% ArchivKreisGenesen_df$Datum) {
  ArchivKreisGenesen_df[ArchivKreisGenesen_df$Datum == ts,] <- archiv_tmp_df
} else {
  ArchivKreisGenesen_df <- rbind(ArchivKreisGenesen_df,archiv_tmp_df)
}

write_sheet(ArchivKreisGenesen_df,ss=aaa_id, sheet="ArchivKreisGenesen")
write_csv2(ArchivKreisGenesen_df,"daten/ArchivKreisGenesen.csv")

# ---- Aufbereitung Alter und Geschlecht Neufälle/Tote ----

# Tabelle Altersgruppen/Population einlesen

msg("Aufschlüsselung Neufälle nach Alter...")

# Die Tabelle altersgruppen_df holen wir von oben
# Auf aktive Fälle filtern, nach Alter und Geschlecht anordnen

neu7tage_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1)) %>%
  # Neufälle der letzten 7 Tage: neuestes Meldedatum und die sechs davor
  mutate(Meldedatum = as_date(Meldedatum)) %>% 
  filter(Meldedatum > heute-8) %>%
  
  # filter(NeuGenesen %in% c(0,1)) %>%
  # Alter unbekannt? Ausfiltern. 
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% 
  # Genesene und Todesfälle ausfiltern - nur aktive Fälle
  # filter(AnzahlGenesen == 0) %>%              
  # filter(AnzahlTodesfall == 0) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = AnzahlFall,values_fill=0) %>%
  select(Altersgruppe, männlich = M, weiblich = W, unbekannt) %>%
  ungroup()

# Berechne Inzidenzen für die Altersgruppen
# Quelle Bevölkerungsstatistik Hessen statistik.hessen.de
# Spalte pop mit den Bevölkerungszahlen für die jeweilige Alterskohorte

# Inzidenzen für Altersgruppen berechnen
neu7tage_df <- neu7tage_df %>%
  right_join(altersgruppen_df, by= c("Altersgruppe"="Altersgruppe")) %>%
  mutate(Inzidenz = (männlich+weiblich+unbekannt)/pop*100000) %>%
  mutate(Altersgruppe = paste0(str_replace_all(Altersgruppe,"A","")," Jahre")) %>%
  select(Altersgruppe, männlich, weiblich, Inzidenz) # Spalte pop wieder rausnehmen


# Die Fälle, die nicht zuzuordnen sind - Alter, Geschlecht - in letzte Zeile
unbek_df <- rki_he_df %>% 
  # Genesene und Todesfälle ausfiltern - nur aktive Fälle
  filter(AnzahlGenesen == 0) %>%              #
  filter(AnzahlTodesfall == 0) %>%
  filter(!str_detect(Altersgruppe,"A[0-9]") | 
           !(Geschlecht %in% c("M","W")))


# Freie Zeile für die Fälle, bei denen Alter/Geschlecht unbekannt ist
neu7tage_df[nrow(neu7tage_df)+1,] <- NA
neu7tage_df$Altersgruppe[nrow(neu7tage_df)] <- "unbekannt" 
neu7tage_df$männlich[nrow(neu7tage_df)] <- sum(unbek_df$AnzahlFall)

# Tote nach Alter und Geschlecht aufschlüsseln 

msg("Aufschlüsselung Tote nach Alter...")

tote_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1)) %>%
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% # Alter unbekannt -> filtern
  mutate(Altersgruppe = paste0(str_replace_all(Altersgruppe,"A","")," Jahre")) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlFall = sum(AnzahlFall),
            AnzahlTodesfall = sum(AnzahlTodesfall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = c(AnzahlTodesfall, AnzahlFall),values_fill=0) %>%
  mutate(AnzahlFall = AnzahlFall_M+AnzahlFall_W+AnzahlFall_unbekannt) %>%
  mutate(inz = (AnzahlTodesfall_M+AnzahlTodesfall_W+AnzahlTodesfall_unbekannt)/(AnzahlFall)*100) %>%
  select(Altersgruppe,M = AnzahlTodesfall_M, W = AnzahlTodesfall_W,
         unbekannt = AnzahlTodesfall_unbekannt, CFR = inz)

# Anteil Todesfälle in der Altersgruppe berechnen
# Die Todesfälle, die nicht zuzuordnen sind - Alter, Geschlecht - in letzte Zeile
unbek_tote_df <- rki_he_df %>% 
  filter(!str_detect(Altersgruppe,"A[0-9]")) %>%
# Todesfälle ausfiltern
  filter(NeuerTodesfall %in% c(0,1)) %>%
  select(Altersgruppe,Geschlecht,AnzahlTodesfall)

unbek_tote_df <- data.frame(Altersgruppe = "unbekannt",M = NA,W = NA,unbekannt=sum(unbek_tote_df$AnzahlTodesfall),CFR = NA)
 
tote_df <- rbind(tote_df, unbek_tote_df) 
  

# Die beiden Tabellen mit der Aufschlüsselung Neufälle und Tote schreiben

write_sheet(neu7tage_df, ss = aaa_id, sheet="NeufälleAlter")
write_sheet(tote_df,ss = aaa_id, sheet="ToteAlter")

write_csv2(neu7tage_df, "daten/rki-alter.csv")
write_csv2(tote_df,"daten/rki-tote.csv")

# sheets_append(laender_faelle_df,rki_alter_id,sheet ="faelle")
# sheets_append(laender_tote_df,rki_alter_id,sheet ="tote")

# Zeitstempel 
range_write(aaa_id,as.data.frame(as.character(heute)),sheet="NeufälleAlter",range= "A1",col_names = FALSE)
range_write(aaa_id,as.data.frame(as.character(heute)),sheet="ToteAlter",range= "A1",col_names = FALSE)

# ---- Todesfälle je Woche ----

msg("Wochenweise Darstellung Todesfälle...")

tote_woche_df <- fallzahl_df %>%
  select(datum,tote_steigerung) %>%
  mutate(w = as.integer(as.Date(datum) - as.Date("2020-03-02")) %/% 7 +1) %>%
  group_by(w) %>%
  # Wochensummen; Sonntag der jeweiligen Woche ist Stichtag
  summarize(datum = max(datum),
            tote_steigerung = sum(tote_steigerung)) %>%
  select(Stichtag = datum, Tote = tote_steigerung) %>%
  filter(wday(Stichtag) == 1)

# Leichte Verzerrungen in der Sterbestatistik nach Meldedatum, 
# weil insbesondere in Wochen, in denen die Sterbezahlen hoch sind, 
# Verzögerungen entstehen und der Meldetag später ist als der Sterbetag. 
# Deshalb die RKI-Statistik nach Sterbetag auswerten
#
# Am 2.9. war die Tabelle nicht vorhanden; deshalb try(),
# damit nicht das ganze Skript rausfliegt. 
try(rki_tote_df <- read.xlsx("https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Projekte_RKI/COVID-19_Todesfaelle.xlsx?__blob=publicationFile",sheet=2) %>% 
  filter(Bundesland=="HE") %>% 
  select(2,3,n=4) %>% 
  mutate(n = ifelse(str_detect(n,"<4"),0,as.numeric(n))) %>%
  # Sterbewoche muss zweistellig sein, sonst kommt ISOweek durcheinander
  mutate (Sterbewoche = ifelse(str_length(Sterbewoche)<2,
                               paste0("0",Sterbewoche),Sterbewoche)) %>% 
  mutate(datum= paste0(Sterbejahr,"-W",Sterbewoche,"-7")) %>% 
  # Stichtag aus Jahr und Woche berechnen
  mutate(datum=ISOweek::ISOweek2date(datum)) %>%
  # aufsteigend nach Datum sortieren
  arrange(datum) %>% 
  select(Stichtag = datum,n) %>% 
  # Spätesttes Datum der RKI-Zeitreihe brauchen wir noch
  mutate(kill_here = Stichtag < max(Stichtag)) %>% 
  full_join(tote_woche_df,by="Stichtag")%>% 
  # aus der Zeitreihe der gemeldeten Daten alle Daten vor dem letzten
  # Stichtag in der RKI-Tabelle rausnehmen
  mutate(kill_here = ifelse(is.na(kill_here),F,kill_here)) %>% 
  mutate(Tote = ifelse(kill_here==TRUE,NA,Tote)) %>% 
  select(Stichtag,`Nach Sterbedatum` = n,`Nach Meldedatum` = Tote))

# Fallback: Falls die RKI-Tabelle nicht geliefert, weise der 
# Tabelle allein die gemeldeten Toten zu
if (!exists("rki_tote_df")) {
  rki_tote_df <- tote_woche_df
} else {
  # Datawrapper-Grafik mit den Toten je Woche nach Sterbedatum
  dw_data_to_chart(rki_tote_df,chart_id="At51m")  
  dw_publish_chart("At51m")
}


# Datawrapper-Grafik Tote wochenweise aktualisieren
dw_data_to_chart(tote_woche_df, chart_id="KCHmS") # Flächengrafik 
dw_publish_chart(chart_id="KCHmS")

# ---- Wochensummen, Anteile der Altersgruppen im zeitlichen Verlauf ----
# Wochensummen der Neufälle (gruppiert nach Meldedatum), aufgeschlüsselt nach Altersgruppe
# Daraus errechnet: die prozentualen Anteile an den Neufällen

msg("Neufälle wochenweise nach Altersgruppe...")

# Der Tag, an dem die KW-Zählung begann

tag0 <- ymd("2019-12-30")


alter_woche_df <- rki_he_df %>%
  # Zählung der neuen Fälle
  filter(NeuerFall %in% c(0,1)) %>%
  select(Meldedatum, AnzahlFall,Altersgruppe) %>%
  # "A00-04" etc. umbenennen in "00-04 Jahre"
  mutate(Altersgruppe = 
           ifelse(Altersgruppe == 'unbekannt',
                  'unbekannt',
                  paste0(str_replace_all(Altersgruppe,"A","")," Jahre"))) %>%
  mutate(Meldedatum = as_date(Meldedatum)) %>%
  # Kalenderwoche berechnen; tag0 war der 1. Tag der KW1/2020
  # Rechnen mit fiktiven Kalenderwochen; da die nicht nach draußen gehen, 
  # sondern nur der Gruppierung dienen, ist das nicht schlimm. 
  mutate(A_KW = 1 + as.integer(Meldedatum-tag0) %/% 7) %>%
  group_by(A_KW,Altersgruppe) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Altersgruppe, values_from = AnzahlFall, values_fill=0) %>%
  select(sort(names(.))) %>%
  rename(KW = A_KW) %>%
  mutate(Stichtag = ymd(tag0+6+((KW-1)*7))) %>%
  # Stichtag: 11. März - erst danach zählen
  filter(Stichtag > ymd("2020-03-11")) %>%
  # Stichtag: angefangene Woche?
  filter(Stichtag <= ymd(today())) %>%
  # Prozentzahlen erzeugen 
  mutate(summe = `00-04 Jahre`+`05-14 Jahre`+`15-34 Jahre`+`35-59 Jahre`+`60-79 Jahre`+`80+ Jahre`+unbekannt) %>%
  mutate( `00-04 Jahre` = `00-04 Jahre`/summe,
          `05-14 Jahre` = `05-14 Jahre`/summe,
          `15-34 Jahre` = `15-34 Jahre`/summe,
          `35-59 Jahre` = `35-59 Jahre`/summe,
          `60-79 Jahre` = `60-79 Jahre`/summe,
          `80+ Jahre` = `80+ Jahre`/summe) %>%
  # Kalenderwoche korrigieren
  mutate(KW = isoweek(Stichtag))

 
range_write(alter_woche_df,ss=aaa_id,sheet="NeufaelleAlterProzentWoche", reformat=FALSE)
write_csv2(alter_woche_df,"daten/alter-woche.csv")

# ---- Aufräumarbeiten, Grafiken pingen ---- 

msg("Alte Basisdaten-Seite pflegen...")
basisdaten_df <- range_read(ss=aaa_id,sheet="Basisdaten") %>% 
  select(Indikator = 1,Wert = 2) %>%
  mutate(Wert = as.character(Wert)) %>%
  mutate(Wert = str_replace(Wert,"NULL"," "))

alte_basisdaten_id = "1m6hK7s1AnDbeAJ68GSSMH24z4lL7_23RHEI8TID24R8"
write_sheet(basisdaten_df, ss=alte_basisdaten_id,sheet="LIVEDATEN")
basisdaten_alt_df <- basisdaten_df %>% 
# Den ganzen HTML-Kram aus der Steigerung zur Vorwoche verschwinden lassen
  mutate(Wert = str_replace(Wert,"<b.+>(?=.)|<\\/>",""))
write_csv2(basisdaten_alt_df,"daten/Basisdaten.csv",escape="double")


#msg("Daten auf alte Basisdaten-Seite kopiert")
# Nur auf dem Server ausführen


if (server) {
  # Google-Bucket befüllen
  msg("Lokale Daten ins Google-Bucket schieben...")
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/KreisdatenAktuell.csv gs://d.data.gcp.cloud.hr.de/scrape-hsm.csv')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/KreisdatenAktuell.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/Basisdaten.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/rki-alter.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/rki-tote.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/hessen_rki_df.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/ArchivKreisFallzahl.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/ArchivKreisGenesen.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/ArchivKreisTote.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/ArchivKreisInzidenz.csv gs://d.data.gcp.cloud.hr.de/')
}

msg(as.character(now()),"Datawrapper-Grafiken pingen...","\n")

# Alle einmal ansprechen, damit sie die neuen Daten ziehen
# - Neu publizieren, damit der DW-Server einmal die Google-Sheet-Daten zieht.

# Sicherheitsfeature: Basisdaten einmal direkt schreiben
dw_data_to_chart(basisdaten_df,chart_id="OXn7r")
# ...und auch die Neufälle und Karten-Daten
kreisdaten_df <- read_sheet(aaa_id,sheet="KreisdatenAktuell")
dw_data_to_chart(kreisdaten_df,chart_id="m7sqt")

dw_publish_chart(chart_id = "OXn7r") # Basisdaten
dw_publish_chart(chart_id = "NrBYs") # Neufälle und Trend letzte 4 Wochen
dw_publish_chart(chart_id = "jLkVj") # Neufälle je Woche seit März
dw_publish_chart(chart_id = "k8nUv") # Flächengrafik
# dw_publish_chart(chart_id = "ALaUp") # Choropleth-Karte Fallinzidenz
dw_publish_chart(chart_id = "m7sqt") # Choropleth 7-Tage-Dynamik
dw_publish_chart(chart_id = "XpbpH") # Neufälle 7-Tage nach Alter und Geschlecht
dw_publish_chart(chart_id = "JQobx") # Todesfälle nach Alter und Geschlecht
dw_publish_chart(chart_id = "JQiOo") # Anteil der Altersgruppen an den Neufällen
#
dw_publish_chart(chart_id = "8eMAz") # Liniengrafik Inzidenz nach Kreisen für Dirk Kunze
dw_publish_chart(chart_id = "eTpGf") # 14-Tage-Prognose Neufälle

# Die barrierefreie Seite auch pingen

msg("Die barrierefreien Datawrapper-Grafiken pingen...")
# Grafik: Basisdaten OXn7r - wie normale Seite
dw_edit_chart(chart_id="4yvyB", annotate=paste0("Stand: ",datumsstring))
dw_publish_chart(chart_id = "4yvyB")       # Tabelle Corona-Kreis-Inzidenzen
dw_publish_chart(chart_id = "QxCwd")       # Tabelle R-Wert
# Tabelle Aktive Fälle XpbpH - wie normale Seite
# Tabelle Todesfälle JQobx - wie normale Seite
dw_publish_chart(chart_id = "1urhZ")       # Tabelle Schwere Fälle
# Tabelle DIVI-Auslastung tYJGs - wie normale Seite (DIVI-Skript)
dw_publish_chart(chart_id = "byXbs")        # Tabelle Neufälle je Woche
dw_publish_chart(chart_id = "aLtJ0")        # Tabelle Tests
dw_publish_chart(chart_id = "KyrDx")        # Tabelle Altersschichtung




# Kein Update DIVI-Scraper
# Kein Update dieser Grafiken: 
# dw_publish_chart(chart_id = "KP1H3") # Trendlinien-Grafik -> scrape-jhu.R
# dw_publish_chart(chart_id = "82BUn") # Helmholtz-R-Kurve -> scrape-helmholtz.R
msg("Alle Datawrapper-Grafiken aktualisiert.")



# Die Anschluss-Skripte aufrufen

# ---- Bundesnotbremse ----
# ...die natürlich längst keine BNB mehr ist.
# Im Winter 2021/22 gilt als einziges in Hessen eine "Hotspot"-Regelung

# Dies Skript erstellt und aktualisiert die Ampel-Tabelle aus den Archivdaten
source("./berechne-inzidenz-ampel.R")

# ---- Newswire-Meldung Inzidenzen Hessen und Bund ----

# Alles schnell nochmal neu berechnen. 

sink(file = "daten/nw_inzidenzen.txt")
cat('Aktuelle Corona-Inzidenzen für Hessen und D - ',format.Date(ts,"%d.%m.%Y"),', 0 Uhr \n\n')

cat('\n# Deutschland\n\n')

# Kurze Plausibilitätsprüfung: haben alle Kreise Neufälle gemeldet?
meldekreise <- rki_df %>% group_by(IdLandkreis) %>% 
  filter(NeuerFall %in% c(-1,1)) %>% summarize(n = sum(AnzahlFall)) %>% 
  filter(n==0) %>% pull(IdLandkreis)
# Haben Kreise keine Fälle gemeldet? 
if (length(meldekreise)!=0) {
  cat("!!!Keine Neufälle gemeldet aus folgenden Kreisen - bitte prüfen!!!\n")
  for (i in meldekreise) {cat(i,", ")}
  cat("\n\n")
}

# schnell noch die Bevölkerungszahlen holen
load("index/pop.rda")
# Inzidenz
cat("- Inzidenz: ", format(
  round(sum(rki_df %>% filter(Meldedatum > ts-8) %>%
                       filter(NeuerFall %in% c(0,1)) %>% 
                       pull(AnzahlFall)) / sum(as.integer(pop_bl_df$Insgesamt)) *100000, digits = 1),big.mark = ".",decimal.mark=","),
  "\n")
cat("- Neufälle zum Vortag: ",
    format(sum(rki_df %>% 
                  filter(NeuerFall %in% c(-1,1)) %>% 
                           pull(AnzahlFall)),
           big.mark = ".",decimal.mark=","),
    "\n")
cat("- Neufälle letzte 7 Tage (nach Meldedatum): ",
    format(sum(rki_df %>% filter(Meldedatum > ts-8) %>% 
                 filter(NeuerFall %in% c(0,1)) %>% 
                 pull(AnzahlFall)),
           big.mark = ".",decimal.mark=","),
    "\n")
cat("- Todesfälle zum Vortag: ",
    format(sum(rki_df %>% filter(NeuerTodesfall %in% c(-1,1)) %>% 
                 pull(AnzahlTodesfall)),
           big.mark = ".",decimal.mark=","),
    "\n- Todesfälle insgesamt: ",
    format(sum(rki_df %>% filter(NeuerTodesfall %in% c(0,1)) %>% 
                 pull(AnzahlTodesfall)),
           big.mark = ".",decimal.mark=","),
    
    "\n")

cat('\n# Hessen \n')

# Inzidenz
cat("- Inzidenz: ", format(
  round(sum(rki_he_df %>% filter(Meldedatum > ts-8) %>%
              filter(NeuerFall %in% c(0,1)) %>% 
              pull(AnzahlFall)) / sum(pop_bl_df %>% 
                                                   filter(id=="06") %>%
                                        mutate(n = as.integer(Insgesamt)) %>% 
                                                   pull(n))
                            * 100000, digits = 1),big.mark = ".",decimal.mark=","),
  "\n")
cat("- Neufälle zum Vortag: ",
    format(sum(rki_he_df %>% 
                 filter(NeuerFall %in% c(-1,1)) %>% 
                 pull(AnzahlFall)),
           big.mark = ".",decimal.mark=","),
    "\n")
cat("- Neufälle letzte 7 Tage (nach Meldedatum): ",
    format(sum(rki_he_df %>% filter(Meldedatum > ts-8) %>% 
                 filter(NeuerFall %in% c(0,1)) %>% 
                 pull(AnzahlFall)),
           big.mark = ".",decimal.mark=","),
    "\n")
cat("- 7 Tage Vorwoche",paste0(format(steigerung_7t_vorwoche,
                                big.mark = ".", 
                                decimal.mark = ",", nsmall =0),
                         " (",ifelse(steigerung_7t-steigerung_7t_vorwoche > 0,"+",""),
                         format(steigerung_7t - steigerung_7t_vorwoche,big.mark = ".", decimal.mark = ",", nsmall =0),
                         ")\n"))
cat("- Todesfälle zum Vortag: ",
    format(sum(rki_he_df %>% filter(NeuerTodesfall %in% c(-1,1)) %>% 
                 pull(AnzahlTodesfall)),
           big.mark = ".",decimal.mark=","),
    "\n- Todesfälle insgesamt: ",
    format(sum(rki_he_df %>% filter(NeuerTodesfall %in% c(0,1)) %>% 
                 pull(AnzahlTodesfall)),
           big.mark = ".",decimal.mark=","),
    
    "\n")


cat('\n\nQuelle: Github-Repository des Robert-Koch-Instituts \n')
cat('Skript: hole-hmsi-hospitalisierungsdaten.R auf 35.207.90.86 \n')
cat('Redaktionelle Fragen an jan.eggers@hr.de')
sink()

# fuehre Befehl aus um Datei an gwuenschten Ort zu kopieren
if (server) {
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/nw_inzidenzen.txt gs://d.data.gcp.cloud.hr.de/nw_inzidenzen.txt')
}
msg('Daten wurden fuer Newswire abgelegt!')


# ---- Generiere Infokarte in Teams ----

# Legt eine Karte mit den aktuellen Kennzahlen im Teams-Team "hr-Datenteam", 
# Channel "Corona" an. 


# Webhook aus dem Environment lesen, Karte generieren
cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
cc$text(paste0("RKI-Corona-Datenabfrage, Stand: ",datumsstring))

sec <- card_section$new()

sec$text(paste0("<strong>7-Tage-Inzidenz hessenweit: ",
                format(steigerung_7t_inzidenz,big.mark = ".",
                       decimal.mark=",",nsmall=1),
                "<strong>"))
sec$add_fact("Neufälle",format(faelle_neu,big.mark=".",
                               decimal.mark = ","))
sec$add_fact("7 Tage",format(steigerung_7t,big.mark=".",decimal.mark=","))
sec$add_fact("7 Tage Vorwoche",paste0(format(steigerung_7t_vorwoche,
                                             big.mark = ".", 
                                             decimal.mark = ",", nsmall =0),
         " (",ifelse(steigerung_7t-steigerung_7t_vorwoche > 0,"+",""),
         format(steigerung_7t - steigerung_7t_vorwoche,big.mark = ".", decimal.mark = ",", nsmall =0),
         trend_string,")"))


sec$add_fact("Tote heute: ",
             paste0(format(tote_neu,big.mark=".",
                           decimal.mark = ",")))
sec$add_fact("Tote gesamt: ",
             paste0(format(tote_gesamt,big.mark=".",
                           decimal.mark = ",")))

sec$add_fact("Gesamtfälle: ",
             paste0(format(faelle_gesamt,big.mark=".",
                           decimal.mark = ",")))
sec$add_fact("Genesen: ",
             paste0(format(genesen_gesamt,big.mark=".",
                           decimal.mark = ",")))
sec$add_fact("Aktive Fälle: ",
             paste0(format(faelle_gesamt-genesen_gesamt-tote_gesamt,big.mark=".",
                           decimal.mark = ",")))

# Wenn du auf dem Server bist: 
# Importiere eine PNG-Version des Impffortschritts, 
# schiebe sie auf den Google-Bucket, und 
# übergib die URL an die Karte. 


if (server) {
  # Google-Bucket befüllen
  png <- dw_export_chart(chart_id = "NrBYs",type = "png",unit="px",mode="rgb", scale = 1, 
                         width = 360, height = 360, plain = FALSE)
  image_write(png,"./png/fall4w-tmp.png")
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./png/fall4w-tmp.png gs://d.data.gcp.cloud.hr.de/fall4w-tmp.png')
  sec$add_image(sec_image="https://d.data.gcp.cloud.hr.de/fall4w-tmp.png", sec_title="Letzte 4 Wochen und Trend")
}

# Karte vorbereiten und abschicken. 
cc$add_section(new_section = sec)
if(cc$send()) msg("OK, Teams-Karte abgeschickt") else msg("OK")

