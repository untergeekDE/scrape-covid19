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
# Die entsprechenden Grafiken werden am Ende gepingt, um aktualisiert zu werden. 
# CSVs der hessischen Fälle, der Kreisdaten und der letzten 4 Wochen werden archiviert und 
# als Aktuell-Kopie auf den Google-Bucket geschoben. 
#
# Weiter NICHT von diesem Skript betreut sind:
# - die DIVI-Daten (-> divi-zahlen-aufbereiten.R)
# - die Reproduktionszahlen-Daten (->scrape-helmholtz.R)
# - die Übertragung der Prognose und der RKI-Testdaten (-> mittwochsskript.R)
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 27.4.2021

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- "B12:C12"

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

library(hystReet) # Zugriff auf Hystreet-Passantendaten



# ---- Start, RKI-Daten lesen, Hessen-Fälle filtern, Kopie schreiben ----

msg("\n\n-- START ",as.character(today())," --")

# Vorbereitung: Index-Datei einlesen; enthält Kreise/AGS und Bevölkerungszahlen
msg("Lies index/kreise-index-pop.xlsx","...")
# Jeweils aktuelle Bevölkerungszahlen; zuletzt aktualisiert Juli 2020
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))

# ---- Funktion RKI-Daten lesen ----

# Die Daten liegen nicht bei der RKI, sondern im Data Warehouse des Daten-Dienstleisters ESRI,
# der auch das Dashboard betreibt. 

# ESRI-Status-Abfrage; danke Björn Schwendker, NDR

get_esri_status <- function() {
  # Holt und säubert Statusinformationen über die Bereitstellung des RKI Covid-19-Datensatzes durch ESRI
  # (Gemeint ist dieser Datensatz: https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0/data)
  # Beschreibung des Services: https://www.arcgis.com/home/item.html?id=cd0eda38e31d41259465f9c763de1941  esri_base_url <- "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/rki_service_status_v/FeatureServer/0/query"
  esri_base_url <- "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/rki_service_status_v/FeatureServer/0/query"
  response <- httr::GET(esri_base_url, 
                        query = list(f = "json", 
                                     outfields = "*",
                                     where = "Url='https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0'") ) %>% 
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


read_rki_data <- function(use_json = TRUE) {
  
  # Wenn ESRI-Datenbank noch nicht OK, warte!
  
  if (use_json) {
    while(get_esri_status()$Status != "OK") {
      msg("ESRI-Status: ", get_esri_status()$Status)
      Sys.sleep(60)
    }
    
    # Anmerkung zur Schnittstelle (März 2021):
    # Der Service RKI_COVID19 wird zwar vom Corona-Dashboard der ESRI
    # selbst nicht mehr genutzt, dient aber weiter zur Bereitstellung der Daten.
    # Mehr hier: https://arcgis.esri.de/neue-datenstrukturen-im-dashboard/
    
    # JSON-Abfrage-Code von Till (danke!)
    # Dokumentation: https://github.com/br-data/corona-deutschland-api/blob/master/RKI-API.md
    rki_json_link <- paste0("https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/",
                            "rest/services/RKI_COVID19/FeatureServer/0/query?",
                            "where=1%3D1",            # where 1=1
                            "&outFields=*",           # alle Ausgabefelder
                            "&returnGeometry=false",
                            "&cacheHint=true",
                            "&f=json",
                            "&resultRecordCount=2000")  # 2000 Fälle pro Abfrage (Maximum sind 5k)
    
    
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
    
    # Muss in Stückchen gelesen werden, weil per JSON nicht mehr als 5000 Datensätze zurückgegeben werden
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
    rki_url <- "https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv"  
    ndr_url <- "https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/rki_api.current.csv"
    # Zwei Schritte: Erst der Download, dann einlesen
    download.file(rki_url,"RKI_COVID19.csv",method="curl")
    rki_ = read.csv("RKI_COVID19.csv") 
    if (ncol(rki_)> 17 & nrow(rki_) > 100000) {
      msg("Daten erfolgreich vom RKI-CSV gelesen")
    } else {
      # Rückfall: Aus dem NDR-Data-Warehouse holen
      rki_ = read.csv(url(ndr_url))
      msg("Daten erfolgreich aus dem NDR Data Warehouse gelesen")
    }
  }
  # Sollte die Spalte mit der Landkreis kein String sein, umwandeln und mit führender 0 versehen
  if(class(rki_$IdLandkreis) != "character") {
    rki_$IdLandkreis <- paste0("0",rki_$IdLandkreis)
  }
  # Wenn 'Datenstand' ein String ist, in ein Datum umwandeln. Sonst das Datum nutzen. 
  if (class(rki_$Datenstand) == "character") {
    rki_$Datenstand <- parse_datetime(rki_$Datenstand[1],format = "%d.%m.%Y, %H:%M Uhr")
  }
  return(rki_)
}


# ---- Datenabfrage ESRI: -----

# Wenn ESRI-Datenbank noch nicht OK, warte!
# while(get_esri_status()$Status != "OK") {
#   msg("ESRI-Status: ", get_esri_status()$Status)
#   Sys.sleep(60)
# }


# Sicherheitscheck: Neue Daten? ----

#RKI-Abfragestring für die Länder konstruieren

esri_url <- "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/"

esri_service <- "rki_key_data_v/"

rki_rest_query1 <- paste0(esri_url,esri_service,
                         "FeatureServer/0/",
                         "query?f=json&",
                         "where=1%3D1&",
                         "outFields=*")

# rki_rest_query <- paste0(esri_url,
#                          "Coronaf%C3%A4lle_in_den_Bundesl%C3%A4ndern/FeatureServer/0/",
#                          "query?f=json&where=1%3D1",
#                          "&returnGeometry=false&spatialRel=esriSpatialRelIntersects",
#                          "&outFields=*&groupByFieldsForStatistics=LAN_ew_GEN&orderByFields=LAN_ew_GEN%20asc",
#                          "&outStatistics=%5B%7B%22statisticType%22%3A%22max%22%2C%22onStatisticField%22%3A%22Fallzahl%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D",
#                          "&outSR=102100&cacheHint=true")

# Das JSON einlesen. Gibt eine ziemlich chaotische Liste zurück.
# Tabelle für alle Länder und Kreise ist in daten_liste$features$attributes.
esri_daten_tabelle <- read_json(rki_rest_query1, simplifyVector = TRUE)$features$attributes
faelle_gesamt_direkt <- esri_daten_tabelle$AnzFall[esri_daten_tabelle$AdmUnitId==6]

hessen_esri_df <- esri_daten_tabelle %>% filter(BundeslandId==6)
msg("Schreibe ESRI-Tabelle für Land und Kreise...")
write.csv(hessen_esri_df,"daten/esri-tabelle-hessen.csv")
write_sheet(hessen_esri_df,ss=aaa_id,sheet="ESRI direkt")
if (server) {
  # Google-Bucket befüllen
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./daten/esri-tabelle-hessen.csv gs://d.data.gcp.cloud.hr.de/esri-tabelle-hessen.csv')
}  
msg("Geschrieben. ESRI-Status: ",get_esri_status()$Status)

# Was wo liegt, bekommt man über daten_liste$features$attributes$LAN_ew_GEN
# Daten von gestern holen und vergleichen
old_data <- read_sheet(aaa_id,sheet="Basisdaten") %>% select(x=1,y=2)
od <- old_data %>% filter(str_detect(x,"Fälle gesamt"))
od2 <- str_remove(od$y," \\(ca.+\\)")
faelle_gestern <- as.integer(str_remove(od2,"[^0-9]"))

# Test auf gleiches Ergebnis nur, wenn die Basisdaten alt sind
if (as.Date(old_data$x[1],format="%d.%m.%y%y") < today()) {
  # Gleiche Daten wie gestern??? Warte. 
  starttime <- now()
  while (faelle_gesamt_direkt == faelle_gestern) {
    msg("!!!JSON-Direktabfrage ergibt keine Veränderung zu gestern!!!")
    Sys.sleep(300) # 5min schlafen
    daten_liste <- read_json(rki_rest_query, simplifyVector = TRUE)
    faelle_gesamt_direkt <- daten_liste$features$attributes$value[7]
    if (now() > starttime+10800) {
      msg("JSON: Keine neue Fallzahl")
      simpleError("Timeout, keine aktuellen RKI-Daten nach 3 Stunden")
      quit()
    }
  }
}

# Plausibilitätsprüfung 1: Fehler, wenn neue Fallzahl kleiner als die gestern
if (faelle_gesamt_direkt < faelle_gestern) {
  msg("!!!Fälle heute < Fälle gestern!!!")
  simpleError("Fälle heute < Fälle gestern")
  quit()
}

# Plausibilitätsprüfung 2: Fehler, Absurd hohe Steigerung
if (faelle_gesamt_direkt - faelle_gestern > 50000) {
  msg("!!!Wirklich mehr als 50k neue Fälle in Hessen?!!!")
  simpleError(">50k Neufälle")
  quit()
}


# ---- Kompletten Datensatz herunterladen ----
# RKI-Daten lesen und auf Hessen filtern
# Wird vom RKI-Scraper hier abgelegt. 

# rki_df anlegen, damit er sie als globale Variable kennt
rki_df <- data.frame(2,2)

# Daten lesen; wenn noch Daten von gestern, warten. 
use_json <- FALSE
if (use_json) msg("Daten vom RKI via JSON anfordern...") else msg("RKI-CSV lesen...")

rki_df <- read_rki_data(use_json)

# Fallback: Von Hand laden
fallback <- FALSE
if (fallback) {
  rki_df <- read.csv("~/Downloads/RKI_COVID19.csv")
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

# Alte Daten? 
starttime <- now()
while (ts < today()) {
  msg("!!!RKI-Daten sind Stand ",ts)
  Sys.sleep(60)   # Warte eine Minute
  if (now() > starttime+36000) {
    msg("--- TIMEOUT ---")
    simpleError("Timeout, keine aktuellen RKI-Daten nach 10 Stunden")
    quit()
  }
  rki_df <- read_rki_data(use_json)
  # alternierend versuchen, das CSV zu lesen
  use_json <- !use_json
  # Nur, wenn Anzahl der Zeilen laut Statusabfrage der Anzahl der Zeilen
  # via JSON entspricht, weitermachen
  if (get_esri_status)
  ts <- rki_df$Datenstand[1]
}

# Plausibilität: Datensätze per JSON 

msg("RKI-Daten gelesen - ",nrow(rki_df)," Zeilen ",ncol(rki_df)," Spalten - ",ts)

# Daten für Hessen filtern; tagesaktuelle Kopie lokal ablegen

rki_he_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>% 
  # wenn die AGS-Spalte eine Zahl ist, mit führender Null versehen
  group_by(Meldedatum)

# CSV-Archivkopien von rki_he_df anlegen
heute <- as_date(ymd(today()))
write_csv2(rki_he_df,"daten/hessen_rki_df.csv")
write_csv2(rki_he_df,paste0("archiv/rki-",heute,".csv"))


# Paranoia-Polizei: 
if (nrow(esri_daten_tabelle)<429) {
  msg("ESRI-Daten-Tabelle unvollständig?")
  simpleError("ESRI-Daten-Tabelle unvollständig?")
}


# Paranoia-Polizei 22
heute_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1))
faelle_gesamt <- sum(heute_df$AnzahlFall)
if (faelle_gesamt !=faelle_gesamt_direkt)  simpleError("JSON-Summe != CSV-Summe")

heute_df <- rki_he_df %>% 
  filter(NeuerFall %in% c(-1,1))    # so zählt man laut RKI die Summe der Fälle
faelle_neu <- sum(heute_df$AnzahlFall)
heute_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1)) 
genesen_gesamt <- sum(heute_df$AnzahlGenesen)

heute_df <- rki_he_df %>% 
  filter(NeuerTodesfall %in% c(0,1))
tote_gesamt <- sum(heute_df$AnzahlTodesfall)
heute_df <- rki_he_df %>% 
  filter(NeuerTodesfall %in% c(-1,1))
tote_neu <- sum(heute_df$AnzahlTodesfall)

aktiv_gesamt <- faelle_gesamt - genesen_gesamt  - tote_gesamt

datumsstring <- paste0(day(ts),".",month(ts),".",year(ts),", 00:00 Uhr")


# ---- Verlauf Fallzahl ergänzen, letzte 4 Wochen berechnen ----

msg("Berechne fallzahl und fallzahl4w...")
fallzahl_df <- read_sheet(aaa_id,sheet="FallzahlVerlauf")

fallzahl_df$datum <- as_date(fallzahl_df$datum)
fallzahl_ofs <- as.numeric(heute - fallzahl_df$datum[1]) + 1

if (fallzahl_ofs > nrow(fallzahl_df)) {
  fallzahl_df[fallzahl_ofs,]<- NA
  fallzahl_df$datum[fallzahl_ofs] <- heute
  
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
#range_write(aaa_id,as.data.frame('neue Fälle'),range="Basisdaten!A3")
range_write(aaa_id,as.data.frame(faelle_neu),
            range="Basisdaten!B3", col_names = FALSE, reformat=FALSE)


## ACHTUNG: ##
# Seit 23.8. machen wir es wie das RKI und berechnen 7-Tage-Inzidenz und Vergleich
# zu den 7 Tagen davor aus den korrigierten Daten nach Meldedatum. 


# Neufälle letzte 7 Tage - (Zeile 4)
# **ACHTUNG** Berechnung nach Meldedatum, nicht aus den gemeldeten "Briefkastendaten" der Neufälle
#range_write(aaa_id,as.data.frame("letzte 7 Tage (pro 100.000)"),range="Basisdaten!A4")
steigerung_7t=sum(f28_df$AnzahlFall[22:28])
steigerung_7t_inzidenz <- round(steigerung_7t/sum(kreise$pop)*100000,1)
range_write(aaa_id,as.data.frame(steigerung_7t),
            range="Basisdaten!B4", col_names = FALSE, reformat=FALSE)

# Vergleich Vorwoche (Zeile 5)
#range_write(aaa_id,as.data.frame("Vergleich Vorwoche"),range="Basisdaten!A5")
steigerung_7t_vorwoche <- sum(f28_df$AnzahlFall[15:21])
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
  paste0(format(steigerung_7t_vorwoche,big.mark = ".", decimal.mark = ",", nsmall =0),
         " (",ifelse(steigerung_7t-steigerung_7t_vorwoche > 0,"+",""),
         format(steigerung_7t - steigerung_7t_vorwoche,big.mark = ".", decimal.mark = ",", nsmall =0),
         trend_string,")")),
            range="Basisdaten!B5", col_names = FALSE, reformat=FALSE)


# Wachstumsrate durch Inzidenz ersetzt
# Durchschnitt der letzten 7 Steigerungsraten (in fall4w_df sind die letzten 4 Wochen)
#steigerung_prozent <- round(mean(fall4w_df$steigerung[22:28]) * 100,1)
#v_zeit <- round(log(2)/log(1+mean(fall4w_df$steigerung[22:28])),1)

# Inzidenz (Zeile 6)

range_write(aaa_id,as.data.frame(format(steigerung_7t_inzidenz,big.mark = ".",decimal.mark=",",nsmall=1)),
            range="Basisdaten!B6", col_names = FALSE, reformat=FALSE)

# Gesamt und aktiv (Zeile 7)
# range_write(aaa_id,as.data.frame(paste0("Fälle gesamt/aktiv")),range="Basisdaten!A7")
aktiv_str <- format(round((faelle_gesamt-genesen_gesamt-tote_gesamt)/100) * 100,
                    big.mark = ".", decimal.mark = ",", nsmall =0)
range_write(aaa_id,as.data.frame(paste0(format(faelle_gesamt,big.mark = ".", decimal.mark = ",", nsmall =0),
                                           " (ca. ",aktiv_str,")")),
            range="Basisdaten!B7", col_names = FALSE, reformat=FALSE)

# Immunisiert und geimpft (Zeile 8 und 9)

msg("Impfzahlen und Immunisierungsquote")
# Geimpft (Zeile 8)
impfen_df <- read_sheet(ss=aaa_id,sheet = "ArchivImpfzahlen") %>%
  filter(am == max(am))
range_write(aaa_id,as.data.frame(paste0("Geimpft (",
                                        format.Date(impfen_df$am,"%d.%m."),")")),
            range="Basisdaten!A8",col_names=FALSE,reformat=FALSE)
range_write(aaa_id, as.data.frame(paste0(
  format(impfen_df$personen,big.mark=".",decimal.mark = ","),
  " (+", format(impfen_df$differenz_zum_vortag_erstimpfung,big.mark = ".", decimal.mark = ",", nsmall =0),
  ")")),
  range="Basisdaten!B8", col_names = FALSE, reformat=FALSE)

# Immun (Zeile 9)
impf_alt_df <- read_sheet(ss=aaa_id,sheet="ArchivImpfzahlen") %>% filter(am <= today()-14)
immun <- max(as.numeric(impf_alt_df$personen)) + genesen_gesamt
hessen=sum(read.xlsx("index/kreise-index-pop.xlsx") %>% select(pop))

range_write(aaa_id,as.data.frame("Immunisiert sind ca. "),range="Basisdaten!A9", col_names=FALSE,reformat=FALSE)
immun_str <- paste0(format(round(immun/hessen*100,1),
                           big.mark = ".", decimal.mark = ",", nsmall =0),
                    " %")
range_write(aaa_id,as.data.frame(immun_str),
            range="Basisdaten!B9", col_names = FALSE, reformat=FALSE)


# Todesfälle heute (Zeile 10)
#range_write(aaa_id,as.data.frame("neue Todesfälle"),range="Basisdaten!A10")
range_write(aaa_id,as.data.frame(tote_neu),
            range="Basisdaten!B10",col_names = FALSE, reformat=FALSE)

# Todesfälle gesamt (Zeile 11)
#range_write(aaa_id,as.data.frame("Todesfälle gesamt"),range="Basisdaten!A11")
range_write(aaa_id,as.data.frame(format(tote_gesamt,big.mark=".",decimal.mark = ",")),
            range="Basisdaten!B11",
            col_names = FALSE,reformat=FALSE)

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

msg("Hystreet-Daten lesen und auswerten...")

# Hystreet API Token lesen
tag0 <- ymd("2019-12-30")
if (server) {
  token <- read_lines("/home/jan_eggers_hr_de/key/.hystreettoken")
} else {
  token <- read_lines(".hystreettoken")
}
set_hystreet_token(token)

# Lies die verfügbaren Messstationen
stations <- get_hystreet_locations()
msg(nrow(stations)," Hystreet-Stationen abfragen...")

# Helper: Download data from Feb. 1st until given date for given station
get_station_data <- function(id, date){
  data = get_hystreet_station_data(
    hystreetId = id,
    query = list(from = "2020-02-01", to = date, resolution = "day")
  )
  
  ret = data[["measurements"]] %>%
    mutate(id = data[["id"]],
           city = data[["city"]],
           station = data[["name"]],
           label = paste0(city, ", ", station)) %>%
    select(id, city, station, label, timestamp,
           weather_condition, temperature, min_temperature,
           pedestrians_count)
  
  return(ret)
}

# Collect data for every station available
# Filter out today. Why not collect only until yesterday?
# -> Data for last day
# in request seems weird
all_data <- lapply(stations$id, function(x){
  get_station_data(x, today())
}) %>% bind_rows %>%
  filter(timestamp != today())

# Pivot for datawrapper
data_by_city <- all_data %>%
  pivot_wider(id_cols = timestamp, names_from = label, values_from = c(pedestrians_count,weather_condition)) %>%
  # Woche unter Einbeziehung des Jahres
  # Rechnerische Woche, damit die Rechnung über den Jahrswechsel hinweg funktioniert
  mutate(week = 1+as.integer(ymd(timestamp)-ymd(tag0)) %/% 7)

# Summiere Städte auf, dann: 
# Kalkuliere Wochenmittel und prozentuale Veränderung zu Referenzwoche

reference_week = 38 # KW38 war Mitte September 

data_for_dw_weeks <- data_by_city %>%
  # Nur Wochentage!
  filter(wday(timestamp) %in% 2:7) %>% # Passanten montags bis samstags
  mutate(Frankfurt = `pedestrians_count_Frankfurt a.M., Goethestraße`+
           `pedestrians_count_Frankfurt a.M., Zeil (Mitte)` +
           `pedestrians_count_Frankfurt a.M., Große Bockenheimer Straße`,
         Frankfurt_Wetter = `weather_condition_Frankfurt a.M., Zeil (Mitte)`) %>%
  mutate(Limburg = `pedestrians_count_Limburg, Werner-Senger-Straße`,
         Limburg_Wetter = `weather_condition_Limburg, Werner-Senger-Straße`) %>%
  # Für wiesbaden nur den Zähler in Mitte; den in der Kirchgasse Nord
  # gab es erst ab Anfang April. Vermutung: Das schafft Probleme!
  mutate(Wiesbaden = `pedestrians_count_Wiesbaden, Kirchgasse (Mitte)`,
         Wiesbaden_Wetter = `weather_condition_Wiesbaden, Kirchgasse (Mitte)`) %>%
  mutate(Darmstadt = `pedestrians_count_Darmstadt, Schuchardstraße`+
           `pedestrians_count_Darmstadt, Ernst-Ludwig-Straße`,
         Darmstadt_Wetter = `weather_condition_Darmstadt, Schuchardstraße`) %>%
  mutate(`Gießen` = `pedestrians_count_Gießen, Seltersweg`,
         `Gießen_Wetter` = `weather_condition_Gießen, Seltersweg`) %>%
  # Erst mal ohne Wetter
  select(timestamp,week,
         Darmstadt, Darmstadt_Wetter,
         Frankfurt,Frankfurt_Wetter,
         `Gießen`, Gießen_Wetter,
         Limburg,Limburg_Wetter,
         Wiesbaden, Wiesbaden_Wetter) %>%
  group_by(week) %>%
  summarize(Darmstadt = sum(Darmstadt),
            Darmstadt_Wetter = first(Darmstadt_Wetter),
            Frankfurt = sum(Frankfurt),
            Frankfurt_Wetter = first(Darmstadt_Wetter),
            `Gießen` = sum(`Gießen`),
            `Gießen_Wetter` = first(`Gießen_Wetter`),
            Limburg = sum(Limburg),
            Limburg_Wetter = first(Limburg_Wetter),
            Wiesbaden = sum(Wiesbaden),
            Wiesbaden_Wetter = first(Wiesbaden_Wetter))

# Referenzwerte bestimmen
ref_week <- data_for_dw_weeks %>%
  select(Frankfurt, `Gießen`, Darmstadt, Limburg, Wiesbaden,week) %>%
  filter(week == reference_week)

data_for_dw_weeks <- data_for_dw_weeks %>%
  # laufende Woche ausfiltern
  filter((tag0+week*7) > ymd("2020-03-03")) %>%
  filter((tag0+week*7-2) < today()) %>%
  mutate(Frankfurt = round(Frankfurt/ref_week$Frankfurt*100,1)-100,
         Darmstadt = round(Darmstadt/ref_week$Darmstadt*100,1)-100,
         `Gießen` = round(`Gießen`/ref_week$`Gießen`*100,1)-100,
         Wiesbaden = round(Wiesbaden/ref_week$Wiesbaden*100,1)-100,
         Limburg = round(Limburg/ref_week$Limburg*100,1)-100) %>%
  mutate(Mittel = (Frankfurt+Darmstadt+`Gießen`+Wiesbaden+Limburg)/5) %>%
  mutate(wtext = paste0(day(as_date(tag0+week*7-7)),".",
                        ifelse(month(as_date(tag0+week*7-7)) == month(as_date(tag0+week*7-3)),"",month(as_date(week*7+4))),
                        ifelse(month(as_date(tag0+week*7-7)) == month(as_date(tag0+week*7-3)),"","."),
                        "-",day(as_date(tag0+week*7-3)),
                        ".",month(as_date(tag0+week*7-3)),"."))


dw_data_to_chart(data_for_dw_weeks,"U89m9",parse_dates =FALSE)
dw_publish_chart(chart_id = "U89m9")


# ---- Aufbereitung nach Kreisen ----

msg("Aufbereitung nach Kreisen...")

# Änderung: Als "Notizen" evtl. Ausgangssperren vermerken
# (aus dem Sperren-Dokument)
# Das letzte Kreis-Dokument ziehen und die Notizen isolieren

sperren_id = "1zdR1rDOt0H6THUke_W6Hb-Lt3luzh0ZJtBPxvf3cZbI"

notizen_df <- range_read(sperren_id, sheet = "Ausgangssperren") %>%
  mutate(Infolink = ifelse(!is.na(Infolink),Infolink,Gesundheitsamt)) %>%
  mutate(Infos = ifelse(!is.na(Infos),
                        paste0("<a href=\'",
                               Infolink,
                               "\' target=\'_blank\'>",
                               Infos,
                               "</a>"),
                        paste0("<a href=\'",
                               Infolink,
                               "\' target=\'_blank\'>",
                               "[Kreisinfos]",
                               "</a>"))) %>%
  select (AGS, notizen = Infos)

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

# Daten für Hessen nach Kreis und Datum pivotieren
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

# Für alle 26 Kreise: 
for ( k in 2:27){
  # Bevölkerung aus der Kreis-Tabelle ziehen, via AGS
  p <- kreise$pop[kreise$AGS == colnames(hessen_neu_df[,k])]
  # Kopiere aus hessen_df eine gleitende 7-Tage-Summe und berechne die Inzidenz
  # Stumpfer Algorithmus ohne jede Raffinesse, dauert entsprechend lang
  for (i in 8:nrow(hessen_neu_df)){
    hsum_df[i,k] <- round(sum(hessen_neu_df[(i-6):i,k])/p*100000,1)
  }
}

# Kreisnamen als Spaltenköpfe
k <- kreise %>%
  arrange(AGS)
  
colnames(hsum_df) <- c("Datum",k$kreis)

# Datum auf Datumsspalte
# Um eins erhöht, weil die Inzidenz ja die Meldedaten bis gestern berücksichtigt. 

hsum_df$Datum <- as_date(hsum_df$Datum)+1

# Als Excel-Blatt exportieren
write_sheet(hsum_df,ss=aaa_id,sheet="ArchivKreisInzidenz")
write_csv2(hsum_df,"daten/ArchivKreisInzidenz.csv")

# ---- Tabelle Inzidenz-Ampel erstellen ----


sperren_df <- kreise %>% select(kreis)


for (i in 1:26) {
  # Alle Kreise durchgehen
  inz7t_v <- hsum_df %>% filter(Datum > today()-8) %>% 
    select(all_of(sperren_df$kreis[i])) %>% pull(.)
  # Jetzt die Variablen setzen: 
  v100 <- ""
  v150 <- ""
  v165 <- ""
  for (j in inz7t_v) {
    v100 <- paste0(v100,ifelse(j>100,"X","-"))
    v150 <- paste0(v150,ifelse(j>150,"X","-"))
    v165 <- paste0(v165,ifelse(j>165,"X","-"))
  }
  # Ersetze X durch schwarzen großen Unicode-Punkt, 
  # den - durch weißen kleinen. 
  sperren_df$ü100[i] <- paste0(
    "<b style=\'font-family:courier;\'>",
    v100,
    "</b>"  )
  sperren_df$ü150[i] <- paste0(
    "<b style=\'font-family:courier;\'>",
    v150,
    "</b>"  )
  
  sperren_df$ü165[i] <- paste0(
    "<b style=\'font-family:courier;\'>",
    v165,
    "</b>"  )
    # str_replace_all(str_replace_all(v165,"X","\u25CF"),"-","\u25E6") 
  sperren_df$text100[i] <- "aufgehoben"
  sperren_df$text150[i] <- "aufgehoben"
  sperren_df$text165[i] <- "aufgehoben"
  if(str_detect(v100,"XXX..")) sperren_df$text100[i] <- "aktiv"
  if(str_detect(v100,"----XXX.")) sperren_df$text100[i] <- " kommen"
  if(str_detect(v100,"-----XXX")) sperren_df$text100[i] <- " kommen"
  if(str_detect(v100,"..XXX---")) sperren_df$text100[i] <- " könnten auslaufen"
  if(str_detect(v100,"XXX-----")) sperren_df$text100[i] <- " laufen aus"
  if(str_detect(v150,"XXX..")) sperren_df$text150[i] <- "aktiv"
  if(str_detect(v150,"----XXX.")) sperren_df$text150[i] <- " kommen"
  if(str_detect(v150,"-----XXX")) sperren_df$text150[i] <- " kommen"
  if(str_detect(v150,"..XXX---")) sperren_df$text150[i] <- " könnten auslaufen"
  if(str_detect(v150,"XXX-----")) sperren_df$text150[i] <- " laufen aus"
  if(str_detect(v165,"XXX..")) sperren_df$text165[i] <- "aktiv"
  if(str_detect(v165,"----XXX.")) sperren_df$text165[i] <- " kommen"
  if(str_detect(v165,"-----XXX")) sperren_df$text165[i] <- " kommen"
  if(str_detect(v165,"..XXX---")) sperren_df$text165[i] <- " könnten auslaufen"
  if(str_detect(v165,"XXX-----")) sperren_df$text165[i] <- " laufen aus"
  # 
  if (sperren_df$text100[i] =="aufgehoben") {
    sperren_df$text[i] <- "--"
  } else {
    t <- paste0("<a href=\'",
          "https://www.hessenschau.de/politik/bundesnotbremse-kommt-das-aendert-sich-jetzt-in-hessen,infektionsschutzgesetz-hessen-100.html#Ausgangssperre",
          "\'>Ausgangssperren</a>",
         ifelse(sperren_df$text100[i]!="aktiv",sperren_df$text100,""))
    # Wird nur überprüft, wenn ohnehin 100er: 150er Grenze erreicht?
    if (sperren_df$text150[i] !="aufgehoben") {
      t <- paste0(t,", ",
                  "<a href=\'",
                  "https://www.hessenschau.de/politik/bundesnotbremse-kommt-das-aendert-sich-jetzt-in-hessen,infektionsschutzgesetz-hessen-100.html#Geschaefte",
                  "\'>Geschäftsschließungen</a>",
                  ifelse(sperren_df$text150[i]!="aktiv",sperren_df$text150[i],""))
      # wird nur überprüft, wenn 150er Grenze erreicht: 165er Grenze?
      if (sperren_df$text165[i] !="aufgehoben") {
        t <- paste0(t,", ",
                    "<a href=\'",
                    "https://www.hessenschau.de/politik/bundesnotbremse-kommt-das-aendert-sich-jetzt-in-hessen,infektionsschutzgesetz-hessen-100.html#Schulen",
                    "\'>Schulschließungen</a>",
                    ifelse(sperren_df$text165[i]!="aktiv",sperren_df$text165[i],""))
        
      }
    }
      sperren_df$text[i] <- t
  } # end else (wenn irgendwas ist)
}

# Schnell noch einen Datumsstring konstruieren

#saveRDS(sperren_df,"sperren_df.rds")


sperren_id <- "1zdR1rDOt0H6THUke_W6Hb-Lt3luzh0ZJtBPxvf3cZbI"
sperren_info_df <- read_sheet(ss=sperren_id,sheet="Ausgangssperren") %>% 
  mutate(Infolink = ifelse(!is.na(Infolink),Infolink,Gesundheitsamt)) %>%
  mutate(Infos = ifelse(!is.na(Infos),
                        paste0("<a href=\'",
                               Infolink,
                               "\' target=\'_blank\'>",
                               Infos,
                               "</a>"),
                        paste0("<a href=\'",
                               Infolink,
                               "\' target=\'_blank\'>",
                               "[Link]",
                               "</a>"))) %>%
  mutate(Infos = ifelse(is.na(Infos),"",Infos)) %>%
  select(kreis,Infos)
sperren_df <- sperren_df %>% left_join(sperren_info_df,by="kreis") 

dw_data_to_chart(sperren_df,chart_id="NV9FT")
dw_edit_chart(chart_id="NV9FT",annotate = paste0("Stand: ",format(heute, "%d.%m.%Y")))
dw_publish_chart(chart_id="NV9FT")

write_sheet(sperren_df,ss=sperren_id,sheet="Sperren-Tabelle")

# experimentelle andere Darstellungsform: 

inz7t_df <- hsum_df %>% filter(Datum>today()-7) %>% # 7 Tage 
  # Transponieren: 
  # erst in lange Tabelle verwandeln
  pivot_longer(cols=-Datum,names_to="Kreis",values_to="inz7t") %>%
  mutate(Datum = wday(Datum,label=TRUE,abbr=TRUE,locale="de_DE")) %>%
  pivot_wider(names_from=Datum,values_from=inz7t) %>%
  left_join(sperren_df %>% select(kreis,text100,text150,text165,Infos),
            by=c("Kreis" = "kreis")) %>%
  mutate(Infos = paste0(ifelse(text100!="aufgehoben",
                               ifelse(text100=="aktiv","A","(A)"),""),
                        ifelse(text150!="aufgehoben",
                               ifelse(text150=="aktiv","/G","/(G)"),""),
                        ifelse(text100!="aufgehoben",
                               ifelse(text100=="aktiv","/S","/(S)"),""),
                        " ",Infos))

dw_data_to_chart(inz7t_df,chart_id="psn2l")


dw_publish_chart(chart_id="psn2l")

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

# ---- Aufbereitung Alter und Geschlecht Aktive/Tote ----

# Tabelle Altersgruppen/Population einlesen

msg("Aufschlüsselung Neufälle nach Alter...")

altersgruppen_df <- range_read(aaa_id,sheet="AltersgruppenPop") %>%
  mutate(Altersgruppe = as.factor(Altersgruppe))

# Auf aktive Fälle filtern, nach Alter und Geschlecht anordnen

neu7tage_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1)) %>%
  # Neufälle der letzten 7 Tage
  filter(Meldedatum > heute-8) %>%
  
  # filter(NeuGenesen %in% c(0,1)) %>%
  # Alter unbekannt? Ausfiltern. 
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% 
  # Genesene und Todesfälle ausfiltern - nur aktive Fälle
  # filter(AnzahlGenesen == 0) %>%              #
  # filter(AnzahlTodesfall == 0) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = AnzahlFall) %>%
  select(Altersgruppe, männlich = M, weiblich = W) %>%
  ungroup()

# Berechne Inzidenzen für die Altersgruppen
# Quelle Bevölkerungsstatistik Hessen statistik.hessen.de
# Spalte pop mit den Bevölkerungszahlen für die jeweilige Alterskohorte

# Inzidenzen für Altersgruppen berechnen
neu7tage_df <- neu7tage_df %>%
  right_join(altersgruppen_df, by= c("Altersgruppe"="Altersgruppe")) %>%
  mutate(Inzidenz = (männlich+weiblich)/pop*100000) %>%
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
  pivot_wider(names_from = Geschlecht, values_from = c(AnzahlTodesfall, AnzahlFall)) %>%
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


options(scipen=100,           # Immer normale Kommazahlen ausgeben, Keine wissenschaftlichen Zahlen
        OutDec=","	          # Komma ist Dezimaltrennzeichen bei Ausgabe
)  

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
basisdaten <- range_read(ss=aaa_id,sheet="Basisdaten")
alte_basisdaten_id = "1m6hK7s1AnDbeAJ68GSSMH24z4lL7_23RHEI8TID24R8"
write_sheet(basisdaten, ss=alte_basisdaten_id,sheet="LIVEDATEN")
basisdaten <- basisdaten %>%
  select(1,Messzahl = 2) %>%
  mutate(Messzahl = as.character(Messzahl)) %>%
  mutate(Messzahl = str_replace(Messzahl,"NULL"," "))
# Den ganzen HTML-Kram aus der Steigerung zur Vorwoche verschwinden lassen
basisdaten$Messzahl[4] <- as.character(steigerung_prozent_vorwoche)
write_csv2(basisdaten,"daten/Basisdaten.csv",quote_escape="double")


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

dw_publish_chart(chart_id = "OXn7r") # Basisdaten
dw_publish_chart(chart_id = "NrBYs") # Neufälle und Trend letzte 4 Wochen
dw_publish_chart(chart_id = "jLkVj") # Neufälle je Woche seit März
dw_publish_chart(chart_id = "k8nUv") # Flächengrafik
dw_publish_chart(chart_id = "ALaUp") # Choropleth-Karte Fallinzidenz
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


msg("OK!")

