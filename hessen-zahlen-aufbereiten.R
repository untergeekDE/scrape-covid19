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
# - die DIVI-Daten (-> scrape-divi.R)
# - die Reproduktionszahlen-Daten (->scrape-helmholtz.R)
# - die logarithmische Wachstumsgrafik (-> scrape-jhu.R)
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 10.6.2020


# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
library(dplyr)
library(stringr)
library(readr)
library(tidyr)
library(openxlsx)
library(googlesheets4) #v0.2.0 - viele Änderungen
library(lubridate)
library(DatawRappr)
library(jsonlite)

# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Init-Werte fürs Logging, das Arbeitsverzeichnis und die Keyfile-Auswahl
server <- FALSE

# Logging und Update der Semaphore-Seite
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B12, Statuszeile in C12
  d <- data.frame(b = now(tzone= "CEST"), c = paste0(x,...))
  range_write(id_msg,d,sheet="Tabellenblatt1",
              range="B12:C12",col_names = FALSE,reformat=FALSE)
  if (server) Sys.sleep(5)     # Skript ein wenig runterbremsen wegen Quota
  if (logfile != "") {
    cat(x,...,file = logfile, append = TRUE)
  }
}

# Argumente werden in einem String-Vektor namens args übergeben,
# wenn ein Argument übergeben wurde, dort suchen, sonst Unterverzeichnis "src"

args = commandArgs(trailingOnly = TRUE)
if (length(args)!=0) { 
  server <- args[1] %in% c("server","logfile")
  if(args[1] == "logfile") logfile <- "./logs/hessen.log"
} 

sheets_email <- "googlesheets4@scrapers-272317.iam.gserviceaccount.com"
sheets_keypath <- "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json"
  
# VERSION FÜR DEN SERVER 
if (server) {
  # Arbeitsverzeichnis, Logdatei beschreiben
  setwd("/home/jan_eggers_hr_de/rscripts/")
  # Authentifizierung Google-Docs umbiegen
  sheets_keypath <- "/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json"
} 


gs4_deauth() # Authentifizierung löschen
gs4_auth(email=sheets_email,path=sheets_keypath)

# ---- Start, RKI-Daten lesen, Hessen-Fälle filtern, Kopie schreiben ----

msg("\n\n-- START ",as.character(today())," --")

# Vorbereitung: Index-Datei einlesen; enthält Kreise/AGS und Bevölkerungszahlen
msg("Lies index/kreise-index-pop.xlsx","\n")
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))

# RKI-Daten lesen und auf Hessen filtern
# Wird vom RKI-Scraper hier abgelegt. 

use_json <- TRUE

if (use_json) {
  msg("Achtung, experimenteller JSON-Datenzugang aktiv")
  
  # JSON-Abfrage-Code von Till (danke!)
  rki_json_link <- "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"
  
  # Muss in Stückchen gelesen werden, weil per JSON nicht mehr als 5000 Datensätze zurückgegeben werden
  rekursiver_offset <- function(alte_faelle = NULL, offset = 0){
    neuer_rki_link = str_c(rki_json_link, "&resultOffset=", offset)
    neue_liste = read_json(neuer_rki_link, simplifyVector = TRUE)
    neue_faelle = neue_liste$features$attributes
  
    if(is.null(alte_faelle)){
      alle_faelle = neue_faelle
    } else{
      alle_faelle = bind_rows(alte_faelle, neue_faelle) 
    }
    
    if(is.null(neue_faelle)){
      return(alle_faelle)
      
    } else {
      neuer_offset = as.integer(offset + nrow(neue_faelle)) # as.integer() vermeidet einen Bug, bei dem die Zahl 100000 als "1e+05" übergeben wird.
      return(rekursiver_offset(alle_faelle, neuer_offset))        
    }
    
  }
  
  rki_df <- rekursiver_offset()
  
  # Datumsspalten in solche umwandeln
  # Die Spalten "Meldedatum" und "Refdatum" sind in UTC-Millisekunden(!) angegeben.
  
  rki_df <- rki_df %>%
    mutate(Meldedatum = as_datetime(Meldedatum/1000, origin = lubridate::origin, tz = "UTC"),
           Refdatum = as_datetime(Refdatum/1000, origin = lubridate::origin, tz = "UTC"))

  rki_df$Datenstand <- as_date(dmy_hm(rki_df$Datenstand))

} else {
  rki_url <- "https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv"  
  ndr_url <- "https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/rki_api.current.csv"
  rki_df <- read.csv(url(rki_url)) 
  if (ncol(rki_df)> 17 & nrow(rki_df) > 100000) {
    msg("Daten erfolgreich vom RKI-CSV gelesen")
  } else {
    rki_df <- read.csv(url(ndr_url))
    msg("Daten erfolgreich aus dem NDR Data Warehouse gelesen")
  }
  
}

# Daten für Hessen filtern; tagesaktuelle Kopie lokal ablegen

rki_he_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>% 
  group_by(Meldedatum)

# Der Datenstand ist bei allen Einträgen gleich; Format: "23.05.2020, 00:00 Uhr"
# Gern aber auch mal: "2020-05-26"
# Extrahiere Datum
ts = rki_he_df$Datenstand[1]

if (is.Date(ymd(ts))){
  ts = max(ymd(rki_he_df$Datenstand))
} else {
  if(is.Date(as.Date(ts,format = "%d.%m.%Y"))) {
    ts = (as.Date(ts,format = "%d.%m.%Y"))
  } else {
    if (is.Date(as.Date(ts,format = "%d.%m.%y"))) {
      ts = as.Date(ts,format = "%d.%m.%y")
    }
  }
}


msg("RKI-Daten gelesen - ",nrow(rki_df)," Zeilen ",ncol(rki_df)," Spalten - ",ts)

write_csv2(rki_he_df,"hessen_rki_df.csv")

# Tabelle vorbereiten

gsheet_id = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"

# ---- Daten für heute berechnen

#RKI-Abfragestring für die Länder konstruieren
heute <- as_date(ymd(today()))
rki_rest_query <- paste0("https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/",
                         "Coronaf%C3%A4lle_in_den_Bundesl%C3%A4ndern/FeatureServer/0/",
                         "query?f=json&where=1%3D1",
                         "&returnGeometry=false&spatialRel=esriSpatialRelIntersects",
                         "&outFields=*&groupByFieldsForStatistics=LAN_ew_GEN&orderByFields=LAN_ew_GEN%20asc",
                         "&outStatistics=%5B%7B%22statisticType%22%3A%22max%22%2C%22onStatisticField%22%3A%22Fallzahl%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D",
                         "&outSR=102100&cacheHint=true")

# Das JSON einlesen. Gibt eine ziemlich chaotische Liste zurück. 
daten_liste <- read_json(rki_rest_query, simplifyVector = TRUE)

# Was wo liegt, bekommt man über daten_liste$features$attributes$LAN_ew_GEN

# Paranoia-Polizei: 
if (daten_liste$features$attributes$LAN_ew_GEN[7] != "Hessen") {
  msg("Kein Hessen im JSON - ",daten_liste$features$attributes$LAN_ew_GEN[7])
  simpleError("Kein Hessen im JSON!")
}

faelle_gesamt <- daten_liste$features$attributes$value[7]

# Paranoia-Polizei 22
heute_df <- rki_he_df %>%
  filter(NeuerFall %in% c(0,1))
if (faelle_gesamt != sum(heute_df$AnzahlFall)) simpleError("JSON-Summe != CSV-Summe")

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

# ---- Basisdaten ----

msg("Basisdaten-Seite (Google) schreiben...","\n")

 
#      filter(ymd(Meldedatum) == ymd(Datenstand)-1)  %>%
  
datumsstring <- paste0(day(ts),".",month(ts),".",year(ts),", 00:00 Uhr")
# Datumsstring schreiben (Zeile 2)
range_write(gsheet_id,as.data.frame(datumsstring),range="Basisdaten!A2",
            col_names = FALSE, reformat=FALSE)

# Steigerung zum Vortag absolut und prozentual schreiben (Zeile 3)
range_write(gsheet_id,as.data.frame(
  paste0(faelle_neu," (+",
         round((faelle_gesamt/(faelle_gesamt-faelle_neu)-1)*100,1)," %)")),
  range="Basisdaten!B3", col_names = FALSE, reformat=FALSE)

# Anzahl Fälle schreiben (Zeile 4)
range_write(gsheet_id,as.data.frame(faelle_gesamt),range="Basisdaten!B4",
            col_names = FALSE, reformat=FALSE)

# Steigerung Todesfälle zum Vortag absolut und prozentual schreiben (Zeile 5)
range_write(gsheet_id,as.data.frame(
  paste0(tote_neu," (+",
         round((tote_gesamt/(tote_gesamt-tote_neu)-1)*100,1)," %)")),
  range="Basisdaten!B5", col_names = FALSE, reformat=FALSE)

# Gesamtzahl Tote schreiben (Zeile 6)
range_write(gsheet_id,as.data.frame(tote_gesamt),range="Basisdaten!B6",
            col_names = FALSE,reformat=FALSE)

# Aktive Fälle (= Gesamt-Tote-Genesene), nur in Prozent (Zeile 7)
range_write(gsheet_id, as.data.frame(paste0(
  as.character(round((faelle_gesamt-genesen_gesamt-tote_gesamt) / faelle_gesamt * 100))," %")),
  range="Basisdaten!B7", col_names = FALSE, reformat=FALSE)

# Genesene (laut RKI) (Zeile 8)
# RKI-Daten zu Beginn in rki_df eingelesen - zeitaufwändig
# Absolute Zahl und Anteil an den Fällen
range_write(gsheet_id, as.data.frame(paste0(
  as.character(round(genesen_gesamt / faelle_gesamt * 100))," %")),
  range="Basisdaten!B8", col_names = FALSE, reformat=FALSE)


# ---- Verlauf Fallzahl ergänzen, letzte 4 Wochen berechnen ----

fallzahl_df <- read_sheet(gsheet_id,sheet="FallzahlVerlauf")

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
fallzahl_df$tote_steigerung[fallzahl_ofs] <- tote_gesamt/(tote_gesamt-tote_neu)-1
fallzahl_df$aktiv[fallzahl_ofs] <- faelle_gesamt-genesen_gesamt-tote_gesamt
fallzahl_df$neu[fallzahl_ofs] <- faelle_neu
fallzahl_df$aktiv_ohne_neu[fallzahl_ofs] <- faelle_gesamt-genesen_gesamt-tote_gesamt-faelle_neu


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

# Letzte 4 Wochen und Verlauf auf die Sheets
write_sheet(fallzahl_df, ss=gsheet_id, sheet="FallzahlVerlauf")
range_write(fall4w_df,ss = gsheet_id, sheet = "Fallzahl4Wochen",reformat=FALSE)
msg("FallzahlVerlauf (",fallzahl_ofs," Zeilen) -> Fallzahl4Wochen von ",
    fall4w_df$datum[1]," bis ",fall4w_df$datum[28])


# ---- Basisdaten 7-Tage-Werte und Trend Woche zu Woche ----

# Wachstumsrate (Zeile 10)
# Durchschnitt der letzten 7 Steigerungsraten (in fall4w_df sind die letzten 4 Wochen)
steigerung_prozent <- round(mean(fall4w_df$steigerung[22:28]) * 100,1)
v_zeit <- round(log(2)/log(1+mean(fall4w_df$steigerung[22:28])),1)

range_write(gsheet_id,as.data.frame(str_replace(paste0(steigerung_prozent," %"),"\\.",",")),
            range="Basisdaten!B10", col_names = FALSE, reformat=FALSE)

# Neufälle/100.000 in letzten sieben Tagen
steigerung_7t=sum(fall4w_df$neu[22:28])
steigerung_7t_inzidenz <- round(steigerung_7t/sum(kreise$pop)*100000,1)
range_write(gsheet_id,as.data.frame(str_replace(paste0(steigerung_7t_inzidenz,
                                                           " (",steigerung_7t," Fälle)"),"\\.",",")),
            range="Basisdaten!B11", col_names = FALSE, reformat=FALSE)

# Tendenz Woche zu Woche mit Fallzahl (Zeile 12)
# Neufälle vorige Woche zu vergangener Woche
steigerung_7t_vorwoche <- sum(fall4w_df$neu[15:21])
steigerung_prozent_vorwoche <- (steigerung_7t/steigerung_7t_vorwoche*100)-100

trend_string <- "&#9632;"
if (steigerung_prozent_vorwoche < -10) # gefallen
  trend_string <- "<b style='color:#019b72'>&#9660;</b><!--gefallen-->"
if (steigerung_prozent_vorwoche > 10) # gestiegen
  trend_string <- "<b style='color:#cc1a14'>&#9650;</b><!--gestiegen-->"
if (steigerung_prozent_vorwoche < -25) # stark gefallen
  trend_string <- "<b style='color:#019b72'>&#9660;&#9660;</b><!--stark gefallen-->"
if (steigerung_prozent_vorwoche > 25) # stark gestiegen
  trend_string <- "<b style='color:#cc1a14'>&#9650;&#9650;</b><!--stark gestiegen-->"

range_write(gsheet_id,as.data.frame(paste0(trend_string,
                                               " (",ifelse(steigerung_7t-steigerung_7t_vorwoche > 0,"+",""),
                                               steigerung_7t - steigerung_7t_vorwoche," Fälle)")),
            range="Basisdaten!B12", col_names = FALSE, reformat=FALSE)

# ---- Aufbereitung nach Kreisen ----

# Das letzte Kreis-Dokument ziehen und die Notizen isolieren
notizen_df <- range_read(gsheet_id, sheet = "KreisdatenAktuell") %>% 
  select (AGS = ags_kreis, notizen)

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
         AktivProz)
         
  
write_sheet(kreise_summe_df,ss=gsheet_id,sheet="KreisdatenAktuell")
write_csv2(kreise_summe_df,"KreisdatenAktuell.csv")

msg("Kreisdaten geschrieben.")         

# ---- Archivdaten in die GSheets ArchivKreisFallzahl, (...Tote, ...Genesen) -----
archiv_df <- kreise_summe_df %>% select(ags_kreis, gesamt) %>%
  rename(!!as.character(heute) := gesamt)

archiv_fallzahl_df <- range_read(ss=gsheet_id,sheet="ArchivKreisFallzahl") %>%
  full_join(archiv_df,by="ags_kreis")
write_sheet(archiv_fallzahl_df,ss=gsheet_id,sheet="ArchivKreisFallzahl")
write_csv2(archiv_fallzahl_df,"ArchivKreisFallzahl.csv")

archiv_df <- kreise_summe_df %>% select(ags_kreis, tote) %>%
  rename(!!as.character(heute) := tote)

archiv_tote_df <- range_read(ss=gsheet_id,sheet="ArchivKreisTote") %>%
  full_join(archiv_df,by="ags_kreis")
write_sheet(archiv_tote_df,ss=gsheet_id,sheet="ArchivKreisTote")
write_csv2(archiv_tote_df,"ArchivKreisTote.csv")

archiv_df <- kreise_summe_df %>% select(ags_kreis, AnzahlGenesen) %>%
  rename(!!as.character(heute) := AnzahlGenesen)

archiv_genesen_df <- range_read(ss=gsheet_id,sheet="ArchivKreisGenesen") %>%
  full_join(archiv_df,by="ags_kreis")
write_sheet(archiv_genesen_df,ss=gsheet_id,sheet="ArchivKreisGenesen")
write_csv2(archiv_genesen_df,"ArchivKreisGenesen.csv")

msg("Archivkopie Kreisdaten RKI ",heute," angelegt")

# ---- Aufbereitung Alter und Geschlecht Aktive/Tote ----

# Tabelle Altersgruppen/Population einlesen

altersgruppen_df <- range_read(gsheet_id,sheet="AltersgruppenPop") %>%
  mutate(Altersgruppe = as.factor(Altersgruppe))

# Auf aktive Fälle filtern, nach Alter und Geschlecht anordnen

aktive_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>%
  filter(NeuerFall %in% c(0,1)) %>%
  # filter(NeuGenesen %in% c(0,1)) %>%
  # Alter unbekannt? Ausfiltern. 
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% 
  # Genesene und Todesfälle ausfiltern - nur aktive Fälle
  filter(AnzahlGenesen == 0) %>%              #
  filter(AnzahlTodesfall == 0) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = AnzahlFall) %>%
  select(Altersgruppe, männlich = M, weiblich = W) 

# Berechne Inzidenzen für die Altersgruppen
# Quelle Bevölkerungsstatistik Hessen statistik.hessen.de
# Spalte pop mit den Bevölkerungszahlen für die jeweilige Alterskohorte

# Inzidenzen für Altersgruppen berechnen
aktive_df <- aktive_df %>%
  right_join(altersgruppen_df, by= c("Altersgruppe"="Altersgruppe")) %>%
  mutate(Inzidenz = (männlich+weiblich)/pop*100000) %>%
  select(Altersgruppe, männlich, weiblich, Inzidenz) # Spalte pop wieder rausnehmen


# Die Fälle, die nicht zuzuordnen sind - Alter, Geschlecht - in letzte Zeile
unbek_df <- rki_df %>% 
  filter(Bundesland == "Hessen") %>%
  # Genesene und Todesfälle ausfiltern - nur aktive Fälle
  filter(AnzahlGenesen == 0) %>%              #
  filter(AnzahlTodesfall == 0) %>%
  filter(!str_detect(Altersgruppe,"A[0-9]") | 
           !(Geschlecht %in% c("M","W")))

# Freie Zeile für die Fälle, bei denen Alter/Geschlecht unbekannt ist
aktive_df[nrow(aktive_df)+1,] <- NA
aktive_df$Altersgruppe[nrow(aktive_df)] <- "unbekannt" 
aktive_df$männlich[nrow(aktive_df)] <- sum(unbek_df$AnzahlFall)

# Tote nach Alter und Geschlecht aufschlüsseln 

tote_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>%
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% # Alter unbekannt -> filtern
  mutate(Altersgruppe = paste0(str_replace_all(Altersgruppe,"A","")," Jahre")) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlTodesfall = sum(AnzahlTodesfall),
            AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = c(AnzahlTodesfall, AnzahlFall)) %>%
  mutate(inz = (AnzahlTodesfall_M+AnzahlTodesfall_W)/(AnzahlFall_W+AnzahlFall_M)*100) %>%
  select(Altersgruppe,männlich = AnzahlTodesfall_M, weiblich = AnzahlTodesfall_W, Inzidenz = inz)

# Anteil Todesfälle in der Altersgruppe berechnen


# Die beiden Tabellen mit der Aufschlüsselung Aktive und Tote schreiben

msg("Daten Aktive/Tote nach Alter und Geschlecht ausgeben")

write_sheet(aktive_df, ss = gsheet_id, sheet="AktiveAlter")
write_sheet(tote_df,ss = gsheet_id, sheet="ToteAlter")


options(scipen=100,           # Immer normale Kommazahlen ausgeben, Keine wissenschaftlichen Zahlen
        OutDec=","	          # Komma ist Dezimaltrennzeichen bei Ausgabe
)  

msg("Altersaufschlüsselung Aktive/Tote in CSV, Länderzahlen fortschreiben\n")

write_csv2(aktive_df, "rki-alter.csv")
write_csv2(tote_df,"rki-tote.csv")

# sheets_append(laender_faelle_df,rki_alter_id,sheet ="faelle")
# sheets_append(laender_tote_df,rki_alter_id,sheet ="tote")

# Zeitstempel 
range_write(gsheet_id,as.data.frame(as.character(heute)),sheet="AktiveAlter",range= "A1",col_names = FALSE)
range_write(gsheet_id,as.data.frame(as.character(heute)),sheet="ToteAlter",range= "A1",col_names = FALSE)


# ---- Aufräumarbeiten, Grafiken pingen ---- 

basisdaten <- range_read(ss=gsheet_id,sheet="Basisdaten")
alte_basisdaten_id = "1m6hK7s1AnDbeAJ68GSSMH24z4lL7_23RHEI8TID24R8"
write_sheet(basisdaten, ss=alte_basisdaten_id,sheet="LIVEDATEN")
basisdaten <- basisdaten %>%
  select(1,Messzahl = 2) %>%
  mutate(Messzahl = as.character(Messzahl)) %>%
  mutate(Messzahl = str_replace(Messzahl,"NULL"," "))
basisdaten$Messzahl[11] <- as.character(steigerung_prozent_vorwoche)
write_csv2(basisdaten,"Basisdaten.csv",quote_escape="double")
msg("Daten auf alte Basisdaten-Seite kopiert")


#msg("Daten auf alte Basisdaten-Seite kopiert")

msg(as.character(now()),"Datawrapper-Grafiken pingen...","\n")

# Alle einmal ansprechen, damit sie die neuen Daten ziehen
# - Neu publizieren, damit der DW-Server einmal die Google-Sheet-Daten zieht.

dw_publish_chart(chart_id = "OXn7r") # Basisdaten
dw_publish_chart(chart_id = "NrBYs") # Neufälle und Trend letzte 4 Wochen
dw_publish_chart(chart_id = "k8nUv") # Flächengrafik
dw_publish_chart(chart_id = "ALaUp") # Choropleth-Karte Fallinzidenz
dw_publish_chart(chart_id = "nQY0P") # Choropleth 7-Tage-Dynamik
dw_publish_chart(chart_id = "XpbpH") # Aktive Fälle nach Alter und Geschlecht
dw_publish_chart(chart_id = "JQobx") # Todesfälle nach Alter und Geschlecht


# Kein Update DIVI-Scraper
# Kein Update dieser Grafiken: 
# dw_publish_chart(chart_id = "KP1H3") # Trendlinien-Grafik -> scrape-jhu.R
# dw_publish_chart(chart_id = "82BUn") # Helmholtz-R-Kurve -> scrape-helmholtz.R

#
msg("OK!")
