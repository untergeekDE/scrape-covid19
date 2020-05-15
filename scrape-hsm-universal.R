################ scrape-hsm-universal.R
# Ministeriums-Scraper
#
# Greift die Corona-Tagesinfos vom Server soziales.hessen.de ab
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 15.5.2020

library("rvest")
library("tidyverse")
library(openxlsx)
library(readxl)
library(googlesheets4)
library(lubridate)
library(openssl)
library(httr)
library(DatawRappr)
#library(pdftools)
library(jsonlite)
library(fuzzyjoin)

# Init-Werte fürs Logging, das Arbeitsverzeichnis und die Keyfile-Auswahl
server <- FALSE

# ---- Logging und Update der Semaphore-Seite ----
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B6, Statuszeile in C6
  d <- data.frame(b6 = now(tzone= "CEST"), c6 = paste0(x,...))
  sheets_edit(id_msg,d,sheet="Tabellenblatt1",
              range="B6:C6",col_names = FALSE,reformat=FALSE)
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
  if(args[1] == "logfile") logfile <- "./logs/scrape-hsm.log"
} 

sheets_email <- "googlesheets4@scrapers-272317.iam.gserviceaccount.com"
sheets_keypath <- "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json"
  
###### VERSION FÜR DEN SERVER #####
if (server) {
  # Arbeitsverzeichnis, Logdatei beschreiben
  setwd("/home/jan_eggers_hr_de/rscripts/")
  # Authentifizierung Google-Docs umbiegen
  sheets_keypath <- "/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json"
} 


sheets_deauth() # Authentifizierung löschen
sheets_auth(email=sheets_email,path=sheets_keypath)

msg("\n\n---------------- START ",as.character(today()),"------------------\n")

# Vorbereitung: Index-Datei einlesen; enthält Kreise/AGS und Bevölkerungszahlen
msg("Lies index/kreise-index-pop.xlsx","\n")
kreise <- read.xlsx("index/kreise-index-pop.xlsx")

# RKI-Daten lesen und auf Hessen filtern
# Wird vom RKI-Scraper hier abgelegt. 

rki_df <- read.csv2("rki_df.csv") 

# Achtung: Wenn Datei von gestern, also: today() größer als Zeitstempel der Datei, 
# dann vielleicht doch noch mal aktualisieren.
mdate <- file.info("rki_df.csv")$mtime
if (today()>mdate) {
  ndr_url <- "https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/rki_api.current.csv"
  rki_df <- read.csv(url(ndr_url)) %>% filter(Bundesland == "Hessen")
}

genesen_df <- rki_df %>% 
  select(IdLandkreis,AnzahlGenesen) %>%
  group_by(IdLandkreis) %>%
  summarize(AnzahlGenesen = sum(AnzahlGenesen)) %>%
  mutate(IdLandkreis = paste0("0",as.character(IdLandkreis)))


# ---- Google-Tabellen fallzahl, wachstum, cck einlesen ----
msg("Lies die Google-Tabellen cck_id, wachstum, fallzahl_id (Daten), ...","\n")


# Tabelle: "Covid Choropleth Kreise"
id_cck="1h0bvmSjSC-7osQpt94iGre9K5o_Atfj0UnLyQbuN9l4"
# Tabelle: "Wachstum daily"
id_wachstum="1NXHj4JXfD_jaf-P3YjA7jv3BYuOwU9_WKoVcDknIG64"
# Tabelle: "Covid Choropleth Kreise ALT
id_test = "1Q8iOCUu5aka3SiGDHGFsOLBM5QTXnM1hftyotMr08ZI"
# Tabelle: "Corona-Fallzahlen-Hessen"
id_fallzahl = "1OhMGQJXe2rbKg-kCccVNpAMc3yT2i3ubmCndf-zX0JU"

wachstum_df <- read_sheet(id_wachstum,sheet = "Tabellenblatt1")
fallzahl_df <- read_sheet(id_fallzahl,sheet ="daten")
cck_df <- read_sheet(id_cck,sheet="daten")


# ---- Ministeriums-Seite lesen und auf Aktualisierung prüfen ----
msg("URL der Ministeriums-Seite abfragen...","\n")

url <- "https://soziales.hessen.de/gesundheit/infektionsschutz/coronavirus-sars-cov-2/taegliche-uebersicht-der-bestaetigten-sars-cov-2-faelle-hessen"
m <- c("Januar","Februar","März","April","Mai","Juni","Juli","August","September","November","Dezember")

# Die Aktualisierung wird einfach über das Datum angestoßen: 
# Wenn das aus dem Dokument gelesene Datum niedriger ist als das höchste
# aus fallzahl_df gelesene, warte und probiers noch mal.  

heute <- today()
last_date <- max(ymd(fallzahl_df$datum))
# last_date <- as.Date(paste0(str_extract(last_date,"[12][0-9][0-9][0-9]"),"-",
#                             as.character(str_which(
#                               str_extract(last_date,"\\s[JFMAMJJASOND][a-zä]*\\s"),
#                               fixed(m))),"-",
#                         str_extract(last_date,"[0-9]+")))

this_date <- last_date  #

# Schleife wird ausgeführt,  solange noch kein neueres Dokument da ist. 
while(this_date <= last_date) {
  # Fiese kleine Warte-Schleife: Seite nicht erreichbar? Warte 120 Sekunden, versuch es wieder.
  # Fehlerbehandlung: ab und zu ist die Seite kurz nicht erreichbar, bevor sie online geht. 
  # Dann einfach warten und nochmal probieren. 
  #
  # Diese Art der Fehlerbehandlung scheint auf der VM Probleme zu bereiten, 
  # lokal läuft sie. 
  while(tryCatch(
    stop_for_status(GET(url)),
    http_404 = function (e) {404},
    http_403 = function (e) {403}, # Forbidden
    http_405 = function (e) {405}, 
    http_408 = function (e) {408}, # Timeout
    http_502 = function (e) {502} # Bad Gateway
  ) %in% c(403,404,405,408,502)) {
    simpleWarning("Seite nicht erreichbar ")
    msg("Warte 60 Sekunden...\n")
    Sys.sleep(60)
  } 
  tryCatch(webpage <- read_html(url)) # Seite einlesen. Versuchs halt. 

    # Datum grabben - irgendwo, wo "Stand: " steht
  ts_htm <- html_nodes(webpage,"p")
  for(x in ts_htm) {
    httxt = html_text(x)
    if (str_detect(httxt,"Stand[\\s]+[0-9]")) 
      ts <- str_extract(html_text(x),"[0-9]+\\.\\s[JFMAMJJASOND][a-zä]*\\s?2020\\,\\s?[0-9]+\\:[0-9][0-9]\\sUhr")
  }
            
  ts_clean <- str_replace(str_replace_all(ts,"[\\.\\ \\:]",""),"\\,","-")

  tag <- str_extract(ts,"[0-9]+")
  jahr <- str_extract(ts,"[12][0-9][0-9][0-9]")
  monat <- str_extract(ts,"\\s[JFMAMJJASOND][a-zä]*\\s")
  m <- c("Januar","Februar","März","April","Mai","Juni","Juli","August","September","November","Dezember")
  monat <- as.character(str_which(monat,fixed(m)))
  if (as.numeric(monat) < 10) monat <- paste0("0",monat)
  if (as.numeric(tag) < 10) tag <- paste0("0",tag)
  datum <- paste0(tag,".",monat,".",jahr)
  this_date <- make_date(year = jahr, month = monat, day = tag)
  if (this_date == last_date) {
    msg("Warte 60 Sekunden auf Refresh...\n")
    Sys.sleep(60)
  }
}

if (!server) {
  library(beepr)
  beep(2)
}
msg("GELESEN: --- Seite vom ",jahr,"-",monat,"-",tag,"---\n")

# Den PDF-Link holen und sichern. 
# pdf_link <- str_extract(str_detect(html_nodes(webpage,"a"),"pdf"),'https://.+\\.pdf')
# pdf <- GET(pdf_link)

# ---- Tabellendaten aus der Webseite extrahieren, Kreise matchen ----
# Die erste Tabelle auf der Seite auslesen
tables <- html_node(webpage,"table")

# #Kopfzeile weg, NA durch 0 ersetzen
# table_df <- table_df[3:nrow(table_df),]


# viel simpler: html_table nutzen
table_df <- html_table(tables)
colnames(table_df) <- c("kreis","gesamt","tote","inz","neu7tage","inz7t")

# Jetzt erst mal das Matching mit den Kreisnamen
# Die komischen Namen des HMSI standardisieren
table_df$kreis[table_df$kreis == "SK Darmstadt"] <- "Darmstadt (Stadt)"
table_df$kreis[table_df$kreis == "SK Wiesbaden"] <- "Wiesbaden (Stadt)"
table_df$kreis[table_df$kreis == "LK Offenbach"] <- "Offenbach (Landkreis)"
table_df$kreis[table_df$kreis == "LK Kassel"] <- "Kassel (Landkreis)"

table_df$kreis <- str_replace(table_df$kreis,"SK Offenbach","Offenbach (Stadt)")
table_df$kreis <- str_replace(table_df$kreis,"SK Kassel","Kassel (Stadt)")
table_df$kreis <- str_remove(table_df$kreis,"LK ")
table_df$kreis <- str_remove(table_df$kreis,"SK ")

# Von den Daten aus dem Google-Doc "Covid Choropleth Karte" nur die Notizen und Todesfälle behalten
notizen_df <- cck_df %>% select(kreis,notizen,tote_hsde = tote)

# Daten in all_df  zusammenführen: AGS, Bevölkerungszahl  
all_df <- table_df %>%
  # bis 27.4.: c("kreis","gesamt","hospitalisiert","tote","inz","neu7tage","inz7t")
  # ab 28.4.: kreis,gesamt,tote,inz,neu7tage,inz7t
  select(k = 1,
         gesamt = 2,
         tote = 3,
         inzidenz = 4,
         neu7tage = 5,
         inz7t = 6) %>%
  # Fuzzyjoin - um Bindestriche und ähnlichen Kram zu ignorieren
  stringdist_right_join(kreise, by = c("k" = "kreis"), max_dist=1) %>%
  # Werte in Zahlen umwandeln
  mutate(gesamt = as.integer(str_replace(gesamt,"\\.","")),
         tote = as.integer(tote),
         inzidenz = as.integer(gesamt)/pop*100000, # rechnen, nicht glauben
         neu7tage = as.integer(neu7tage),
         inz7t = as.integer(inz7t)) %>%
  select(kreis,gesamt,tote,inzidenz,neu7tage,inz7t,AGS,pop,Lat,Lon) %>%
  # RightJoin: Nur die Kreise!
  mutate(AGS = paste0("06",str_replace(AGS,"000",""))) %>%
  mutate(stand = ts) %>% # Stand auf Zeitstempel des hmsi-Dokuments setzen
  right_join(notizen_df, by = c("kreis" = "kreis")) %>%
  # Anzahle der Toten auf hsde-Informationsstand # FRÜHER
  # mutate(tote = ifelse(tote < tote_hsde,tote_hsde,tote)) %>% 
  # Anzahl der Genesenen nach Kreis
  left_join(genesen_df, by = c("AGS" = "IdLandkreis"))


# ---- Plausibilitäts-Prüfung! ----

if (!server) View(all_df)
msg("Gelesen:",nrow(all_df),"Zeilen",ncol(table_df),"Spalten")
if (nrow(all_df) > 26) simpleError("Zu viele Einträge?")
if (nrow(all_df) < 26) simpleError("Kreis fehlt")
if (ncol(table_df)!=6 | ncol(table_df)<2) simpleError("Formatänderung!")
# Test mit anti_join: nicht gematchter Kreis?
if (nrow(stringdist_anti_join(kreise,table_df,by =c("kreis" = "kreis"),max_dist=1)) > 0) {
  print(stringdist_anti_join(kreise,table_df,by = c("kreis" = "kreis")))
  simpleError("Nicht gematchter Eintrag\n")
}


summary(all_df)

# ---- CODE für Überschreiben des letzten Wertes anpassen!!!
# Wenn der Wert kleiner oder gleich dem letzten: Fehlermeldung!
if (table_df$gesamt[27] <= fallzahl_df$faelle[nrow(fallzahl_df)] ) {
  simpleWarning("Keine Erhöhung der Fallzahl gegen letzten Scan\n")
  simpleWarning(paste0("Gelesen: ",as.character(table_df$gesamt[27]),"\n"))
  simpleWarning(paste0("Letzte Tabellenzeile: ",as.character(fallzahl_df[nrow(fallzahl_df),]),"\n"))
  no_increment <- TRUE
} else { no_increment <- FALSE }


msg("Tabelle all_df zusammenführen...","\n")


# ---- Kreisdaten für Choropleth-Karte und für den Google Bucket ausgeben ----
# Tabellenwerte in Zahlen umwandeln, NA durch 0 ersetzen - nicht mehr nötig
# table_df$gestern <- as.numeric(ifelse(is.na(as.numeric(table_df$gestern)),"0",table_df$gestern))
# all_df$neu <- as.numeric(ifelse(is.na(as.numeric(table_df$neu)),"0",table_df$neu))
# table_df$gesamt <- as.numeric(ifelse(is.na(as.numeric(table_df$gesamt)),"0",table_df$gesamt))

# Tabelle umsortieren und ausdünnen und umbenennen 
final_df <- all_df %>%
  mutate(AnzahlAktiv = gesamt - tote - AnzahlGenesen) %>%
  select(ags_text = AGS,kreis,gesamt,stand,pop,inzidenz,tote,neu7tage,inz7t,AnzahlGenesen,AnzahlAktiv,notizen) %>%
  mutate(TotProz = round(tote/gesamt*100),
         GenesenProz = round(AnzahlGenesen/gesamt*100),
         AktivProz = round(AnzahlAktiv/gesamt*100))


############################# Daten ausgeben ############################
# Etwas verwinkelte Struktur: 
# - id_cck - das Dokument mit den aktuellen Zahlen nach Kreisen. 
#   Enthält auch eine (redaktionell befüllbare)
#   *DW-CHOROPLETH-KARTE AKTUALISIERT SICH AUTOMATISCH DARÜBER*
# - id_wachstum "Corona-Fallzahlen-Wachstumsraten daily" ist ein sehr komplexes
#   Dokument mit (leider) einer Menge Formeln für Steigerungsraten und Trendberechnung. 
#   Die Vergleichszahlen aus NRW (RKI) und ITA (Johns Hopkins) trage ich derzeit
#   noch von Hand ein; sie werden gegen Hessen verschoben; 1 ist der Tag, an dem Inzidenz  ==1
#   - HE: 11.03. (Inzidenz war 0,8)
#   - NRW: 5.03.
#   - ITA: 27.02. 
#   *AUS id_wachstum ZIEHT SICH DW DIE LOGARITHMISCHE TREND-DARSTELLUNG*
# - Daraus zieht sich (derzeit) id_fallzahl alles. (über Google)
# - id_fallzahl Corona-Fallzahlen Hessen ist ein Hilfsdokument.
#   *DARAUS ZIEHT DW DAS BARGRAPH ENTWICKLUNG DER FÄLLE*
#   (Wichtig dafür: das auf T.M. formatierte Datum!!!)
# - Daraus zieht sich das Google-Doc "Corona-Geschwindigkeit Tabelle" die Daten auf 
#   den Reiter Tabellenblatt2, sortiert sie antichronologisch um, berechnet 
#   auf diesem Blatt die gemittelte Wachstumsrate und Verdoppelungszeit. 
#   Die werden auf Tabellenblatt1 angezeigt *UND VON DW GESCHWINDIGKEIT GELESEN.*
# - Was nicht hier passiert: Ein weiteres Skript zieht - täglich um 6 Uhr morgens - die Zahlen
#   des RKI aus dem Dashboard und bereitet daraus Aufstellungen der Fälle und Todesfälle
#   nach Alter und Geschlecht auf. 

msg(as.character(now()),": Starte Datenausgabe","\n")

# Authentifizierung aktualisieren - wenn es zu lange gedauert hat, ist sie verfallen
sheets_deauth() # Authentifizierung löschen
sheets_auth(email=sheets_email,path=sheets_keypath)

# Schreibe Archivkopie, aktualisiere Google-Sheet Karte

# Tabelle für den heutigen Tag mit Zeitstempel abspeichern
write.xlsx(final_df,file=paste0("archiv/",ts_clean,".xlsx"),overwrite = TRUE)
write.csv2(final_df,file="../scrape-hsm.csv")
msg("Archivkopie nach archiv/ und ins Home-Verzeichnis geschrieben","\n")

# Tabelle in das GoogleDoc "Covid Choropleth Kreise" schreiben
sheets_write(final_df,ss = id_cck, sheet = "daten")


# ---- Aktualisiere das ARD-Sheet aktuelle Daten ----
ard_df <- final_df %>%
  mutate(Genesene = AnzahlGenesen, Quelle = url) %>%
  # passende Spalten auswählen 
  select(AGS = ags_text, 
         Kreisname = kreis, 
         Fallzahlen = gesamt, 
         Genesene, 
         Tote = tote, 
         Quelle, 
         Zeitstempel = stand)

msg("ARD-Seite Hessen (Google) aktualisieren","\n")

ard_id = "1OKodgGnSTFRrF51cIrsL7qz0xwAdR8DXovGzD01dqEM"
sheets_write(ard_df,ss = ard_id,sheet = "06_hessen")
# ---- Aktualisiere Fälle/4 Wochen über Google-Doc id_fallzahl ----

# NEU: Frontseite "livedaten" enthält nur die letzten vier Wochen, 
# sonst wird die grafische Darstellung zu voll. 
# Auf der dahinter liegenden Seite "daten" ist die komplette Zeitreihe. 
# Alle Bezüge bleiben. 
# Ganz simpel: 4 Spalten, datum, faelle, steigerung, tote
# Neue Zeile dranhauen
# RKI-Genesenen-Zahlen mit aufnehmen
# Achtung: liegen immer nur für den Vortag vor

faelle_gesamt <- sum(all_df$gesamt)
tote_gesamt <- sum(all_df$tote)
genesen_gesamt <- sum(rki_df$AnzahlGenesen)

# Zahl der Genesenen für gestern nachtragen
fallzahl_df$gsum[fallzahl_df$datum == heute-1] <- genesen_gesamt

# Falls keine Erhöhung, letzte Zeile löschen (und so überschreiben)
fallzahl_df <- fallzahl_df %>%
  # Format erzwingen
    select(datum = 1, faelle = 2, steigerung = 3, tote= 4, tote_steigerung = 5, gsum = 6, aktiv = 7) %>%
    filter(datum != this_date) %>%
    rbind(data.frame(datum = this_date,
                     faelle = faelle_gesamt,
                     steigerung = 0,
                     tote = tote_gesamt,
                     tote_steigerung = 0,
                     gsum = genesen_gesamt,
                     aktiv = 0))  %>%
  # Steigerungsrate
  mutate(steigerung = (faelle/lag(faelle)-1)) %>%
  # Steigerungsrate Todesfälle
  mutate(tote_steigerung = (tote/lag(tote)-1)) %>%
  mutate(tote_steigerung = if_else(is.na(tote_steigerung) | is.infinite(tote_steigerung),0,tote_steigerung)) %>%
  mutate(steigerung = if_else(is.na(steigerung) | is.infinite(steigerung),0,steigerung)) %>%
  # Aktive Fälle 
  mutate(aktiv = faelle-tote-gsum) %>%
  mutate(neu = faelle-lag(faelle)) %>%
  mutate(aktiv_ohne_neu = aktiv-neu)

 


msg("Fallzahlen Hessen (Google), daten/livedaten aktualisieren","\n")

#sheets_edit statt sheets_write, um den reformat-Parameter zu haben
sheets_edit(fallzahl_df,ss = id_fallzahl, sheet = "daten",reformat=FALSE)

# Letzte 4 Wochen auf das livedaten-Blatt
fall4w_df <- fallzahl_df[(nrow(fallzahl_df)-27):nrow(fallzahl_df),]
sheets_edit(fall4w_df,ss = id_fallzahl, sheet = "livedaten",reformat=FALSE)

# Daten für das Tab "livedaten-barchart"
bar4w_df <- t(fall4w_df %>% select(datum,tote,gsum,aktiv))
colnames(bar4w_df) <- bar4w_df[1,]
bar4w_df <- cbind(tibble(t = c("Tote","Genesene","Aktiv")),as.data.frame(bar4w_df[-1,]))
sheets_write(bar4w_df,ss = id_fallzahl, sheet = "livedaten-barchart" )
# ---- Schreibe aktualisierte Werte in Google-Doc id_wachstum ----  

msg("Wachstumsberechnung (Google) aktualisieren","\n")

# Aktuelle Fallzahl und Datum - Stichtag ist der 11.3. (ungefähr Inzidenz 1)
#
he_ofs <-   8+as.numeric(today() - as.Date("2020-03-11"))

# Aktuelle Fallzahl schreiben
sheets_edit(id_wachstum,as.data.frame(faelle_gesamt),sheet="Tabellenblatt1",
            range=paste0("C",as.character(he_ofs)),col_names = FALSE,reformat=FALSE)
# Datum schreiben
sheets_edit(id_wachstum,as.data.frame(heute),sheet="Tabellenblatt1",
            range=paste0("B",as.character(he_ofs)),col_names = FALSE,reformat=FALSE)
# Inzidenz als Formel schreiben
sheets_edit(id_wachstum,
            as.data.frame(sheets_formula(paste0("=C",he_ofs,"/6265809*100000"))),
            sheet="Tabellenblatt1",range=paste0("D",as.character(he_ofs)),col_names=FALSE,reformat=FALSE)
# Prozentuales Wachstum 
sheets_edit(id_wachstum,
            as.data.frame(sheets_formula(paste0("=C",he_ofs,"/C",he_ofs-1,"-1"))),
            sheet="Tabellenblatt1",range=paste0("E",as.character(he_ofs)),col_names=FALSE,reformat=FALSE)

# Trend-Formel anpassen =VARIATION(D24:D30;$A$24:$A$30;$A$8:$A$40)
sheets_edit(id_wachstum, 
            as.data.frame(sheets_formula(paste0(
              "=VARIATION(D",he_ofs-6,
              ":D",he_ofs,
              ";$A$",he_ofs-6,
              ":$A$",he_ofs,
              ";$A$8:$A$",he_ofs+20,")")
              )),
            sheet="Tabellenblatt1",range="F8",col_names=FALSE,reformat=FALSE
            )

msg("Trendformel hessen aktualisiert","\n")

#RKI-Abfragestring konstruieren

rki_rest_query2 <- paste0("https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/",
  "Coronaf%C3%A4lle_in_den_Bundesl%C3%A4ndern/FeatureServer/0/",
  "query?f=json&where=1%3D1",
  "&returnGeometry=false&spatialRel=esriSpatialRelIntersects",
  "&outFields=*&groupByFieldsForStatistics=LAN_ew_GEN&orderByFields=LAN_ew_GEN%20asc",
  "&outStatistics=%5B%7B%22statisticType%22%3A%22max%22%2C%22onStatisticField%22%3A%22Fallzahl%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D",
  "&outSR=102100&cacheHint=true")
  
msg("RKI-JSON-Query:",rki_rest_query2,"\n")

library(jsonlite)
# Das JSON einlesen. Gibt eine ziemlich chaotische Liste zurück. 
daten_liste <- read_json(rki_rest_query2, simplifyVector = TRUE)
zahl_nrw <- daten_liste$features$attributes$value[10]

# Rausfinden, welche Tabellenzelle dran ist. 
# Die Basiszelle für NRW ist J8, das Referenzdatum (Inzidenz ca. 1) der 5.3.2020
nrw_ofs <-   8+as.numeric(today() - as.Date("2020-03-07"))

sheets_edit(id_wachstum,as.data.frame(zahl_nrw),sheet="Tabellenblatt1",
    paste0("J",as.character(nrw_ofs)),col_names = FALSE,reformat=FALSE)
# Inzidenz als Formel schreiben
sheets_edit(id_wachstum,
    as.data.frame(sheets_formula(paste0("=J",nrw_ofs,"/17932651*100000"))),
    sheet="Tabellenblatt1", paste0("K",as.character(nrw_ofs)),col_names=FALSE,reformat=FALSE)
#Prozentuales Wachstum 
sheets_edit(id_wachstum,
            as.data.frame(sheets_formula(paste0("=J",nrw_ofs,"/J",nrw_ofs-1,"-1"))),
            sheet="Tabellenblatt1", paste0("L",as.character(nrw_ofs)),col_names=FALSE,reformat=FALSE)

# Trend-Formel NRW in M8 anpassen
sheets_edit(id_wachstum, 
            as.data.frame(sheets_formula(paste0(
              "=GROWTH(K",nrw_ofs-6,
              ":K",nrw_ofs,
              ";$A$",nrw_ofs-6,
              ":$A$",nrw_ofs,
              ";$A$8:$A$",nrw_ofs+7,")")
            )),
            sheet="Tabellenblatt1",range="M8",col_names=FALSE,reformat=FALSE
)

msg("NRW-Daten und -Formeln angepasst","\n")


# Johns-Hopkins-Zahlen von Gestern lesen
# Dummerweise die Dateinamen genau anders herum mm-tt-yyyy. Scheiß Amis. 

g_tag <- as.character(day(heute-1))
if (day(heute-1) < 10) g_tag <- paste0("0",g_tag)
g_monat <- as.character(month(heute-1))
if (month(heute-1) < 10) g_monat <- paste0("0",g_monat)
g_jahr <- as.character(year(heute-1))

msg("JHU-Daten: Daily Report ",g_monat,"-",g_tag,"-",g_jahr,".csv","\n")

jhu_df <- read.csv(url(paste0("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/",
  g_monat,"-",g_tag,"-",g_jahr,".csv"))) %>%
  filter(Combined_Key == "Italy")

# Zahlen für Italien eintragen
# Formeln für Italien eintragen
# Anpassung Trendlinien-Formel in Q8 (für ITA)

ita_ofs <-   7+as.numeric(today() - as.Date("2020-02-27"))

sheets_edit(id_wachstum,as.data.frame(jhu_df$Confirmed),sheet="Tabellenblatt1",
            paste0("N",as.character(ita_ofs)),col_names = FALSE,reformat=FALSE)
# Inzidenz als Formel schreiben
sheets_edit(id_wachstum,
            as.data.frame(sheets_formula(paste0("=N",ita_ofs,"/60262701*100000"))),
            sheet="Tabellenblatt1", paste0("O",as.character(ita_ofs)),col_names=FALSE,reformat=FALSE)
#Prozentuales Wachstum 
sheets_edit(id_wachstum,
            as.data.frame(sheets_formula(paste0("=N",ita_ofs,"/N",ita_ofs-1,"-1"))),
            sheet="Tabellenblatt1", paste0("P",as.character(nrw_ofs)),col_names=FALSE,reformat=FALSE)

# Trend-Formel ITA in Q8 anpassen
sheets_edit(id_wachstum, 
            as.data.frame(sheets_formula(paste0(
              "=GROWTH(O",ita_ofs-6,
              ":O",ita_ofs,
              ";$A$",ita_ofs-6,
              ":$A$",ita_ofs,
              ";$A$8:$A$",ita_ofs,")")
            )),
            sheet="Tabellenblatt1",range="Q8",col_names=FALSE,reformat=FALSE
)


# ---- Master-Tabelle mit allen Meldungen nach Tagen aktualisieren ----
msg("Master-Tabelle schreiben...","\n")

# Master-Tabelle mit allen Meldungen nach Tagen holen
library(readxl)
master_df <- read_excel("meldungen-master.xlsx", 
                             col_types = c("date", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric", 
                                                     "numeric", "numeric", "numeric"))

#Sicherheitskopie
write.xlsx(master_df,"meldungen-master-bak.xlsx",overwrite = TRUE)

# Leeres df mit Breite der Master-Tabelle anlegen
x <- data.frame(matrix(ncol = ncol(master_df)))

#Erste Spalte: Datum der aktuellen Tabelle
x[,1] <- this_date

#Spaltensummen bilden
for (i in 2:ncol(master_df)){
  x[,i] <- all_df$gesamt[i-1]-sum(master_df[,i])
}
colnames(x) <- colnames (master_df)
master_df <- rbind(master_df,x)

# ergänzt abspeichern
write.xlsx(master_df,"meldungen-master.xlsx",overwrite = TRUE)
master_id = "1kOEoXMmMOlkaq50ed0ToYeIJZ7sK2KivqbIMKnvpib0"
sheets_write(master_df,ss = master_id, sheet = "fallzahlen")

# ---- Basisdaten-Seite schreiben ----
msg("Basisdaten-Seite (Google) schreiben...","\n")

id_basisdaten <- "1m6hK7s1AnDbeAJ68GSSMH24z4lL7_23RHEI8TID24R8"
# Datumsstring schreiben (Zeile 2)
sheets_edit(id_basisdaten,as.data.frame(ts),range="livedaten!A2",
            col_names = FALSE, reformat=FALSE)

# Anzahl Fälle schreiben (Zeile 3)
sheets_edit(id_basisdaten,as.data.frame(faelle_gesamt),range="livedaten!B3",
            col_names = FALSE, reformat=FALSE)

# Steigerung zum Vortag absolut und prozentual schreiben (Zeile 4)
steigerung_absolut <- fallzahl_df$faelle[nrow(fallzahl_df)]-fallzahl_df$faelle[nrow(fallzahl_df)-1]
steigerung_prozent <- round(fallzahl_df$steigerung[nrow(fallzahl_df)] * 100,1)
steigerung_prozent <- str_replace(paste0(steigerung_prozent," %"),"\\.",",")

sheets_edit(id_basisdaten,as.data.frame(
            paste0(steigerung_absolut," (",steigerung_prozent,")")),
            range="livedaten!B4", col_names = FALSE, reformat=FALSE)

# Anzahl Tote schreiben (Zeile 5)
sheets_edit(id_basisdaten,as.data.frame(as.character(tote_gesamt)),range="livedaten!B5",
            col_names = FALSE,reformat=FALSE)

# Veränderung Tote absolut und prozentual schreiben (Zeile 6)
toteneu_absolut <- fallzahl_df$tote[nrow(fallzahl_df)]-fallzahl_df$tote[nrow(fallzahl_df)-1]
toteneu_prozent <- str_replace(paste0(round(
  fallzahl_df$tote[nrow(fallzahl_df)] / fallzahl_df$tote[nrow(fallzahl_df)-1] * 100 - 100,1),
  " %"),"\\.",",")

sheets_edit(id_basisdaten,as.data.frame(
  paste0(toteneu_absolut," (",toteneu_prozent,")")),
  range="livedaten!B6", col_names = FALSE, reformat=FALSE)

# Genesene (laut RKI) (Zeile 7)
# RKI-Daten zu Beginn in rki_df eingelesen - zeitaufwändig
# Absolute Zahl und Anteil an den Fällen

sheets_edit(id_basisdaten, as.data.frame(paste0(
  as.character(round(genesen_gesamt / faelle_gesamt * 100))," %")),
  range="livedaten!B7", col_names = FALSE, reformat=FALSE)

# Aktive Fälle (= Gesamt-Tote-Genesene), nur in Prozent (Zeile 8)


sheets_edit(id_basisdaten, as.data.frame(paste0(
  as.character(round((faelle_gesamt-genesen_gesamt-tote_gesamt) / faelle_gesamt * 100))," %")),
  range="livedaten!B8", col_names = FALSE, reformat=FALSE)

# Wachstumsrate (Zeile 10)
# Durchschnitt der letzten 7 Steigerungsraten (in fall4w_df sind die letzten 4 Wochen)
steigerung_prozent <- round(mean(fall4w_df$steigerung[22:28]) * 100,1)
v_zeit <- round(log(2)/log(1+mean(fall4w_df$steigerung[22:28])),1)

sheets_edit(id_basisdaten,as.data.frame(str_replace(paste0(steigerung_prozent," %"),"\\.",",")),
            range="livedaten!B10", col_names = FALSE, reformat=FALSE)

# Neufälle/100.000 in letzten sieben Tagen
steigerung_7t=sum(fall4w_df$neu[22:28])
steigerung_7t_inzidenz <- round(steigerung_7t/sum(kreise$pop)*100000,1)
sheets_edit(id_basisdaten,as.data.frame(str_replace(paste0(steigerung_7t_inzidenz,
                                                          " (",steigerung_7t," Fälle)"),"\\.",",")),
            range="livedaten!B11", col_names = FALSE, reformat=FALSE)

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

# 

 sheets_edit(id_basisdaten,as.data.frame("Trend Woche zu Woche"),
               range="livedaten!A12", col_names = FALSE, reformat=FALSE)
# 
 sheets_edit(id_basisdaten,as.data.frame(paste0(trend_string,
              " (",ifelse(steigerung_7t_vorwoche > 0,"+",""),
              steigerung_7t - steigerung_7t_vorwoche," Fälle)")),
             range="livedaten!B12", col_names = FALSE, reformat=FALSE)

##### Datawrapper-Grafiken pingen und so aktualisieren #####
msg(as.character(now()),"Datawrapper-Grafiken pingen...","\n")

# Alle einmal ansprechen, damit sie die neuen Daten ziehen
# - Neu publizieren, damit der DW-Server einmal die Google-Sheet-Daten zieht.

dw_publish_chart(chart_id = "7HWCI") # Basisdaten
dw_publish_chart(chart_id = "aWy4J") # Flächengrafik
dw_publish_chart(chart_id = "YBBaK") # Choropleth-Karte Fallinzidenz
dw_publish_chart(chart_id = "B68Gx") # Choropleth 7-Tage-Dynamik
dw_publish_chart(chart_id = "0CS3h") # Fälle-Todesfälle-Prozent-Barchart
dw_publish_chart(chart_id = "KP1H3") # Trendlinien-Grafik

# Kein Update RKI-Barchart Aktive Fälle - RKI-Scraper
# Kein Update RKI-Barchart Todesfälle - RKI-Scraper
# Kein Update DIVI-Scraper
# dw_publish_chart(chart_id = "Kedm4") # Was-wäre-wenn
# dw_publish_chart(chart_id = "82BUn") # Helmholtz-R-Kurve
# dw_publish_chart(chart_id = "V4qmF") # Stacked Barchart

#
msg(as.character(now()),"---- FERTIG ----","\n")
msg("OK!")

