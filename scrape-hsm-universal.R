################ scrape-hsm-universal.R
# Ministeriums-Scraper
#
# Greift die Corona-Tagesinfos vom Server soziales.hessen.de ab
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 26.4.2020 vormittags

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
  sheets_edit(id_msg,as.data.frame(now(tzone = "CEST")),sheet="Tabellenblatt1",
              range="B6",col_names = FALSE,reformat=FALSE)
  sheets_edit(id_msg,as.data.frame(paste0(x,...)),sheet="Tabellenblatt1",
              range="C6",col_names = FALSE,reformat=FALSE)
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

msg(as.character(now()),"\n\n---------------- START ",as.character(today()),"------------------\n")

# Vorbereitung: Index-Datei einlesen; enthält Kreise/AGS und Bevölkerungszahlen
msg(as.character(now()),"Lies index/kreise-index-pop.xlsx","\n")
kreise <- read.xlsx("index/kreise-index-pop.xlsx")

# RKI-Daten lesen und auf Hessen filtern
msg(as.character(now()),"Lies RKI-Daten (>100k Fälle, das dauert)","\n")
rki_url <- "https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv"
rki_df <- read.csv(url(rki_url)) %>% 
  filter(Bundesland == "Hessen")


# ---- Google-Tabellen fallzahl, wachstum, cck einlesen ----
msg(as.character(now()),"Lies die Google-Tabellen...","\n")


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
msg(as.character(now()),"URL der Ministeriums-Seite abfragen...","\n")

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
    http_408 = function (e) {408} # Timeout
  ) %in% c(403,404,405,408)) {
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
      ts <- str_extract(html_text(x),"[0-9]+\\.\\s[JFMAMJJASOND][a-zä]*\\s2020\\,\\s[0-9]+\\:[0-9][0-9]\\sUhr")
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
    msg(as.character(now()),"Warte 60 Sekunden auf Refresh...\n")
    Sys.sleep(60)
  }
}

if (!server) {
  library(beepr)
  beep(2)
}
msg(as.character(now()),"GELESEN: --- Seite vom",tag,monat,jahr,"---\n")

# Den PDF-Link holen und sichern. 
#pdf_link <- str_extract(str_detect(html_nodes(webpage,"a"),"pdf"),'https://.+\\.pdf')
#pdf <- GET(pdf_link)

# ---- Tabellendaten aus der Webseite extrahieren und aufarbeiten ----
# Die erste Tabelle auf der Seite auslesen
tables <- html_node(webpage,"table")

# #Kopfzeile weg, NA durch 0 ersetzen
# table_df <- table_df[2:nrow(table_df),]


# viel simpler: html_table nutzen
table_df <- html_table(tables)
colnames(table_df) <- c("kreis","gesamt","hospitalisiert","tote","inz","neu7tage","inz7t")
table_df <- table_df[3:nrow(table_df),] 


# Diese Tabellenspalte hat noch viele Leerstellen, die bei is.numeric() 
# in NA-Werte umgewandelt werden. Die machen nur Ärger!
table_df$tote <- as.numeric(table_df$tote)
table_df$tote[is.na(table_df$tote)] <- 0

#Plausibilitäts-Prüfung!

if (!server) View(table_df)
msg(as.character(now()),"Gelesen:",nrow(table_df),"Zeilen",ncol(table_df),"Spalten")
if (nrow(table_df)>27) simpleError("Zu viele Einträge?")
if (ncol(table_df)!=7 | ncol(table_df)<2) simpleError("Formatänderung!")

#Mehr Tests!!!!!!!!!!!!!!!!!!!!!!!!!!
if (!is.numeric(table_df$gesamt)){
  table_df$gesamt <- as.numeric(table_df$gesamt)
  simpleWarning("Tabellenspalte Gesamt nicht numerisch")
  table_df$gesamt[is.na(table_df$gesamt)] <- 0 # Gesamt-NA-Werte auf 0
}


# Die komischen Namen des HMSI standardisieren
table_df$kreis[table_df$kreis == "SK Darmstadt"] <- "Darmstadt (Stadt)"
table_df$kreis[table_df$kreis == "SK Wiesbaden"] <- "Wiesbaden (Stadt)"
table_df$kreis[table_df$kreis == "LK Offenbach"] <- "Offenbach (Landkreis)"
table_df$kreis[table_df$kreis == "LK Kassel"] <- "Kassel (Landkreis)"

table_df$kreis <- str_replace(table_df$kreis,"SK Offenbach","Offenbach (Stadt)")
table_df$kreis <- str_replace(table_df$kreis,"SK Kassel","Kassel (Stadt)")
table_df$kreis <- str_remove(table_df$kreis,"LK ")
table_df$kreis <- str_remove(table_df$kreis,"SK ")

# Spalten in Zahlenwerte umwandeln
for (i in 2:7) {
  table_df[,i] <- as.numeric(table_df[,i])
}

summary(table_df)
# Wenn der Wert kleiner oder gleich dem letzten: Fehlermeldung!
if (table_df$gesamt[27] <= fallzahl_df$faelle[nrow(fallzahl_df)] ) {
  simpleWarning("Keine Erhöhung der Fallzahl gegen letzten Scan\n")
  simpleWarning(paste0("Gelesen: ",as.character(table_df$gesamt[27]),"\n"))
  simpleWarning(paste0("Letzte Tabellenzeile: ",as.character(fallzahl_df[nrow(fallzahl_df),]),"\n"))
  no_increment <- TRUE
} else { no_increment <- FALSE }

# Von den Daten aus dem Google-Doc "Covid Choropleth Karte" nur die Notizen und Todesfälle behalten
notizen_df <- cck_df %>% select(kreis,notizen,tote_hsde = tote)

msg(as.character(now()),"Tabelle all_df zusammenführen...","\n")

# ---- RKI-Daten Genesene nach Kreis ----
genesen_df <- rki_df %>% 
  select(IdLandkreis,AnzahlGenesen) %>%
  group_by(IdLandkreis) %>%
  summarize(AnzahlGenesen = sum(AnzahlGenesen)) %>%
  mutate(IdLandkreis = paste0("0",as.character(IdLandkreis)))


# Daten zusammenführen: AGS, Bevölkerungszahl  
all_df <- table_df %>%
  select(k = 1,2,4,6,7,3) %>%
  # Fuzzyjoin - um Bindestriche und ähnlichen Kram zu ignorieren
  stringdist_right_join(kreise, by = c("k" = "kreis"), max_dist=1) %>%
  select(kreis,2,3,4,5,6,8:ncol(.)) %>%
  # Nur die Kreise!
  mutate(inzidenz = as.numeric(gesamt)/pop*100000) %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000",""))) %>%
  mutate(kidx = ts) %>% # Stand auf Zeitstempel des hmsi-Dokuments setzen
  right_join(notizen_df, by = c("kreis" = "kreis")) %>%
  # Anzahle der Toten auf hsde-Informationsstand
  mutate(tote = ifelse(tote < tote_hsde,tote_hsde,tote)) %>% 
  # Anzahl der Genesenen nach Kreis
  left_join(genesen_df, by = c("AGS" = "IdLandkreis"))

if (nrow(table_df)<25) simpleError("Zu wenig Einträge!")
if (nrow(stringdist_anti_join(kreise,table_df,by =c("kreis" = "kreis"),max_dist=1)) > 1) {
  print(stringdist_anti_join(kreise,table_df,by = c("kreis" = "kreis")))
  simpleError("Nicht gematchter Eintrag\n")
}

#zur Sicherheit - alter Code, sollte nicht mehr notwendig sein
all_df$gesamt <- as.numeric(all_df$gesamt)
all_df$hospitalisiert <- as.numeric(all_df$hospitalisiert)


# Tabellenwerte in Zahlen umwandeln, NA durch 0 ersetzen - nicht mehr nötig
# table_df$gestern <- as.numeric(ifelse(is.na(as.numeric(table_df$gestern)),"0",table_df$gestern))
# all_df$neu <- as.numeric(ifelse(is.na(as.numeric(table_df$neu)),"0",table_df$neu))
# table_df$gesamt <- as.numeric(ifelse(is.na(as.numeric(table_df$gesamt)),"0",table_df$gesamt))

# Tabelle umsortieren und ausdünnen und umbenennen 
final_df <- all_df %>%
  select(AGS,kreis,gesamt,kidx,pop,inzidenz,tote,neu7tage,inz7t,AnzahlGenesen,hospitalisiert,notizen) %>%
  rename(stand = kidx, ags_text = AGS) %>%
  mutate(TotProz = round(tote/gesamt*100),
         GenesenProz = round(AnzahlGenesen/gesamt*100))

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

msg(as.character(now()),"Starte Datenausgabe","\n")

# Authentifizierung aktualisieren - wenn es zu lange gedauert hat, ist sie verfallen
sheets_deauth() # Authentifizierung löschen
sheets_auth(email=sheets_email,path=sheets_keypath)

# ---- Schreibe Archivkopie, aktualisiere Google-Doc KARTE -----

# Tabelle für den heutigen Tag mit Zeitstempel abspeichern
write.xlsx(final_df,file=paste0("archiv/",ts_clean,".xlsx"),overwrite = TRUE)
msg(as.character(now()),"Archivkopie nach archiv/ geschrieben","\n")

# Tabelle in das GoogleDoc "Covid Choropleth Kreise" schreiben
sheets_write(final_df,ss = id_cck, sheet = "daten")


b# ---- Aktualisiere das ARD-Sheet aktuelle Daten ----
ard_df <- final_df %>%
  mutate(Genesene = AnzahlGenesen, Quelle = url) %>%
  select(1,2,3,11,7,12,4) %>%
  rename(AGS = ags_text,Kreisname = kreis,Fallzahlen=gesamt,Tote=tote,Zeitstempel= stand)
  
msg(as.character(now()),"ARD-Seite Hessen (Google) aktualisieren","\n")

ard_id = "1OKodgGnSTFRrF51cIrsL7qz0xwAdR8DXovGzD01dqEM"
sheets_write(ard_df,ss = ard_id,sheet = "06_hessen")
# ---- Aktualisiere Google-Doc id_fallzahl ----

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
hospitalisiert_gesamt <- sum(all_df$hospitalisiert)

# Zahl der Genesenen für gestern nachtragen
fallzahl_df$gsum[fallzahl_df$datum == heute-1] <- genesen_gesamt

# Falls keine Erhöhung, letzte Zeile löschen (und so überschreiben)
if (!no_increment) {
  fallzahl_df <- fallzahl_df %>%
    rbind(data.frame(datum = this_date,
                     faelle = faelle_gesamt,
                     steigerung = 0,
                     tote = tote_gesamt,
                     tote_steigerung = 0,
                     gsum = NA,
                     hospitalisiert = hospitalisiert_gesamt,
                     hosp_steigerung = 0))
} else {
  fallzahl_df$faelle[fallzahl_df$datum == this_date] <- faelle_gesamt
  fallzahl_df$tote[fallzahl_df$datum == this_date] <- tote_gesamt
  fallzahl_df$hospitalisiert[fallzahl_df$datum == this_date] <- hospitalisiert_gesamt
}

fallzahl_df <- fallzahl_df  %>%
  # Steigerungsrate
  mutate(steigerung = (faelle/lag(faelle)-1)) %>%
  # Steigerungsrate Todesfälle
  mutate(tote_steigerung = (tote/lag(tote)-1)) %>%
  mutate(tote_steigerung = if_else(is.na(tote_steigerung) | is.infinite(tote_steigerung),0,tote_steigerung)) %>%
  mutate(steigerung = if_else(is.na(steigerung) | is.infinite(steigerung),0,steigerung)) %>%
  # Steigerungsrate Hospitalisierung
  mutate(hosp_steigerung = (hospitalisiert/lag(hospitalisiert)-1))

 


msg(as.character(now()),"Fallzahlen Hessen (Google), daten/livedaten aktualisieren","\n")

#sheets_edit statt sheets_write, um den reformat-Parameter zu haben
sheets_edit(fallzahl_df,ss = id_fallzahl, sheet = "daten",reformat=FALSE)
fall4w_df <- fallzahl_df[(nrow(fallzahl_df)-27):nrow(fallzahl_df),] %>%
  select(1,2,3,4,5,6) # Hospitalisiert ausblenden
sheets_edit(fall4w_df,ss = id_fallzahl, sheet = "livedaten",reformat=FALSE)




# ---- Schreibe aktualisierte Werte in Google-Doc id_wachstum ----  

msg(as.character(now()),"Wachstumsberechnung (Google) aktualisieren","\n")

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

msg(as.character(now()),"Trendformel hessen aktualisiert","\n")

#RKI-Abfragestring konstruieren

rki_rest_query2 <- paste0("https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/",
  "Coronaf%C3%A4lle_in_den_Bundesl%C3%A4ndern/FeatureServer/0/",
  "query?f=json&where=1%3D1",
  "&returnGeometry=false&spatialRel=esriSpatialRelIntersects",
  "&outFields=*&groupByFieldsForStatistics=LAN_ew_GEN&orderByFields=LAN_ew_GEN%20asc",
  "&outStatistics=%5B%7B%22statisticType%22%3A%22max%22%2C%22onStatisticField%22%3A%22Fallzahl%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D",
  "&outSR=102100&cacheHint=true")
  
msg(as.character(now()),"RKI-JSON-Query:",rki_rest_query2,"\n")

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

msg(as.character(now()),"NRW-Daten und -Formeln angepasst","\n")


# Johns-Hopkins-Zahlen von Gestern lesen
# Dummerweise die Dateinamen genau anders herum mm-tt-yyyy. Scheiß Amis. 

g_tag <- as.character(day(heute-1))
if (day(heute-1) < 10) g_tag <- paste0("0",g_tag)
g_monat <- as.character(month(heute-1))
if (month(heute-1) < 10) g_monat <- paste0("0",g_monat)
g_jahr <- as.character(year(heute-1))

msg(as.character(now()),"JHU-Daten von",g_monat,"-",g_tag,"-",g_jahr,".csv","\n")

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


msg(as.character(now()),"Johns-Hopkins-Zahlen Italien gelesen und geschrieben","\n")

# ---- Master-Tabelle mit allen Meldungen nach Tagen aktualisieren ----
msg(as.character(now()),"Master-Tabelle schreiben...","\n")

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
msg(as.character(now()),"Basisdaten-Seite (Google) schreiben...","\n")

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
  as.character(genesen_gesamt), " (",
  as.character(round(genesen_gesamt / faelle_gesamt * 100))," %)")),
  range="livedaten!B7", col_names = FALSE, reformat=FALSE)

# Aktive Fälle (= Gesamt-Tote-Genesene), nur in Prozent (Zeile 8)


sheets_edit(id_basisdaten, as.data.frame(paste0(
  as.character(round((faelle_gesamt-genesen_gesamt-tote_gesamt) / faelle_gesamt * 100))," %")),
  range="livedaten!B8", col_names = FALSE, reformat=FALSE)

# Wachstumsrate (Zeile 10)
# Verdoppelungszeit (Zeile 11)
# Durchschnitt der letzten vier Steigerungsraten (in fall4w_df sind die letzten 4 Wochen)
steigerung_prozent <- round(mean(fall4w_df$steigerung[22:28]) * 100,1)
v_zeit <- round(log(2)/log(1+mean(fall4w_df$steigerung[22:28])),1)

sheets_edit(id_basisdaten,as.data.frame(str_replace(paste0(steigerung_prozent," %"),"\\.",",")),
            range="livedaten!B10", col_names = FALSE, reformat=FALSE)

sheets_edit(id_basisdaten,as.data.frame(str_replace(paste0(v_zeit," Tage"),"\\.",",")),
            range="livedaten!B11", col_names = FALSE, reformat=FALSE)


##### Datawrapper-Grafiken pingen und so aktualisieren #####
msg(as.character(now()),"Datawrapper-Grafiken pingen...","\n")

dw_cck_id = "YBBaK"       # Choropleth-Karte
dw_tabelle_id = "KP1H3"   # Tabelle mit den aktuellen Werten Steigerung Verdoppelungszeit
dw_trends_id = "D2CJm"     # Logarithmische Trendlinienkarte
dw_fallzahl_id = "0CS3h"  # Barchart Tote, Fälle, Steigerungsraten
dw_basisdaten_id= "7HWCI" # Basisdaten
dw_waswenn_id = "Kedm4" # Was-wäre-wenn
dw_dynamik_id ="B68Gx"  # 7-Tage-Dynamik nach Kreisen

# Alle einmal ansprechen, damit sie die neuen Daten ziehen
# - Neu publizieren, damit der DW-Server einmal die Google-Sheet-Daten zieht.

dw_publish_chart(chart_id = dw_cck_id)
dw_publish_chart(chart_id = dw_fallzahl_id)
dw_publish_chart(chart_id = dw_tabelle_id)
dw_publish_chart(chart_id = dw_trends_id)
dw_publish_chart(chart_id = dw_basisdaten_id)
dw_publish_chart(chart_id = dw_waswenn_id)
dw_publish_chart(chart_id = dw_dynamik_id)

#
msg(as.character(now()),"---- FERTIG ----","\n")
msg("OK!")

