################ scrape-jhu.R
# Ja, dieses Skript liest *auch* Daten der Johns-Hopkins-Uni (JHU).
# Eigentlich ist es aber nur ein ausgelagerter Bestandteil des alten
# Ministeriums-Scrapers, umgeschrieben auf die Daten des RKI. 
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 19.5.2020

library(dplyr)
library(googlesheets4)
library(lubridate)
library(DatawRappr)
library(jsonlite)

# Init-Werte fürs Logging, das Arbeitsverzeichnis und die Keyfile-Auswahl
server <- FALSE

# ---- Logging und Update der Semaphore-Seite ----
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B11, Statuszeile in C11
  d <- data.frame(b = now(tzone= "CEST"), c = paste0(x,...))
  range_write(id_msg,d,sheet="Tabellenblatt1",
              range="B11:C11",col_names = FALSE,reformat=FALSE)
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

gs4_email <- "googlesheets4@scrapers-272317.iam.gserviceaccount.com"
gs4_keypath <- "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json"
  
if (server) {
  # Arbeitsverzeichnis, Logdatei beschreiben
  setwd("/home/jan_eggers_hr_de/rscripts/")
  # Authentifizierung Google-Docs umbiegen
  gs4_keypath <- "/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json"
} 


gs4_deauth() # Authentifizierung löschen
gs4_auth(email=gs4_email,path=gs4_keypath)

msg("\n\n--- START ",as.character(today())," ---\n")


############################# Daten ausgeben ############################
# Etwas verwinkelte Struktur: 
# - id_wachstum "Corona-Fallzahlen-Wachstumsraten daily" ist ein sehr komplexes
#   Dokument mit (leider) einer Menge Formeln für Steigerungsraten und Trendberechnung. 
#   Die Vergleichszahlen aus NRW (RKI) und ITA (Johns Hopkins) werden gegen Hessen 
#   verschoben; 1 ist der Tag, an dem Inzidenz == 1
#   - HE: 11.03. (Inzidenz war 0,8)
#   - NRW: 5.03.
#   - ITA: 27.02. 
#   *AUS id_wachstum ZIEHT SICH DW DIE LOGARITHMISCHE TREND-DARSTELLUNG*

msg("Lies die Daten für Hessen und NRW über JSON-Queries")

id_wachstum <- "1NXHj4JXfD_jaf-P3YjA7jv3BYuOwU9_WKoVcDknIG64"


#RKI-Abfragestring für die Länder konstruieren

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

zahl_hessen <- daten_liste$features$attributes$value[7]
zahl_nrw <- daten_liste$features$attributes$value[10]
faelle_gesamt <- zahl_hessen
heute <- ymd(today())

msg("Hessen: ",zahl_hessen," NRW: ", zahl_nrw)

# Aktuelle Fallzahl und Datum - Stichtag ist der 11.3. (ungefähr Inzidenz 1)
he_ofs <-   8+as.numeric(today() - as.Date("2020-03-11"))

# Aktuelle Fallzahl schreiben
range_write(id_wachstum,as.data.frame(faelle_gesamt),sheet="Tabellenblatt1",
            range=paste0("C",as.character(he_ofs)),col_names = FALSE,reformat=FALSE)
# Datum schreiben
range_write(id_wachstum,as.data.frame(heute),sheet="Tabellenblatt1",
            range=paste0("B",as.character(he_ofs)),col_names = FALSE,reformat=FALSE)
# Inzidenz als Formel schreiben
range_write(id_wachstum,
            as.data.frame(gs4_formula(paste0("=C",he_ofs,"/6265809*100000"))),
            sheet="Tabellenblatt1",range=paste0("D",as.character(he_ofs)),col_names=FALSE,reformat=FALSE)
# Prozentuales Wachstum 
range_write(id_wachstum,
            as.data.frame(gs4_formula(paste0("=C",he_ofs,"/C",he_ofs-1,"-1"))),
            sheet="Tabellenblatt1",range=paste0("E",as.character(he_ofs)),col_names=FALSE,reformat=FALSE)

# Trend-Formel anpassen =VARIATION(D24:D30;$A$24:$A$30;$A$8:$A$40)
range_write(id_wachstum, 
            as.data.frame(gs4_formula(paste0(
              "=VARIATION(D",he_ofs-6,
              ":D",he_ofs,
              ";$A$",he_ofs-6,
              ":$A$",he_ofs,
              ";$A$8:$A$",he_ofs+20,")")
              )),
            sheet="Tabellenblatt1",range="F8",col_names=FALSE,reformat=FALSE
            )

msg("Trendformel hessen aktualisiert","\n")

# Rausfinden, welche Tabellenzelle dran ist. 
# Die Basiszelle für NRW ist J8, das Referenzdatum (Inzidenz ca. 1) der 5.3.2020
nrw_ofs <-   8+as.numeric(today() - as.Date("2020-03-07"))

range_write(id_wachstum,as.data.frame(zahl_nrw),sheet="Tabellenblatt1",
    paste0("J",as.character(nrw_ofs)),col_names = FALSE,reformat=FALSE)
# Inzidenz als Formel schreiben
range_write(id_wachstum,
    as.data.frame(gs4_formula(paste0("=J",nrw_ofs,"/17932651*100000"))),
    sheet="Tabellenblatt1", paste0("K",as.character(nrw_ofs)),col_names=FALSE,reformat=FALSE)
#Prozentuales Wachstum 
range_write(id_wachstum,
            as.data.frame(gs4_formula(paste0("=J",nrw_ofs,"/J",nrw_ofs-1,"-1"))),
            sheet="Tabellenblatt1", paste0("L",as.character(nrw_ofs)),col_names=FALSE,reformat=FALSE)

# Trend-Formel NRW in M8 anpassen
range_write(id_wachstum, 
            as.data.frame(gs4_formula(paste0(
              "=GROWTH(K",nrw_ofs-6,
              ":K",nrw_ofs,
              ";$A$",nrw_ofs-6,
              ":$A$",nrw_ofs,
              ";$A$8:$A$",nrw_ofs+7,")")
            )),
            sheet="Tabellenblatt1",range="M8",col_names=FALSE,reformat=FALSE
)

msg("NRW-Daten und -Formeln angepasst","\n")


# ---- Johns-Hopkins-Zahlen von Gestern lesen ----
# Dummerweise die Dateinamen genau anders herum mm-tt-yyyy. Scheiß Amis. 

g_tag <- as.character(day(heute-1))
if (day(heute-1) < 10) g_tag <- paste0("0",g_tag)
g_monat <- as.character(month(heute-1))
if (month(heute-1) < 10) g_monat <- paste0("0",g_monat)
g_jahr <- as.character(year(heute-1))

msg("JHU-Daten: Daily Report ",g_monat,"-",g_tag,"-",g_jahr,".csv","\n")

jhu_url = paste0("https://raw.githubusercontent.com/CSSEGISandData",
        "/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/",
  g_monat,"-",g_tag,"-",g_jahr,".csv")


# Italienische Daten seit 5.5. aufgegliedert nach Regionen - Summenbildung nötig
jhu_df <- read.csv(jhu_url) %>%
  filter(Country_Region == "Italy")
zahl_ita <- sum(jhu_df$Confirmed)
# Zahlen für Italien eintragen
# Formeln für Italien eintragen
# Anpassung Trendlinien-Formel in Q8 (für ITA)

ita_ofs <-   7+as.numeric(heute - as.Date("2020-02-27"))

range_write(id_wachstum,as.data.frame(sum(zahl_ita)),sheet="Tabellenblatt1",
            paste0("N",as.character(ita_ofs)),col_names = FALSE,reformat=FALSE)
# Inzidenz als Formel schreiben
range_write(id_wachstum,
            as.data.frame(gs4_formula(paste0("=N",ita_ofs,"/60262701*100000"))),
            sheet="Tabellenblatt1", paste0("O",as.character(ita_ofs)),col_names=FALSE,reformat=FALSE)
#Prozentuales Wachstum 
range_write(id_wachstum,
            as.data.frame(gs4_formula(paste0("=N",ita_ofs,"/N",ita_ofs-1,"-1"))),
            sheet="Tabellenblatt1", paste0("P",as.character(nrw_ofs)),col_names=FALSE,reformat=FALSE)

# Trend-Formel ITA in Q8 anpassen
range_write(id_wachstum, 
            as.data.frame(gs4_formula(paste0(
              "=GROWTH(O",ita_ofs-6,
              ":O",ita_ofs,
              ";$A$",ita_ofs-6,
              ":$A$",ita_ofs,
              ";$A$8:$A$",ita_ofs,")")
            )),
            sheet="Tabellenblatt1",range="Q8",col_names=FALSE,reformat=FALSE
)


##### Datawrapper-Grafiken pingen und so aktualisieren #####
msg(as.character(now()),"Datawrapper-Grafik pingen...","\n")

# - Neu publizieren, damit der DW-Server einmal die Google-Sheet-Daten zieht.

dw_publish_chart(chart_id = "D2CJm") # Trendlinien-Grafik

msg("OK!")

