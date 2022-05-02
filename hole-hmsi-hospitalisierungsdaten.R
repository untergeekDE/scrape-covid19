#--- hole-hmsi-hospitalisierungsdaten.R ----
# Sucht aus der Aktuelle-Daten-Seite des Sozialministeriums 
# die CSV mit den tagesaktuellen Hospitalisierungsdaten
# und bereitet sie auf (bzw. gibt Alarm, wenn was schief geht)
#
# Aktualisiert außerdem die Kurve der schweren Verläufe daraus
# und das Ungeimpften-Intensiv-Risiko
#
# Stand: 17.3.2022



# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- NULL

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")

# Die R-Scraping-Library
pacman::p_load(rvest)
# MS-Teams-Messaging-Library
pacman::p_load(teamr)

# Funktion, um Fehler und Warnungen zu werfen

teams_meldung <- function(...) {
  cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
  cc$title(paste0("hole-hmsi-hospitalisierungsdaten.R",with_tz(now(),"Europe/Berlin")))
  alert_str <- paste0(...)
  cc$text(alert_str)
  cc$print()
  cc$send()
} 

teams_error <- function(...) {
  alert_str <- paste0(...)
  teams_meldung(...)
  stop(alert_str)
} 

teams_warning <- function(...) {
  alert_str <- paste0(...)
  teams_meldung(...)
  warning(alert_str)
} 

#---- Hole die tagesaktuelle CSV beim Ministerium ab ----
# URL der Ministeriumsseite mit CSV und Corona-Daten
hmsi_url <- "https://soziales.hessen.de/Corona/Bulletin/Tagesaktuelle-Zahlen"

# Default-Datum, Zeitstempel. 
i_d <- today()-1 #### ersetzen durch max(Datum!)
tsi <- now()
# Noch keine Daten für heute? Warte, prüfe auf Abbruch; loope
while(i_d < today()) {
  # Die URL der CSV-Datei von der Seite holen
  csv_url <- read_html(hmsi_url) %>% 
    html_nodes(".d-inline-flex.link--download.link") %>% 
    html_attr('href') %>% 
    as_tibble(.) %>% 
    rename(url = 1) %>% 
    filter(str_detect(url,"csv")) %>% 
    pull(url)
  
  # ...und sie einlesen. Kann hier schon schief gehen, deshalb Check
  # mit try() und Fehler, falls nach 2h kein brauchbares Datum aus der 
  # CSV-Datei gelesen werden kann. 
  try(hmsi_daten <- read_delim(csv_url, 
                               delim = ";", 
                               escape_double = FALSE, 
                               col_types = cols(Inzidenz_Datum = col_date(format = "%d.%m.%Y"),
                                                Inzidenz_letzte_Woche_Datum = col_date(format = "%d.%m.%Y"),
                                                Bettenauslastung_Datum = col_date(format = "%d.%m.%Y")), 
                               locale = locale(date_names = "de", decimal_mark = ",", 
                                               grouping_mark = ".", encoding = "WINDOWS-1252"), 
                               trim_ws = TRUE) %>%  
        # falls mehr als eine Zeile, wirf alles außer der letzten weg
        tail(1) %>% 
    mutate(Zeitstempel = now()) %>% 
    # Zeitstempel in die erste Spalte
    relocate(Zeitstempel))
  
  if(exists("hmsi_daten")) {
    # Hart codierter Test: falsche Spaltenanzahl?
    if (ncol(hmsi_daten) != 34) { teams_error("Formatänderung")}
    # aktuelles Datum aus dem CSV lesen
    # Wegen Bezugs auf Spaltennamen mit Try
    try(i_d <- as_date(hmsi_daten$Inzidenz_Datum))
    if (!is.Date(i_d)) i_d <- today()-1
  }
  
  if (i_d < today())  {
    # Leider keine Daten von heute. 
    # Timeout? (nach 6 Stunden)
    if (now() > tsi+(8*3600)) {
      msg("KEINE NEUEN DATEN GEFUNDEN")
      # TEAMS-NACHRICHT ERGÄNZEN
      stop("Timeout")
    }
    # Warte fünf Minuten, und dann probier's nochmal. 
    Sys.sleep(300)
    msg("Warte auf Hospitalisierungsdaten...")
  }
  # Ende While-Schleife
}

# Kurz piepsen; auf dem Server geht das natürlich schief, deshalb try. 
try(beepr::beep(2),silent=TRUE)

write.xlsx(hmsi_daten,
           paste0("archiv/",
                  "hmsi-hosp-",
                  format.Date(i_d,"%Y-%m-%d"),
                  ".xlsx"),overwrite=T)

# Archivierte Daten aus dem Google Sheet holen
hosp_daten_df <- read_sheet(ss=aaa_id, sheet="Krankenhauszahlen") 
  

# hier kurz mit den Impfquoten das Intensiv-Risiko ausrechnen

impfquote <- hmsi_daten$Impfquote_alle
ungeimpft <- 100-impfquote
intensiv_ungeimpft <- hmsi_daten$ITS_Hospitalisierte_ungeimpft
intensiv_geimpft <- hmsi_daten$ITS_Hospitalisierte_geimpft
risiko <- round((intensiv_ungeimpft/ungeimpft)/(intensiv_geimpft/impfquote), digits=1) # auf eine Nachkommastelle gerundet

#---- Daten formatieren und prüfen ----

# Diese Spalten hätte ich gern. 
try(hmsi_daten <- hmsi_daten %>% 
  select(Zeitstempel = 1,
         Inzidenz_Datum = 3,
         Hospitalisierungsinzidenz_aktuell = 2,
         Hospitalisierungsinzidenz_letzte_Woche = 4,
         Bettenauslastung_Datum = 10,
         Intensivbettenauslastung_aktuell = 6,
         Intensivbettenauslastung_bestaetigt = 7,
         Intensivbettenauslastung_Verdacht = 8,
         Normalbettenauslastung_aktuell = 11,
         Normalbettenauslastung_bestaetigt = 12,
         Normalbettenauslastung_Verdacht = 13,
         ITS_Hospitalisierte_ungeimpft = 15,
         ITS_Hospitalisierte_unbekannt = 17,
         ITS_Hospitalisierte_geimpft = 16
  ))
####TODO: Fehlerroutine Spaltennamen, Spalten nach Nummer? 

if (SPALTEN_ERR <- (ncol(hmsi_daten) != 14)) {
  teams_warning(paste0("Spaltenanzahl: ",ncol(hmsi_daten)," statt 14"))
}


# Das Datenblatt aus dem Google-Sheet lesen und vereinheitlichen
# und evtl. Einträge von heute (i_d) wegfiltern 
hosp_daten_df <- read_sheet(ss=aaa_id, sheet="Krankenhauszahlen") %>% 
  rename(Zeitstempel = 1,
         Inzidenz_Datum = 2,
         Hospitalisierungsinzidenz_aktuell = 3,
         Hospitalisierungsinzidenz_letzte_Woche = 4,
         Bettenauslastung_Datum = 5,
         Intensivbettenauslastung_aktuell = 6,
         Intensivbettenauslastung_bestaetigt = 7,
         Intensivbettenauslastung_Verdacht = 8,
         Normalbettenauslastung_aktuell = 9,
         Normalbettenauslastung_bestaetigt = 10,
         Normalbettenauslastung_Verdacht = 11,
         ITS_Hospitalisierte_ungeimpft = 12,
         ITS_Hospitalisierte_unbekannt = 13,
         ITS_Hospitalisierte_geimpft = 14,
  ) %>% 
  mutate(Zeitstempel = as_datetime(Zeitstempel)) %>% 
  filter(Zeitstempel < i_d) %>% 
  bind_rows(hmsi_daten)
  
# In die Tabelle im Google Doc
write_sheet(hosp_daten_df,ss=aaa_id,sheet="Krankenhauszahlen")

# Plausibilitätscheck: Veränderung gegenüber letztem Eintrag um mehr als 50%?
n <-nrow(hosp_daten_df)
for (i in c(3,4,6,7,9:12,14)) {
  if (abs(1-hosp_daten_df[n,i]/hosp_daten_df[n-1,i])>=.80) {
    teams_error("Veränderung zum Vortag > 80% in Spalte",i)
  }
}



#---- Basisdaten anpassen----
# Intensiv-Patienten Hessen (Zeile 6)

range_write(aaa_id,as.data.frame(paste0("Intensiv-Patienten (",
                                        format.Date(hmsi_daten$Bettenauslastung_Datum,"%d.%m."),
                                        ")")),range="Basisdaten!A6",col_names=FALSE)

range_write(aaa_id,as.data.frame(format(hmsi_daten$Intensivbettenauslastung_aktuell,
                                        big.mark = ".",decimal.mark=",",nsmall=0)),
            range="Basisdaten!B6", col_names = FALSE, reformat=FALSE)

# H-Inzidenz (Zeile 7)
range_write(aaa_id,as.data.frame(paste0("H-Inzidenz (",
                                        format.Date(hmsi_daten$Inzidenz_Datum,"%d.%m."),
                                        ")")),
            range="Basisdaten!A7", col_names=F, reformat=F)

range_write(aaa_id,as.data.frame(paste0(
  format(hmsi_daten$Hospitalisierungsinzidenz_aktuell,
          big.mark = ".",decimal.mark=",",nsmall=2),
  " (Vorwoche ",
  format(hmsi_daten$Hospitalisierungsinzidenz_letzte_Woche,
         big.mark = ".",decimal.mark=",",nsmall=2),")")),
  
            range="Basisdaten!B7", col_names = FALSE, reformat=FALSE)

# Corona-Warnstufe (Zeile 8)
range_write(aaa_id,as.data.frame("Corona-Warnstufe Hessen"),
            range="Basisdaten!A8", col_names=F, reformat=F)


# CoSchuV § 29 i.d.F. vom 3.3.2022: 
if (hmsi_daten$Hospitalisierungsinzidenz_aktuell > 9 |
    hmsi_daten$Intensivbettenauslastung_aktuell > 400) {
  range_write(aaa_id,as.data.frame("<b style='color:#cc1a14'>ERREICHT</b>"),
              range="Basisdaten!B8", col_names=F, reformat=F)
  
} else {
  range_write(aaa_id,as.data.frame("<b style='color:#cc1a14'>--</b>"),
              range="Basisdaten!B8", col_names=F, reformat=F)
}

#---- Grafiken pushen, Intensivrisiko-String berechnen ----
# Leider nötig: Daten nicht nur ins Google Doc, sondern auch direkt über die API
# 
basisdaten_df <- range_read(ss=aaa_id,sheet="Basisdaten") %>% 
  select(Indikator = 1,Wert = 2) %>%
  mutate(Wert = as.character(Wert)) %>%
  mutate(Wert = str_replace(Wert,"NULL"," "))

dw_data_to_chart(basisdaten_df,chart_id = "OXn7r")
dw_publish_chart(chart_id = "OXn7r") # Basisdaten
dw_publish_chart(chart_id = "I1p2e") # Schwere Fälle

intensivrisiko_df <- tibble(Zahl=paste(format(risiko, big.mark = "."
                                              ,decimal.mark=",",nsmall=1),"x"),
                            Text="so viele Ungeimpfte wie Geimpfte")
  
# Diese Mini-Tabelle an Datawrapper übergeben...
dw_data_to_chart(intensivrisiko_df,chart_id = "1XuU9")
#... und den String mit den Daten generieren. 
dw_edit_chart(chart_id = "1XuU9",
              annotate = paste0("<strong>Stand ",
                format.Date(i_d,"%d.%m.%Y"),
                ":</strong> Grundimmunisierte (2x geimpft o. genesen und geimpft) ",
                "machen mindestens ",
                format(impfquote,big.mark = ".",decimal.mark=",",nsmall=1),
                "% der Bevölkerung aus und ",
                format(intensiv_geimpft,big.mark = ".",decimal.mark=",",nsmall=1),
                "% der Intensivpatienten. Ungeimpfte und Teilgeimpfte ",
                "machen höchstens ",
                format(100-impfquote,big.mark = ".",decimal.mark=",",nsmall=1),
                "% der Bevölkerung aus und ",
                format(intensiv_ungeimpft,big.mark = ".",decimal.mark=",",nsmall=1),
                "% der Intensivpatienten. Die ",
                format(100-intensiv_geimpft-intensiv_ungeimpft,big.mark = ".",decimal.mark=",",nsmall=1),
                "% der Corona-Intensivpatienten, deren Impfstatus unbekannt ist, ",
                "gehen nicht in die Berechnung ein."
                
                
              ))

dw_publish_chart(chart_id ="1XuU9") # Ungeimpfte Intensivrisiko



#---- Newswire-Meldung generieren ----
# Eine schnöde Textdatei, die der Newswire-Cron-Job sich abholt. 

sink(file = "daten/newswiremeldung.txt")
cat('Corona-Update: Klinikzahlen Hessen \n')
cat('Quelle: Hessisches Ministerium für Soziales und Integration \n\n')

cat('Update Leitindikatoren zur Bestimmung des Pandemiegeschehens \n')
cat('- letzte Aktualisierung: ',format.Date(hmsi_daten$Zeitstempel,"%d.%m.%Y %H:%M"),' Uhr \n\n')

cat('# Hospitalisierungsinzidenz \n')
cat('- aktuell ',format(hmsi_daten$Hospitalisierungsinzidenz_aktuell,
                        big.mark = ".",decimal.mark=",",nsmall=2),'\n')
cat('- letzte Woche ',format(hmsi_daten$Hospitalisierungsinzidenz_letzte_Woche,
                             big.mark = ".",decimal.mark=",",nsmall=2),'\n\n')

cat('# Intensivbettenauslastung \n')
cat('COVID-Fälle auf hessischen Intensivstationen nach der IVENA-Sonderlage, Stand: ',
    format.Date(hmsi_daten$Bettenauslastung_Datum,"%d.%m."), '\n')
cat('- belegte Betten ',format(hmsi_daten$Intensivbettenauslastung_aktuell,
                        big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- davon laborbestätigt ',format(hmsi_daten$Intensivbettenauslastung_bestaetigt,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- Verdachtsfälle ',format(hmsi_daten$Intensivbettenauslastung_Verdacht,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n\n')

cat('# Normalbettenauslastung \n')
cat('COVID-Fälle in hessischen Krankenhäusern auf Normalstationen  nach der IVENA-Sonderlage, Stand: ',format.Date(hmsi_daten$Bettenauslastung_Datum,"%d.%m."),'\n')
cat('- belegte Betten ',format(hmsi_daten$Normalbettenauslastung_aktuell,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- davon laborbestätigt ',format(hmsi_daten$Normalbettenauslastung_bestaetigt,
                                big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- Verdachtsfälle ',format(hmsi_daten$Normalbettenauslastung_Verdacht,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n\n')

cat('# Impfstatus der Covid-Intensivpatient:innen \n')
cat('- ungeimpft oder teilgeimpft',format(hmsi_daten$ITS_Hospitalisierte_ungeimpft,
                                          big.mark = ".",decimal.mark=",",nsmall=1),' % \n')
cat('- geimpft ',format(hmsi_daten$ITS_Hospitalisierte_geimpft,
                        big.mark = ".",decimal.mark=",",nsmall=1),' % \n\n')
cat('- Impfstatus unbekannt ',format(hmsi_daten$ITS_Hospitalisierte_unbekannt,
                                     big.mark = ".",decimal.mark=",",nsmall=1),' % \n')
cat('Das heißt bei den derzeitigen Impfquoten: Das Risiko, infolge einer Covid-Erkrankung intensivmedizinisch behandelt zu werden, ist für Ungeimpfte in Hessen ',
    format(risiko, big.mark = ".",decimal.mark=",",nsmall=1),
    'mal höher als für Geimpfte.')

cat('Skript: hole-hmsi-hospitalisierungsdaten.R auf 35.207.90.86 \n')
cat('Redaktionelle Fragen an jan.eggers@hr.de')
sink()

# fuehre Befehl aus um Datei an gwuenschten Ort zu kopieren
if (server) {
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/newswiremeldung.txt gs://d.data.gcp.cloud.hr.de/newswiremeldung.txt')
}
msg('Daten wurden fuer Newswire abgelegt!')

#---- Teams-Karte schicken ----
cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
cc$title(paste0("Corona-Update: Klinikzahlen Hessen - ",format.Date(i_d,"%d.%m.%y")))
cc$text("hole-hmsi-hospitalisierungsdaten.R")

sec1 <- card_section$new()

sec1$text(paste0("<h4>Hospitalisierungsinzidenz</h4>"))
sec1$add_fact("aktuell",format(hmsi_daten$Hospitalisierungsinzidenz_aktuell,
                                        big.mark = ".",decimal.mark=",",nsmall=2))
sec1$add_fact("letzte Woche",format(hmsi_daten$Hospitalisierungsinzidenz_letzte_Woche,
                                             big.mark = ".",decimal.mark=",",nsmall=2))
cc$add_section(new_section = sec1)

# Intensivbetten
sec2 <- card_section$new()
sec2$text(paste0("<h4>Intensivbetten</h4>"))
sec2$add_fact("COVID-Fälle auf hessischen Intensivstationen nach der IVENA-Sonderlage, Stand: ",
    format.Date(hmsi_daten$Bettenauslastung_Datum,"%d.%m."))
sec2$add_fact("Belegte Betten",hmsi_daten$Intensivbettenauslastung_aktuell)
sec2$add_fact("davon laborbestätigt",hmsi_daten$Intensivbettenauslastung_bestaetigt)
sec2$add_fact("Verdachtsfälle",hmsi_daten$Intensivbettenauslastung_Verdacht)
sec2$add_fact("Anteil Ungeimpfte/Teilgeimpfte",
              paste0(format(hmsi_daten$ITS_Hospitalisierte_ungeimpft,
                      big.mark = ".",decimal.mark=",",nsmall=1),"%"))
sec2$add_fact("Anteil Geimpfte",
              paste0(format(hmsi_daten$ITS_Hospitalisierte_geimpft,
                            big.mark = ".",decimal.mark=",",nsmall=1),"%"))
sec2$add_fact("Intensiv-Risiko Ungeimpfte",
              paste0(format(risiko,
                            big.mark = ".",decimal.mark=",",nsmall=1),"x höher"))

cc$add_section(new_section = sec2)

# Normalbetten
sec3 <- card_section$new()
sec3$text(paste0("<h4>Normalbetten</h4>"))
sec3$add_fact("COVID-Fälle in hessischen Normalbetten nach der IVENA-Sonderlage, Stand: ",
              format.Date(hmsi_daten$Bettenauslastung_Datum,"%d.%m."))
sec3$add_fact("Belegte Betten",hmsi_daten$Normalbettenauslastung_aktuell)
sec3$add_fact("davon laborbestätigt",hmsi_daten$Normalbettenauslastung_bestaetigt)
sec3$add_fact("Verdachtsfälle",hmsi_daten$Normalbettenauslastung_Verdacht)
cc$add_section(new_section = sec3)
# Karte vorbereiten und abschicken. 

cc$send()

