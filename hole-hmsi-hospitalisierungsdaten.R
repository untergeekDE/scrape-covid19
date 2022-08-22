#--- hole-hmsi-hospitalisierungsdaten.R ----
# Sucht aus der Aktuelle-Daten-Seite des Sozialministeriums 
# die CSV mit den tagesaktuellen Hospitalisierungsdaten
# und bereitet sie auf (bzw. gibt Alarm, wenn was schief geht)
#
# Aktualisiert außerdem die Kurve der schweren Verläufe daraus
# und das Ungeimpften-Intensiv-Risiko
#
# Stand: 19.8.2022



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

#---- Funktion liest die Google-Tabelle ----
get_hosp_daten_df <- function() {
  t <- read_sheet(ss=aaa_id, sheet="Krankenhauszahlen") %>% 
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
         ITS_Hospitalisierte_geimpft = 14,) %>%
  mutate(Zeitstempel = as_datetime(Zeitstempel))
  return(t)
}


#---- Funktionen zur Auswertung der Website - falls keine CSV ----


hmsi_url <- "https://soziales.hessen.de/Corona/Bulletin/Tagesaktuelle-Zahlen"


scrape_ivena_texte <- function(url=hmsi_url) {
  ivena_texte <- read_html(url(hmsi_url)) %>% 
    html_nodes('p') %>% 
    html_text() 
  # Bisschen fieses Base R gibt String-Vektor zurück. 
 return(ivena_texte[str_detect(ivena_texte,"IVENA")])
}

scrape_h_index_text <- function(url=hmsi_url) {
  texte <- read_html(url(hmsi_url)) %>% 
    html_nodes('p') %>% 
    html_text() 
  # Bisschen fieses Base R gibt String-Vektor zurück. 
  return(texte[str_detect(texte,"Hospitalisierung")])
}

scrape_ivena <- function() {
  ivena_texte <- scrape_ivena_texte()
  h_index_text <- scrape_h_index_text()
  # Extrahiere Text ab "am " bis zum nächsten Whitespace und verwandle in Datum
  ivena_neu <- str_extract(ivena_texte[1],"(?<=am )[0-9\\.]+") %>%  
    as_date(.,format = "%d.%m.")
  # Kleiner Sicherheitscheck: 
  if (!str_detect(ivena_texte[1],"Intensivstationen")) {
    teams_warning("Intensivstationen nicht im ersten Absatz")
    return(F)
  }
  if (!str_detect(ivena_texte[2],"Normalstationen")) {
    teams_warning("Normalstationen nicht im ersten Absatz")
    return(F)
  }
  # Zahlen aus den Strings ziehen.
  i_alle <- str_extract(ivena_texte[1],"(?<=Uhr )[0-9\\.]+(?= Betten)") %>%
    # Punkte raus
    str_replace(.,"\\.","") %>% 
    as.numeric()
  i_bestätigt <- str_extract(ivena_texte[1],"(?<=Bei )[0-9\\.]+(?=.+bestätigt)") %>%
    # Punkte raus
    str_replace(.,"\\.","") %>% 
    as.numeric() 
  i_verdacht <- str_extract(ivena_texte[1],"(?<=bei )[0-9\\.]+(?=.+Verdacht)") %>%
    # Punkte raus
    str_replace(.,"\\.","") %>% 
    as.numeric()
  h_alle <- str_extract(ivena_texte[2],"(?<=Uhr )[0-9\\.]+(?= Betten)") %>%
    # Punkte raus
    str_replace(.,"\\.","") %>% 
    as.numeric()
  h_bestätigt <- str_extract(ivena_texte[2],"(?<=Bei )[0-9\\.]+(?=.+bestätigt)") %>%
    # Punkte raus
    str_replace(.,"\\.","") %>% 
    as.numeric() 
  h_verdacht <- str_extract(ivena_texte[2],"(?<=bei )[0-9\\.]+(?=.+Verdacht)") %>%
    # Punkte raus
    str_replace(.,"\\.","") %>% 
    as.numeric()
  if (i_alle != i_bestätigt + i_verdacht) {
    teams_warning("Summe für Intensivbetten stimmt nicht")
    return(FALSE)
  }
  if (h_alle != h_bestätigt + h_verdacht) {
    teams_warning("Summe für Normalbetten stimmt nicht")
    return(FALSE)
  }
  # Hole die Hospitalisierungsinzidenz diese und letzte Woche und Datum
  h_i_aktuell <- str_extract(h_index_text[1],"(?<=aktuell bei )[0-9\\,]+(?= pro)") %>%
    # Dezimalpunkt statt Komma
    str_replace("\\,",".") %>% 
    as.numeric()
  h_i_vorwoche <- str_extract(h_index_text[1],"(?<=betrug der Wert )[0-9\\,]+(?= pro)") %>%
    # Dezimalpunkt statt Komma
    str_replace("\\,",".") %>% 
    as.numeric()
  h_i_datum <- str_extract(h_index_text[1],"[0-9]+\\.[0-9]+\\.[0-9][0-9]+") %>% 
    as_date(format = "%d.%m.%Y")
  return(tibble(Zeitstempel = now(),
                Bettenauslastung_Datum = ivena_neu,
                Intensivbettenauslastung_aktuell = i_alle,
                Intensivbettenauslastung_bestaetigt = i_bestätigt,
                Intensivbettenauslastung_Verdacht = i_verdacht,
                Normalbettenauslastung_aktuell = h_alle,
                Normalbettenauslastung_bestaetigt = h_bestätigt,
                Normalbettenauslastung_Verdacht = h_verdacht,
                Hospitalisierungsinzidenz_aktuell = h_i_aktuell,
                Hospitalisierungsinzidenz_letzte_Woche = h_i_vorwoche,
                Inzidenz_Datum = h_i_datum))
}

# Hilfsfunktion: extrahiert die Textblöcke aus der Webseite, 
# schaut nach aktuellen Werten für die Bettenbelegung und
# schreibt sie wieder in die Tabelle. 
# Wenn keine aktuellen Werte gefunden: FALSE zurückgeben


#---- Main ----

# Lies die Google-Tabelle
hosp_daten_df <- get_hosp_daten_df()
# Letztes in der Tabelle vermerktes Datum
ivena_alt <- hosp_daten_df %>% pull(Bettenauslastung_Datum) %>% last()
# Solange 
ivena_neu <- ivena_alt-1
while(ivena_neu < ivena_alt) {
  if((scrape_df <- scrape_ivena())[1] == FALSE) stop()
  ivena_neu <- scrape_df$Bettenauslastung_Datum
  
}
# Falls das Scraping keine sinnvollen Daten ergibt: brich ab.

# Existiert das heutige Datum schon? Dann schreibe in das Dataframe,
# überschreibe die letzte Zeile (bzw. lösche die letzte)
if (ivena_neu == ivena_alt) {
  hosp_daten_df <- hosp_daten_df %>%  slice_head(n = nrow(.)-1)
}
# Neue Zeile anfügen und zurückschreiben
hosp_daten_df <- bind_rows(hosp_daten_df,scrape_df)
write_sheet(hosp_daten_df,ss=aaa_id,sheet="Krankenhauszahlen")

#---- Plausibilitätsprüfung ----

# Plausibilitätscheck: Veränderung gegenüber letztem Eintrag um mehr als 80%?

n <-nrow(hosp_daten_df)
for (i in c(1:ncol(hosp_daten_df))) {
  # für alle numerischen Spalten
  if (is.numeric(hosp_daten_df[n-1,i])){
    if (abs(1 - as.numeric(hosp_daten_df[n,i])
            / as.numeric(hosp_daten_df[n-1,i])) >= .80) {
      teams_error("Veränderung zum Vortag > 80% in Spalte",i)
    }
  }
}

dw_publish_chart(chart_id = "I1p2e") # Schwere Fälle
  
#---- Basisdaten anpassen----
# Intensiv-Patienten Hessen (Zeile 6)

range_write(aaa_id,as.data.frame(paste0("Intensiv-Patienten (",
                                        format.Date(scrape_df$Bettenauslastung_Datum,"%d.%m."),
                                        ")")),range="Basisdaten!A6",col_names=FALSE)

range_write(aaa_id,as.data.frame(format(scrape_df$Intensivbettenauslastung_aktuell,
                                        big.mark = ".",decimal.mark=",",nsmall=0)),
            range="Basisdaten!B6", col_names = FALSE, reformat=FALSE)

# H-Inzidenz (Zeile 7)
range_write(aaa_id,as.data.frame(paste0("H-Inzidenz (",
                                        format.Date(scrape_df$Inzidenz_Datum,"%d.%m."),
                                        ")")),
            range="Basisdaten!A7", col_names=F, reformat=F)

range_write(aaa_id,as.data.frame(paste0(
  format(scrape_df$Hospitalisierungsinzidenz_aktuell,
         big.mark = ".",decimal.mark=",",nsmall=2),
  " (Vorwoche ",
  format(scrape_df$Hospitalisierungsinzidenz_letzte_Woche,
         big.mark = ".",decimal.mark=",",nsmall=2),")")),
  
  range="Basisdaten!B7", col_names = FALSE, reformat=FALSE)

# Corona-Warnstufe (Zeile 8)
range_write(aaa_id,as.data.frame("Corona-Warnstufe Hessen"),
            range="Basisdaten!A8", col_names=F, reformat=F)


# CoSchuV § 29 i.d.F. vom 3.3.2022: 
if (scrape_df$Hospitalisierungsinzidenz_aktuell > 9 |
    scrape_df$Intensivbettenauslastung_aktuell > 400) {
  range_write(aaa_id,as.data.frame("<b style='color:#cc1a14'>ERREICHT</b>"),
              range="Basisdaten!B8", col_names=F, reformat=F)
  
} else {
  range_write(aaa_id,as.data.frame("<b style='color:#cc1a14'>--</b>"),
              range="Basisdaten!B8", col_names=F, reformat=F)
}


basisdaten_df <- range_read(ss=aaa_id,sheet="Basisdaten") %>% 
  select(Indikator = 1,Wert = 2) %>%
  mutate(Wert = as.character(Wert)) %>%
  mutate(Wert = str_replace(Wert,"NULL"," "))

dw_data_to_chart(basisdaten_df,chart_id = "OXn7r")
dw_publish_chart(chart_id = "OXn7r") # Basisdaten

#---- Newswire-Meldung generieren ----
# Eine schnöde Textdatei, die der Newswire-Cron-Job sich abholt. 

sink(file = "daten/newswiremeldung.txt")
cat('Corona-Update: Klinikzahlen Hessen \n')
cat('Quelle: Hessisches Ministerium für Soziales und Integration \n\n')

cat('Update Leitindikatoren zur Bestimmung des Pandemiegeschehens \n')
cat('- letzte Aktualisierung: ',format.Date(scrape_df$Zeitstempel,"%d.%m.%Y %H:%M"),' Uhr \n\n')

cat('# Hospitalisierungsinzidenz \n')
cat('- aktuell ',format(scrape_df$Hospitalisierungsinzidenz_aktuell,
                        big.mark = ".",decimal.mark=",",nsmall=2),'\n')
cat('- letzte Woche ',format(scrape_df$Hospitalisierungsinzidenz_letzte_Woche,
                             big.mark = ".",decimal.mark=",",nsmall=2),'\n\n')

cat('# Intensivbettenauslastung \n')
cat('COVID-Fälle auf hessischen Intensivstationen nach der IVENA-Sonderlage, Stand: ',
    format.Date(scrape_df$Bettenauslastung_Datum,"%d.%m."), '\n')
cat('- belegte Betten ',format(scrape_df$Intensivbettenauslastung_aktuell,
                        big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- davon laborbestätigt ',format(scrape_df$Intensivbettenauslastung_bestaetigt,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- Verdachtsfälle ',format(scrape_df$Intensivbettenauslastung_Verdacht,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n\n')

cat('# Normalbettenauslastung \n')
cat('COVID-Fälle in hessischen Krankenhäusern auf Normalstationen  nach der IVENA-Sonderlage, Stand: ',format.Date(scrape_df$Bettenauslastung_Datum,"%d.%m."),'\n')
cat('- belegte Betten ',format(scrape_df$Normalbettenauslastung_aktuell,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- davon laborbestätigt ',format(scrape_df$Normalbettenauslastung_bestaetigt,
                                big.mark = ".",decimal.mark=",",nsmall=0),'\n')
cat('- Verdachtsfälle ',format(scrape_df$Normalbettenauslastung_Verdacht,
                               big.mark = ".",decimal.mark=",",nsmall=0),'\n\n')

# cat('# Impfstatus der Covid-Intensivpatient:innen \n')
# cat('- ungeimpft oder teilgeimpft',format(scrape_df$ITS_Hospitalisierte_ungeimpft,
#                                           big.mark = ".",decimal.mark=",",nsmall=1),' % \n')
# cat('- geimpft ',format(scrape_df$ITS_Hospitalisierte_geimpft,
#                         big.mark = ".",decimal.mark=",",nsmall=1),' % \n\n')
# cat('- Impfstatus unbekannt ',format(scrape_df$ITS_Hospitalisierte_unbekannt,
#                                      big.mark = ".",decimal.mark=",",nsmall=1),' % \n')
# # cat('Das heißt bei den derzeitigen Impfquoten: Das Risiko, infolge einer Covid-Erkrankung intensivmedizinisch behandelt zu werden, ist für Ungeimpfte in Hessen ',
# #     format(risiko, big.mark = ".",decimal.mark=",",nsmall=1),
# #     'mal höher als für Geimpfte.')

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
cc$title(paste0("Corona-Update: Klinikzahlen Hessen - ",format.Date(ivena_neu,"%d.%m.%y")))
cc$text("hole-hmsi-hospitalisierungsdaten.R")

sec1 <- card_section$new()

sec1$text(paste0("<h4>Hospitalisierungsinzidenz</h4>"))
sec1$add_fact("aktuell",format(scrape_df$Hospitalisierungsinzidenz_aktuell,
                                        big.mark = ".",decimal.mark=",",nsmall=2))
sec1$add_fact("letzte Woche",format(scrape_df$Hospitalisierungsinzidenz_letzte_Woche,
                                             big.mark = ".",decimal.mark=",",nsmall=2))
cc$add_section(new_section = sec1)

# Intensivbetten
sec2 <- card_section$new()
sec2$text(paste0("<h4>Intensivbetten</h4>"))
sec2$add_fact("COVID-Fälle auf hessischen Intensivstationen nach der IVENA-Sonderlage, Stand: ",
    format.Date(scrape_df$Bettenauslastung_Datum,"%d.%m."))
sec2$add_fact("Belegte Betten",scrape_df$Intensivbettenauslastung_aktuell)
sec2$add_fact("davon laborbestätigt",scrape_df$Intensivbettenauslastung_bestaetigt)
sec2$add_fact("Verdachtsfälle",scrape_df$Intensivbettenauslastung_Verdacht)
# sec2$add_fact("Anteil Ungeimpfte/Teilgeimpfte",
#               paste0(format(scrape_df$ITS_Hospitalisierte_ungeimpft,
#                       big.mark = ".",decimal.mark=",",nsmall=1),"%"))
# sec2$add_fact("Anteil Geimpfte",
#               paste0(format(scrape_df$ITS_Hospitalisierte_geimpft,
#                             big.mark = ".",decimal.mark=",",nsmall=1),"%"))
# sec2$add_fact("Intensiv-Risiko Ungeimpfte",
#               paste0(format(risiko,
#                             big.mark = ".",decimal.mark=",",nsmall=1),"x höher"))

cc$add_section(new_section = sec2)

# Normalbetten
sec3 <- card_section$new()
sec3$text(paste0("<h4>Normalbetten</h4>"))
sec3$add_fact("COVID-Fälle in hessischen Normalbetten nach der IVENA-Sonderlage, Stand: ",
              format.Date(scrape_df$Bettenauslastung_Datum,"%d.%m."))
sec3$add_fact("Belegte Betten",scrape_df$Normalbettenauslastung_aktuell)
sec3$add_fact("davon laborbestätigt",scrape_df$Normalbettenauslastung_bestaetigt)
sec3$add_fact("Verdachtsfälle",scrape_df$Normalbettenauslastung_Verdacht)
cc$add_section(new_section = sec3)
# Karte vorbereiten und abschicken. 

cc$send()
