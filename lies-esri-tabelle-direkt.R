############### lies-esri-tabelle-direkt.R ############
#
# Liest die ESRI-Tagestabelle und schreibt sie als vorläufige Daten
# auf die Grafiken für die Fallentwicklung, die Kreise und die Basisdaten.
#
# Ausgelagert aus hessen-zahlen-aufbereiten.R,
# wird per source() daraus aufgerufen. 

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")


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

if (as.Date(str_extract(old_data$x[1],"[0-9]+\\.[0-9]+\\.[0-9]+"),format="%d.%m.%y%y") < today()) {
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

# ---- ESRI-Daten vorläufig in die Dokumente schreiben ----

esri_he <- esri_daten_tabelle %>% filter(AdmUnitId == 6)

msg("ESRI-Tabelle direkt: ",esri_he$AnzFall," Fälle gesamt, ",
    esri_he$AnzFallNeu," heute -- werte aus")

# Basisdaten

datumsstring = paste0(format(today(),"%d.%m.%y%y")," (VORLÄUFIG)")

# Datumsstring schreiben (Zeile 2)
range_write(aaa_id,as.data.frame(datumsstring),range="Basisdaten!A2",
            col_names = FALSE, reformat=FALSE)

# Neufälle heute (Zeile 3)
#range_write(aaa_id,as.data.frame('neue Fälle'),range="Basisdaten!A3")
range_write(aaa_id,as.data.frame(esri_he$AnzFallNeu),
            range="Basisdaten!B3", col_names = FALSE, reformat=FALSE)

# Neufälle letzte 7 Tage - (Zeile 4)
range_write(aaa_id,as.data.frame(esri_he$AnzFall7T),
            range="Basisdaten!B4", col_names = FALSE, reformat=FALSE)

# Vergleich Vorwoche (Zeile 5)
#range_write(aaa_id,as.data.frame("Vergleich Vorwoche"),range="Basisdaten!A5")
f4w_df <- read_sheet(ss=aaa_id,sheet="Fallzahl4Wochen") 
steigerung_7t_vorwoche <- sum(f4w_df$neu[17:23])
steigerung_prozent_vorwoche <- (esri_he$AnzFall7T/steigerung_7t_vorwoche*100)-100

trend_string <- "&#9632;"
if (steigerung_prozent_vorwoche < -10) # gefallen
  trend_string <- "<b style='color:#019b72'>&#9660;</b><!--gefallen-->"
if (steigerung_prozent_vorwoche > 10) # gestiegen
  trend_string <- "<b style='color:#cc1a14'>&#9650;</b><!--gestiegen-->"
if (steigerung_prozent_vorwoche < -33) # stark gefallen
  trend_string <- "<b style='color:#019b72'>&#9660;&#9660;</b><!--stark gefallen-->"
if (steigerung_prozent_vorwoche > 33) # stark gestiegen
  trend_string <- "<b style='color:#cc1a14'>&#9650;&#9650;</b><!--stark gestiegen-->"

range_write(aaa_id,as.data.frame(
  paste0(format(steigerung_7t_vorwoche,big.mark = ".", decimal.mark = ",", nsmall =0),
         " (",ifelse(esri_he$AnzFall7T-steigerung_7t_vorwoche > 0,"+",""),
         format(esri_he$AnzFall7T - steigerung_7t_vorwoche,big.mark = ".", decimal.mark = ",", nsmall =0),
         trend_string,")")),
  range="Basisdaten!B5", col_names = FALSE, reformat=FALSE)

# Inzidenz (Zeile 6)

range_write(aaa_id,as.data.frame(format(esri_he$Inz7T,big.mark = ".",decimal.mark=",",nsmall=1)),
            range="Basisdaten!B6", col_names = FALSE, reformat=FALSE)

# Gesamt und aktiv (Zeile 7)
# range_write(aaa_id,as.data.frame(paste0("Fälle gesamt/aktiv")),range="Basisdaten!A7")
aktiv_str <- format(round((esri_he$AnzAktiv)/100) * 100,
                    big.mark = ".", decimal.mark = ",", nsmall =0)
range_write(aaa_id,as.data.frame(paste0(format(esri_he$AnzFall,big.mark = ".", decimal.mark = ",", nsmall =0),
                                        " (ca. ",aktiv_str,")")),
            range="Basisdaten!B7", col_names = FALSE, reformat=FALSE)

# Todesfälle heute (Zeile 10)
#range_write(aaa_id,as.data.frame("neue Todesfälle"),range="Basisdaten!A10")
range_write(aaa_id,as.data.frame(esri_he$AnzTodesfallNeu),
            range="Basisdaten!B10",col_names = FALSE, reformat=FALSE)

# Todesfälle gesamt (Zeile 11)
#range_write(aaa_id,as.data.frame("Todesfälle gesamt"),range="Basisdaten!A11")
range_write(aaa_id,as.data.frame(format(esri_he$AnzTodesfall,big.mark=".",decimal.mark = ",")),
            range="Basisdaten!B11",
            col_names = FALSE,reformat=FALSE)

dw_publish_chart(chart_id = "OXn7r") # Basisdaten

msg("Basisdaten OK, Verlaufstabelle aus ESRI-Tabelle aktualisieren...")

# Fallzahl und Fallzahl4Wochen aktualisieren

msg("Berechne vorläufig ESRI-fallzahl und fallzahl4w...")
fallzahl_df <- read_sheet(aaa_id,sheet="FallzahlVerlauf")

fallzahl_df$datum <- as_date(fallzahl_df$datum)
fallzahl_ofs <- as.numeric(today() - fallzahl_df$datum[1]) + 1

# Neue Zeile?
if (fallzahl_ofs > nrow(fallzahl_df)) {
  fallzahl_df[fallzahl_ofs,]<- NA
  fallzahl_df$datum[fallzahl_ofs] <- as_date(today())
  
}
# Werte für heute

fallzahl_df$gsum[fallzahl_ofs] <- esri_he$AnzGenesen
fallzahl_df$faelle[fallzahl_ofs] <- esri_he$AnzFall
fallzahl_df$steigerung[fallzahl_ofs] <- esri_he$AnzFall/(esri_he$AnzFall-esri_he$AnzFallNeu)-1
fallzahl_df$tote[fallzahl_ofs] <- esri_he$AnzTodesfall
fallzahl_df$tote_steigerung[fallzahl_ofs] <- esri_he$AnzTodesfallNeu
fallzahl_df$aktiv[fallzahl_ofs] <- esri_he$AnzAktiv
fallzahl_df$neu[fallzahl_ofs] <- esri_he$AnzFallNeu
fallzahl_df$aktiv_ohne_neu[fallzahl_ofs] <- esri_he$AnzAktiv-esri_he$AnzFallNeu

# 4-Wochen-Fallzahl darstellen

#7-Tage-Trend der Neuinfektionen
fall4w_df <- fallzahl_df %>% mutate(neu7tagemittel = (lag(neu)+
                                                        lag(neu,n=2)+
                                                        lag(neu,n=3)+
                                                        lag(neu,n=4)+
                                                        lag(neu,n=5)+
                                                        lag(neu,n=6)+
                                                        neu)/7) 
fall4w_df <- fall4w_df[(nrow(fall4w_df)-27):nrow(fall4w_df),]

write_sheet(fallzahl_df, ss=aaa_id, sheet="FallzahlVerlauf")
range_write(fall4w_df,ss = aaa_id, sheet = "Fallzahl4Wochen",reformat=FALSE)

# Prognose-Sheet updaten

msg("Fallzahlen geschrieben, Verlaufskurve mit Prognose aktualisieren")

neu_p_df <- read_sheet(aaa_id,sheet = "NeuPrognose")

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
dw_publish_chart(chart_id = "NrBYs")  # Neufälle und Trend letzte 4 Wochen

# Kreisdaten-Tabelle aktualisieren

esri_kreis <- read_sheet(ss=aaa_id,sheet="KreisdatenAktuell") %>%
  left_join(esri_daten_tabelle %>% mutate(ags_kreis = paste0("0",AdmUnitId)),
            by="ags_kreis") %>%
  mutate(stand=datumsstring) %>%
  mutate(inzidenz = AnzFall/pop*100000) %>%
  mutate(TotProz = round(AnzTodesfall/AnzFall*100,1),
         GenesenProz = round(AnzGenesen/AnzFall*100,1),
         AktivProz = round(AnzAktiv/AnzFall*100,1)) %>%
  select(ags_kreis,
         kreis,
         gesamt = AnzFall,
         stand,
         pop,
         inzidenz,
         tote = AnzTodesfall,
         neu7tage = AnzFall7T,
         inz7t = Inz7T, #Achtung: kleines t = meine Daten, großes T = ESRI-Daten
         AnzahlGenesen = AnzGenesen, 
         AnzahlAktiv = AnzAktiv,
         notizen, 
         TotProz,
         GenesenProz,
         GA_link,
         AktivProz,
         f28_21,
         f21_14,
         f14_7,
         neu = AnzFallNeu,
         w1, w2, w3, w4, 
         rt, 
         vom,
         vzeit, 
         rrt,
         Abk)

# Tabelle schreiben, Grafik aktualisieren

write_sheet(esri_kreis,ss=aaa_id,sheet="KreisdatenAktuell")

dw_publish_chart(chart_id="m7sqt")
