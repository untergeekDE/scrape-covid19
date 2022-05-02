################ meldeverzug-inzidenzfehler.R
# 
# Wochenweise Auswertung der im Archiv gespeicherten Daten auf den Meldeverzug - 
# - wie stark war der durchschnittliche Meldeverzug - anders gesagt: wie alt
#   waren die gemeldeten Fälle? (Vergleich Meldedatum/Datenstand-Datum)
# - Wie stark haben die Verzögerungen die Fallzahlen (und damit die Inzidenz) 
#   beeinflusst? Vergleich der 7-Tage Inzidenz mit der 7TI, wie sie sich eine
#   Woche später darstellt, mit Nachmeldungen - und dann die prozentuale
#   Abweichung. 
#
#   Überblick je Tag und je Kreis für die letzten sechs Wochen. 
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 27.1.2022
#
# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

msgTarget <- NULL

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")

p_load(data.table)

# Kreisnamen


kreise <- read.xlsx("index/kreise-namen-index.xlsx") %>%
  select(AGS, kreis) # nur AGS und Kreisnamen
  

# Abfragedaten ab start_date im definierten Pfad
# 42 Tage (6 Wochen) zurück


start_date <- as_date(today()-42)
# Auf dem Server: Archivverzeichnis, überall sonst: lokales Archiv versuchen
archiv_path <- ifelse(server,
                      "./archiv/",
                      "~/rki-archiv-lokal/")

#---- Importfunktionen ----
# Funktion liest den jeweiligen Tag aus dem RKI-Archiv
read_github_rki_data <- function(d = today()) {
  # Repository auf Github
  repo <- "robert-koch-institut/SARS-CoV-2_Infektionen_in_Deutschland/"
  # Pfad
  path <- "Archiv/"
  fn <- paste0(as_date(d),"_Deutschland_SarsCov2_Infektionen.csv")
  # Wann war der letzte Commit des Github-Files? Das als Prognosedatum prog_d.
  
  csv_path <- paste0("https://github.com/",
                 repo,
                 "raw/master/",
                 path,
                 fn
                 )
  # Sicherheitsfeature: wenn kein Dataframe, probiere nochmal. 
  err <- try(rki_ <- read_csv(csv_path))
  if ("try-error" %in% class(err)) {
    stop(err)
  }
  # Einfacher Check: Jüngste Fälle mit Meldedatum gestern?
  # (Oh, dass dieser Check eines Tages scheitern möge!)
  if (max(rki_$Meldedatum) != d-1) {
    warning("Keine Neufälle von gestern")
  }
  # Daten in der Tabelle ergänzen, um kompatibel zu bleiben
  rki_ <- rki_ %>% 
    # Datenstand (Lesedatum)
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

# Spaltenbeschreibungen Tagesmeldungs-Tabelle
col_descr <- cols(
  IdBundesland = col_double(),
  Bundesland = col_character(),
  Landkreis = col_character(),
  Altersgruppe = col_character(),
  Geschlecht = col_character(),
  AnzahlFall = col_double(),
  AnzahlTodesfall = col_double(),
  ObjectId = col_double(),
  Meldedatum = col_character(),
  IdLandkreis = col_character(),
  Datenstand = col_character(),
  NeuerFall = col_double(),
  NeuerTodesfall = col_double(),
  Refdatum = col_character(), # als String lesen und wandeln
  NeuGenesen = col_double(),
  AnzahlGenesen = col_double()
)

get_archived_data <- function(d = as.integer(today())) {
  file_name <- paste0(archiv_path,"rki-",as_date(d),".csv")
  if(file.exists(file_name)) {
    tagesmeldung_df <- read_csv2(file_name,col_types = col_descr)
  } else {
    # Kein lokaler File gefunden; hole ihn vom RKI-Github
    tagesmeldung_df <- read_github_rki_data(d) %>% 
      # gleich auf Hessen filtern
      filter(Bundesland == "Hessen")
    write_csv2(tagesmeldung_df,file_name)
  }
  # Rückgabe der Hessen-Fallmeldungs-Tabelle für diesen Meldetag
  return(tagesmeldung_df)
}

# ---- Erste Runde: Meldeverzug je Tag in eine Tabelle schreiben. -----

verzuege_df <- NULL

for (d in start_date:today()) { 
  # Irritierenderweise ist d eine Integer-Zahl
  #
  # Lies die "Briefkastenmeldung" (also die Meldungen vom Tag d)
  tagesmeldung_df <- kreise %>% 
    select(AGS) %>% 
    # Kreise als Basis
    mutate(datum = as_date(d)) %>% 
    full_join(get_archived_data(d) %>% 
                rename(AGS=IdLandkreis) %>% 
                # Neufälle ausfiltern
                # Normalerweise wäre der Filter: in c(-1,1) - 
                # unter -1 verbergen sich die Korrekturen. Die wollen wir aber nicht.
                filter(NeuerFall == 1),
              by="AGS") %>% 
    mutate(Meldedatum = as_date(Meldedatum),
             Refdatum = as_date(Refdatum)) %>%
    # Ganz einfach: für jeden Fall (bzw. Gruppe von Fällen)
    # die Differenz von Meldedatum (wann wurde der Fall dem GA gemeldet)
    # und Publikationsdatum (das Datum des Briefkasten-Meldedatensatzes)

          mutate(delta = as_date(datum) - as_date(Meldedatum)) %>%
    mutate(AnzahlFall = ifelse(is.na(AnzahlFall),0,AnzahlFall)) %>% 
    # Alles außer Datum, Delta, Kreis und Anzahl der Fälle in dieser Meldung weg
      select(datum, delta, n = AnzahlFall, AGS)
    # Die Fallmeldungen auspacken: 
    # Funktion rep wiederholt die jeweilige df-Zeile n mal
    tmn_df <- as.data.frame(lapply(tagesmeldung_df,rep,tagesmeldung_df$n)) %>%
      select(delta, AGS, datum)
    # in tmn_df hängen jetzt alle Fallmeldungen dran.   
    if (is.null(verzuege_df)) {
      verzuege_df <- tmn_df 
    } else {
      verzuege_df <- bind_rows(verzuege_df, tmn_df)
    }
  msg("Meldeverzüge ",as_date(d))
}

# Jetzt nach Tagen das mittlere Alter der gemeldeten Fälle.
# Am Ende hat man eine Tabelle, die für jeden Kreis und jeden Tag
# das Durchschnittsalter zeichnet. 
alter_faelle_df <- verzuege_df %>% 
  arrange(datum) %>% 
  group_by(datum,AGS) %>%
  # Lubridate-Tage in eine Integer-Zahl umwandeln
  mutate(delta = as.integer(delta)) %>% 
  # Anteil der Meldungen, die älter als 3 Tage sind
  mutate(verspätet = delta > 3) %>% 
  summarize(delta = mean(delta,na.rm=TRUE),
            alte_meldungen = sum(verspätet,na.rm=TRUE)/n()*100) %>% 
  ungroup() %>% 
  right_join(kreise, by = "AGS")

# Kleine Anmerkung zu dieser Tabelle: 
# Da sie die mittleren Alter pro Tag und die prozentualen Anteile
# der Fälle älter 3 Tage enthält, und da sie "lang" ist, kann man
# sie wunderbar von Hand filtern.

wi_verspätung_df <- alter_faelle_df %>% 
  # Wiesbaden
  filter(AGS == "06414") %>% 
  select(-AGS,-kreis)
write.xlsx(wi_verspätung_df,
           paste0("daten/verspätung-wi-",
                  today(),
                  ".xlsx"),overwrite=T)


tab_alte_meldungen <- alter_faelle_df %>%
  mutate(datum = format.Date(datum,"%d.%m.%Y")) %>% 
  select(-AGS,-delta) %>%  
  pivot_wider(names_from=datum,values_from=alte_meldungen)

tab_medianalter <- alter_faelle_df %>% 
  select(-AGS,-alte_meldungen) %>%
  mutate(datum = format.Date(datum,"%d.%m.%Y")) %>% 
  pivot_wider(names_from=datum, values_from=delta)

# 
small_multiples <- tab_alte_meldungen %>% 
  full_join(tab_medianalter,by = "kreis")
  
# in die DW-Grafik packen
dw_data_to_chart(tab_alte_meldungen, chart_id = "qTjK4")


write.xlsx(small_multiples,"daten/mean_meldeverzug.xlsx",overwrite=T)
sheet_write(tab_alte_meldungen, ss = aaa_id, sheet = "MeldeverzugWocheKreis") 
sheet_write(tab_medianalter,ss = aaa_id, sheet = "Datenqualität")

msg("Meldeverzug hessenweit")

#---- Einzelanalysen ----
# Die drei großen Sünder: Wiesbaden, Bergstraße, LDK

wi_df <- verzuege_df %>%
  filter(AGS == "06414") %>%
  # Histogramm:
  count(delta)

write.xlsx(wi_df,"daten/wi-histogramm.xlsx",overwrite=T)

bergstr_df <- verzuege_df %>%
  filter(AGS == "06431") %>%
  # Histogramm:
  count(delta)

write.xlsx(bergstr_df,"daten/bergstr-histogramm.xlsx",overwrite=T)

ldk_df <- verzuege_df %>%
  filter(AGS == "06532") %>%
  # Histogramm:
  count(delta)

write.xlsx(ldk_df,"daten/ldk-histogramm.xlsx",overwrite=T)

ffm_df <- verzuege_df %>%
  filter(AGS == "06412") %>%
  # Histogramm:
  count(delta)

write.xlsx(ffm_df,"daten/ffm-histogramm.xlsx",overwrite=T)

rtk_df <- verzuege_df %>%
  filter(AGS == "06439") %>%
  # Histogramm:
  count(delta)

write.xlsx(rtk_df,"daten/rtk-histogramm.xlsx", overwrite=T)

wk_df <- verzuege_df %>%
  filter(AGS == "06440") %>%
  # Histogramm:
  count(delta)

write.xlsx(wk_df,"daten/wk-histogramm.xlsx",overwrite=T)

of_df <- verzuege_df %>%
  filter(AGS == "06413") %>%
  # Histogramm:
  count(delta)

write.xlsx(of_df,"daten/of-histogramm.xlsx",overwrite=T)
msg("Histogrammdaten WI, LDK, Bergstrasse, FFM lokal geschrieben")
# ---- Jetzt berechnen: Abweichung Inzidenz nach Tagen ----

# Es wird der Inzidenz-Stand vom Meldetag mit dem Stand eine Woche später verglichen: 
# Wie sehr weicht der Inzidenzwert vom Meldetag vom korrekten Wert ab, der eine Woche später
# mit allen bis dahin eingetroffenen Nachmeldungen berechnet wird. 


# Einmal den kompletten Datensatz

# die Tabelle mit den prozentualen Abweichungen
inz_delta_df <- NULL
# Die Zeit, die wir für Korrekturen einräumen
korr <- 5

for (d in start_date:(today()-korr)) {
  

  # Referenz: Vom aktuellen Tag 7 Tage in die Zukunft gehen
  # und aus dem Datensatz von diesem Tag die Fallzahlen berechnen
  # d ist das Referenzdatum - der Tag, für den die Inziden berechnet
  # werden soll.
  #
  # Referenz: Korrigierte Fallzahl (korr) Tage später
  korr7tage_df <- get_archived_data(as_date(d+korr)) %>% 
    mutate(Meldedatum = as_date(Meldedatum)) %>%
    # Meldedatum in den 7 zurückliegenenden Tagen
    filter(Meldedatum > as_date(d-8) & Meldedatum < as_date(d)) %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall) %>%
    # Nach Kreis sortieren
    group_by(AGS) %>%
    #  pivot_wider(names_from = datum, values_from = AnzahlFall)
    # Summen für Fallzahl, Genesen, Todesfall bilden
    summarize(AnzahlFall = sum(AnzahlFall)) %>%
    ungroup() %>%
    select(AGS,ref7t = AnzahlFall)
  orig7tage_df <- get_archived_data(d) %>% 
    mutate(Meldedatum = as_date(Meldedatum)) %>%
    filter(Meldedatum > as_date(d-8) & Meldedatum < as_date(d)) %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall) %>%
    # Nach Kreis sortieren
    group_by(AGS) %>%
    #  pivot_wider(names_from = datum, values_from = AnzahlFall)
    # Summen für Fallzahl, Genesen, Todesfall bilden
    summarize(AnzahlFall = sum(AnzahlFall)) %>%
    ungroup() %>%
    select(AGS,alt7t = AnzahlFall) %>%
    # Die Fallzahl dazuholen, wie sie (korr) Tage später vorliegt
    full_join(korr7tage_df, by = "AGS") %>%
    # Prozentuale Abweichung
    mutate(abw = (100*alt7t/ref7t)-100) %>%
    mutate(datum = as_date(d)) %>% 
    select(datum,AGS, abw) #%>%
    # Gib der Variablen den Namen des Referenzdatums
    #rename(!!as.character(as_date(d)):=vollst)
    
  # Spalte zur Gesamttabelle hinzufügen
  if (is.null(inz_delta_df)) {
    inz_delta_df <- orig7tage_df  
  } else {
    inz_delta_df <- bind_rows(inz_delta_df, orig7tage_df)
  }
  msg("Inzidenz-Fehler ",as_date(d)," berechnet")
}

# Kreisnamen dazuholen
inz_delta2_df <- kreise %>%
  full_join(inz_delta_df, by = "AGS") %>%
  # AGS raus
  select(-AGS) %>%
  # ...und dann nach Kreis pivotieren. 
  pivot_wider(names_from = kreis, values_from = abw)
 
# Berechne nach Wochentag 
inz_delta_wt_df <- inz_delta_df %>% 
  full_join(kreise, by = "AGS") %>%
  # AGS raus
  select(-AGS) %>%
  
  mutate(wt = wday(datum)) %>% 
  group_by(wt,kreis) %>% 
  summarize(abw = mean(abw)) %>% 
  pivot_wider(names_from=wt,values_from = abw)
 

write.xlsx(inz_delta2_df,"daten/vollstaendigkeit-kreise-wochen.xlsx", overwrite=T)
sheet_write(inz_delta2_df, ss = aaa_id, sheet = "Abweichung Inzidenz")
dw_data_to_chart(inz_delta2_df, chart_id = "2j6K9")
dw_publish_chart(chart_id = "2j6K9")
msg("Abweichungen berechnet und geschreiben")

sheet_write(inz_delta_wt_df,ss=aaa_id,sheet="Abweichung Inzidenz")
write.xlsx(inz_delta_wt_df,"vollstaendigkeit-kreise-wochentage.xlsx", overwrite=T)
sheet_write(inz_delta_wt_df, ss = aaa_id, sheet = "Abweichung Wochentage" )
msg("Abweichungen Wochentage berechnet und geschreiben")

msg("OK")
