################ hole-covid-simulator.R
# 
# Holt sich wöchentlich die Daten vom "CoVID-Simulator" der Uni Saarbrücken
#
# (Quelle: https://covid-simulator.com/)
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 1.7.2021

rm(list=ls())
msgTarget <- "B13:C13"

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

library(ggplot2)

# ---- Main: Covid-Simulator COSIM lesen ----


repo <- "onwhenrdy/cosimhessen/"
path <- "Hessen_LKs.csv"



# Wann war der letzte Commit des Github-Files? Das als Prognosedatum prog_d.

github_api_url <- paste0("https://api.github.com/repos/",
                         repo,
                         "commits?path=",path,
                         "&page=1&per_page=1")
github_data <- read_json(github_api_url, simplifyVector = TRUE)
prog_d <- as_date(github_data$commit$committer$date)
msg("Aktuelle Prognose vom ",prog_d)

end_date <- prog_d+14
path <- paste0("https://github.com/",
               repo,
               "raw/main/",
               path)


# Daten lesen und schauen, ob es schon eine Archivdatei dazu gibt - 
# wenn ja, sind wir durch, 
# wenn nein, alles aktualisieren. 

if (file.exists(paste0("./daten/cosim-",prog_d,".csv"))) {
  msg("OK - Stand ",prog_d)
} else{
  msg("Prognose anpassen")
  # Kreisdaten einlesen
  
  sim_df <- read_csv(path) %>%
    mutate(vom = prog_d) %>%
    mutate(var = as.factor(var))
  
  
  sim_lk_df <- sim_df %>%
    filter(state != "Hessen")

  sim_he_df <- sim_df %>%
    filter(state == "Hessen") %>%
    select(-state)
  
  
  
  daily_df <- sim_he_df %>%
    filter(var == "DAILY_CASES" ) %>%
    # gleitendes 7-Tage-Mittel
    mutate(neu7_min  = (lag(minsim)+
                          lag(minsim,n=2)+
                          lag(minsim,n=3)+
                          lag(minsim,n=4)+
                          lag(minsim,n=5)+
                          lag(minsim,n=6)+
                          minsim)/7) %>%
    mutate(neu7_mean  = (lag(meansim)+
                           lag(meansim,n=2)+
                           lag(meansim,n=3)+
                           lag(meansim,n=4)+
                           lag(meansim,n=5)+
                           lag(meansim,n=6)+
                           meansim)/7) %>%
    mutate(neu7_max  = (lag(maxsim)+
                          lag(maxsim,n=2)+
                          lag(maxsim,n=3)+
                          lag(maxsim,n=4)+
                          lag(maxsim,n=5)+
                          lag(maxsim,n=6)+
                          maxsim)/7) %>%
    filter(date >= prog_d) %>%
    select(date,neu7_min, neu7_mean, neu7_max)
  
  hospital_df <- sim_he_df %>%
    filter(var == "HOSPITAL" ) %>%
    filter(date >= prog_d) %>%
    select(date,hosp_min=minsim, hosp_mean=meansim, hosp_max=maxsim)
  
  daily_dead_df <- sim_he_df %>%
    filter(var == "DAILY_DEAD" ) %>%
    filter(date >= prog_d) %>%
    select(date,tote_min=minsim, tote_mean=meansim, tote_max=maxsim)
  
  icu_df <- sim_he_df %>%
    filter(var == "ICU" ) %>%
    filter(date >= prog_d) %>%
    select(date,icu_min=minsim, icu_mean=meansim, icu_max=maxsim)
  
  inz_df <- sim_he_df %>%
    filter(var == "INCIDENCE" ) %>%
    filter(date >= prog_d) %>%
    select(date,inz_min=minsim, inz_mean=meansim, inz_max=maxsim)
  
  msg("Prognosen gelesen und gefiltert")
  
  # ---- Daten ausgeben ----
  
  # Google Sheet mit Krankenhausdaten
  
  p_str <- paste0("Trend vom ",day(prog_d),".",month(prog_d),".")
  
  # GSheet AAA
#  hosp_id = "12S4ZSLR3H7cOd9ZsHNmxNnzKqZcbnzShMxaWUcB9Zj4"
  aaa_id = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"
  # Fallzahl 4 Wochen vom Google Bucket holen
  
  rki_he_df <- read.csv("https://d.data.gcp.cloud.hr.de/hessen_rki_df.csv",sep=";")
  
  # Archivdaten Intensivbettenbelegung nach DIVI letzte Wochen
  hosp_df <- read_sheet(ss = aaa_id, sheet = "DIVI Hessen-Archiv") %>%
    select(datum = Datum, icu = faelle_covid_aktuell)
  
  # 7-Tage-Inzidenz errechnen, Krankenhausdaten reinrechnen
  # f28_df <- rki_he_df %>%
  #   mutate(Meldedatum = as_date(Meldedatum)) %>%
  #   # letzte 5 Wochen, rechneet schneller
  #   filter(Meldedatum > today()-37) %>%
  #   # Auf die Summen filtern?
  #   filter(NeuerFall %in% c(0,1)) %>%
  #   select(Meldedatum,neu=AnzahlFall,tote =AnzahlTodesfall) %>%
  #   group_by(Meldedatum) %>%
  #   #  pivot_wider(names_from = datum, values_from = AnzahlFall)
  #   # Summen für Fallzahl, Genesen, Todesfall bilden
  #   summarize(neu = sum(neu),
  #             tote = sum(tote)) %>%
  #   select(datum = Meldedatum,neu,tote) %>%
  #   mutate(neu7tagemittel = (lag(neu)+
  #                              lag(neu,n=2)+
  #                              lag(neu,n=3)+
  #                              lag(neu,n=4)+
  #                              lag(neu,n=5)+
  #                              lag(neu,n=6)+
  #                              neu)/7) %>%
  #   filter(datum > today()-29) %>%
  #   left_join(hosp_df, by = c("datum" = "datum"))
  
  # alternativ: Neufall-Meldungen der letzten vier Wochen 
  f28_meldung_df <- read_sheet(ss = aaa_id,sheet = "Fallzahl4Wochen") %>%
    select(datum,neu,neu7tagemittel) %>%
    # DIVI-Intensivbetten-Daten dazu
    left_join(hosp_df, by = c("datum" = "datum"))
  
  # DIVI-Daten für die Kapazitäten
  divi_he_df <- read.csv("https://d.data.gcp.cloud.hr.de/divi_laender.csv") %>%
    filter(bundesland == "Hessen")
  
  # Kapazitätsprognose: 
  max_beds <- divi_he_df$faelle_covid_aktuell+divi_he_df$betten_frei
  
  
  # Prognosen dranhängen
  
  f4w_df <- f28_meldung_df %>%
    full_join(daily_df, by = c("datum" = "date")) %>%
    full_join(daily_dead_df, by = c("datum" = "date")) %>%
    # Prognose zwei Wochen in die Zukunft
    #filter(datum <= end_date) %>%
    select(datum,neu,neu7tagemittel,min = neu7_min, neu7_mean, max = neu7_max, icu) %>%
    # ICU-Prognose dranhängen
    full_join(icu_df, by = c("datum" = "date")) %>%
    # Inzidenz-Prognose dranhängen
    full_join(inz_df, by = c("datum" = "date")) %>%
    filter(datum <= today()+14) %>%
    mutate(icu_limit = max_beds)
  
  
  # In Sheet "NeuPrognose" ausgeben
  
  # write.xlsx(f4w_df, paste0("prognose-",prog_d,"-vs-",today(),".xlsx"))
  
  neuprognose_df <- f4w_df %>%
    select(datum =1, neu = 2, neu7tagemittel=3, min = 4, mean = 5, max = 6) %>%
    mutate(prognosedatum = prog_d)
  
  msg("Prognoseseite generiert, schreiben...")
  
  write_sheet(neuprognose_df,ss=aaa_id,sheet="NeuPrognose")  
  
  # Früher: rename(!!p_str:=neu7_mean), das hat aber den Nachteil, dass man in Datawrapper
  # die Gestaltung der Zeile von Hand umkonfigurieren muss. 
  # Besser: Über die API die entsprechende Datenreihe umbenennen. 
  # --- GEKILLT: Bezeichung "Trendlinie" reicht! ---
  
  # ---- Neufall-Prognose-Chart ----
  
  prog_chart_id = "eTpGf" # Neufälle-Prognose
  # chart_data <- dw_retrieve_chart_metadata(prog_chart_id)
  # # chart_data$content$metadata$data$changes
  # # Liste der Änderungen durchgehen, die mit dem Wort "Prognose" finden und updaten
  # for (i in 1:length(chart_data$content$metadata$data$changes)) {
  #   if (str_detect(chart_data$content$metadata$data$changes[[i]]$value,"Prognose")) chart_data$content$metadata$data$changes[[i]]$value = p_str
  # }
  pp_str <- paste0("Die dicke Linie ist der gleitende Mittelwert über 7 Tage. ",
                   p_str, " - beruht auf dem SEIR-Modell der Universität des ",
                   "Saarlandes, Forschungsgruppe von Prof. Thorsten Lehr")
  
  dw_edit_chart(chart_id = prog_chart_id,annotate = pp_str)
  # dw_edit_chart(chart_id = prog_chart_id, data = chart_data$content$metadata$visualize)
  dw_publish_chart(prog_chart_id)
  # DIVI-freie Betten - Hypothese: maximale Kapazität entspricht
  # der Anzahl der derzeit freien Betten plus der COVID-Intensivfälle
  
  msg("Grafik Neufälle (",prog_chart_id,") aktualisiert")
  
  # ---- Intensivbetten-Prognose erstellen und Chart schreiben ----
  
  # DIVI-Daten einlesen
  divi_df <- read_sheet(aaa_id, sheet="DIVI Hessen-Archiv") %>%
    select(datum = 1,intensiv = 2) %>%
    mutate(datum = as_date(datum)) %>%
    full_join(icu_df,by = c("datum" = "date")) %>%
    arrange(datum) %>%
    #letzte 4 Wochen
    filter(datum > today()-29) %>%
    # nächste 14 Tage
    filter(datum < today()+15) %>%
    mutate(kapazitaet = max_beds) %>%
    select(datum,intensiv,icu_min,icu_mean, icu_max,`ungefähre derzeitige Kapazität` = kapazitaet)
  #  rename(!!p_str:=mean) %>%
  #  mutate(abs_max = 2400)
  
  msg("Intensivbetten-Prognose erstellt, schreiben...")
  
  write_sheet(divi_df, ss=aaa_id, sheet="ICUPrognose")
  
  # Grafik anpassen
  
  intensiv_chart_id = "kc2ot" # Intensivbetten mit Prognose

  msg("Grafik Intensivbetten (",intensiv_chart_id,") aktualisieren")
  
  dw_edit_chart(chart_id = intensiv_chart_id,annotate = p_str)
  dw_publish_chart(intensiv_chart_id)
  # DIVI-freie Betten - Hypothese: maximale Kapazität entspricht
  # der Anzahl der derzeit freien Betten plus der COVID-Intensivfälle
  
  # ---- Regionale R-Wert-Tabelle ----
  msg("Berechne regionales R")
  # Abkürzungen dazuholen
  aküverz <- read.xlsx("index/kreise-namen-index.xlsx")
  r_lk_df <- sim_lk_df %>%
    # Das R in der Prognose bleibt im prognostizierten Zeitraum konstant
    filter(date == today()) %>%
    # Kreisbezeichner dazu
    full_join(aküverz,by=c("state" = "StatName")) %>%
    # nutze irgendeine Variable
    filter(var == "DAILY_CASES") %>%
    select(AGS,Kreis=kreis,rt,vom) %>%
    # Errechnung der Verdoppelungs- bzw. Halbwertszeit: 
    # t(1/2) = ln(2)/(1-rt)*gamma (mal Generationenzeit).
    # Da das Modell des Covid-Simulators mit einer Generationenzeit
    # von 7 Tagen rechnet, wird hier berechnet mit: 
    mutate(vzeit=(log(2)/(1-rt)*7))
  #
  rr_lk_df <- r_lk_df %>% 
    left_join(read_sheet(aaa_id,sheet = "KreisdatenAktuell") %>%
                             select(ags_kreis,neu,neu7tage,inz7t,stand),
              by= c("AGS" = "ags_kreis")) %>%
    left_join(read.xlsx("index/kreise-index-pop.xlsx") %>% 
                select(kreis,Lat=Lon,Lon=Lat),
              by= c("Kreis" = "kreis")) %>%
    select(Lat,Lon,Kreis,rt,vom,vzeit,inz7t,neu,neu7tage,stand) %>%
    # Das hier ist ne etwas dreckige Formel: 
    # den niedrigsten Wert als Basislinie, ein paar Pixel dazu - und 100fach
    # skalieren...
    mutate(rrt = (rt-min(rt))/(max(rt)-min(rt))*50+1) %>%
    left_join(read.xlsx("index/kreise-namen-index.xlsx") %>%
                select(kreis,Abk),
              by = c("Kreis" = "kreis"))
  
  write_sheet(rr_lk_df,ss=aaa_id,sheet="Regionales Rt neu")
  rr_str <- paste0("Berechnung vom ",format.Date(prog_d,"%d.%m.%Y"),
    " - Aufgrund der niedrigen Fallzahlen ist der Fehlerbereich ",
    "relativ groß; die Angaben in Tagen dienen nur der Verdeutlichung.", 
    "Der R-Wert ist mit einer Generationenzeit von 7 Tagen berechnet und deshalb mit ",
    "den Werten des RKI und HZI nicht direkt vergleichbar. ")
  
  # Kopie der Karte mit den Sechsecken
  dw_edit_chart(chart_id = "eessR", annotate=paste0(
    "Lesebeispiel: Ein großes, tiefrotes Symbol zeigt einen Kreis mit hoher",
    " Fallzahl an, dessen Inzidenzwerte nur langsam sinken. ",rr_str))
  dw_publish_chart(chart_id = "eessR")
  
  # Liniengrafik  
  dw_edit_chart(chart_id = "9UVBF",annotate = rr_str)
  dw_publish_chart(chart_id ="9UVBF")
#  ggplot(rr_lk_df, aes(rt,inz7t,label = Abk)) +  geom_text()
  msg("Archivkopie schreiben...")
  # Archivkopie ablegen
  write_csv(sim_df,paste0("./daten/cosim-",prog_d,".csv"))
  
   
  # ---- Rufe Animation-Skript und Teams-Karten-Generierung auf ----
  # Erstellt ein GIF, das die Veränderung der Inzidenzen in den Kreisen
  # in den letzten 4 Wochen animiert - und mit dem derzeitigen Trend
  # des COVID-Simulators 2 Wochen vorausschaut.
  source("./animiere-prognose-kreise.R")
  msg("OK!")
} 

