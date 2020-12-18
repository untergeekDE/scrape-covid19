################ hole-covid-simulator.R
# 
# Holt sich wöchentlich die Daten vom "CoVID-Simulator" der Uni Saarbrücken
#
# (Quelle: https://covid-simulator.com/)
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 18.12.2020

rm(list=ls())
msgTarget <- "B13:C13"

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# ---- Main: Covid-Simulator COSIM lesen ----


repo <- "onwhenrdy/cosimhessen/"
path <- "Hessen_LKs.csv"
# später Github-Commit-Datum lesen, derzeit nur Statisch



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

  sim_df <- read_csv(path) %>%
    filter(state == "Hessen") %>%
    select(-state) %>%
    mutate(vom = prog_d) 
  
  sim_df$var <- as.factor(sim_df$var)
  
  # Archivkopie ablegen
  write_csv(sim_df,paste0("./daten/cosim-",prog_d,".csv"))
  
  daily_df <- sim_df %>%
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
  
  hospital_df <- sim_df %>%
    filter(var == "HOSPITAL" ) %>%
    filter(date >= prog_d) %>%
    select(date,hosp_min=minsim, hosp_mean=meansim, hosp_max=maxsim)
  
  daily_dead_df <- sim_df %>%
    filter(var == "DAILY_DEAD" ) %>%
    filter(date >= prog_d) %>%
    select(date,tote_min=minsim, tote_mean=meansim, tote_max=maxsim)
  
  icu_df <- sim_df %>%
    filter(var == "ICU" ) %>%
    filter(date >= prog_d) %>%
    select(date,icu_min=minsim, icu_mean=meansim, icu_max=maxsim)
  
  inz_df <- sim_df %>%
    filter(var == "INCIDENCE" ) %>%
    filter(date >= prog_d) %>%
    select(date,inz_min=minsim, inz_mean=meansim, inz_max=maxsim)
  
  msg("Prognosen gelesen und gefiltert")
  
  # ---- Daten ausgeben ----
  
  # Google Sheet mit Krankenhausdaten
  
  p_str <- paste0("Prognose vom ",day(prog_d),".",month(prog_d),".")
  
  # GSheet AAA
  hosp_id = "12S4ZSLR3H7cOd9ZsHNmxNnzKqZcbnzShMxaWUcB9Zj4"
  aaa_id = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"
  # Fallzahl 4 Wochen vom Google Bucket holen
  
  rki_he_df <- read.csv("http://d.data.gcp.cloud.hr.de/hessen_rki_df.csv",sep=";")
  
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
  divi_he_df <- read.csv("http://d.data.gcp.cloud.hr.de/divi_laender.csv") %>%
    filter(bundesland == "Hessen")
  
  # Kapazitätsprognose: 
  max_beds <- divi_he_df$faelle_covid_aktuell+divi_he_df$betten_frei
  
  
  # Prognosen dranhängen
  
  f4w_df <- f28_meldung_df %>%
    full_join(daily_df, by = c("datum" = "date")) %>%
    full_join(daily_dead_df, by = c("datum" = "date")) %>%
    # Prognose zwei Wochen in die Zukunft
    filter(datum <= end_date) %>%
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
  
  write_sheet(neuprognose_df,ss=hosp_id,sheet="NeuPrognose")  
  
  # Früher: rename(!!p_str:=neu7_mean), das hat aber den Nachteil, dass man in Datawrapper
  # die Gestaltung der Zeile von Hand umkonfigurieren muss. 
  # Besser: Über die API die entsprechende Datenreihe umbenennen. 
  
  # ---- Neufall-Prognose-Chart ----
  
  prog_chart_id = "g2CwK" # Neufälle-Prognose
  chart_data <- dw_retrieve_chart_metadata(prog_chart_id)
  # chart_data$content$metadata$data$changes
  # Liste der Änderungen durchgehen, die mit dem Wort "Prognose" finden und updaten
  for (i in 1:length(chart_data$content$metadata$data$changes)) {
    if (str_detect(chart_data$content$metadata$data$changes[[i]]$value,"Prognose")) chart_data$content$metadata$data$changes[[i]]$value = p_str
  }
  pp_str <- paste0("Trendlinie ist der gleitende Mittelwert über 7 Tage<br>",
                   p_str, " - beruht auf dem SEIR-Modell der Universität des ",
                   "Saarlandes, Forschungsgruppe von Prof. Thorsten Lehr")
  
  dw_edit_chart(chart_id = prog_chart_id,annotate = pp_str)
  dw_edit_chart(chart_id = prog_chart_id, data = chart_data)
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
  
  write_sheet(divi_df, ss=hosp_id, sheet="ICUPrognose")
  
  # Grafik anpassen
  msg("Grafik Intensivbetten (",intensiv_chart_id,") aktualisieren")
  
  intensiv_chart_id = "kc2ot" # Neufälle-Prognose
  
  dw_edit_chart(chart_id = intensiv_chart_id,annotate = p_str)
  dw_publish_chart(intensiv_chart_id)
  # DIVI-freie Betten - Hypothese: maximale Kapazität entspricht
  # der Anzahl der derzeit freien Betten plus der COVID-Intensivfälle
  
  
  
  msg("OK!")
} 