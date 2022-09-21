# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- NULL

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("../Helferskripte/server-msg-googlesheet-include.R")


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
    mutate(Datenstand = d)
      # Raus damit. 
  return(rki_)
}


#---- Festlegungen ----
# Intensiv-Aufnahmezahlen erst ab Sept. 2021
start_date <- as_date("2021-10-01")
# Mediandauer von Meldedatum bis Intensiv
# Methodisch nicht ganz korrekt, weil 
# das RKI in seiner Quelle die Mediandauer 
# "Symptombeginn bis Intensiv" angibt und nicht "Meldedatum"

tage_bis_intensiv <- 5
#  # vorbereitete Datenbank mit den Kreis- und Länder-Bevölkerungszahlen nach Altersjahren laden
# enthält zwei Dataframes:
# jeweils id | Name | ag | Insgesamt | männlich | weiblich 
# - pop_bl_df
# - pop_kr_df

# Hier schummmele ich ein wenig - das sind die Bevölkerungszahlen für 2020,
# die eigentlich erst ab 8/2021 zum Einsatz kamen. 

load("index/pop.rda")

# Hessen-Summe für die 7TInzidenz
hessen <- pop_bl_df %>% 
  filter(id=="06") %>% 
  mutate(n = as.integer(Insgesamt)) %>% 
  pull(n) %>% 
  sum(.)

ue60 <- pop_bl_df %>% 
  filter(id=="06") %>% 
  filter(as.integer(str_extract(ag,"^[0-9].")) >= 60) %>% 
  mutate(n = as.integer(Insgesamt)) %>% 
  pull(n) %>% 
  sum(.)



#---- Hole 7-Tage-Inzidenz (kalkuliert mit akt. Daten) ----
# heutigen Datensatz
rki <- read_github_rki_data() 
rki_inz_df <- rki %>% 
  filter(IdLandkreis >=6000 & IdLandkreis < 7000) %>% 
  filter(NeuerFall %in% c(0,1)) %>%
  mutate(Meldedatum = as_date(Meldedatum))%>% 
  group_by(Meldedatum) %>% 
  summarize(n = sum(AnzahlFall),
            n_ue60 = sum(ifelse(Altersgruppe %in%
                              c("A60-A79","A80+"),
            AnzahlFall,0))) %>% 
  ungroup() %>% 
  mutate(n7t = 
            n + 
           lag(n) +
           lag(n,2) +
           lag(n,3) +
           lag(n,4) +
           lag(n,5) +
           lag(n,6)) %>% 
  mutate(n7t_ue60 = 
           n_ue60 + 
           lag(n_ue60) +
           lag(n_ue60,2) +
           lag(n_ue60,3) +
           lag(n_ue60,4) +
           lag(n_ue60,5) +
           lag(n_ue60,6)) %>% 
  mutate(inz7t = n7t /hessen * 100000,
         in7t_ue60 = n7t_ue60 / ue60 *100000) %>% 
  filter(Meldedatum >= start_date) %>%
  rename(Datum = Meldedatum) %>% 
  arrange(Datum)
  
  
  

#---- Zeitreihe Neuaufnahmen ----

laender_url <- "https://diviexchange.blob.core.windows.net/%24web/zeitreihe-bundeslaender.csv"

# Erstaufnahme-Daten aus dem Länderdatensatz filtern
# (gilt nur für Erwachsene)
# Isoliere die letzten 6 Wochen 
try(erstaufnahmen_df <- read_csv(url(laender_url)) %>% 
      filter(Bundesland =="HESSEN") %>% 
      select(-Bundesland,-Behandlungsgruppe) %>% 
      mutate(Datum = as_date(Datum)) %>% 
      select(Datum,Erstaufnahmen =faelle_covid_erstaufnahmen) %>% 
      filter(!is.na(Erstaufnahmen)) %>%
      # Gleitendes 7-Tage-Mittel
      mutate(en7t = Erstaufnahmen+
                         lag(Erstaufnahmen)+
                         lag(Erstaufnahmen,2)+
                         lag(Erstaufnahmen,3)+
                         lag(Erstaufnahmen,4)+
                         lag(Erstaufnahmen,5)+
                         lag(Erstaufnahmen,6)) %>%
     mutate(erstm7t = en7t/7) %>% 
      # Letzte 6 Wochen filtern
      filter(Datum >= start_date))

# ---- Intensivbetten/Beatmungsbetten ----
#
# Statlab-Formel für Schätzung von Zugängen
# DIVI-Daten Intensivbetten

# Lade die DIVI-Daten
divi_hessen_df <- read_sheet(aaa_id, sheet = "DIVI Hessen-Archiv") %>% 
  filter(Datum >= as_date(start_date)) %>% 
  mutate(beatmung_anteil = faelle_covid_aktuell_beatmet/faelle_covid_aktuell*100)


#---- Zusammenfassung ----
library(ggplot2)

# Altersstruktur als Korrekturfaktor für Ü60
alter_url <- "https://diviexchange.blob.core.windows.net/%24web/bund-covid-altersstruktur-zeitreihe_ab-2021-04-29.csv"

alter_df <- read_csv(url(alter_url)) %>% 
      filter(Bundesland == "DEUTSCHLAND") %>% 
      select(-Stratum_Unbekannt) %>% 
  mutate(summe = rowSums(across(where(is.numeric)))) %>% 
  
      mutate(ue60 = Stratum_60_Bis_69 + 
               Stratum_70_Bis_79 +
               Stratum_80_Plus) %>% 
  mutate(ue60prozent = ue60/summe) %>% 
  select(Datum, ue60prozent) %>% 
  mutate(Datum = as_date(Datum))


# Die Intensivbetten-Erstaufnahmen dazufügen
# und als Quotienten ausrechnen: Intensivfälle je 100.000
# gemeldete Neufälle. 
# Dabei den Median-Verzug berücksichtigen. (Siehe ganz oben)



intensiv_inz_df <- rki_inz_df %>% 
  left_join(erstaufnahmen_df,by="Datum") %>% 
  left_join(alter_df,by="Datum") %>% 
  mutate(int_inz = en7t / lag(n7t,tage_bis_intensiv) * 100) %>% 
  # Rechnung für ü60 mit einem etwas geschätzten Korrekturfaktor: 
  # Anteil der Ü60 an den Intensivpatienten. 
  mutate(int_ue60_inz = (en7t * ue60prozent) / lag(n7t_ue60,tage_bis_intensiv)*100) %>% 
  left_join(divi_hessen_df, by="Datum")



# ggplot
plot <- ggplot2::ggplot(data = intensiv_inz_df) +
  ggplot2::geom_line(aes(x = Datum, y = int_inz)) +
  ggplot2::geom_line(aes(x = Datum, y = int_ue60_inz,
                         color = "red")) +
  ggplot2::labs(color = "nur Ü60",
                title = "Intensivfälle als Prozentanteil an den Neufällen")
  
  
plot
ggplot2::ggsave(paste0("daten/intensiv-inzidenz-",ymd(today()),".png"),device = png)

plot2 <- ggplot2::ggplot(data = intensiv_inz_df) +
  ggplot2::geom_line(aes(x = Datum, y = beatmung_anteil,
                         color = "Prozent")) 

plot2
ggplot2::ggsave(paste0("daten/intensiv-beatmung-anteil-",ymd(today()),".png"),device = png)


write.xlsx(intensiv_inz_df,
           paste0("daten/intensiv_inz_df-",
                  today(),
                  ".xlsx"),overwrite=T)



# ---- Schnell für Björn Schaffrinna noch: Neufälle seit Januar

neu_wochen_df <- rki_inz_df %>%
  # Aktuellste Meldedaten: gestern; das als Referenz
  filter(wday(Datum) == wday(today()-1)) %>% 
  filter(Datum >= as_date("2022-01-01")) %>% 
  select(Datum, n7t, n7t_ue60)

write.xlsx(neu_wochen_df,
           paste0("daten/neu_wochenweise-",
                  today()-1,
                  ".xlsx"),overwrite=T)
