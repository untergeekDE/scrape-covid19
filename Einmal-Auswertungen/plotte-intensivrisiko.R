#### plotte-intensivrisiko.R ####
# Nimm die Intensiv-Zahlen des Landes, 
# berechne das Verhältnis geimpft/ungeimpft skaliert auf die Impfquote Ü18
# und plotte. 

# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- NULL

pacman::p_load(this.path)
setwd(this.path::this.dir())
source("../Helferskripte/server-msg-googlesheet-include.R")

# start- und enddatum
# Gibt die Zahlen seit Ende September 2021
startdatum <- as_date("2021-11-01")
enddatum <- today()
# enddatum <- today()-1

#---- Hole Impfdaten historisch aus dem RKI-Github ----

git_user <- "robert-koch-institut"
git_repo <- "COVID-19-Impfungen_in_Deutschland"
git_path <- "Aktuell_Deutschland_Bundeslaender_COVID-19-Impfungen.csv"

# Wann war der letzte Commit des Github-Files? Das als Datum i_d
github_api_url <- paste0("https://api.github.com/repos/",
                         git_user,"/",git_repo,"/",
                         "commits?path=",
                         git_path)
github_data <- read_json(github_api_url, simplifyVector = TRUE)
i_d <- max(as_date(github_data$commit$committer$date))

tsi <- now()
# Noch keine Daten für heute? Warte, prüfe auf Abbruch; loope
while(i_d < today()) {
  # Abbruchbedingung: 8x3600 Sekunden vergangen?
  if (now() > tsi+(8*3600)) {
    msg("KEINE NEUEN DATEN GEFUNDEN")
    stop("Timeout")
  }
  # Warte 5 Minuten, suche wieder den letzten Commit
  Sys.sleep(300)
  github_data <- read_json(github_api_url, simplifyVector = TRUE)
  i_d <- max(as_date(github_data$commit$committer$date))
}

# Kurz piepsen; auf dem Server geht das natürlich schief, deshalb try. 
try(beepr::beep(2),silent=TRUE)
# Zeitstempel erneuern: Das ist das Abfragedatum
tsi <- now()
msg("Impfdaten vom ",i_d)


# Brauchen nur die Impfserien nach Landkreis
lk_tbl <- read_csv(paste0("https://raw.githubusercontent.com/",
                          git_user,"/",git_repo,"/",
                          "master/",
                          "Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv")) %>% 
  select(Datum=1,id_lk = 2,3,4,5)

# Generiere Impfserie Drittimpfungen Hessen



# Impfzahlen Booster pro Tag holen

drittimpfung_df <- lk_tbl %>% 
  # Hessische Kreise rausfiltern
  filter(str_detect(id_lk,"^06")) %>%
  # Drittimpfungen
  filter(Impfschutz == 3) %>% 
  group_by(Datum) %>% 
  summarize(dritt_d = sum(Anzahl)) %>% 
  ungroup()

# Zeitreihe Impfquote vollständig 
# bzw. vollständig Ü18

load("index/pop.rda")
# Altersgruppen; 90 (die höchste) umfasst alle darüber. 
bev_he_df <- pop_bl_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # Zusätzliche Variable mit der Altersgruppe
  filter(id == "06") %>% 
  mutate(n = as.numeric(Insgesamt)) %>% 
  select(ag,n)

hessen = sum(bev_he_df %>% pull(n) )
he_ue18 = bev_he_df %>% filter (ag>=18) %>% pull(n) %>% sum(.)
he_ue60 = bev_he_df %>% filter (ag>=60) %>% pull(n) %>% sum(.)

impfquoten_df <- lk_tbl %>% 
  filter(str_detect(id_lk,"^06")) %>%
  # Zweitimpfungen
  filter(Impfschutz == 2) %>%
  # Brauchen Datum, Altersgruppen, Anzahl
  select(Datum,Altersgruppe, Anzahl) %>% 
  mutate(Anzahl = ifelse(is.na(Anzahl),0,Anzahl)) %>% 
  pivot_wider(names_from=Altersgruppe,
              values_from=Anzahl,
              values_fn=sum,
              values_fill=0) %>% 
  # Impfquoten berechnen
  mutate(quote_zweit = (cumsum(`05-11`)+
                          cumsum(`12-17`)+
                          cumsum(`18-59`)+
                          cumsum(`60+`))/hessen*100) %>% 
  mutate(quote_zweit_ue18 = (cumsum(`12-17`)+
                          cumsum(`18-59`)+
                          cumsum(`60+`))/he_ue18*100) %>%
  mutate(quote_zweit_ue60 = (cumsum(`60+`))/he_ue18*100)

  



# Zeitreihe Viertimpfung holen - derzeit nur die Zahlen der Hausärzte
# Q: KBV; bereitgestellt über das ZI


viert_url <- "https://www.zidatasciencelab.de/covid19dashboard/data/impfdax/viertimpfungen_praxen.csv"

viert_hessen_df <- read_csv(viert_url) %>% 
  filter(Bundesland=="Hessen") %>% 
  select(Datum = vacc_date,viert_d = 3)


# write.xlsx(viert_hessen_df,"daten/viertimpfungen_hessen_hausärzte.xlsx",overwrite=T)

#---- Hole Intensiv-Impfstatus-Angaben ----
intensiv_df <- read_sheet(aaa_id, sheet = "Krankenhauszahlen") %>% 
  select(Datum = Bettenauslastung_Datum,
         icu =  Intensivbettenauslastung_aktuell,
         # Prozentanteile geimpft/ungeimpft auf der Intensiv, 
         # zu 100 fehlende Prozent: unbekannt
         intensiv_geimpft = ITS_Hospitalisierte_geimpft,
         intensiv_ungeimpft = ITS_Hospitalisierte_ungeimpft) %>%
  # Startdatum
  filter(Datum >= startdatum) %>% 
  # Die Impfdaten dazupacken
  left_join(impfquoten_df, by = "Datum") %>% 
  # Datum filtern
  filter(Datum  >= startdatum & Datum <= enddatum) %>% 
  
  # Risiko berechnen
  # Anteil 
  mutate(risiko = round( (intensiv_ungeimpft/(100-quote_zweit)) / 
                           (intensiv_geimpft/quote_zweit), digits=1),
         risiko_ue18 = round( (intensiv_ungeimpft/(100-quote_zweit_ue18)) / 
                                (intensiv_geimpft/quote_zweit_ue18), digits=1)) 
# auf eine Nachkommastelle gerundet

drittimpfung_df <- impfquoten_df %>%
  mutate(dritt_d = `05-11`+`12-17` + `18-59` + `60+`) %>% 
  select(Datum,dritt_d)

#Daten zusammenführen 
impfrisiko_daten_df <- intensiv_df %>% 
  select(Datum,risiko,risiko_ue18,icu) %>%
  # Datum ist schon gefiltert auf Startdatum
  left_join(drittimpfung_df, by="Datum") %>% 
  left_join(viert_hessen_df, by = "Datum") %>% 
  arrange(Datum) %>% 
  mutate(y = isoyear(Datum), wk = isoweek(Datum)) %>% 
  group_by(y,wk) %>%
  mutate(dritt_w = sum(dritt_d,na.rm=T)) %>% 
  mutate(viert_w = sum(viert_d,na.rm = T)) %>% 
  ungroup() %>% 
  # auf Achse skalieren
  mutate(viert = viert_w / 10000,
         dritt = dritt_w / 10000)
 
plotdaten_df <- impfrisiko_daten_df %>% 
  select(Datum,risiko,risiko_ue18,dritt_w,viert_w) %>% 
  pivot_longer(cols = -Datum,
               names_to = "Indikator",
               values_to = "Wert")



# ggplot
library(ggplot2)
plot <- ggplot2::ggplot(data = impfrisiko_daten_df) +
  geom_line(aes(x = Datum, y = dritt_w)) +
  geom_line(aes(x = Datum, y = risiko)) +
  scale_y_continuous(sec.axis = sec_axis(~ . / 10000), name= "Impfdosen")

plot
ggsave(paste0("daten/intensivrisiko-impfung-",ymd(today()),".png"),device = png)
write.xlsx(impfrisiko_daten_df,
           paste0("daten/intensivrisiko-impfung-",
                  today(),
                  ".xlsx"),overwrite=T)

dw_data_to_chart(impfrisiko_daten_df,chart_id="0lQXf")
