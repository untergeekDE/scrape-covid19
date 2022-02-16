# Auswertung: Anteil AG 60-79 und 80+ an den Neuinfektionen; Inzidenz
# 


# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- NULL

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# start- und enddatum
startdatum <- as_date("2022-01-01")
enddatum <- today()
# enddatum <- today()-1



# Kreis-Indexdatei brauchen wir auch für die Abfrage-Funktion
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))


read_github_rki_data <- function() {
  # Repository auf Github
  repo <- "robert-koch-institut/SARS-CoV-2_Infektionen_in_Deutschland/"
  path <- "Aktuell_Deutschland_SarsCov2_Infektionen.csv"
  # Wann war der letzte Commit des Github-Files? Das als Prognosedatum prog_d.
  
  github_api_url <- paste0("https://api.github.com/repos/",
                           repo,
                           "commits?path=",path,
                           "&page=1&per_page=1")
  github_data <- read_json(github_api_url, simplifyVector = TRUE)
  d <- as_date(github_data$commit$committer$date)
  if (d<today()) {
    warning("Keine aktuellen Daten im Github-Repository")
    return(NULL)
  }
  msg("Aktuelle Daten vom ",d)
  path <- paste0("https://github.com/",
                 repo,
                 "raw/master/",
                 path)
  # Am 12.8.2021 scheiterte das Programm mit einem SSL ERROR 104 - 
  # Sicherheitsfeature: wenn kein Dataframe, probiere nochmal. 
  try(rki_ <- read_csv(path))
  # Kein Dataframe zurückbekommen (also vermutlich Fehlermeldung)?
  starttime <- now()
  while (!"data.frame" %in% class(rki_)) {
    msg(rki_," - neuer Versuch in 60s")
    Sys.sleep(60)
    try(rki_ <- read_csv(path))
    # Wenn 15min ergebnislos probiert, abbrechen
    if (now()>starttime+900){
      msg("Github-Leseversuch abgebrochen")
      return(NULL)
    } 
  }
  # Einfacher Check: Jüngste Fälle mit Meldedatum gestern?
  # (Oh, dass dieser Check eines Tages scheitern möge!)
  if (max(rki_$Meldedatum) != d-1) {
    warning("Keine Neufälle von gestern")
  }
  # Daten in der Tabelle ergänzen, um kompatibel zu bleiben
  rki_ <- rki_ %>% 
    # Datenstand (heute!)
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


# Tabelle, in der die Bevölkerungszahlen in der jeweiligen RKI-Altersgruppe aufgerechnet sind
# Nutzt die Daten aus der Regionalstatistik.de 12411-04-02-4

# Bevölkerungstabelle für alle Bundesländer vorbereiten,
# Altersgruppen: 0-4, 5-14, 15-34, 35-59, 60-79, 80+
# Ausgangspunkt ist der File aus GENESIS mit der Altersschichtung
# nach Lebensjahren und Bundesland. 
# Q: 12411-04-02-4, Stichtag 31.12.2020
load("index/pop.rda")

# Altersgruppen; 90 (die höchste) umfasst alle darüber. 
bev_bl_df <- pop_bl_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # Zusätzliche Variable mit der Altersgruppe
  mutate(Altersgruppe = case_when(
    ag < 5 ~ "A00-A04",
    ag < 15 ~ "A05-A14",
    ag < 35 ~ "A15-A34",
    ag < 60 ~ "A35-A59",
    ag < 80 ~ "A60-A79",
    TRUE    ~ "A80+")) %>% 
  select(id,Bundesland=Name,Altersgruppe,Insgesamt) %>% 
  mutate(Insgesamt = as.numeric(Insgesamt)) %>% 
  # Tabelle bauen: Bevölkerung in der jeweiligen Altersgruppe nach BL,
  # Werte aus der Variable aufsummieren. 
  pivot_wider(names_from=Altersgruppe, values_from=Insgesamt,values_fn=sum) %>% 
  # Aus irgendwelchen Gründen enthält die GENESIS-Tabelle die Landesbezeichnung
  # "Baden-Württemberg, Land". Korrigiere das. 
  mutate(Bundesland = str_replace(Bundesland,"\\, Land$",""))

# Für Hessen!
# Ländercode hier ggf. anpassen. 
bl = "06"

altersgruppen_df <- bev_bl_df %>% 
  filter(id == bl) %>% 
  pivot_longer(cols=starts_with("A"), 
               names_to="Altersgruppe",
               values_to="pop") %>% 
  select(-id, -Bundesland)

popA6079 = altersgruppen_df %>% filter(Altersgruppe=="A60-A79") %>% pull(pop) 
popA80plus = altersgruppen_df %>% filter(Altersgruppe=="A80+") %>% pull(pop) 
popu60 = sum(altersgruppen_df$pop)-popA6079-popA80plus


# Fälle aus Hessen ausfiltern
rki_bl_df <- read_github_rki_data() %>% 
  filter(IdBundesland==as.numeric(bl))

inzidenz_alter_df <- rki_bl_df %>%
  # Zählung der neuen Fälle
  filter(NeuerFall %in% c(0,1)) %>% 
  select(Meldedatum, AnzahlFall,Altersgruppe) %>%
  mutate(Altersgruppe = case_when(
    Altersgruppe == "A60-A79" ~ "A6079",
    Altersgruppe == "A80+" ~ "A80plus",
    TRUE    ~ "u60")) %>% 
  mutate(Meldedatum = as_date(Meldedatum)) %>%
  # Nach Meldedatum gruppieren; Inzidenzen
  group_by(Meldedatum,Altersgruppe) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Altersgruppe, values_from = AnzahlFall, values_fill=0) %>%
  ungroup() %>% 
  # Neufälle 7 Tage für Altersgruppen
  mutate(S7A6079= A6079+lag(A6079,n=1,default=0)+lag(A6079,n=2,default=0)+
           lag(A6079,n=3,default=0)+lag(A6079,n=4,default=0)+
           lag(A6079,n=5,default=0)+lag(A6079,n=6,default=0)) %>%
  mutate(S7A80plus= A80plus+lag(A80plus,n=1,default=0)+lag(A80plus,n=2,default=0)+
           lag(A80plus,n=3,default=0)+lag(A80plus,n=4,default=0)+
           lag(A80plus,n=5,default=0)+lag(A80plus,n=6,default=0)) %>%
  mutate(S7u60 = u60+lag(u60,n=1,default=0)+lag(u60,n=2,default=0)+
           lag(u60,n=3,default=0)+lag(u60,n=4,default=0)+
           lag(u60,n=5,default=0)+lag(u60,n=6,default=0)) %>%
  mutate(InzA6079=S7A6079/popA6079*100000,
         InzA80plus=S7A80plus/popA80plus*100000,
         Inzu60=S7u60/popu60*100000) %>% 
  # 6 Wochen zurück
  filter(Meldedatum >= startdatum) %>% 
  filter(Meldedatum <= enddatum)
# Mal nix ausfiltern, sondern im Zweifelsfall lieber ausblenden
#  select(Meldedatum,InzA0514,Inzu60,A0514)

# ggplot
library(ggplot2)
plot <- ggplot2::ggplot(data = inzidenz_alter_df %>% 
                          select(Meldedatum, 
                                 starts_with("Inz")),
                        aes(x=Meldedatum)) +
                          geom_line(aes(y = InzA6079, colour = "A60-79")) +
                          geom_line(aes(y = InzA80plus, colour = "A80")) +
                          geom_line(aes(y = Inzu60, colour="unter 60")) 

plot
ggsave(paste0("daten/a6080-",ymd(today()),".png"),device = png)

