################ auswertung-todesfaelle-dez-dez.R
# #
# 
# Grundidee: alle Gesund-  und Verstorben-Meldungen scannen. 
# - Wie lange waren Menschen, die an COVID-19 gestorben sind, krank?
# - Wie lange waren Menschen, die gesundet sind, krank?
#
# Hypothesen: 
# - Aufschlüsselung nach Altersgruppen U60,Ü60-79,Ü80 zeigt Unterschiede in den
#   Wellen
# - Unterschiede in der Zeit Erkrankungs- bis Sterbedatum in den Wellen
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 1.3.2022
#
# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt

rm(list=ls())

msgTarget <- NULL # Messaging zu Google abschalten

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# Die brauchen wir noch: 
library(data.table)

# Definitionen
# Die aktuelle Welle läuft noch; wegen Meldeverzugs endet sie rechnerisch eine Woche vor dem Auswertungsdatum

wellen <- wellen_def <- data.frame(from=c("2020-12-01",
                                          "2021-01-01",
                            "2021-12-01",
                            "2022-01-01"),
                     to=c("2020-12-31",
                          "2021-01-31",
                          "2021-12-31",
                          "2022-01-31")) %>% 
  mutate_all(as_date)

# Abfragedaten ab start_date im definierten Pfad
archiv_path <- "~/rki-archiv-lokal/" # für lokales Archiv
#path <- "./archiv/"
# Die Kreise nur wegen der Namensdefinitionen
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))


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

# ---- Bevölkerungstabelle vorhalten
breakpoints <- tibble(datum = c("2020-01-01",
                                "2020-10-08",
                                "2021-08-26"),
                      stichtag = c("31.12.2018",
                                   "31.12.2019",
                                   "31.12.2020"))

bev_df <- read_delim("index/12411-0015.csv",
                     delim = ";", 
                     escape_double = FALSE, 
                     locale = locale(date_names = "de", 
                                     decimal_mark = ",", 
                                     grouping_mark = ".", 
                                     encoding = "ISO-8859-1"), 
                     trim_ws = TRUE, 
                     skip = 5) %>% 
  rename(AGS = 1, Kreis = 2) %>% 
  # in Langformat umformen
  pivot_longer(cols = -c(AGS,Kreis),
               names_to = "Stichtag",
               values_to = "pop")

#  # vorbereitete Datenbank mit den Kreis- und Länder-Bevölkerungszahlen nach Altersjahren laden
# enthält zwei Dataframes:
# jeweils id | Name | ag | Insgesamt | männlich | weiblich 
# - pop_bl_df
# - pop_kr_df

# Hier schummmele ich ein wenig - das sind die Bevölkerungszahlen für 2020,
# die eigentlich erst ab 8/2021 zum Einsatz kamen. 

load("index/pop.rda")
bev_6079 <- pop_bl_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # filtere Ü60
  filter(ag >= 60 & ag < 80) %>% 
  mutate(Insgesamt = as.numeric(Insgesamt)) %>% 
  # nur hessen
  filter(id == "06") %>% 
  pull(Insgesamt) %>% 
  sum()

bev_ue80 <- pop_bl_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # filtere Ü60
  filter(ag >= 80) %>% 
  mutate(Insgesamt = as.numeric(Insgesamt)) %>% 
  # nur hessen
  filter(id == "06") %>% 
  pull(Insgesamt) %>% 
  sum()

bev_ue60 <- bev_ue80 + bev_6079

# ---- Alle Meldungen durchgehen, Todesfälle und Gesundmeldungen isolieren -----

# Jetzt durchlaufen: 
# Für jede "Welle" vergleichen, wieviele Menschen, die sich im 
# Zeitraum der Welle angesteckt haben, im Monat drauf als verstorben
# gemeldet wurden. 
#
# Altersgruppe Ü60
u60v <- c("A00-A04","A05-A14","A15-A34","A35-A59")
ue60v <- c("A60-A79","A80+")
ue6079 <- "A60-A79"
ue80 <- "A80+"

# Impfzahlen
impftabelle_df <- read_sheet(ss = aaa_id, sheet = "ImpfzahlenHistorie")


for (i in 1:nrow(wellen_def)) {
  # 
  # Alle Tage bis 31 Tage nach der Welle durchsteppen
  # 
  # Das geht wirklich nur mit der un-R-artigen Schleife,
  # weil wir alle Melde-Datensätze des RKI einzeln laden müssen.
  # Nur so können wir die Todesfälle/Genesenen sehen, die in 
  # diesem Zeitraum gemeldet wurden.
  
  tote_n <- 0
  tote_t <- NULL
  tote_t_e <- NULL
  tote_ue60 <- 0
  tote_6079 <- 0
  tote_ue80 <- 0
  
  # Mach's einfach: Bevölkerungszahl für Inzidenzberechnung
  # orientiert sich am letzten Tag des Zeitraums
  # Erstelle eine Kopie der Kreise-Index-Pop mit dem an diesem 
  # Tag gültigen Bevölkerungszahlen
  d <- wellen_def$to[i]
  bev_Stichtag <- breakpoints %>% 
    # Nutze nur die Zeilen mit Datum ab heute
    filter(as_date(d) >= as_date(datum)) %>% 
    tail(1) %>% 
    pull(stichtag)
  # Gesamtbevölkerung zum Stichtag
  hessen <- bev_df %>% 
    filter(bev_Stichtag == Stichtag) %>% 
    filter(str_detect(AGS,"^06")) %>% 
    mutate(pop = as.integer(pop)) %>% 
    pull(pop) %>% 
    sum()
  # Bevölkerung Ü60 zum Stichtag
  # gibt's leider nicht, durchgängig mit Werten von 2020 rechnen
  
  # Jetzt noch schnell die Impfquote zu Beginn des Zeitraums

  wellen_def$durchgeimpft[i] = impftabelle_df %>% 
                                  filter(Datum == wellen_def$from[i]) %>% 
                                  pull(quote_zweit) %>% 
                                  ifelse(is.na(.),0,.)
  wellen_def$impf_ue60[i] = impftabelle_df %>% 
                              filter(Datum == wellen_def$from[i]) %>% 
                              pull(quote_zweit_ue60) %>% 
                              ifelse(is.na(.),0,.)

  # Enddatum der "Welle" plus 31 Tage suchen: wer ist in diesem Zeitraum gestorben?
  # Am Ende noch mal schauen: wie viele Fälle gemeldet in dieser Zeit?
  for (d in wellen_def$from[i]:(wellen_def$to[i]+28)) {
    tmp_df <- get_archived_data(d)
    
    # Zähle die in dieser Meldung neu gemeldeten Toten
    tote_df <- tmp_df %>% 
      # Nur neu gemeldete Todesfälle
      filter(NeuerTodesfall %in% c(-1,1)) %>%
      # Nur die mit Meldedatum in der "Welle"
      filter(Meldedatum >= wellen_def$from[i] &
               Meldedatum <= wellen_def$to[i])
    
    # Gesamtzahl Todesfälle in der Welle erhöhen
    tote_n_heute <- tote_df %>% 
      pull(AnzahlTodesfall) %>% 
      sum()
    tote_n <- tote_n + tote_n_heute
    tote_ue60 <- tote_ue60 + tote_df %>% 
      filter(Altersgruppe %in% ue60v) %>% 
      pull(AnzahlTodesfall) %>%  sum()
    tote_6079 <- tote_6079 + tote_df %>% 
      filter(Altersgruppe == "A60-A79") %>% 
      pull(AnzahlTodesfall) %>% sum()
    tote_ue80 <- tote_ue80 + tote_df %>% 
      filter(Altersgruppe == "A80+") %>% 
      pull(AnzahlTodesfall) %>% sum()
    
    # Habe mich entschieden, hier einfach einen Vektor mit allen
    # Einzelwerten anzuhängen. 
    # Dauer bis zum Tod
    tote_t <- c(tote_t,
                # Differenz Meldedatum bis zum Tod
                     tote_df %>% 
                       mutate(delta = as.integer(as_date(d)-as_date(Meldedatum))) %>%  
                        pull(delta))
    msg("Stand ",as_date(d)," - ",tote_n_heute)
    # nur für die Fälle mit Erkrankungsdatum
    tote_t_e <- c(tote_t_e, tote_df %>% 
                    filter(IstErkrankungsbeginn == 1) %>% 
                    mutate(delta = as.integer(as_date(d)-as_date(Refdatum))) %>% 
                    pull(delta))
  }
  # in tmp_df ist dann die letzte Meldedatei 1 Monat nach der Welle.
  # Die für die Inzidenzberechnung nutzen.
  # Inzidenz: Alle Fälle mit Meldedatum im Wellen-Monat
  n <- tmp_df %>% 
    filter(Meldedatum >= wellen_def$from[i] &
             Meldedatum <= wellen_def$to[i]) %>% 
    filter(NeuerFall %in% c(0,1)) %>% 
    pull(AnzahlFall) %>% 
    sum()
  # Mit fiesem Base R die Werte in die Tabelle eintragen. 
  wellen_def$n[i] <- n
  # Nur Meldungen über 60-Jährige
  n_ue60 <- tmp_df %>% 
    filter(Meldedatum >= wellen_def$from[i] &
             Meldedatum <= wellen_def$to[i]) %>% 
    filter(NeuerFall %in% c(0,1)) %>% 
    filter(Altersgruppe %in% ue60v) %>% 
    pull(AnzahlFall) %>% 
    sum()
  # Nur Meldungen60-79Jährige
  n_6079 <- tmp_df %>% 
    filter(Meldedatum >= wellen_def$from[i] &
             Meldedatum <= wellen_def$to[i]) %>% 
    filter(NeuerFall %in% c(0,1)) %>% 
    filter(Altersgruppe == "A60-A79") %>% 
    pull(AnzahlFall) %>% 
    sum()
  # Nur Meldungen über 80-Jährige
  n_ue80 <- tmp_df %>% 
    filter(Meldedatum >= wellen_def$from[i] &
             Meldedatum <= wellen_def$to[i]) %>% 
    filter(NeuerFall %in% c(0,1)) %>% 
    filter(Altersgruppe == "A80+") %>% 
    pull(AnzahlFall) %>% 
    sum()
  
  wellen_def$n_ue60[i] <- n_ue60
  # 7-Tage-Inzidenz: Alle Fälle, umgerechnet auf Hessen, 
  # Tagesmittelwert mal 7
  wellen_def$inz7t[i] <- (n*7/31) / hessen * 100000
  wellen_def$inz7t_ue60[i] <- (n_ue60*7/31) / bev_ue60 * 100000
  wellen_def$tote[i] <- tote_n
  wellen_def$tote_ue60[i] <- tote_ue60
  # dito nur 60-79
  wellen_def$n_6079[i] <- n_6079
  wellen_def$inz7t_6079[i] <- (n_6079*7/31) / bev_6079 * 100000
  wellen_def$tote_6079[i] <- tote_6079
  # dito nur ü80
  wellen_def$n_ue80[i] <- n_ue80
  wellen_def$inz7t_ue80[i] <- (n_ue80*7/31) / bev_ue80 * 100000
  wellen_def$tote_ue80[i] <- tote_ue80
  # Mittelwerte der Krankheitsfälle 
  wellen_def$krankheitsdauer_alle_median[i] <- median(tote_t)
  wellen_def$krankheitsdauer_alle_mean[i] <- mean(tote_t)
  wellen_def$krankheitsdauer_median[i] <- median(tote_t_e)
  wellen_def$krankheitsdauer_mean[i] <- mean(tote_t_e)
}

write.xlsx(wellen_def,
           paste0("daten/sterblichkeit-monate-",
                  today(),
                  ".xlsx"), overwrite=T)

# Erst mal bis hier. Weitere Analyse von Hand.
