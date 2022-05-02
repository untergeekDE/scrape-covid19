# rki-omikron-prognose.R ####
# Aufbereitung und Umrechnung der RKI-Omikron-Schätzung auf 
# hessische Verhältnisse
# 
# aktualisiert 21.3.2022 zur Frage: Warum traf die Prognose nicht ein?

library(data.table)

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# Das ist die Datei für den Verlauf der Neuinfektionen
scan_df <- fread("~/Downloads/de-omicron-wave-results/data/results_large_scan.csv")
# Diese Datei enthält alles
stoch_df <- fread("~/Downloads/de-omicron-wave-results/data/results_stoch.csv")

# Kurve für Hessen aus den GDOCs rausziehen
fallzahl_df <- read_sheet(aaa_id, sheet = "FallzahlVerlauf") %>% 
  select(datum,neu,inzidenz_gemeldet) %>% 
  mutate(datum = as_date(datum)) %>% 
  # 7-Tage-Mittel
  mutate(neu7t = (neu + 
                    lag(neu,1) +
                    lag(neu,2) +
                    lag(neu,3) +
                    lag(neu,4) +
                    lag(neu,5) +
                    lag(neu,6)) / 7) %>% 
  filter(datum >= min(stoch_df$date)) %>% 
  left_join(read_sheet(aaa_id,sheet="Krankenhauszahlen") %>% 
              select(datum = Bettenauslastung_Datum, 
                     icu = Intensivbettenauslastung_aktuell),
            by="datum")

# Das vom RKI gewählte Szenario für die ICUs
icu_df <- scan_df %>% 
  # ICU-Belegung
  filter(value_type == "icu") %>% 
  # keine Kontaktreduktion
  filter(contact_reduction_scenario_id == 0) %>% 
  filter(as.character(relative_risk_hospitalization) == "0.35") %>% 
  filter(relative_risk_icu == 0.15) %>% 
  filter(omicron_latent_period == 2) %>% 
  filter(infectious_period_both ==2) %>% 
  filter(booster_reach == "md") %>% 
  filter(booster_VE == "lo") %>% 
  mutate(date = as_date(date)) %>% 
  select(datum = date, icu_prog = value) 

# Annahme: Hessen entspricht im Prinzip dem Mittel im Rest der
# Republik, nur in zwei Punkten geändert: 
# - Umrechnung auf die Bevölkerungsanteile
# - Um einige Tage verschoben, weil Omikron früher hier war
#

load("index/pop.rda")
bev_hessen <- pop_bl_df %>% 
  filter(id == "06") %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                   as.numeric(str_extract(ag,"^[0-9]+")))) %>%
  mutate(Insgesamt = as.integer(Insgesamt)) %>% 
  pull(Insgesamt) %>% 
  sum()
bev_de <- pop_bl_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>%
  mutate(Insgesamt = as.integer(Insgesamt)) %>% 
  pull(Insgesamt) %>% 
  sum()
  
hessify <- function(i) {
  return(i/bev_de*bev_hessen)
}
# Zum Herausfinden des Verschiebe-Faktors eine
# Insgesamt-DF herstellen
match_df <- stoch_df %>% 
  select(datum=date,value=median) %>% 
  mutate(value = hessify(value)) %>% 
  mutate(datum = as_date(datum)) %>% 
  left_join(fallzahl_df %>% select(datum,neu), by="datum") 
write.xlsx(match_df,"daten/match_df.xlsx")  

# Hessen ist 4-5 Tage früher. 
# Okay, produziere Kurven für Hessen. 

# Alternatives Szenario
alt_df <- scan_df %>% 
  # Neufälle
  filter(value_type == "inc") %>% 
  # Kontaktreduktion -50% bis 15. Februar
  filter(contact_reduction_scenario_id == 2) %>% 
  # filter(as.character(relative_risk_hospitalization) == "0.35") %>% 
  # filter(relative_risk_icu == 0.15) %>% 
  filter(omicron_latent_period == 2) %>% 
  filter(infectious_period_both ==2) %>% 
  filter(booster_reach == "md") %>% 
  filter(booster_VE == "lo") %>% 
  mutate(date = as_date(date)) %>%
  mutate(value = hessify(value)) %>% 
  select(datum = date, neu7t_prog = value)

hessen_neu_prog <- stoch_df %>%
  filter(value_type=="inc") %>% 
  # Kurve um 4 Tage nach vorn verschieben
  mutate(datum = as_date(date)-4) %>% 
  select(-value_type,-date) %>% 
  mutate_at(vars(-"datum"),hessify) %>% 
  # Daten auf Hessen 
  left_join(fallzahl_df %>% select(datum,neu7t), by="datum") %>% 
  filter(datum >= as_date("2022-01-01"))

hessen_alt_prog <- alt_df %>% 
  mutate(datum = as_date(datum)-4) %>% 
  # Daten auf Hessen 
  left_join(fallzahl_df %>% select(datum,neu7t), by="datum") %>% 
  filter(datum >= as_date("2022-01-01"))


hessen_icu_prog <- stoch_df %>%
  filter(value_type=="icu") %>% 
  mutate(datum = as_date(date)-4) %>% 
  select(-value_type,-date) %>% 
  mutate_at(vars(-"datum"), hessify) %>% 
  left_join(fallzahl_df %>% select(datum,icu),by="datum") %>% 
  filter(datum >= as_date("2022-01-01"))
  

write.xlsx(hessen_neu_prog,"daten/rki-omikron-neu.xlsx",overwrite=T)
write.xlsx(hessen_alt_prog,"daten/rki-omikron-alt.xlsx",overwrite=T)
write.xlsx(hessen_icu_prog,"daten/rki-omikron-icu.xlsx",overwrite=T)
