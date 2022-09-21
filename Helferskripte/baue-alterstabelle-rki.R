# Baue die Tabelle mit den Bevölkerungszahlen für Hessen nach RKI-Einteilung.
# Grundlage war Destatis-Tabelle 12411-02-04-02, die die Bevölkerung nach
# Lebensjahren enthält (je Land und je Kreis).
# Da diese Zahlen zur Umstellung am 21.9.2022 noch nicht vorliegen, nimm
# die Tabelle des Landes von https://statistik.hessen.de/zahlen-fakten/bevoelkerung-gebiet-haushalte-familien/bevoelkerung/tabellen
#
# 20.9.2022

msgTarget <- NULL

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")

# Dieser Code ist 1:1 ins Impf-Skript rübergewandert. Den sparen wir uns jetzt. 
# if (!file.exists("index/pop.rda")) {
#   # Gibt es die Länder-Datei mit den Bevölkerungsschichten schon?
#   pop_df <- read_delim("index/12411-04-02-4.csv", 
#                        delim = ";", escape_double = FALSE, 
#                        locale = locale(date_names = "de",         
#                                        decimal_mark = ",", grouping_mark = ".",
#                                        encoding = "ISO-8859-1"), trim_ws=TRUE, 
#                        skip = 6,
#                        show_col_types=FALSE) %>%
#     # Länder-ID und Altersgruppen insgesamt/männlich/weiblich nutzen
#     select(id=1,Bundesland=2,ag=3,4:6) %>% 
#     # Datei enthält auch "Insgesamt"-Zeilen mit den Ländersummen - weg damit
#     filter(ag!="Insgesamt") %>% 
#     # IDs auf Länder- und Kreis-
#     filter((str_length(id)==2 &  str_detect(id,"[01][0-9]")) | str_length(id)==5)
#   # Die lange 12411-04-02-04 auf die Angaben für die Bundesländer eindampfen
#   pop_bl_df <- pop_df %>% filter(str_length(id)==2) 
#   pop_kr_df <- pop_df %>% filter(str_length(id)==5)
#   save(pop_bl_df,pop_kr_df,file = "index/pop.rda")
# } else {
#   load("index/pop.rda")
# }


# if (!file.exists("index/pop.rda")) {

# Lies die Landestabelle ein
he_ag2021_k_df <- read_csv("../index/he_Altersstruktur_Bevoelkerung_Kreise_2021.csv", 
                           locale = locale(date_names = "de"), skip = 2) %>% 
  select(ags = 1, ag = 2, Insgesamt = 3) %>%
  # Sonderding für Vogelsberg, bei dem "535 Vogelsberg" in der Tabelle steht. Grrr.
  mutate(ags = str_sub(ags,1,3)) %>% 
  # Spalten auffüllen:
  # Wenn eine Zahl, in AGS-String umwandeln. (06...)
  # Wenn keine Zahl, leeren - damit es im nächsten Schritt aufgefüllt werden kann. 
  mutate(ags = ifelse(is.na(as.numeric(ags)),
                      NA,
                      paste0("06",ags))) %>% 
  fill(ags, .direction = "down") %>% 
  filter(ag != "Insgesamt") %>% 
  mutate(ag = ifelse(str_detect(ag,"^[Uu]nter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) 

# Jetzt habe ich eine Tabelle, die für alle Kreise die Bevölkerung nach Altersjahren in Hessen nennt
# ags - Kreis-AGS
# ag  - Altersgruppe "von"

t12411_0012_2021_df <- read_delim("../index/12411-0012.csv", 
              delim = ";", escape_double = FALSE, 
              locale = locale(date_names = "de", encoding = "ISO-8859-1"), trim_ws = TRUE, 
              skip = 5) %>% 
  # Spalte mit dem Stichtag raus
  select(-1) %>% 
  rename(ag = 1) %>% 
  pivot_longer(cols = -ag, names_to = "bundesland", values_to = "n") %>% 
  group_by(bundesland) %>% 
  filter(!is.na(ag)) %>% 
  filter(ag!="Insgesamt")

# Altersgruppen 
bev_bl_tab_df <- t12411_0012_2021_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^[Uu]nter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # Zusätzliche Variable mit der Altersgruppe
  mutate(Altersgruppe = case_when(
    ag < 5 ~ "A00-A04",
    ag < 15 ~ "A05-A14",
    ag < 35 ~ "A15-A34",
    ag < 60 ~ "A35-A59",
    ag < 80 ~"A60-A79",
    TRUE    ~ "A80+")) %>%
  # nur Hessen
  filter(bundesland == "Hessen") %>%
  select(Altersgruppe,pop = n) %>% 
  # Altersgruppen aufsummieren
  group_by(Altersgruppe) %>% 
  summarize(pop = sum(pop)) %>% 
  ungroup() %>% 
  # Prozentanteil an der Gesamtbevölkerung
  mutate(prozent = pop/sum(pop))

# Auf die AAA-Seite schreiben
sheet_write(bev_bl_tab_df,ss=aaa_id,sheet="AltersgruppenPop")

# BL-Tabelle bauen
load("../index/pop.rda")

bl_df <- pop_bl_df %>% 
  mutate(bundesland = str_replace(Name,", L.+","")) %>% 
  distinct(id,bundesland) 

write.xlsx(bl_df, "../index/bltabelle.xlsx", overwrite = T)
write_csv2(bl_df,"bltabelle.csv")

# kreise-index-pop-Datei anpassen - ist augenblicklich Stand 31.12.2020!

kreise <- read.xlsx("../index/kreise-index-pop.xlsx") %>% 
  # Tabelle mit den Kreis-Bevölkerungszahlen vorbereiten:
  # - id == 06xxx ausfiltern
  # - Hintere drei Zeichen der id als AGS
  left_join(he_ag2021_k_df %>% 
              mutate(ags = str_sub(ags,3,5)) %>% 
              select(AGS = ags,Insgesamt) %>% 
              group_by(AGS) %>% 
              summarize(pop_n = sum(Insgesamt)) %>% 
              ungroup(),
            by = "AGS") %>% 
  # Neue Bevölkerungszahlen für die Kreise statt der alten
  mutate(pop = pop_n) %>%
  # Spalte mit den neuen Zahlen wieder wegwerfen
  select(-pop_n)
  
write.xlsx(kreise,"../index/kreise-index-pop.xlsx",overwrite = T)
