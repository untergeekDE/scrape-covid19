# Baue die Tabelle mit den Bevölkerungszahlen für Hessen nach RKI-Einteilung.
# Grundlage ist Destatis-Tabelle 12411-02-04-02, die die Bevölkerung nach
# Lebensjahren enthält (je Land und je Kreis).
# 3.12.2021

msgTarget <- NULL

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}


# Bevölkerungstabelle für alle Bundesländer vorbereiten,
# Altersgruppen: 12-17, 18-59,60+.
# Ausgangspunkt ist der File aus GENESIS mit der Altersschichtung
# nach Lebensjahren und Bundesland. 
# Q: 12411-04-02-4, Stichtag 31.12.2020
if (!file.exists("index/pop.rda")) {
  # Gibt es die Länder-Datei mit den Bevölkerungsschichten schon?
  pop_df <- read_delim("index/12411-04-02-4.csv", 
                       delim = ";", escape_double = FALSE, 
                       locale = locale(date_names = "de",         
                                       decimal_mark = ",", grouping_mark = ".",
                                       encoding = "ISO-8859-1"), trim_ws=TRUE, 
                       skip = 6,
                       show_col_types=FALSE) %>%
    # Länder-ID und Altersgruppen insgesamt/männlich/weiblich nutzen
    select(id=1,Bundesland=2,ag=3,4:6) %>% 
    # Datei enthält auch "Insgesamt"-Zeilen mit den Ländersummen - weg damit
    filter(ag!="Insgesamt") %>% 
    # IDs auf Länder- und Kreis-
    filter((str_length(id)==2 &  str_detect(id,"[01][0-9]")) | str_length(id)==5)
  # Die lange 12411-04-02-04 auf die Angaben für die Bundesländer eindampfen
  pop_bl_df <- pop_df %>% filter(str_length(id)==2) 
  pop_kr_df <- pop_df %>% filter(str_length(id)==5)
  save(pop_bl_df,pop_kr_df,file = "index/pop.rda")
} else {
  load("index/pop.rda")
}

# Altersgruppen 
bev_bl_tab_df <- pop_bl_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
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
  filter(id == "06") %>% 
  mutate(Insgesamt = as.numeric(Insgesamt)) %>% 
  select(Altersgruppe,pop = Insgesamt) %>% 
  # Altersgruppen aufsummieren
  group_by(Altersgruppe) %>% 
  summarize(pop = sum(pop)) %>% 
  ungroup() %>% 
  # Prozentanteil an der Gesamtbevölkerung
  mutate(prozent = pop/sum(pop))

# Auf die AAA-Seite schreiben
sheet_write(bev_bl_tab_df,ss=aaa_id,sheet="AltersgruppenPop")

# kreise-index-pop-Datei anpassen - ist augenblicklich Stand 31.12.2020!

kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>% 
  # Tabelle mit den Kreis-Bevölkerungszahlen vorbereiten:
  # - id == 06xxx ausfiltern
  # - Hintere drei Zeichen der id als AGS
  left_join(pop_kr_df %>% 
              filter(str_detect(id,"06\\d\\d\\d")) %>%
              mutate(AGS = str_sub(id,3,5)) %>% 
              select(AGS,Insgesamt) %>% 
              mutate(Insgesamt = as.numeric(Insgesamt)) %>% 
              group_by(AGS) %>% 
              summarize(pop_n = sum(Insgesamt)) %>% 
              ungroup(),
            by = "AGS") %>% 
  # Neue Bevölkerungszahlen für die Kreise statt der alten
  mutate(pop = pop_n) %>%
  # Spalte mit den neuen Zahlen wieder wegwerfen
  select(-pop_n)
  
write.xlsx(kreise,"index/kreise-index-pop.xlsx",overwrite = T)
