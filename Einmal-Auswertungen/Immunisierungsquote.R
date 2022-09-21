# Kurzes Skript, das ausrechnet, wie hoch der Anteil der
# Bevölkerung sein könnte, die bereits eine BA.5-Infektion
# überstanden hat:
# - Hole Anteile BA.5 vom RKI
# - Nimm Infektions-Meldezahlen
# - Addiere Dunkelziffer, nimm BA.5-Anteil, summiere auf
#
# 17.9.2022 - für Artikel 18.9.


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

# Parameter: 

dunkelziffer <- 2 # Auf einen gefundenen kommt ein unentdeckter Fall
immunwochen <- 26 # Drei Monate gut vor Reinfektion geschützt
wellenbeginn <- as_date("2022-05-01")
hessen <- 6293154

pacman::p_load(RcppRoll)

# Mutationsdaten holen 
voc_url = "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/VOC_VOI_Tabelle.xlsx?__blob=publicationFile"

voc_df <- read.xlsx(voc_url,sheet="VOC") %>% 
  # Spalte 19 enthält BA5-Anteile
  select(woche = 1, ba5_prozent = 19) %>%
  # Nur Jahr 2022
  filter(str_detect(woche,"^2022")) %>% 
  mutate(woche = as.integer(str_extract(woche,"[0-9][0-9]$"))) %>% 
  mutate(Datum = ymd("2022-01-02")+7*(woche-1))

# Daten holen


meldewochen_df <- read_sheet(aaa_id,sheet="NeufaelleAlterProzentWoche") %>% 
  select(Datum = Stichtag, summe) %>% 
  filter(Datum >= as_date("2022-01-01")) %>% 
  left_join(voc_df,by="Datum") %>% 
  fill(ba5_prozent) %>% 
  mutate(real = (summe * ba5_prozent /100) * dunkelziffer) %>% 
  # roll_sum kriege ich derzeit nicht dazu, vor dem Fenster zu rechnen
  # mutate(immun = roll_sum(real,n = immunwochen, align="right", fill = NA)) %>% 
  mutate(immun = cumsum(real)) %>% 
  mutate(immun_prozent = immun / 62931.54) %>% 
  filter(Datum >= wellenbeginn)


# Export

write.xlsx(meldewochen_df,"daten/anteil_ueberstanden.xlsx", overwrite = T)
