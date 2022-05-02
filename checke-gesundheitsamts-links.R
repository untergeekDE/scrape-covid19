#### checke-gesundheitsamts-links.R ####
# Kurzes Skript, das die Links aus dem Index-File abgrast und 
# schaut, ob einer einen 404er-Fehler wirft - dann schickt es eine
# Teams-Nachricht zum zerbrochenen Link. 

# Stand: 10.1.2022

# Todo: Mit rvest auf veränderte Seitenstruktur checken. 

# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- NULL

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")

# Die R-Scraping-Library
library(rvest)
# MS-Teams-Messaging-Library
library(teamr)

# Funktion, um Fehler und Warnungen zu werfen



# Vorbereitung: Index-Datei einlesen; enthält Kreise/AGS und Bevölkerungszahlen
msg("Lies index/kreise-index-pop.xlsx","...")
# Jeweils aktuelle Bevölkerungszahlen; zuletzt aktualisiert Juli 2020
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))


link_tab <- NULL
for (link in kreise$GA_link) {
  x <- httr::GET(link)
  link_tab <- bind_rows(link_tab, tibble(s = httr::http_status(x)$category,
                          err = httr::http_status(x)$reason,
                          link = link))
}

ll <- link_tab %>% filter(err!="OK") %>%
  left_join(kreise %>% select(link=GA_link,kreis),by="link") %>% 
  select(kreis,err,link) 
if (nrow(ll)>0) {
  # Teams-Meldung absetzen
  cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
  cc$title(paste0("checke-gesundheitsamts-links.R",with_tz(now(),"Europe/Berlin")))
  cc$text("<h4>Problematische Links gefunden</h4>")
  sec <- card_section$new()
  for (i in 1:nrow(ll)) {
    # Oldschool-R mit Zeilennummer - what the hell
    sec$add_fact(
      ll$kreis[i],
      ll$link[i])
  }
  cc$add_section(new_section = sec)
  cc$send()
  msg(nrow(ll)," kaputte Links gefunden!")
} else {
  msg("OK, keine Probleme gefunden")
}


