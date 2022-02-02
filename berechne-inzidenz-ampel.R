###################### berechne-inzidenz-ampel.R ######################
# Wird vom hessen-zahlen-aufbereiten.R aufgerufen
#
# berechne-notbremse.R, aber ohne den historischen Kram - 
# berechnet die tagesaktuelle Tabelle und setzt "HOTSPOT"-Marker, 
# wenn 3 Tage hintereinander über der 350er Grenze 
# (Löschung nach 5 Tagen drunter)
#
# Der AAA-Daten-Tab "Sperren-Status-Tabelle" wird fortgeschrieben,
# aber nicht mehr neu aufgebaut (was der alte Code immer getan hatte)
#
# Stand: 25.1.22

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!


if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# ---- Archiv vorige 7 Tage auswerten, Inzidenztabelle bauen ----
# Aus den Kreisdaten mit der Population erst mal ein df anlegen

msg("Lies index/kreise-index-pop.xlsx","...")
# Jeweils aktuelle Bevölkerungszahlen; zuletzt aktualisiert 2021
# 
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000",""))) %>%
  select(AGS,kreis,pop,GA_link) %>%
  arrange(kreis)


# Archivierte Inzidenzen einlesen, Feiertage markieren
# (Die Tabelle mit den archivierten Inzidenzen legt 
# hessen-zahlen-aufbereiten.R an.)

# Filtere alles außer den letzten 14 Tagen weg.
# Eigentlich bräuchten wir nur 7 Tage +5, aber 14 ist so schön rund.

ampel_erster_tag <- today()-14
inz_archiv_df <- read_sheet(ss=aaa_id, sheet="ArchivInzidenzGemeldet") %>% 
  filter(as_date(datum) >= ampel_erster_tag)  

# Lange Tabelle der Kreise
inz_work_df <- inz_archiv_df %>% 
  pivot_longer(cols = -c("datum"),names_to="kreis",
               values_to="inz") %>% 
  mutate(status = "") %>%
  arrange(kreis)


msg("Inzidenz-Ampel-Tabelle errechnen...")


# ---- Tabelle bis zum heutigen Tag nachbauen  ----

# Wenn inz > Grenzwert und...
# - gestern und vorgestern schon inz > Grenzwert: -> Hotspot
# - sonst: <über 350>
#
# Wenn inz <= Grenzwert und...
# - die letzten 5 Tage unter Grenzwert: -> nix ändern 
# - in den letzten 5 Tagen inz>Grenzwert: HOTSPOT<.>

inz_work_df$status <- ""
# Grenzwerte 
# Ergänzt am 3.12.2021 um den Grenzwert 350 - und eine Regel, die die Kreise dann als
# "Hotspot" benennt

# Der alte Code steppte durch verschiedene Werte von g durch.
# Hier brauchen wir derzeit nur einen: Inzidenz von 350 als Grenzwert. 

g <- 350
for (k in kreise$kreis) {
  inz_work_df <- inz_work_df %>% 
    mutate(status = ifelse(inz>g,
                           # Inzidenz über der Grenze?
                           
                          # Inzidenz auch gestern und vorgestern
                          # schon über der Grenze? 
                          ifelse((lag(inz)>g) & (lag(inz,2)>g),
                                 # Schauen: war der Kreis schon Hotspot?
                                 # Oder wird er grad erst?
                                 ifelse(lag(inz,3)>g,"HOTSPOT",">HOTSPOT<"),
                                 paste0("<",g," erreicht>")),
                          # Inzidenz unter der Grenze?
                          # Wenn in den letzten 5 Tagen durchgängig
                          # unter der Grenze: Stufe mit Aufhebungs-Zeichen,
                          # sonst: Wert behalten
                          ifelse((lag(inz)>g |
                                   lag(inz,2)>g |
                                   lag(inz,3)>g |
                                   lag(inz,4)>g |
                                   lag(inz,5)>g),
                                 paste0("<HOTSPOT>"),
                                 status)))   
  }

# ---- Ausgabe in Archivtabelle ----



#  Neu erstellte Tabelle vom Langformat ins Querformat ausdehnen
inz_work2_df <- inz_work_df %>% 
  filter(datum >= as_date(max(datum))-7) %>%
  select(datum,kreis,status) %>%
  pivot_wider(names_from=kreis,values_from=status) 

# Alte Tabelle einlesen, neue Tabellenzeilen dranhängen
inz_status_df <- read_sheet(ss=aaa_id,sheet="Sperren-Status-Tabelle") %>% 
  mutate(datum = as_date(datum)) %>% 
  filter(datum < ampel_erster_tag+7) %>% 
  bind_rows(inz_work2_df)

# Historisch gab es noch ein Sheet 
# sperren_id <- "1zdR1rDOt0H6THUke_W6Hb-Lt3luzh0ZJtBPxvf3cZbI"
# Wird nicht mehr benötigt. 

sheet_write(inz_status_df,ss=aaa_id,sheet="Sperren-Status-Tabelle")


# ---- Ausgabe der Block-Tabelle im Datawrapper ----
msg("Tabelle für Ausgabe vorbereiten...")
inz_dw_df <- inz_work_df %>% 
  # letzte 7 Tage
  filter(datum > as_date(max(datum))-7) %>%
  mutate(tag = lubridate::wday(as_date(datum),label=TRUE,abbr=TRUE,locale="de_DE")) %>%
  select(kreis,tag,inz) %>%
  pivot_wider(names_from=tag,values_from=inz) %>% 
  left_join(inz_work_df %>% 
              filter(datum == as_date(max(datum))) %>%
              select(kreis,text = status),by="kreis") %>% 
  # Links der Gesundheitsämter noch drankleben
  left_join(kreise %>% select(kreis,Infolink = GA_link),by="kreis") %>%
  # Nur noch allgemeine Maßnahmen gültig.
  mutate(text = paste0(text,"<br>","<a href=\'",
                       Infolink,
                       "\' target=\'_blank\'>",
                       "[Link]",
                       "</a>")) %>%
  select(-Infolink) %>%
  arrange(kreis)



msg("Schreibe Tabellen in die Grafik...")
# Daten schreiben, einmal direkt...
dw_data_to_chart(inz_dw_df,chart_id="psn2l")
# ...einmal ins Google Sheet. 
dw_publish_chart(chart_id="psn2l")

msg("OK!")
