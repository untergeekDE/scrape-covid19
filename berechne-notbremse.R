###################### berechne-notbremse.R ######################
# Wird vom hessen-zahlen-aufbereiten.R aufgerufen
#
# Stand: 22.7.2021

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
# Jeweils aktuelle Bevölkerungszahlen; zuletzt aktualisiert Juli 2020
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000",""))) %>%
  select(AGS,kreis,pop,GA_link) %>%
  arrange(kreis)



# ---- Vektor mit den gesetzlichen Feiertagen ----

msg("Feiertage lesen...")
feiertage <- (read_csv2("index/feiertage-hessen.csv") %>%
                mutate(Tag = as_date(Tag,format="%d.%m.%Y")))$Tag

# Archivierte Inzidenzen einlesen, Feiertage markieren
# (Die Tabelle mit den archivierten Inzidenzen legt 
# hessen-zahlen-aufbereiten.R an.)

bnb_erster_tag <- as_date("2021-04-24")
inz_archiv_df <- read_sheet(ss=aaa_id, sheet="ArchivInzidenzGemeldet") %>% 
  filter(as_date(datum) >= bnb_erster_tag)  %>%      
  # Erster Tag der BNB: Sa. 24.4.2021
  # Feiertage und Sonntage markieren
  mutate(feiertag = (wday(datum) == 1) | 
           (as_date(datum) %in% feiertage)) 


# Lange Tabelle der Kreise
inz_work_all_df <- inz_archiv_df %>% 
  pivot_longer(cols = -c("datum","feiertag"),names_to="kreis",
               values_to="inz") %>% 
  mutate(status = "") %>%
  arrange(kreis)


msg("Notbremsen-Tabelle errechnen...")

bnb_nachrechnen <- TRUE
#---- historisch: Bundesnotbremse und Lockerungsstufe 1 und 2 ----
# Baut die Tabelle mit den Maßnahmen in den einzelnen Kreisen nochmal auf. 
# Hört am letzten Tag der BNB auf (30.6.2021) - danach waren alle 
# Kreise bis zum 22.7. in der Stufe 2. 

if (bnb_nachrechnen) {
  # Letzter Gültigkeitstag der Stufenregelungen
  bnb_letzter_tag <- as_date("2021-06-30")
  inz_bnb_df <- inz_work_all_df %>% 
    filter(datum <= bnb_letzter_tag)
  tage <- as.integer(bnb_letzter_tag - bnb_erster_tag + 1)
  
  
  for (k in kreise$kreis) {
    # Aus den Inzidenzen für den jeweiligen Kreis einen Vektor machen
    # Auswahl über den Kreisnamen (also: den Spaltennamen), deshalb
    # funktioniert es unabhängig von der Sortierung. 
    vector = inz_bnb_df %>% filter(kreis==k) %>% 
      pull(inz)
    # Zählvariablen für die State Machine
    über100 = 0
    über150 = 0
    über165 = 0
    unter100w = 0
    unter150w = 0
    unter165w = 0 
    unter50 = 0
    unter100= 0 
    status="Stufe 1" 
    countdown = 0 # Vom Auslösen einer Maßnahme bis zur Umsetzung
    # Jetzt gehe die Tage der Reihe nach durch, vom letzten bis heute
    for (i in 1:tage) {
      # In Datum umwandeln
      d <- as_date(bnb_erster_tag-1+i)
      v <- vector[i]  # aktueller Wert, Kurzschreibweise
      # Maßnahme schon ausgelöst, aber noch nicht inkraft?
      if (countdown > 0) {
        countdown = countdown - 1
        if (countdown == 0) status <- status %>% 
            # Wurde Stufe gerade aufgehoben? Weg damit. 
            str_replace("<.>","") %>%
            str_replace_all("[<>]","") 
      }
      # Notbremsen-Stufe ausgelöst?
      
      # Schauen: sind wir 3 Tage über der Grenze?
      # Wenn wir unter der Grenze sind, Zähler zurücksetzen. 
      
      if (v >= 100) {
        über100 = über100+1
        if (v >= 150)  {
          über150 = über150+1 
          if (v >= 165) über165 = über165+1 else über165=0
        } else über150=0
      } else über100 = 0
      
      # Unter 50? Kann Stufe 2 auslösen, wenn, HMSI-Zitat: 
      #   - die Bundesnotbremse vor dem 17.05.2021 außer Kraft getreten ist 
      #     und an weiteren 14 Tagen in Folge die Inzidenz unter 100 liegt 
      #     ab dem nächsten Tag oder sobald die Inzidenz fünf Tage in Folge 
      #     unter 50 liegt ab dem nächsten Tag.
      #   - die Inzidenz in Stufe 1 weitere 14 Tage in Folge unter 100 ist 
      #     ab dem nächsten Tag oder sobald die Inzidenz fünf Tage in Folge 
      #     unter 50 liegt ab dem nächsten Tag (Fälle, in denen die 
      #     Bundesnotbremse am 17.05.2021 oder nach dem 17.05.2021 außer Kraft getreten ist).
      
      # Anmerkung: Die hessische Stufe 2 kann laut Aussage des HMSI
      # nur durch erneute Auslösung der BNB beendet werden. 
      # Es ist also kein Trigger Stufe2 -> Stufe1 erforderlich. 
      
      if (v < 50) unter50 = unter50+1 else unter50 = 0
      if (v < 100) unter100 = unter100+1 else unter100=0
      if (d == as_date("2021-05-16") ) { 
        if (!str_detect(status,"tufe")) {
          unter50 = 0
          unter100 = 0
        }
      } 
      
      # Für Unterschreitung der BNB-Grenzen nur Werktage zählen.
      # 5 WT in Folge unter der Grenze löst Unterschreitung aus. 
      # Wenn ein Tag über der Grenze ist, sind wir wieder drüber.
      if (!inz_archiv_df$feiertag[i]){
        if (v < 165) {
          unter165w = unter165w+1
          if (v < 150) {
            unter150w = unter150w+1 
            if (v < 100) unter100w = unter100w+1 else unter100w=0
          } else unter150w=0
        } else unter165w = 0 
        
      }
      
      # Auswertung der Zähler
      
      # 3 Tage hintereinander über einer Grenze? Triggere Status.
      # Der höchste kommt zuletzt, hat also Priorität.
      if (über100 == 3 ) {
        status=">A<" # bnb100 ausgelöst
        countdown=2 # gilt ab dem übernächsten Tag
      }
      if (über150 == 3) {
        status="A>G<" # bnb150 ausgelöst
        countdown = 2
      } 
      if (über165 == 3) {
        status="AG>S<" # bnb165 ausgelöst
        countdown = 2
      }  
      
      # 5 Wochentage unter einer Grenze? Setze aus. 
      # Die niedrigste kommt zuletzt, hat also Priorität. 
      if (status == ("AGS") & unter165w == 5) {
        status = "AG<S>"
        countdown = 2
      }
      if (status %in% c("AGS","AG","AG<S>") & unter150w == 5) {
        status = "A<G>"
        countdown = 2      
      }
      # Bundesnotbremse aktiv, und 5 Werktage unter 100? 
      if (str_detect(status,"^A") & unter100w == 5) {
        status = "<A>>Stufe 1<"
        countdown = 2 
        unter100 = 1 - countdown # Reset Tageszählung Stufe 1
        unter50 = 1 - countdown
      }
      if (status %in% c("Stufe 1","<A>>Stufe 1<") & (unter50 == 5 | unter100 == 14)) {
        status = "Stufe >2<"
        countdown = 1
      }
      # Tagestabelle ergänzen
      inz_bnb_df$status[inz_bnb_df$datum == as_date(d) &
                           inz_bnb_df$kreis == k] <- 
        ifelse(status=="Stufe 1",paste0(status,"(",unter100,")"),status)
      
    }
  }
  
  
  
  # Tabellen ablegen - neu: leg sie ins 
  # ohnehin schon da: 
  # sheet_write(inz_archiv_df,ss=sperren_id,sheet="Sperren-Inzidenz-Tabelle")
  
} # Ende BNB-Tabelle nachbauen


# ---- Tabelle bis zum heutigen Tag nachbauen  ----
inz_work_df <- inz_work_all_df %>% 
  filter(datum > bnb_letzter_tag)

# Das ist diesmal relativ einfach: 
# Wenn inz > Grenzwert und...
# - gestern schon inz > Grenzwert: -> 35er-Stufe
# - sonst: <35er-Stufe>
#
# Wenn inz <= Grenzwert und...
# - die letzten 5 Tage unter Grenzwert: -> nix ändern 
# - in den letzten 5 Tagen inz>Grenzwert: 35er-Stufe<.>

inz_work_df$status <- ""
# Erster Tag der Gültigkeit des Eskalationskonzepts vom 19.7.2021
he_erster_tag <- as_date("2021-07-22")
# Grenzwerte 
for (g in c(35,50,100)) {
  for (k in kreise$kreis) {
    inz_work_df <- inz_work_df %>% 
      mutate(status =
               # Nur, wenn Datum nicht vor dem ersten Tag der Wirksamkeit
               ifelse(datum < he_erster_tag,status,
                      ifelse(inz>g,
                          # Inzidenz über der Grenze? 
                          ifelse((lag(inz)>g),
                                 paste0(g,"er-Stufe"),
                                 paste0("<",g,"er-Stufe>")),
                          # Inzidenz unter der Grenze?
                          # Wenn in den letzten 5 Tagen durchgängig
                          # unter der Grenze: Stufe mit Aufhebungs-Zeichen,
                          # sonst: Wert behalten
                          ifelse((lag(inz)>g |
                                   lag(inz,2)>g |
                                   lag(inz,3)>g |
                                   lag(inz,4)>g |
                                   lag(inz,5)>g),
                                 paste0(g,"er-Stufe<.>"),
                                 status))))
  }
}
# ---- Ausgabe in Archivtabelle ----

# Tabelle vom Langformat ins Querformat ausdehnen
inz_status_df <- bind_rows(inz_bnb_df,inz_work_df) %>% 
  select(datum,kreis,status) %>%
  pivot_wider(names_from=kreis,values_from=status)

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
