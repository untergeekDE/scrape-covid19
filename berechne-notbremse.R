###################### berechne-notbremse.R ######################
# Wird vom hessen-zahlen-aufbereiten.R aufgerufen
#
# Stand: 24.5.2021



# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!


if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# ---- Archiv vorige 7 Tage auswerten, Inzidenztabelle bauen ----

path <- "./archiv/"

# Beschreibung der Archivdaten
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
  AnzahlGenesen = col_double(),
  IstErkrankungsbeginn = col_double(),
  Altersgruppe2 = col_character()
)

# Aus den Kreisdaten mit der Population erst mal ein df anlegen

msg("Lies index/kreise-index-pop.xlsx","...")
# Jeweils aktuelle Bevölkerungszahlen; zuletzt aktualisiert Juli 2020
kreise <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000",""))) %>%
  select(AGS,kreis,pop) %>%
  arrange(kreis)


# Daten holen - werden von Skript hessen-daten-aufbereiten.R in 
# ein Google Doc geschrieben. 

# ---- Vektor mit den gesetzlichen Feiertagen ----

msg("Feiertage lesen...")
feiertage <- (read_csv2("index/feiertage-hessen.csv") %>%
                mutate(Tag = as_date(Tag,format="%d.%m.%Y")))$Tag

# Archiv anlegen, Feiertage markieren

inz_archiv_df <- read_sheet(ss=aaa_id, sheet="ArchivInzidenzGemeldet") %>% 
  filter(as_date(datum) > as_date("2021-04-23"))  %>%      
  # Erster Tag der BNB: Sa. 24.4.2021
  # Feiertage und Sonntage markieren
  mutate(feiertag = (wday(datum) == 1) | 
           (as_date(datum) %in% feiertage)) 


# Lange Tabelle der Kreise
inz_work_df <- inz_archiv_df %>% 
  pivot_longer(cols = -c("datum","feiertag"),names_to="kreis") %>% 
  mutate(status = NA) %>%
  arrange(kreis)

tage <- nrow(inz_archiv_df)

msg("Notbremsen-Tabelle lesen...")
for (k in kreise$kreis) {
  # Aus den Inzidenzen für den jeweiligen Kreis einen Vektor machen
  # Auswahl über den Kreisnamen (also: den Spaltennamen), deshalb
  # funktioniert es unabhängig von der Sortierung. 
  vector = pull(inz_archiv_df,var = !!k)
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
    d <- as_date(today()-tage+i)
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
  inz_work_df$status[inz_work_df$datum == as_date(d) &
                       inz_work_df$kreis == k] <- 
    ifelse(status=="Stufe 1",paste0(status,"(",unter100,")"),status)
    
  }
}

sperren_id <- "1zdR1rDOt0H6THUke_W6Hb-Lt3luzh0ZJtBPxvf3cZbI"

msg("Tabelle für Ausgabe vorbereiten...")
inz_df <- inz_work_df %>% 
  # letzte 7 Tage
  filter(datum > as_date(max(datum))-7) %>%
  mutate(tag = lubridate::wday(as_date(datum),label=TRUE,abbr=TRUE,locale="de_DE")) %>%
  select(kreis,tag,value) %>%
  pivot_wider(names_from=tag,values_from=value) %>% 
  left_join(inz_work_df %>% 
              filter(datum == as_date(max(datum))) %>%
              select(kreis,text = status),by="kreis") %>% 
  # Links der Gesundheitsämter noch drankleben
  left_join(read_sheet(ss=sperren_id,sheet="Ausgangssperren") %>% 
              mutate(Infolink = ifelse(!is.na(Infolink),Infolink,Gesundheitsamt)) %>%
              select(kreis,Infolink),by="kreis") %>%
  # 30.6.2021: Bundesnotbremse läuft aus. 
  # Nur noch allgemeine Maßnahmen gültig.
  # Einstweilen: String "Stufe 2" einfach rausnehmen.
  mutate(text = str_replace(text,"Stufe 2","")) %>%
  mutate(text = paste0(text,"<br>","<a href=\'",
                        Infolink,
                        "\' target=\'_blank\'>",
                        "[Link]",
                        "</a>")) %>%
  select(-Infolink) %>%
  arrange(kreis)

inz_status_df <- inz_work_df %>%
  select(datum,kreis,status) %>%
  pivot_wider(names_from=kreis,values_from=status)



# ---- Ausgabe ---- 
msg("Schreibe Tabellen in die Grafik...")
# Daten schreiben, einmal direkt...
dw_data_to_chart(inz_df,chart_id="psn2l")
# ...einmal ins Google Sheet. 
sheet_write(inz_df,ss=sperren_id,sheet="Sperren-Tabelle")
sheet_write(inz_status_df,ss=sperren_id,sheet="Sperren-Status-Tabelle")
sheet_write(inz_archiv_df,ss=sperren_id,sheet="Sperren-Inzidenz-Tabelle")

dw_publish_chart(chart_id="psn2l")

msg("OK!")