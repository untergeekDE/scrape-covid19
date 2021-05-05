###################### berechne-notbremse.R ######################

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
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
inz_archiv_df <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000",""))) %>%
  select(AGS,kreis,pop)

# Letzte 10 Tage
# Warum 10? So sind auch mehrere Feiertage hintereinander kein Problem. 

start_date <- as_date(today()-8)
for (d in start_date:today()) { # Irritierenderweise ist d eine Integer-Zahl
  # Deshalb die Woche erst mal so berechnen
  # wk <- isoweek(as_date(d))
  file_name <- paste0(path,"rki-",as_date(d),".csv")
  if(file.exists(file_name)) {
    msg("Archiv-Inzidenzen vom ",as_date(d))
    tagesmeldung_df <- read_csv2(file_name,col_types = col_descr) %>%
      # Neufälle letzte 7 Tage aufsummieren
      mutate(datum = as_date(Meldedatum)) %>%
      # jeweiligen Tag - d - als Referenz nehmen
      filter(datum > as_date(d-8)) %>%
      # Auf die Summen filtern?
      filter(NeuerFall %in% c(0,1)) %>%
      select(AGS = IdLandkreis,AnzahlFall) %>%
      # Nach Kreis sortieren
      group_by(AGS) %>%
      #  pivot_wider(names_from = datum, values_from = AnzahlFall)
      # Summen für Fallzahl, Genesen, Todesfall bilden
      summarize(AnzahlFall = sum(AnzahlFall)) %>%
      ungroup() %>%
      select(AGS,neu7tage = AnzahlFall)
    # Wochentag aus Datum
    tag <- as.character(as_date(d))
    # wochentag = as.character(wday(as_date(d),label=TRUE,abbr=TRUE,locale="de_DE"))
    inz_archiv_df <- inz_archiv_df %>%
      left_join(tagesmeldung_df,by="AGS") %>%
      # Inzidenz berechnen
      mutate(neu7tage = neu7tage / pop * 100000) %>%
      rename(!!tag := neu7tage)
  } else {
    msg("!!!Archivdaten ",as_date(d)," fehlen")
  }
}

# AGS und pop aus der Tabelle schmeißen; nur Kreisname und Inzidenz nach Wochentag
inz_archiv_df <- inz_archiv_df %>%
  select(-AGS,-pop)

# ---- Gesetzliche Feiertage in Hessen ----

# Vektor
feiertage <- (read_csv2("index/feiertage-hessen.csv") %>%
  mutate(Tag = as_date(Tag,format="%d.%m.%Y")))$Tag


# ---- Für alle Kreise: Status bauen ----
# Denn wir zählen am Ende so: 
# - Anzahl der Tage oberhalb einer Grenze (ToG) 
# - Anzahl der Werktage unter einer Grenze (WTuG)
# und setzen folgenden Status:
# - aufgehoben: WToG
# - aktiv: ToG >= 3
# - tritt in Kraft: 5 > ToG >= 3
# - wird ausgesetzt: 7 > WToG >= 5

c <- ncol(inz_archiv_df)

# War irgendwann in den letzten 9 Tagen mal ein Trigger? 
activate_df <- inz_archiv_df %>%
  pivot_longer(cols = -kreis, names_to = "datum", values_to="inz7t") %>%
  mutate(t100 = (inz7t>100 & lag(inz7t)>100 & lag(inz7t,2)>100)) %>%
  mutate(t150 = (inz7t>150 & lag(inz7t)>150 & lag(inz7t,2)>150)) %>%
  mutate(t165 = (inz7t>165 & lag(inz7t)>165 & lag(inz7t,2)>165)) %>%
  group_by(kreis) %>%
  # War der Kreis in den letzten 9 Tagen irgendwann über der Grenze? ta
  # oder nur in den letzten 2 Tagen, also frischer Trigger? tt
  summarize(tt100 = last(t100) | nth(t100,length(t100)-1),
           ta100 = any(t100[1:(length(t100)-2)]),
           tt150 = last(t150) | nth(t150,length(t150)-1),
           ta150 = any(t150[1:(length(t150)-2)]),
           tt165 = last(t165) | nth(t165,length(t165)-1),
             ta165 = any(t165[1:(length(t165)-2)])) %>%
  ungroup() %>%
  # Wenn irgendwann in den letzten 14 Tagen der entsprechende Trigger, 
  # geh davon aus: aktiv. 
  # Rücksetzung im nächsten Schritt. 
  mutate(text100 = ifelse(ta100,"aktiv", 
                          ifelse(tt100,"kommen","aufgehoben"))) %>%
  mutate(text150 = ifelse(ta150,"aktiv", 
                          ifelse(tt150,"kommen","aufgehoben"))) %>%
  mutate(text165 = ifelse(ta165,"aktiv", 
                          ifelse(tt165,"kommen","aufgehoben"))) %>%
  select(kreis,text100,text150,text165)

deactivate_df <- inz_archiv_df %>%
  pivot_longer(cols = -kreis, names_to = "datum", values_to="inz7t") %>%
  filter(!((as_date(datum) %in% feiertage) | wday(datum)==1)) %>%
  filter(as_date(datum) > today()-7) %>%
  mutate(ut100 = (inz7t<100 & lag(inz7t)<100 & lag(inz7t,1)<100 & 
                    lag(inz7t,2) < 100 & lead(inz7t,3) < 100)) %>%
  mutate(ut150 = (inz7t<150 & lag(inz7t)<150 & lag(inz7t,1)<150 & 
                    lag(inz7t,2) < 150 & lead(inz7t,3) < 150)) %>%
  mutate(ut165 = (inz7t<165 & lag(inz7t)<165 & lag(inz7t,1)<165 & 
                    lag(inz7t,2) < 165 & lead(inz7t,3) < 165)) %>%
  group_by(kreis) %>%
  # War der Kreis in den letzten 14 Tagen irgendwann über der Grenze?
  summarize(ut100 = last(ut100),
            ut150 = last(ut150),
            ut165 = last(ut165)) %>%
  ungroup() 
  
inz_work_df <- inz_archiv_df %>%
  pivot_longer(cols=-kreis,names_to="datum",values_to="inz7t") %>%
  filter(as_date(datum) > today()-7) %>%
  mutate(datum = wday(datum,label=TRUE,abbr=TRUE,locale="de_DE")) %>%
  pivot_wider(values_from=inz7t,names_from=datum) %>%
  left_join(activate_df,by="kreis") %>%
  left_join(deactivate_df,by="kreis") %>%
  mutate(text100 = ifelse(text100!="aufgehoben" & ut100,"laufen aus",text100),
         text150 = ifelse(text150!="aufgehoben" & ut150,"laufen aus",text150),
         text165 = ifelse(text165!="aufgehoben" & ut165,"laufen aus",text165))

sperren_id <- "1zdR1rDOt0H6THUke_W6Hb-Lt3luzh0ZJtBPxvf3cZbI"
sperren_info_df <- read_sheet(ss=sperren_id,sheet="Ausgangssperren") %>% 
  mutate(Infolink = ifelse(!is.na(Infolink),Infolink,Gesundheitsamt)) %>%
  select(kreis,Infolink)



# ---- Ausgabe ---- 
inz7t_df <- inz_work_df %>%
  select(-ut100,-ut150,-ut165) %>%
  left_join(sperren_info_df,by="kreis") %>%
  mutate(Infos = paste0(ifelse(text100!="aufgehoben",
                               ifelse(text100=="aktiv","A","(A)"),""),
                        ifelse(text150!="aufgehoben",
                               ifelse(text150=="aktiv","/G","/(G)"),""),
                        ifelse(text165!="aufgehoben",
                               ifelse(text100=="aktiv","/S","/(S)"),""),
                        "<br>","<a href=\'",
                        Infolink,
                        "\' target=\'_blank\'>",
                        "[Link]",
                        "</a>")) %>%
  select(-Infolink)

# Daten schreiben, einmal direkt...
dw_data_to_chart(inz7t_df,chart_id="psn2l")
# ...einmal ins Google Sheet. 
sheet_write(inz7t_df,ss=sperren_id,sheet="Sperren-Tabelle")

dw_publish_chart(chart_id="psn2l")
