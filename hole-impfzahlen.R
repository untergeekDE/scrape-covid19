##################################### hole-impfzahlen.R #########################
# - Impfzahlen holen und ausgeben
# Dieses Skript ist ein Zwischenstand: Das RKI hat zwar jetzt ein Github-
# Repository für die Daten angelegt - das ist zuverlässiger, und die Daten
# sind vollständiger. Leider lässt sich ein Wert - die Quote der Geimpften nach
# Altersgruppe - aus den Daten nicht richtig angeben, weil sich die Logik der
# Zählung verändert hat. Deswegen muss dieses Skript einstweilen auch die alte
# Datenquelle auslesen, die Excel-Datei mit dem "Digitalen Impfquotenmonitoring auf rki.de -
# ...mit allen ihren Schwierigkeiten und Risiken. 

# Stand: 2.12.2021


# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- "B17:C17"

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
    select(id=1,Name=2,ag=3,4:6) %>% 
    # Datei enthält auch "Insgesamt"-Zeilen mit den Ländersummen - weg damit
    filter(ag!="Insgesamt") %>% 
    # IDs auf Länder- und Kreis-
    filter((str_length(id)==2 &  str_detect(id,"[01][0-9]")) | str_length(id)==5)
    # Die lange 12411-04-02-04 auf die Angaben für die Bundesländer eindampfen
  pop_bl_df <- pop_df %>% filter(str_length(id)==2) 
  pop_kr_df <- pop_df %>% filter(str_length(id)==5)
  save(pop_bl_df,pop_kr_df,file = "index/pop.rda")
} else {
  # vorbereitete Datenbank mit den Kreis- und Länder-Bevölkerungszahlen nach Altersjahren laden
  # enthält zwei Dataframes:
  # jeweils id | Name | ag | Insgesamt | männlich | weiblich 
  # - pop_bl_df
  # - pop_kr_df
  load("index/pop.rda")
}
  # Altersgruppen; 90 (die höchste) umfasst alle darüber. 
bev_bl_df <- pop_bl_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                    as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # Zusätzliche Variable mit der Altersgruppe
  mutate(Altersgruppe = case_when(
         ag < 12 ~ "u12",
         ag < 18 ~ "12-17",
         ag < 60 ~"18-59",
         TRUE    ~ "60+")) %>% 
  select(id,Bundesland=Name,Altersgruppe,Insgesamt) %>% 
  mutate(Insgesamt = as.numeric(Insgesamt)) %>% 
  # Tabelle bauen: Bevölkerung in der jeweiligen Altersgruppe nach BL,
  # Werte aus der Variable aufsummieren. 
  pivot_wider(names_from=Altersgruppe, values_from=Insgesamt,values_fn=sum) %>% 
  # Aus irgendwelchen Gründen enthält die GENESIS-Tabelle die Landesbezeichnung
  # "Baden-Württemberg, Land". Korrigiere das. 
  mutate(Bundesland = str_replace(Bundesland,"\\, Land$",""))
  
bev_kr_df <- pop_kr_df %>% 
  mutate(ag = ifelse(str_detect(ag,"^unter"),0,
                     as.numeric(str_extract(ag,"^[0-9]+")))) %>% 
  # Zusätzliche Variable mit der Altersgruppe
  mutate(Altersgruppe = case_when(
    ag < 12 ~ "u12",
    ag < 18 ~ "12-17",
    ag < 60 ~"18-59",
    TRUE    ~ "60+")) %>% 
  select(id,Kreis=Name,Altersgruppe,Insgesamt) %>% 
  filter(!is.na(as.numeric(Insgesamt))) %>% 
  mutate(Insgesamt = as.numeric(Insgesamt,na.rm=T)) %>% 
  # Tabelle bauen: Bevölkerung in der jeweiligen Altersgruppe nach BL,
  # Werte aus der Variable aufsummieren. 
  pivot_wider(names_from=Altersgruppe, values_from=Insgesamt,values_fn=sum)


# Bevölkerung nach Altersgruppen, Hessen
hessen=sum(bev_bl_df %>% filter(id=="06") %>% select(-id,-Bundesland))
ue60 = bev_bl_df %>% filter(id=="06") %>% pull(`60+`)
ue18_59 = bev_bl_df %>% filter(id=="06") %>% pull(`18-59`)
ue12_17 = bev_bl_df %>% filter(id=="06") %>% pull(`12-17`)
u60 = hessen - ue60
# Vergleiche https://corona-impfung.hessen.de/faq/impfstrategie
# Auskunft Innenministerium an Tobias Lübben 15.1.2021

# ---- Daten aus RKI-Github-Repository lesen und aufarbeiten ----
# Seit Juli 2021 werden die Daten nicht mehr als XLSX-Datei auf der
# Website veröffentlicht, sondern im Github-Repository des RKI. 

msg("Nach aktualisierten RKI-Impfdaten suchen")

git_user <- "robert-koch-institut"
git_repo <- "COVID-19-Impfungen_in_Deutschland"
git_path <- "Aktuell_Deutschland_Bundeslaender_COVID-19-Impfungen.csv"

# Wann war der letzte Commit des Github-Files? Das als Datum i_d
github_api_url <- paste0("https://api.github.com/repos/",
                         git_user,"/",git_repo,"/",
                         "commits?path=",
                         git_path)
github_data <- read_json(github_api_url, simplifyVector = TRUE)
i_d <- max(as_date(github_data$commit$committer$date))

tsi <- now()
# Noch keine Daten für heute? Warte, prüfe auf Abbruch; loope
while(i_d < today()) {
  # Abbruchbedingung: 8x3600 Sekunden vergangen?
  if (now() > tsi+(8*3600)) {
    msg("KEINE NEUEN DATEN GEFUNDEN")
    stop("Timeout")
  }
  # Warte 5 Minuten, suche wieder den letzten Commit
  Sys.sleep(300)
  github_data <- read_json(github_api_url, simplifyVector = TRUE)
  i_d <- max(as_date(github_data$commit$committer$date))
}

# Kurz piepsen; auf dem Server geht das natürlich schief, deshalb try. 
try(beepr::beep(2),silent=TRUE)
# Zeitstempel erneuern: Das ist das Abfragedatum
tsi <- now()
msg("Impfdaten vom ",i_d)

bl_tbl <- read_csv(paste0("https://raw.githubusercontent.com/",
                git_user,"/",git_repo,"/",
                "master/",
                "Aktuell_Deutschland_Bundeslaender_COVID-19-Impfungen.csv")) %>% 
  select(Datum=1,id=2,3,4,5)

lk_tbl <- read_csv(paste0("https://raw.githubusercontent.com/",
                          git_user,"/",git_repo,"/",
                          "master/",
                          "Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv")) %>% 
  select(Datum=1,id_lk = 2,3,4,5)

# Letzten Datenstand festhalten
i_d_max <- max(bl_tbl$Datum)

# XLSX-Tabelle dazuholen 

rki_xlsx_url <- "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquotenmonitoring.xlsx?__blob=publicationFile"
# Impfquoten nach Altersgruppe
tabelle2 <- read.xlsx(rki_xlsx_url,sheet=2)
# Impfquoten nach Impfstoff
tabelle3 <- read.xlsx(rki_xlsx_url,sheet=3)
# Zeitreihe Impfungen bundesweit
tabelle4 <- read.xlsx(rki_xlsx_url,sheet=4)

# Tabelle 2 aufarbeiten - Spaltennamen letztmals angepasst 9.9.2021
impfquoten_xlsx_df <- tabelle2 %>% 
                                   select(ID=1,
                                          impfdosen = 3, 
                                          quote_erst = 7,
                                          quote_erst_u18 = 8,
                                          quote_erst_18_60 =10, 
                                          quote_erst_ue60 = 11,
                                          quote_zweit = 12,
                                          quote_zweit_u18 = 13,
                                          quote_zweit_18_60 = 15,
                                          quote_zweit_ue60 =16,
                                          quote_dritt_u18 = 18,
                                          quote_dritt_18_60 = 20,
                                          quote_dritt_ue60=21) %>%
                                   filter(as.numeric(ID) %in% 1:16)

# ---- Länder-Vergleichstabelle erstellen ----
# Erstes Teil-DF: Absolute Summen nach Impfstoff je Land

msg("Baue Ländertabelle absolute Zahlen nach Impfstoff...")
bl_1_df <- bl_tbl %>% 
  # Tabelle Erst- und Zweitimpfungen nach Land und Impfstoff - Summen
  pivot_wider(names_from=c(Impfstoff,Impfserie),names_sep="_",values_from=Anzahl) %>% 
  group_by(id) %>% 
  summarize(biontech_erst = sum(Comirnaty_1,na.rm=TRUE),
            moderna_erst = sum(Moderna_1,na.rm=TRUE),
            az_erst = sum(AstraZeneca_1,na.rm=TRUE),
            janssen = sum(Janssen_1,na.rm=TRUE),
            biontech_zweit = sum(Comirnaty_2,na.rm=TRUE),
            moderna_zweit = sum(Moderna_2,na.rm=TRUE),
            az_zweit = sum(AstraZeneca_2,na.rm=TRUE),
            biontech_dritt = sum(Comirnaty_3,na.rm=TRUE),
            moderna_dritt = sum(Moderna_3,na.rm=TRUE),
            az_dritt = sum(AstraZeneca_3,na.rm=TRUE)) %>% 
  # Summen Personen und Durchgeimpfte bilden 
  # Wie immer zählt Janssen sowohl als Erst- als auch als Durchimpfung
  mutate(personen = biontech_erst+moderna_erst+az_erst+janssen,
         durchgeimpft = biontech_zweit+moderna_zweit+az_zweit+janssen,
         geboostert = biontech_dritt+moderna_dritt+az_dritt) %>% 
  # Spalten umsortieren: Summen erst/zweit nach vorne
  relocate(c(personen,durchgeimpft),.after=id) %>% 
  # Bevölkerungszahlen dazuholen
  left_join(bev_bl_df, by="id") %>% 
  mutate(pop=`u12`+`12-17`+`18-59`+`60+`) %>% 
  # Janssen-Impfstoff wieder zweimal zählen - bei Erst- und Durchgeimpften!
  mutate(quote_erst=personen/pop*100,
         quote_zweit=durchgeimpft/pop*100,
         quote_dritt=geboostert/pop*100) %>% 
  # Spalten aus der Bevölkerungstabelle wieder raus
  select(-Bundesland,-`u12`,-`12-17`,-`18-59`,-`60+`)

# schnell noch: Impfungen gestern

msg("Impfungen am ",i_d_max)
bl_n_df <- bl_tbl %>% 
  filter(Datum == i_d_max) %>% 
  group_by(id, Impfserie) %>% 
  summarize(neu = sum(Anzahl)) %>% 
  pivot_wider(names_from=Impfserie,values_from=neu) %>% 
  rename(neu = `1`, neu_zweit = `2`, neu_dritt=`3`)

# NEU: Bisher konnte man die neuen Impfungen nur als Differenz errechnen, 
# und das hieß: Der Janssen-Impfstoff tauchte sowohl in neu als auch neu_zweit auf.
# Das ist jetzt nicht mehr der Fall - Janssen-Impfungen werden fest der Impfserie 1 zugeordnet.
# Yay!


# Nächste Tabelle: Absolute Summen nach Altersgruppen und Land
# (Kreis-Infos werden hier nicht genutzt, weil sinnlos: 
# Angegeben ist der Impfort, nicht der Wohnort der Geimpften - 
# man kann also keine Impfquote errechnen, und der Wert hängt von 
# der Zahl der Hausärzte und der Größe des Impfzentrums ab - 
# lediglich für eine Zeitreihe für den Kreis wäre es nützlich.)

msg("Baue Ländertabelle Impfquoten nach Altersgruppe...")

# Hier stimmt noch was nicht. Die Impfquoten nach AG sind zu niedrig. 
# Vermutlicher Grund: Die Zählung "Impfschutz" erfasst Vorerkrankte bzw. Janssen-
# Geimpfte mit Impfstatus 2, auch wenn es erst die erste Impfung ist. 
# Zitat aus dem RKI-Github-Repository: "Angabe zum Impfschutz - Vollständiger
# Impfschutz besteht bei zweifacher Impfung, Impfung mit Janssen und einfach 
# Geimpften mit überstandener SARS-CoV-2 Infektion". Super. 

# Deswegen wird für die Impfquote einstweilen weiter die grausige XLS-Tabelle
# von der RKI-Website genutzt. 

# Erst eine Tabelle, die die absoluten Zahlen enthält (brauchen wir für die
# Drittimpfung)
bl_3_df <- lk_tbl %>% 
  # id - die ersten zwei Zeichen der AGS enthalten die Länderkennung 
  mutate(id = str_sub(id_lk,1,2)) %>% 
  # Tabelle Erst- und Zweitimpfungen nach Land und Altersgruppe  
  # Es gibt eine Spalte u - für die paar Fälle ohne Altersangabe - 
  # man darf sie bequem ignorieren. 
  # In der ZEILE u sind die Impfzentren des Bundes - auch die klammern wir
  # für den Ländervergleich aus. 
  # Impfschutz 
  pivot_wider(names_from=c(Altersgruppe,Impfschutz),values_from=Anzahl) %>% 
  group_by(id) %>% 
  # Datum und Landkreis interessieren uns nicht; wir wollen Gesamtsummen
  select(-Datum,-id_lk) %>% 
  summarize_all(sum, na.rm=TRUE) 

bl_2_df <- bl_3_df %>% 
  # Bevölkerung Bundesländer dazuholen - die Tabelle, die oben aus dem 
  # GENESIS-File erstellt wurde
  left_join(bev_bl_df,by="id") %>% 
  # Summen für Erst- und Zweitimpfungsquote berechnen
  mutate(quote_erst_u18 = `12-17_1`/ `12-17` * 100,
         quote_erst_18_60 = `18-59_1`/ `18-59` * 100,
         quote_erst_ue60 = `60+_1`/ `60+` * 100,
         quote_zweit_u18 = `12-17_2`/ `12-17` * 100,
         quote_zweit_18_60 = `18-59_2`/ `18-59` * 100,
         quote_zweit_ue60 = `60+_2`/ `60+` * 100,
         quote_dritt_u18 = `12-17_3`/ `12-17` * 100,
         quote_dritt_18_60 = `18-59_3`/ `18-59` * 100,
         quote_dritt_ue60 = `60+_3`/ `60+` * 100) %>% 
  select(id,starts_with("quote_"))




# Jetzt: Die Tabelle zusammenführen. 
# Die BL-Tabelle, die die Namen der Länder enthält, als Ausgangspunkt,
# dann Summen nach Wirkstoff Erst/Zweit/Dritt, 
# dann Quoten nach Altersgruppe Erst/Zweit/Dritt

# Tabelle Bundesländer

msg("Bundesländer-Tabelle zusammenführen und schreiben...")
impfen_alle_df <- bev_bl_df %>%
  # Datum des letzten gelesenen Tages als erste Spalte
  mutate(am = i_d_max) %>% 
  # Bundesländer-Tabelle als Basis; ID und Bundesland-Name behalten
  select(am,id,Bundesland) %>% 
  # Nach der id sortieren
  arrange(id) %>% 
  # Neue Erst-, Zweit- und Drittimpfungen
  left_join(bl_n_df, by="id") %>% 
  # Die Tabelle mit den Impfstoffen holen
  left_join(bl_1_df, by="id") %>% 
  # Die Tabelle mit den Altersgruppen holen
#  left_join(bl_2_df, by="id") %>% 
  # Wie oben erklärt: einstweilen sind wir hier auf die XLSX-Tabelle angewiesen
  # Tabelle hat die gleichen Spaltennahmen; Spalten 
  left_join(impfquoten_xlsx_df %>% 
              # Spalten aussortieren, die es in der anderen Tabelle schon gibt
              select(-quote_erst,-quote_zweit) %>% 
              # Quoten von Strings in Zahlen umwandeln
              mutate(across(starts_with("quote_"),as.numeric)) %>%
              # Nur die Quoten nach Altersgruppen aufheben
              select(id=ID,starts_with("quote_")),by="id") %>% 
  # Kompatibilität mit bisherigem Format: 
  # Spalte umbenennen, personen und durchgeimpft nach vorn sortieren
  relocate(c(personen,durchgeimpft),.after=Bundesland) %>% 
  rename (ID = id)
  
write_sheet(impfen_alle_df,aaa_id,sheet = "ImpfzahlenNational")

msg("Deutschland-Karte aktualisieren...")
# dw_data_to_chart(impfen_alle_df,chart_id="6PRAe")
dw_publish_chart("6PRAe") # Karte mit dem Impffortschritt nach BL

# ---- Hessen isolieren ----
msg("Hessen-Daten bauen")
impf_df <- impfen_alle_df %>%
  # nur Hessen
  filter(as.numeric(ID) == 6) %>%
  select(-ID,-Bundesland) 

# Tagestabelle schreiben
write_sheet(impf_df,ss=aaa_id,sheet="ImpfenTagestabelle")



# Bestehen bleiben: 
# - am
# - personen
# - differenz_zum_vortag_erst (neu)
# - impfquote
# - personen_durchgeimpft
# - differenz_zum_vortag_zweitimpfung (neu_zweit)
#
# Anders interpretiert werden
# - zentren_biontech  <- biontech (wie früher)
# - zentren_moderna   <- moderna
# - zentren_az        <- az
# - zentren_janssen   <- janssen
# - zentren_dosen     <- impfdosen
#
# Neu hinzu kommen: 
# - quote_erst_u18
# - quote_erst_18_60
# - quote_erst_ue60
# - quote_zweit_u18
# - quote_zweit_18_60
# - quote_zweit_ue60
# - quote_dritt_u18
# - quote_dritt_18_60
# - quote_dritt_ue60
#
# - biontech_zweit
# - moderna_zweit
# - az_zweit
#
# - Tabellenspalte für händische Anmerkungen

# Alle bis Stand gestern
tmp <- (read_sheet(aaa_id,sheet = "ArchivImpfzahlen") %>% filter(am < today()-1))$janssen 

# Janssen-Neuimpfungen aus der Länder-Tabelle extrahieren

janssen_neu <- sum(bl_tbl %>% 
                     # verimpft gestern, in Hessen, mit Janssen
                     filter(Datum == i_d_max) %>% 
                     filter(id == "06") %>% 
                     filter(Impfstoff == "Janssen") %>% 
                     pull(Anzahl))

# ---- Basisdaten-Seite anpassen und aktualisieren ----
msg("Impfzahlen und Immunisierungsquote")
# Geimpft mit Quote und Datum (Zeile 9)
range_write(aaa_id,as.data.frame(paste0("Geimpft ",
                                         "(",
                                         format.Date(i_d,"%d.%m."),
                                        ", 8 Uhr)",
                                        "")),
            range="Basisdaten!A9",col_names=FALSE,reformat=FALSE)

range_write(aaa_id, as.data.frame(paste0(
  # kumulativ: Erstgeimpfte 
  base::format(impf_df$personen,big.mark=".",decimal.mark = ","),
  # Differenz: Erstgeimpfte neu plus Janssen-Geimpfte neu
  " (", base::format(impf_df$quote_erst,
                big.mark = ".", decimal.mark = ",", nsmall =0,digits=4),
  "%)")),
  range="Basisdaten!B9", col_names = FALSE, reformat=FALSE)

# Geboostert
range_write(aaa_id,as.data.frame(
  paste0("Geboostert ",
  #       format.Date(i_d_max,"%d.%m."))
  "")),
  range="Basisdaten!A10",col_names=FALSE,reformat=FALSE)

range_write(aaa_id, as.data.frame(
  paste0(base::format(impf_df$quote_dritt,
               big.mark = ".", decimal.mark = ",", nsmall =0,digits=4),
         "% (+",
         base::format(impf_df$neu_dritt,big.mark=".",decimal.mark = ","),
         ")")),
  range="Basisdaten!B10", col_names = FALSE, reformat=FALSE)


dw_publish_chart("OXn7r") # Basisdaten-Seite


# ---- Impfzahlen-Seite ----

# Geimpfte Personen - Zeile 2
# Gesamtzahl der Janssen-Geimpften muss nicht mehr addiert werden, 
# taucht jetzt ohnehin doppelt auf. 

# Zeile 2 
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$personen,big.mark=".",decimal.mark = ","),
  " (",format(impf_df$quote_erst,decimal.mark = ",",digits=4),"%)")),
  range="Impfzahlen!A2", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "Menschen sind in Hessen wenigstens einmal <strong>geimpft</strong>. (Stand: ",
  format.Date(i_d,"%d.%m."),
  ", 8 Uhr)")),
  range="Impfzahlen!B2", col_names = FALSE, reformat=FALSE)

# Durchgeimpfte - Zeile 3
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$durchgeimpft,big.mark=".",decimal.mark = ","),
  " (",format(impf_df$quote_zweit,big.mark=".",decimal.mark = ",",digits=4),"%)"
)),
range="Impfzahlen!A3", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0("sind <strong>durchgeimpft</strong>.")),
            range="Impfzahlen!B3", col_names = FALSE, reformat=FALSE)

# Auffrischungs-/Boosterimpfungen - Zeile 4

# Erst einmal aus der Ländertabelle isolieren - absolute Zahlen. 
he_3_df <- bl_3_df %>% 
  filter(id=="06") %>% 
  select(ue12_17_3 = `12-17_3`,
         ue18_59_3 = `18-59_3`,
         ue60_3 = `60+_3`)


range_write(aaa_id,as.data.frame(
  paste0(format(sum(he_3_df),big.mark = ".",decimal.mark=","),
         " (",
         format(impf_df$quote_dritt,big.mark=".",decimal.mark = ",",digits=4),
         "%)")),
  range="Impfzahlen!A4", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0("sind <strong>geboostert",
                                        "</strong>.")),
            range="Impfzahlen!B4", col_names = FALSE, reformat=FALSE)

# Neue Impfungen - Zeile 5
range_write(aaa_id,as.data.frame(paste0(
  "+",format(impf_df$neu,big.mark = ".",decimal.mark=","),
  " / +",format(impf_df$neu_zweit,big.mark = ".",decimal.mark=","),
  " / +",format(impf_df$neu_dritt,big.mark = ".",decimal.mark=","))),
  range="Impfzahlen!A5", col_names = FALSE, reformat=FALSE)


range_write(aaa_id,as.data.frame(paste0(
  "<strong>Erst- / Zweit- /Boosterimpfungen</strong> sind am ",
  format.Date(impf_df$am,"%d.%m."),
  " dazugekommen.")),
  range="Impfzahlen!B5", col_names = FALSE, reformat=FALSE)



#Impfquote Ü60 - Zeile 6
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$quote_erst_ue60,big.mark = ".",decimal.mark=","),
  "% / ",format(impf_df$quote_zweit_ue60,big.mark = ".",decimal.mark=","),
  "% / ",format(impf_df$quote_dritt_ue60,big.mark = ".",decimal.mark=","),"%")),
  range="Impfzahlen!A6", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0("der <strong>besonders gefährdeten Menschen über 60",
                                        "</strong>",
                                        " sind inzwischen erst-/ durchgeimpft / geboostert.")),
  range="Impfzahlen!B6", col_names = FALSE, reformat=FALSE)


faelle_df <- read_sheet(aaa_id,sheet="Fallzahl4Wochen")
impf_tabelle <- read_sheet(aaa_id,sheet = "ArchivImpfzahlen") %>% filter(am <= today()-14)

fehlen_str = paste0("Noch nicht geimpft in Hessen:<ul><br>- ",
                    format(100-impf_df$quote_erst_ue60,big.mark=".",decimal.mark = ",",digits = 3),
                    "% der über 60-Jährigen (",
                    format(((100-impf_df$quote_erst_ue60)*ue60 %/% 100000) *1000,
                           big.mark=".",decimal.mark = ","), 
                    " Menschen)<br>- ",
                    format(100-impf_df$quote_erst_18_60,big.mark=".",decimal.mark = ",",digits = 3),
                    "% der 18-59-Jährigen (",
                    format(((100-impf_df$quote_erst_18_60)*ue18_59 %/% 100000)*1000, 
                           big.mark=".",decimal.mark = ","),
                    " Menschen)<br>- ",
                    format(100-impf_df$quote_erst_u18,big.mark=".",decimal.mark = ",",digits = 3),
                    "% der 12-17-Jährigen (",
                    format(((100-impf_df$quote_erst_u18)*ue12_17 %/% 100000)*1000, 
                    big.mark=".",decimal.mark = ","),
                    " Menschen in Hessen)")
                    
# Impfdaten-Tabelle aktualisieren
dw_edit_chart(chart_id ="l5KKN", intro = fehlen_str)
dw_publish_chart("l5KKN")


# TODO: 
# - u18, 18-59,ü60 Tabelle

# ---- Impfstoffe ----


impfstoffe_df <- impf_df %>%
  select(erst_Biontech = biontech_erst,
         erst_Moderna = moderna_erst,
         erst_AstraZeneca = az_erst,
         erst_Janssen = janssen) %>%
  pivot_longer(everything(),names_prefix="erst_",values_to="Erstimpfung") %>%
  left_join(impf_df %>% 
              select(zweit_Biontech = biontech_zweit,
                     zweit_Moderna = moderna_zweit,
                     zweit_AstraZeneca = az_zweit) %>%
              pivot_longer(everything(),names_prefix="zweit_",values_to="Zweitimpfung"),
            by="name")


write_sheet(impfstoffe_df,ss=aaa_id,sheet="Impfstoffe")
dw_edit_chart(chart_id="BfPeh",intro=paste0("Wie oft kam in Hessen welcher Impfstoff zum Einsatz? Stand: ",
                                            format.Date(i_d,"%d.%m."),", 8 Uhr"))
dw_publish_chart(chart_id="BfPeh")



# ---- Quoten nach Alter ----


quoten_alter_df <- impf_df %>%
  select(quote_zweit_u18,
         quote_zweit_18_60,
         quote_zweit_ue60,
         quote_zweit_Hessen = quote_zweit,
         quote_erst_u18,
         quote_erst_18_60,
         quote_erst_ue60,
         quote_erst_Hessen = quote_erst,
  ) %>%
  # Aufbereiten in zwei Spalten "Erstgeimpft" und "Zweitgeimpft"
  pivot_longer(everything()) %>%
  mutate(erstzweit = ifelse(str_detect(name,"_erst_"),"nur erstgeimpft","durchgeimpft")) %>%
  mutate(name = str_replace(name,"quote_.+_","")) %>%
  pivot_wider(names_from=erstzweit,values_from=value) %>%
  # Spalte "Erstgeimpft" um Zweitgeimpfte verringern, um 
  # als gestapelte Säulen anzeigen zu können
  mutate(`nur erstgeimpft`= `nur erstgeimpft`-durchgeimpft)
  

  
write_sheet(quoten_alter_df,ss=aaa_id,sheet="ImpfquotenAlter")
dw_edit_chart(chart_id="vgJSw",annotate = paste0(
  "Mit dem Johnson&Johnson-Impfstoff Geimpfte und Geimpfte mit Vorerkrankung ",
  "werden seit 1.8.21 als Durchgeimpfte gezählt -",
  " Stand: ",
                           format.Date(i_d,"%d.%m.%Y"),
                            ", 8 Uhr"))
dw_publish_chart(chart_id="vgJSw")

# ---- Tag archivieren ----

msg("Archivdaten schreiben")

# Hessen-Daten auf das alte Format umlügen

# Wenn nötig, neue Zeile in der Tabelle anlegen (über Base-R)
lastdate <- impf_df$am
archiv_tabelle <- read_sheet(aaa_id,sheet = "ArchivImpfzahlen")
if (as_date(lastdate) %in% as_date(archiv_tabelle$am)) {
  # impf_tabelle[impf_tabelle$am == hessen_archiv_df$am,] <- NULL
} else {
  archiv_tabelle [nrow(archiv_tabelle)+1,] <- NA
  archiv_tabelle [nrow(archiv_tabelle),]$am <- lastdate
}

# Daten ganz stumpf mit Base-R in die Zellen der Tabelle.
# Viel Schreibaufwand, aber pflegeleicht - und unempfindlich gegen
# Formatänderungen des Archivs. 

#archiv_heute <- archiv_tabelle[archiv_tabelle$am==lastdate,]

archiv_tabelle$personen[archiv_tabelle$am==lastdate] <- impf_df$personen
archiv_tabelle$personen_durchgeimpft[archiv_tabelle$am==lastdate] <- impf_df$durchgeimpft
archiv_tabelle$differenz_zum_vortag_erstimpfung[archiv_tabelle$am==lastdate] <- impf_df$neu
archiv_tabelle$differenz_zum_vortag_zweitimpfung[archiv_tabelle$am==lastdate] <- impf_df$neu_zweit
archiv_tabelle$differenz_zum_vortag_drittimpfung[archiv_tabelle$am==lastdate] <- impf_df$neu_dritt
archiv_tabelle$impfquote[archiv_tabelle$am==lastdate] <- impf_df$quote_erst
archiv_tabelle$durchgeimpft_quote[archiv_tabelle$am==lastdate] <- impf_df$quote_zweit
archiv_tabelle$biontech[archiv_tabelle$am==lastdate] <- impf_df$biontech_erst
archiv_tabelle$moderna[archiv_tabelle$am==lastdate] <- impf_df$moderna_erst
archiv_tabelle$az[archiv_tabelle$am==lastdate] <- impf_df$az_erst
archiv_tabelle$janssen[archiv_tabelle$am==lastdate] <- impf_df$janssen
# Die gab's vor dem 7.6. nicht: 
archiv_tabelle$biontech_zweit[archiv_tabelle$am==lastdate] <- impf_df$biontech_zweit
archiv_tabelle$moderna_zweit[archiv_tabelle$am==lastdate] <- impf_df$moderna_zweit
archiv_tabelle$az_zweit[archiv_tabelle$am==lastdate] <- impf_df$az_zweit
# Und die erst ab 1.9.:
archiv_tabelle$biontech_dritt[archiv_tabelle$am==lastdate] <- impf_df$biontech_dritt
archiv_tabelle$moderna_dritt[archiv_tabelle$am==lastdate] <- impf_df$moderna_dritt
archiv_tabelle$az_dritt[archiv_tabelle$am==lastdate] <- impf_df$az_dritt

# archiv_tabelle$impfdosen[archiv_tabelle$am==lastdate] <- impf_df$neu+impf_df$neu_zweit
# Impfdosen kumulativ: Alle Impfstoffe, alle Impfungen
archiv_tabelle$impfdosen[archiv_tabelle$am==lastdate] <- impf_df$biontech_erst +
  impf_df$biontech_zweit + impf_df$moderna_erst + impf_df$moderna_zweit +
  impf_df$az_erst + impf_df$az_zweit + impf_df$janssen
archiv_tabelle$quote_erst_u18[archiv_tabelle$am==lastdate] <- impf_df$quote_erst_u18
archiv_tabelle$quote_erst_18_60[archiv_tabelle$am==lastdate] <- impf_df$quote_erst_18_60
archiv_tabelle$quote_erst_ue60[archiv_tabelle$am==lastdate] <- impf_df$quote_erst_ue60
archiv_tabelle$quote_zweit_u18[archiv_tabelle$am==lastdate] <- impf_df$quote_zweit_u18
archiv_tabelle$quote_zweit_18_60[archiv_tabelle$am==lastdate] <- impf_df$quote_zweit_18_60
archiv_tabelle$quote_zweit_ue60[archiv_tabelle$am==lastdate] <- impf_df$quote_zweit_ue60

  
write_sheet(archiv_tabelle,aaa_id,sheet = "ArchivImpfzahlen")

# ---- Impftempo ----
# Impftempo-Kurve aktualisieren - aus dem Google Sheet "ArchivImpfzahlen"
dw_publish_chart(chart_id="SS8ta")

# Tabelle mit allen Wochenwerten generieren
impftempo_df <- bl_tbl %>%
  # nur Hessen
  filter(id == "06") %>% 
  # Woche errechnen; Jahr mitnehmen
  # mit isoweek/isoyear, das den Versatz 2020/2021 in Betracht zieht
  mutate(woche = paste0(isoyear(Datum),"_",
                        ifelse(isoweek(Datum)<10,"0",""),isoweek(Datum))) %>% 
  group_by(woche) %>% 
  pivot_wider(names_from=Impfserie,values_from=Anzahl) %>%
  summarize(Datum=max(Datum),
            erstgeimpft=sum(`1`,na.rm = TRUE),
            zweitgeimpft=sum(`2`,na.rm = TRUE),
            geboostert=sum(`3`,na.rm = TRUE),
            p=0) 

# Jetzt ganz stumpf aus dem Trend der letzten 7 Tage im Vergleich zur Vorwoche
# eine Prognose errechnen

n <- nrow(impftempo_df)
# Nimm die Werte für die letzte Woche, 
# verändere sie im Verhältnis zur Vorwoche, 
# nimm das als Prognose, und zieh die schon verimpften Dosen davon ab. 
# p ist das, was man dann noch anzeigen kann. 
impftempo_df$p[n] <- round(
  # Erstimpfungen als die Zahl der letzten Woche mal Veränderung zur Vorwoche
  (impftempo_df$erstgeimpft[n-1] * 
     (impftempo_df$erstgeimpft[n-1]/impftempo_df$erstgeimpft[n-2])) +
    # Zweitimpfungen als die Zahl der letzten Woche mal Veränderung zur Vorwoche
    (impftempo_df$zweitgeimpft[n-1] * 
     (impftempo_df$zweitgeimpft[n-1]/impftempo_df$zweitgeimpft[n-2])) +
    # Boosterimpfungen als die Zahl der letzten Woche mal Veränderung zur Vorwoche
    (impftempo_df$geboostert[n-1] * 
     (impftempo_df$geboostert[n-1]/impftempo_df$geboostert[n-2])) - 
    # Abziehen, was schon verimpft wurde in dieser Woche
  (impftempo_df$erstgeimpft[n]+
     impftempo_df$zweitgeimpft[n]+
     impftempo_df$geboostert[n]))
  
if (impftempo_df$p[n] < 0) impftempo_df$p[n] <- 0

if (wday(impftempo_df$Datum[n]) > 1) {
  # Falls die Woche noch unvollständig ist: 
  # Noch eine Tabellenzeile einfügen mit dem Datum des nächsten Sonntags
  # und dem Prognosewert
  impftempo_df <- bind_rows(impftempo_df, tibble(
    woche = impftempo_df$woche[n],
    Datum = impftempo_df$Datum[n-1]+7,
    erstgeimpft= NULL,
    zweitgeimpft= NULL,
    geboostert= NULL,
    p = impftempo_df$erstgeimpft[n] +
      impftempo_df$zweitgeimpft[n] +
      impftempo_df$geboostert[n] +
      impftempo_df$p[n]))
} else {
  # Falls letzter Tag ein Sonntag; Woche abgeschlossen: Prognose auf 0 setzen
  impftempo_df$p[n] <- 0
}


dw_data_to_chart(impftempo_df %>% select(-woche),"Lch5F")
dw_publish_chart("Lch5F")
write_sheet(impftempo_df,ss=aaa_id,sheet="Impftempo wochenweise")

# ---- Baue eine Gesamt-Tabelle mit den vorliegenden Impfdaten ----

# zum Vergleich mit den gemeldeten "Briefkastendaten"

# Diese Tabelle ist noch "blind" - sie enthält die Daten nach AG, die aus
# den oben erwähnten Gründen nicht ohne weiteres verwendet werden können

impf_alter_hist_df <- lk_tbl %>% 
  # id - die ersten zwei Zeichen der AGS enthalten die Länderkennung 
  mutate(id = str_sub(id_lk,1,2)) %>% 
  # Tabelle Erst- und Zweitimpfungen nach Land und Altersgruppe  
  # Es gibt eine Spalte u - für die paar Fälle ohne Altersangabe - 
  # man darf sie bequem ignorieren. 
  pivot_wider(names_from=c(Altersgruppe,Impfschutz),values_from=Anzahl) %>% 
  # Beschränkung auf Hessen
  filter(id=="06") %>% 
  # Landkreise interessieren uns nicht
  select(-id_lk,-id) %>%
  group_by(Datum) %>% 
  # Tage aufaddieren
  summarize_all(sum,na.rm=TRUE) %>% 
  # mutate_all(~replace(., is.na(.), 0)) %>% 
  # Kumulative Summen der Geimpften-Zahlen nach AG
  mutate(ue12_17_gesamt_erst = cumsum(`12-17_1`),
         ue12_17_gesamt_zweit = cumsum(`12-17_2`),
         ue18_60_gesamt_erst = cumsum(`18-59_1`),
         ue18_60_gesamt_zweit = cumsum(`18-59_2`),
         ue60_gesamt_erst = cumsum(`60+_1`),
         ue60_gesamt_zweit = cumsum(`60+_2`)) %>% 
  # Bevölkerung Bundesländer dazuholen - die Tabelle, die oben aus dem 
  # Summen für Erst- und Zweitimpfungsquote berechnen
  mutate(quote_erst_ue12_17 = ue12_17_gesamt_erst/ue12_17  * 100,
         quote_zweit_ue12_17 = ue12_17_gesamt_zweit/ue12_17 * 100,
         quote_erst_ue18_60 = ue18_60_gesamt_erst/ue18_59 * 100,
         quote_zweit_ue18_60 = ue18_60_gesamt_zweit/ue18_59 * 100,
         quote_erst_ue60 = ue60_gesamt_erst/ue60 * 100,
         quote_zweit_ue60 = ue60_gesamt_zweit/ue60 * 100)



impf_hist_df <- bl_tbl %>% 
  # Tabelle Erst- und Zweitimpfungen nach Land und Impfstoff - Summen
  filter(id == "06") %>% 
  pivot_wider(names_from=c(Impfstoff,Impfserie),names_sep="_",values_from=Anzahl,
              values_fill=0) %>% 
  # Impfungen nach Impfstoff pro Tag aufsummieren
  mutate(biontech_erst = cumsum(Comirnaty_1),
            moderna_erst = cumsum(Moderna_1),
            az_erst = cumsum(AstraZeneca_1),
            janssen = cumsum(Janssen_1),
            biontech_zweit = cumsum(Comirnaty_2),
            moderna_zweit = cumsum(Moderna_2),
            az_zweit = cumsum(AstraZeneca_2)) %>%
  # Impfungen nach Impfstoff pro Tag aus der Tabelle werfen
  # Summen Personen und Durchgeimpfte bilden 
  # Wie immer zählt Janssen sowohl als Erst- als auch als Durchimpfung
  mutate(personen = biontech_erst+moderna_erst+az_erst+janssen,
         durchgeimpft = biontech_zweit+moderna_zweit+az_zweit+janssen) %>% 
  # Spalten umsortieren: Summen erst/zweit nach vorne
  relocate(c(personen,durchgeimpft),.after=id) %>% 
  # Bevölkerungszahlen dazuholen
  left_join(bev_bl_df, by="id") %>% 
  mutate(pop=`u12`+`12-17`+`18-59`+`60+`) %>% 
  # Janssen-Impfstoff wieder zweimal zählen - bei Erst- und Durchgeimpften!
  mutate(quote_erst=personen/pop*100,
         quote_zweit=durchgeimpft/pop*100) %>% 
  # Spalten aus der Bevölkerungstabelle wieder raus
  select(-Bundesland,-`u12`,-`12-17`,-`18-59`,-`60+`) %>% 
  full_join(impf_alter_hist_df,by="Datum")

write_sheet(impf_hist_df,aaa_id,sheet = "ImpfzahlenHistorie")


# ---- Generiere Infokarte in Teams ----

# Legt eine Karte mit den aktuellen Impfzahlen im Teams-Team "hr-Datenteam", 
# Channel "Corona" an. 


library(teamr)
library(magick)

cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
cc$text(paste0("RKI-Impfdaten nach dem ",format.Date(lastdate,"%d.%m.%y")))

sec <- card_section$new()

sec$text(paste0("<h4>",format(impf_df$quote_erst,decimal.mark=",",big.mark=".",
                       digits=4),
                "% mindestens einmal geimpft; ",
                format(impf_df$quote_zweit,decimal.mark=",",big.mark=".",
                       digits=4),
                "% zweimal geimpft; ",
                format(impf_df$quote_dritt,decimal.mark=",",big.mark=".",
                       digits=4),
                "% geboostert</h4>"))
  

sec$add_fact("Erstgeimpfte: ",format(impf_df$personen,decimal.mark=",",big.mark="."))
sec$add_fact("Zweitgeimpfte: ",format(impf_df$durchgeimpft,decimal.mark=",",big.mark="."))
sec$add_fact("Geboosterte: ",format(impf_df$geboostert,decimal.mark=",",big.mark="."))

sec$add_fact(paste0("Erstimpfungen ",format.Date(i_d_max,"%d.%m.:")),
             format(impf_df$neu,decimal.mark=",",big.mark="."))
sec$add_fact(paste0("Zweitimpfungen ",format.Date(i_d_max,"%d.%m.:")),
             format(impf_df$neu_zweit,decimal.mark=",",big.mark="."))
sec$add_fact(paste0("Boosterimpfungen ",format.Date(i_d_max,"%d.%m.:")),
             format(impf_df$neu_dritt,decimal.mark=",",big.mark="."))

sec$add_fact("Impfquote Ü60: ",
             paste0(format(impf_df$quote_erst_ue60,big.mark=".",
                           decimal.mark = ",",digits=4),"%"))
# Bisschen fieses Base R: Hessen ist der 7. Wert, gib dessen (umgekehrten) Rang aus.
sec$add_fact("Hessens Rang Erstimpfungen: ",rank(-impfen_alle_df$quote_erst,
                                                 ties.method = "min")[7])
sec$add_fact("Hessens Rang Zweitimpfungen: ",rank(-impfen_alle_df$quote_zweit,
                                                     ties.method = "min")[7])
# Zahlen Ungeimpfte nach AG: 
sec$add_fact(paste0("Ungeimpfte ü60 (",
                    format(100-impf_df$quote_erst_ue60,big.mark=".",decimal.mark = ",",digits = 3),
                    "%):"),
                    format(((100-impf_df$quote_erst_ue60)*ue60 %/% 100000) *1000,
                           big.mark=".",decimal.mark = ","))  
sec$add_fact(paste0("Ungeimpfte 18-59J (",
                    format(100-impf_df$quote_erst_18_60,big.mark=".",decimal.mark = ",",digits = 3),
                    "%):"),
                    format(((100-impf_df$quote_erst_18_60)*ue18_59 %/% 100000)*1000,
                          big.mark=".",decimal.mark = ","))  
sec$add_fact(paste0("Ungeimpfte 12-17J (",
                    format(100-impf_df$quote_erst_u18,big.mark=".",decimal.mark = ",",digits = 3),
                    "%):"),
                    format(((100-impf_df$quote_erst_u18)*ue12_17 %/% 100000)*1000,
                           big.mark=".",decimal.mark = ","))      

# Wenn du auf dem Server bist: 
# Importiere eine PNG-Version des Impffortschritts, 
# schiebe sie auf den Google-Bucket, und 
# übergib die URL an die Karte. 


if (server) {
  # Google-Bucket befüllen
  png <- dw_export_chart(chart_id = "SS8ta",type = "png",unit="px",mode="rgb", scale = 1, 
                         width = 600, height = 360, plain = FALSE)
  image_write(png,"./png/impf-tmp.png")
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./png/impf-tmp.png gs://d.data.gcp.cloud.hr.de/impf-tmp.png')
  sec$add_image(sec_image="https://d.data.gcp.cloud.hr.de/impf-tmp.png", sec_title="Impffortschritt")
}
  
# Karte vorbereiten und abschicken. 
cc$add_section(new_section = sec)
cc$send()

msg("OK")
