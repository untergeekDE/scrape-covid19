##################################### hole-impfzahlen.R #########################
# - Impfzahlen 

# Stand: 28.1.2021


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

# Bevölkerungszahl
hessen=sum(read.xlsx("index/kreise-index-pop.xlsx") %>% select(pop))
prio1 = 550000 # Menschen in Gruppe höchster Priorität
# Auskunft Innenministerium an Tobias Lübben 15.1.2021

# ---- Ich bin genervt, ich arbeite mit den RKI-Daten jetzt. ----

# XLSX vom Tage
rki_xlsx_url <- "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquotenmonitoring.xlsx?__blob=publicationFile"


msg("Impfzahlen vom RKI lesen...")
impf_tabelle <- read_sheet(aaa_id,sheet="ArchivImpfzahlen")
impfen_meta_df <- na.omit(read.xlsx(rki_xlsx_url,sheet=4)) %>% 
  filter(Datum != "Gesamt") %>%
  mutate(Datum = as.Date(as.numeric(Datum),origin="1899-12-30"))

ts <- now()
impfen_df <- read_sheet(ss=aaa_id,sheet = "ArchivImpfzahlen") %>%
  filter(am == max(am))

# Wenn das Datum inzwischen höher ist als das letzte Archivdatum,
# aber keine neue Datei: 

while (impfen_df$am == max(impfen_meta_df$Datum) &&
       today() > impfen_df$am + 1) {

  # 4 Stunden lang versuchen
  if (now() > ts+(4*3600)) {
    msg("KEINE NEUEN DATEN GEFUNDEN")
    stop("Timeout")
  } else {
    
  }
  # Nochmal versuchen: XLSX neu lesen
  impfen_meta_df <- omit.na(read.xlsx(rki_xlsx_url,sheet=4)) %>% 
    filter(Datum != "Gesamt") %>%
    mutate(Datum = as.Date(as.numeric(Datum),origin="1899-12-30"))
  
}

msg("Daten bis ",max(impfen_meta_df$Datum)," gelesen - Ländertabelle lesen...")
impfen_alle_df <- na.omit(read.xlsx(rki_xlsx_url,sheet=2)) %>%
  inner_join(na.omit(read.xlsx(rki_xlsx_url,sheet=3)),by = "RS") %>%
  # Datum dazupacken
  mutate(am = as.Date(max(impfen_meta_df$Datum))) %>%
  select(am,
         ID=RS,
         Bundesland = 2,
         personen = 4, # Erstimpfung,
         differenz_zum_vortag_erstimpfung = 7,
         impfquote = 8,
         personen_durchgeimpft = 9,
         differenz_zum_vortag_zweitimpfung = 10,
         indikation_nach_alter = 12,
         berufliche_indikation =13,
         medizinische_indikation = 14,
         pflegeheimbewohnerin = 15,
         Biontech = 5,
         Moderna = 6) %>%
  mutate(ID = as.numeric(ID),
          personen = as.numeric(personen),
         differenz_zum_vortag_erstimpfung = as.numeric(differenz_zum_vortag_erstimpfung),
         impfquote = as.numeric(impfquote),
         personen_durchgeimpft = as.numeric(personen_durchgeimpft),
         differenz_zum_vortag_zweitimpfung = as.numeric(differenz_zum_vortag_zweitimpfung),
         indikation_nach_alter = as.numeric(indikation_nach_alter),
         berufliche_indikation = as.numeric(berufliche_indikation),
         medizinische_indikation = as.numeric(medizinische_indikation),
         pflegeheimbewohnerin = as.numeric(pflegeheimbewohnerin),
         Biontech = as.numeric(Biontech),
         Moderna = as.numeric(Moderna)) %>%
        #Sternchen aus dem Bundesland
        mutate(Bundesland = str_replace(Bundesland,"\\*",""))

write_sheet(impfen_alle_df,aaa_id,sheet = "ImpfzahlenNational")

# !!!Plausi-Prüfung nachtragen!!! - Formatänderungen auffangen

# ---- Vergleichskarte D generieren ----
msg("Deutschland-Karte aktualisieren...")
dw_publish_chart("6PRAe") # Basisdaten-Seite


# ---- Hessen isolieren, Basisdaten anpassen, Impfdaten-Seite 
msg("Hessen-Daten bauen")
impf_df <- impfen_alle_df %>%
  # nur Hessen
  filter(as.numeric(ID) == 6) %>%
  select(-ID,-Bundesland)

# Basisdaten-Seite anpassen und aktualisieren
msg("Impfzahlen und Immunisierungsquote")
# Geimpft (Zeile 8)
range_write(aaa_id,as.data.frame(paste0("Geimpft (",
                                        format.Date(impf_df$am,"%d.%m."),")")),
            range="Basisdaten!A8",col_names=FALSE,reformat=FALSE)
range_write(aaa_id, as.data.frame(paste0(
  format(impf_df$personen,big.mark=".",decimal.mark = ","),
  " (+", format(impf_df$differenz_zum_vortag_erstimpfung,big.mark = ".", decimal.mark = ",", nsmall =0),
  ")")),
  range="Basisdaten!B8", col_names = FALSE, reformat=FALSE)
dw_publish_chart("OXn7r") # Basisdaten-Seite

# ---- Impfzahlen-Seite

# Geimpfte Personen - Zeile 2
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$personen,big.mark=".",decimal.mark = ","))),
  range="Impfzahlen!A2", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "Menschen sind in Hessen <strong>geimpft</strong>. (Stand: ",
  format.Date(impf_df$am,"%d.%m."),
  ")")),
  range="Impfzahlen!B2", col_names = FALSE, reformat=FALSE)

# Geimpfte heute (und Datum) - Zeile 3
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$differenz_zum_vortag_erstimpfung,big.mark=".",decimal.mark = ","))),
  range="Impfzahlen!A3", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "sind <strong>am ",
  format.Date(impf_df$am,"%d.%m."),
  "</strong> dazugekommen.")),
  range="Impfzahlen!B3", col_names = FALSE, reformat=FALSE)

# Impfquote - Zeile 4
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$impfquote,big.mark=".",decimal.mark = ",",nsmall = 2))),
  range="Impfzahlen!A4", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "<strong>der Hessinnen und Hessen</strong> sind damit geimpft.")),
  range="Impfzahlen!B4", col_names = FALSE, reformat=FALSE)


# Durchgeimpfte - Zeile 5
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$personen_durchgeimpft,big.mark=".",decimal.mark = ","))),
  range="Impfzahlen!A5", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "haben <strong>beide Impfdosen</strong> erhalten und sind damit durchgeimpft.")),
  range="Impfzahlen!B5", col_names = FALSE, reformat=FALSE)


# Time to tier two - Zeile 6

range_write(aaa_id,as.data.frame(paste0(
  format(((prio1-impf_df$personen) %/% 1000) * 1000,big.mark=".",decimal.mark = ","))),
  range="Impfzahlen!A6", col_names = FALSE, reformat=FALSE)

# Impfgeschwindigkeit letzte 5 Tage
geschwindigkeit = mean(impf_tabelle$differenz_zum_vortag_erstimpfung[nrow(impf_tabelle)-4:0])
tage_bis_x = (prio1-impf_df$personen)/geschwindigkeit
  
range_write(aaa_id,as.data.frame(paste0(
  "Menschen mit der höchsten Priorität sind noch nicht geimpft - ",
  "sie könnten beim derzeitigen Impftempo bis <strong>",
  format.Date(today()+tage_bis_x,"%d.%m.%Y"),
  "</strong> alle an der Reihe gewesen sein.")),
  range="Impfzahlen!B6", col_names = FALSE, reformat=FALSE)

faelle_df <- read_sheet(aaa_id,sheet="Fallzahl4Wochen")
impf_tabelle <- read_sheet(aaa_id,sheet = "ArchivImpfzahlen") %>% filter(am <= today()-14)

hoffnung_str = paste0("<h4>",
                      format((max(impf_tabelle$personen)+max(faelle_df$gsum))/hessen*100,big.mark=".",decimal.mark = ",",digits = 2),
                " % der Menschen in Hessen sind inzwischen wahrscheinlich gegen ",
                "das Coronavirus immunisiert, weil sie eine Infektion überstanden oder ",
                "vor 14 Tagen die erste Impfung erhalten haben.</h4>")
dw_edit_chart(chart_id ="l5KKN", intro = hoffnung_str)
dw_publish_chart("l5KKN")

# ---- Generiere Anteile Gruppen ----

# Erstimpfungs-Indikationen umformatieren

indikation_df <- impf_df %>% select(7,8,9,10) %>% pivot_longer(cols=1:4,names_to = "Kategorie",values_to="Menschen")

indikation_str <- paste0("Impfgründe nach Zahlen, Stand: ",
                         format.Date(impf_df$am,"%d.%m.%Y"))
dw_data_to_chart(indikation_df,chart_id = "69Q8q")
dw_edit_chart(chart_id ="69Q8q", intro = indikation_str)
dw_publish_chart(chart_id= "69Q8q")



# ---- Tag archivieren ----

msg("Archivdaten schreiben")
#impf_tabelle <- read_sheet(aaa_id,sheet = "ArchivImpfzahlen")
if (as.Date(impf_df$am) %in% as.Date(impf_tabelle$am)) {
  impf_tabelle[impf_tabelle$am == impf_df$am,] <- impf_df
} else {
  impf_tabelle <- rbind(impf_tabelle,impf_df)
}

write_sheet(impf_tabelle,aaa_id,sheet = "ArchivImpfzahlen")
### Ergänzen: Archivdaten bauen

# ---- Tabelle Impfzahlen neu bauen - als Funktion ----
# Die Impfzahlen holen wir einfach beim BR - soll sich Michael mit den Format-
# änderungen des RKI rumschlagen :) 
konstruiere_impftabelle_alt <- function() {
  impf_data_url <- ("https://raw.githubusercontent.com/ard-data/2020-rki-impf-archive/master/data/")
  dir_json_url <- "https://api.github.com/repos/ard-data/2020-rki-impf-archive/contents/data/1_parsed?recursive=0"
  impf_json_dir <- read_json(dir_json_url,simplifyVector =TRUE)
  impf_tabelle <- NULL
  impf_files_list <- impf_json_dir$tree$path
  for (f in impf_json_dir$name) {
    impf_json <- read_json(paste0(impf_data_url,"1_parsed/",f),simplifyVector = TRUE)
    impf_df <- data.frame(t(unlist(impf_json$states$HE))) %>%
      # Ländercodes raus
      select(-code,-title) %>%
      mutate(date = as.Date(str_extract(f,"[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"))) %>%
      mutate(am = date-1) 
    
    if(is.null(impf_tabelle)) {
      impf_tabelle <- impf_df
    } else {
      impf_tabelle <- bind_rows(impf_tabelle,impf_df)
    }
  }
  # Jetzt den alten Kram in der Tabelle umformatieren
  impf_tabelle <- impf_tabelle %>%
    filter(am != lag(am)) %>%
    # Differenz zum Vortag aus Erstimpfung errechnen
    mutate(differenz_zum_vortag_erstimpfung =as.numeric( 
             ifelse(is.na(differenz_zum_vortag_erstimpfung),
                    differenz_zum_vortag,differenz_zum_vortag_erstimpfung))) %>%
    mutate(personen = as.numeric(ifelse(is.na(impfungen_kumulativ_erstimpfung),
                            impfungen_kumulativ,
                            impfungen_kumulativ_erstimpfung))) %>%
    mutate(impfquote = as.numeric(personen)/hessen*100) %>%
    # Indikationen
    mutate(indikation_nach_alter = as.numeric(ifelse(is.na(indikation_nach_alter),
                                          indikation_nach_alter_erstimpfung,
                                          indikation_nach_alter))) %>%
    mutate(berufliche_indikation = as.numeric(ifelse(is.na(berufliche_indikation),
                                          berufliche_indikation_erstimpfung,
                                          berufliche_indikation))) %>%
    mutate(medizinische_indikation = as.numeric(ifelse(is.na(medizinische_indikation),
                                          medizinische_indikation_erstimpfung,
                                          medizinische_indikation))) %>%
    mutate(pflegeheimbewohnerin = as.numeric(ifelse(is.na(pflegeheimbewohnerin),
                                          pflegeheimbewohnerin_erstimpfung,
                                          pflegeheimbewohnerin))) %>%
    select(am,personen,differenz_zum_vortag_erstimpfung,impfquote,
           personen_durchgeimpft = impfungen_kumulativ_zweitimpfung,
           differenz_zum_vortag_zweitimpfung,
           indikation_nach_alter,
           berufliche_indikation,
           medizinische_indikation,
           pflegeheimbewohnerin,
           Biontech = impfungen_kumulativ_by_biontech_erstimpfung,
           Moderna = impfungen_kumulativ_by_moderna_erstimpfung) %>%
    mutate(Moderna = as.numeric(ifelse(is.na(Moderna),0,Moderna))) %>%
    mutate(Biontech = as.numeric(ifelse(is.na(Biontech),personen,Biontech)))
    
    write_sheet(impf_tabelle,aaa_id,sheet="ArchivImpfzahlen")
    return(impf_tabelle)
}