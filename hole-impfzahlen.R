##################################### hole-impfzahlen.R #########################
# - Impfzahlen 

# Stand: 8.4.2021


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

# Bevölkerung nach Altersgruppen
bev_df= read_sheet(aaa_id,sheet="AltersgruppenPop")
hessen=sum(bev_df %>% select(pop))
ue60 = sum(bev_df %>% filter(Altersgruppe %in% c("A60-A79","A80+")) %>% select(pop))
u60 = hessen - ue60
# Vergleiche https://corona-impfung.hessen.de/faq/impfstrategie
# Auskunft Innenministerium an Tobias Lübben 15.1.2021

# ---- Ich bin genervt, ich arbeite mit den RKI-Daten jetzt. ----

# XLSX vom Tage
rki_xlsx_url <- "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquotenmonitoring.xlsx?__blob=publicationFile"


msg("Impfzahlen vom RKI lesen...")
impf_tabelle <- read_sheet(aaa_id,sheet="ArchivImpfzahlen")
impfen_meta_df <- read.xlsx(rki_xlsx_url,sheet=4) %>% 
  mutate(Datum = as.Date(as.numeric(Datum),origin="1899-12-30")) %>%
  na.omit()

ts <- now()
impfen_df <- read_sheet(ss=aaa_id,sheet = "ArchivImpfzahlen") %>%
  filter(am == max(am))

# Wenn das Datum inzwischen höher ist als das letzte Archivdatum,
# aber keine neue Datei: 

while (impfen_df$am == max(impfen_meta_df$Datum) &
       today() > impfen_df$am + 1) {

  Sys.sleep(300)
  # Nochmal versuchen: XLSX neu lesen
  msg("Gelesenes Datum: ",max(impfen_meta_df$Datum)," - nochmal versuchen...")
  impfen_meta_df <- impfen_meta_df %>% filter(Datum == today())
  while(nrow(impfen_meta_df) < 1) {
    tryCatch(tmp <- read.xlsx(rki_xlsx_url,sheet=4)) 
    impfen_meta_df <- na.omit(tmp)
  }
  impfen_meta_df <- impfen_meta_df %>% 
    filter(Datum != "Gesamt") %>%
    mutate(Datum = as.Date(as.numeric(Datum),origin="1899-12-30"))
  # 6 Stunden lang versuchen
  if (now() > ts+(6*3600)) {
    msg("KEINE NEUEN DATEN GEFUNDEN")
    stop("Timeout")
  } else {
    
  }  
}

msg("Daten bis ",max(impfen_meta_df$Datum)," gelesen - überprüfen...")

# Archivkopie

pp2 <- read.csv2("daten/impfen-gestern-2.csv")
pp3 <- read.csv2("daten/impfen-gestern-3.csv")
tabelle2 <- read.xlsx(rki_xlsx_url,sheet=2)
tabelle3 <- read.xlsx(rki_xlsx_url,sheet=3)
write.csv2(tabelle2,file = "daten/impfen-gestern-2.csv",row.names=FALSE)
write.csv2(tabelle3,file = "daten/impfen-gestern-3.csv",row.names=FALSE)

# Plausibilität prüfen
# Stumpfer Trick: eine Zeile an das jeweilige Archivdings binden, 
# wenn rbind() scheitert, hat sich die Tabelle verändert
# msg("Sheet 2 verändert?")
# tmp <- rbind(tabelle2,pp2)
# msg("Sheet 3 verändert?")
# tmp <- rbind(tabelle3,pp3)

msg("Ländertabelle vom ",max(impfen_meta_df$Datum)," lesen")
# Tabelle 2 seit dem 8.4. nicht mehr wirklich sinnvoll zu nutzen. 
# Einzige sinnvolle Info: Anteil u60/ü60

impfen_alle_df <- tabelle3 %>%
  filter(!is.na(RS) & !is.na(Bundesland)) %>%
  mutate(am = as.Date(max(impfen_meta_df$Datum))) %>%
  # Bundesländer auswählen und um Sternchen bereinigen
  filter(as.numeric(RS) %in% 1:16) %>%
  
  mutate(Bundesland = str_replace(Bundesland,"\\*","")) %>%
  inner_join(tabelle2 %>% select(-Bundesland),by = "RS") %>%
  # Offset für tabelle2 ist 21
  select(am, ID=RS, Bundesland,
         zentren_erst = 3, # Erstimpfung nur Impfzentren
         zentren_neu = 7,
         zentren_zweit = 8,
         zentren_zweit_neu = 12,
         zentren_biontech = 4,
         zentren_moderna = 5,
         zentren_az = 6,
         aerzte_erst = 13,
         aerzte_neu = 17,
         aerzte_zweit = 18,
         aerzte_zweit_neu = 22,
         aerzte_biontech = 14,
         aerzte_moderna = 15,
         aerzte_az = 16,
         impfquote_erst = 27, 
         impfquote_zweit = 30, 
         zentren_u60 = 33,   # Erstimpfung!
         zentren_ue60 = 34,
         aerzte_u60 = 37,
         aerzte_ue60 = 38) %>%
    mutate(zentren_erst = as.numeric(zentren_erst), # Erstimpfung nur Impfzentren
            zentren_neu = as.numeric(zentren_neu),
            zentren_zweit = as.numeric(zentren_zweit),
            zentren_zweit_neu = as.numeric(zentren_zweit_neu),
            zentren_biontech = as.numeric(zentren_biontech),
            zentren_moderna = as.numeric(zentren_moderna),
            zentren_az = as.numeric(zentren_az),
            aerzte_erst = as.numeric(aerzte_erst),
            aerzte_neu = as.numeric(aerzte_neu),
            aerzte_zweit = as.numeric(aerzte_zweit),
            aerzte_zweit_neu = as.numeric(aerzte_zweit_neu),
            aerzte_biontech = as.numeric(aerzte_biontech),
            aerzte_moderna = as.numeric(aerzte_moderna),
            aerzte_az = as.numeric(aerzte_az),
            impfquote_erst = as.numeric(impfquote_erst), 
            impfquote_zweit = as.numeric(impfquote_zweit),
            zentren_u60 = as.numeric(zentren_u60),   # Erstimpfung!
            zentren_ue60 = as.numeric(zentren_ue60),
            aerzte_u60 = as.numeric(aerzte_u60),
            aerzte_ue60 = as.numeric(aerzte_ue60))
  
# ---- Hessen isolieren ----
  msg("Hessen-Daten bauen")
  impf_df <- impfen_alle_df %>%
    # nur Hessen
    filter(as.numeric(ID) == 6) %>%
    select(-ID,-Bundesland) 


# Impfdaten archivieren für Vergleich

# ---- Vergleichskarte D generieren ----
write_sheet(impfen_alle_df %>%
              mutate(erst=zentren_erst+aerzte_erst,
                     zweit=zentren_zweit+aerzte_zweit,
                     ue60 = zentren_ue60+aerzte_ue60,
                     u60 = zentren_u60+aerzte_u60,
                     neu = zentren_neu+aerzte_neu,
                     zweit_neu = zentren_zweit_neu+aerzte_zweit_neu,
                     zentren_ue60q = zentren_ue60/(zentren_ue60+zentren_u60),
                     aerzte_ue60q = aerzte_ue60/(aerzte_ue60+aerzte_u60)) %>%
              select(am, Bundesland,
                     erst, impfquote_erst,
                     zweit, impfquote_zweit,
                     neu,zentren_neu,aerzte_neu,
                     zweit_neu,zentren_zweit_neu,aerzte_zweit_neu,
                     u60,ue60,
                     zentren_ue60q,aerzte_ue60q),
                aaa_id,sheet = "ImpfzahlenNational")
  
msg("Deutschland-Karte aktualisieren...")
dw_publish_chart("6PRAe") # Basisdaten-Seite


# ---- Basisdaten-Seite anpassen und aktualisieren ----
msg("Impfzahlen und Immunisierungsquote")
# Geimpft (Zeile 8)
range_write(aaa_id,as.data.frame(paste0("Geimpft (",
                                        format.Date(impf_df$am,"%d.%m."),")")),
            range="Basisdaten!A8",col_names=FALSE,reformat=FALSE)
range_write(aaa_id, as.data.frame(paste0(
  format((impf_df$zentren_erst+impf_df$aerzte_erst),
         big.mark=".",decimal.mark = ","),
  " (+", format((impf_df$zentren_neu+impf_df$aerzte_neu),
                big.mark = ".", decimal.mark = ",", nsmall =0),
  ")")),
  range="Basisdaten!B8", col_names = FALSE, reformat=FALSE)
dw_publish_chart("OXn7r") # Basisdaten-Seite

# ---- Impfzahlen-Seite ----

# Geimpfte Personen - Zeile 2
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$zentren_erst+impf_df$aerzte_erst,big.mark=".",decimal.mark = ","))),
  range="Impfzahlen!A2", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "Menschen sind in Hessen <strong>geimpft</strong>. (Stand: ",
  format.Date(impf_df$am,"%d.%m."),
  ")")),
  range="Impfzahlen!B2", col_names = FALSE, reformat=FALSE)

# Geimpfte heute (und Datum) - Zeile 3
range_write(aaa_id,as.data.frame(paste0(
  format((impf_df$zentren_neu+impf_df$aerzte_neu),big.mark=".",decimal.mark = ","))),
  range="Impfzahlen!A3", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "sind <strong>am ",
  format.Date(impf_df$am,"%d.%m."),
  "</strong> dazugekommen - ",
  format(impf_df$zentren_neu,big.mark=".",decimal.mark = ","),
  " in Impfzentren bzw. durch mobile Impfteams, ",
  format(impf_df$aerzte_neu,big.mark=".",decimal.mark = ","),
  " bei den Hausärzten")),
  range="Impfzahlen!B3", col_names = FALSE, reformat=FALSE)

# Impfquote - Zeile 4
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$impfquote_erst,big.mark=".",decimal.mark = ",",nsmall = 2))),
  range="Impfzahlen!A4", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "<strong>der Hessinnen und Hessen</strong> sind damit geimpft.")),
  range="Impfzahlen!B4", col_names = FALSE, reformat=FALSE)


# Durchgeimpfte - Zeile 5
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$aerzte_zweit+impf_df$zentren_zweit,big.mark=".",decimal.mark = ","))),
  range="Impfzahlen!A5", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0(
  "(",format(impf_df$impfquote_zweit,big.mark=".",decimal.mark = ",",digits=3),
  "%) haben <strong>beide Impfdosen</strong> erhalten und sind damit durchgeimpft.")),
  range="Impfzahlen!B5", col_names = FALSE, reformat=FALSE)


# Anteil nicht geimpfte Ü60 - Zeile 6

# range_write(aaa_id,as.data.frame(paste0(
#   format(((ue60-impf_df$zentren_ue60-impf_df$aerzte_ue60) %/% 1000) * 1000,
#          big.mark=".",decimal.mark = ","))),
#   range="Impfzahlen!A6", col_names = FALSE, reformat=FALSE)

# Impfgeschwindigkeit letzte 5 Tage
geschwindigkeit = mean(impf_tabelle$differenz_zum_vortag_erstimpfung[nrow(impf_tabelle)-4:nrow(impf_tabelle)])
tage_bis_x = (hessen-impf_df$erst)/geschwindigkeit
  
# range_write(aaa_id,as.data.frame(paste0(
#   "Menschen über 60 sind noch nicht geimpft. In den Impfzentren sind bislang ",
#   format(impf_df$zentren_ue60/(impf_df$zentren_ue60+impf_df$zentren_u60)*100,
#          big.mark=".",decimal.mark = ",",digits=3),
#   "% der Erstgeimpften über 60, bei den Hausärzten ",
#   format(impf_df$aerzte_ue60/(impf_df$aerzte_ue60+impf_df$aerzte_u60)*100,
#          big.mark=".",decimal.mark = ",",digits=3),
#   "%.")),
#   range="Impfzahlen!B6", col_names = FALSE, reformat=FALSE)

faelle_df <- read_sheet(aaa_id,sheet="Fallzahl4Wochen")
impf_tabelle <- read_sheet(aaa_id,sheet = "ArchivImpfzahlen") %>% filter(am <= today()-14)

hoffnung_str = paste0("<h4>",
                      format((max(impf_tabelle$personen)+max(faelle_df$gsum))/hessen*100,big.mark=".",decimal.mark = ",",digits = 2),
                "% der Menschen in Hessen sind inzwischen wahrscheinlich gegen ",
                "das Coronavirus immunisiert, weil sie eine Infektion überstanden oder ",
                "vor 14 Tagen die erste Impfung erhalten haben.</h4>")
dw_edit_chart(chart_id ="l5KKN", intro = hoffnung_str)
dw_publish_chart("l5KKN")


# ---- Impfstoffe ----
# Tagestabelle schreiben
write_sheet(impf_df,ss=aaa_id,sheet="ImpfenTagestabelle")

impfstoffe_df <- impf_df %>%
  select(zentren_biontech,
         zentren_moderna,
         zentren_az) %>%
  pivot_longer(everything(),names_prefix="zentren_",values_to="Impfzentren") %>%
  left_join(impf_df %>% select(aerzte_biontech,aerzte_moderna,aerzte_az) %>%
              pivot_longer(everything(),names_prefix="aerzte_",values_to="Hausärzte"),
            by="name")

  
write_sheet(impfstoffe_df,ss=aaa_id,sheet="Impfstoffe")
dw_edit_chart(chart_id="BfPeh",intro=paste0("Wie oft kam welcher Impfstoff zum Einsatz? Stand: ",
                           format.Date(impf_df$am,"%d.%m.")))
dw_publish_chart(chart_id="BfPeh")

# ---- Tag archivieren ----

msg("Archivdaten schreiben")

# Hessen-Daten auf das alte Format umlügen

hessen_archiv_df <- impf_df %>% 
  mutate(indikation_nach_alter=NA,
         berufliche_indikation=NA,
         medizinische_indikation=NA,
         pflegeheimbewohnerin=NA) %>%
  mutate(erst = zentren_erst+aerzte_erst,
         zweit = zentren_zweit+aerzte_zweit,
         neu = zentren_neu+aerzte_neu,
         zweit_neu = zentren_zweit_neu+aerzte_zweit_neu,
         zentren_impfdosen_neu = zentren_neu+zentren_zweit_neu,
         aerzte_impfdosen_neu = aerzte_neu+aerzte_zweit_neu) %>%
  select(am,
         personen=erst,
         differenz_zum_vortag_erstimpfung = neu,
         impfquote = impfquote_erst,
         personen_durchgeimpft = zweit,
         differenz_zum_vortag_zweitimpfung = zweit_neu,
         indikation_nach_alter,
         berufliche_indikation,
         medizinische_indikation,
         pflegeheimbewohnerin,
         Biontech= zentren_biontech,
         Moderna=zentren_moderna,
         AstraZeneca=zentren_az,
         durchgeimpft_quote= impfquote_zweit,
         aerzte_biontech,
         aerzte_moderna,
         aerzte_az,
         zentren_impfdosen_neu,
         aerzte_impfdosen_neu)
         

impf_tabelle <- read_sheet(aaa_id,sheet = "ArchivImpfzahlen")
if (as.Date(hessen_archiv_df$am) %in% as.Date(impf_tabelle$am)) {
  impf_tabelle[impf_tabelle$am == hessen_archiv_df$am,] <- hessen_archiv_df
} else {
  impf_tabelle <- rbind(impf_tabelle,hessen_archiv_df)
}

write_sheet(impf_tabelle,aaa_id,sheet = "ArchivImpfzahlen")

# Impftempo-Kurve aktualisieren - aus dem Google Sheet
dw_publish_chart(chart_id="SS8ta")


msg("OK")
### Ergänzen: Archivdaten bauen

# ---- Tabelle Impfzahlen neu bauen - als Funktion ----
# Die Impfzahlen holen wir einfach beim BR - soll sich Michael mit den Format-
# änderungen des RKI rumschlagen :) 
# Wird nicht mehr funktionieren!!!
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
             ifelse(is.na(dosen_differenz_zum_vortag),
                    dosen_erst_differenz_zum_vortag,dosen_differenz_zum_vortag))) %>%
    mutate(personen = as.numeric(ifelse(is.na(personen_erst_kumulativ),
                            dosen_kumulativ,
                            personen_erst_kumulativ))) %>%
    mutate(impfquote = as.numeric(personen)/hessen*100) %>%
    # Indikationen
    mutate(indikation_nach_alter = as.numeric(ifelse(is.na(indikation_alter_erst),
                                          indikation_alter_dosen,
                                          indikation_alter_erst))) %>%
    mutate(berufliche_indikation = as.numeric(ifelse(is.na(indikation_beruf_dosen),
                                          indikation_beruf_erst,
                                          indikation_beruf_dosen))) %>%
    mutate(medizinische_indikation = as.numeric(ifelse(is.na(indikation_medizinisch_erst),
                                          indikation_medizinisch_dosen,
                                          indikation_medizinisch_erst))) %>%
    mutate(pflegeheimbewohnerin = as.numeric(ifelse(is.na(indikation_pflegeheim_erst),
                                          indikation_pflegeheim_dosen,
                                          indikation_pflegeheim_erst))) %>%
    select(am,personen,differenz_zum_vortag_erstimpfung,impfquote,
           personen_durchgeimpft = personen_voll_kumulativ,
           differenz_zum_vortag_zweitimpfung = dosen_voll_differenz_zum_vortag,
           indikation_nach_alter,
           berufliche_indikation,
           medizinische_indikation,
           pflegeheimbewohnerin,
           Biontech = dosen_erst_biontech_kumulativ,
           Moderna = dosen_erst_moderna_kumulativ) %>%
    mutate(Moderna = as.numeric(ifelse(is.na(Moderna),0,Moderna))) %>%
    mutate(Biontech = as.numeric(ifelse(is.na(Biontech),personen,Biontech))) %>%
    mutate(personen_durchgeimpft = as.integer(personen_durchgeimpft)) %>%
    mutate(durchgeimpft_quote = personen_durchgeimpft/hessen*100)
    
    write_sheet(impf_tabelle,aaa_id,sheet="ArchivImpfzahlen")
    return(impf_tabelle)
}
