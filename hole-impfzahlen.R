##################################### hole-impfzahlen.R #########################
# - Impfzahlen 

# Stand: 8.6.2021


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
tryCatch(impfen_meta_df <- read.xlsx(rki_xlsx_url,sheet=4) %>% 
  mutate(Datum = as.Date(as.numeric(Datum),origin="1899-12-30")) %>%
  na.omit())

ts <- now()
# Zahlen aus der Archiv-Tabelle lesen. 
impfen_df <- read_sheet(ss=aaa_id,sheet = "ArchivImpfzahlen") %>%
  filter(am == max(am))



while ( 
  # Zusätzliche Bedingung ausgeklammert: gelesenes Datum neuer als
  # letztes archiviertes. 
  #   impfen_df$am == max(impfen_meta_df$Datum) &
  # Noch keine Datei mit Datum von gestern?
       today() > max(impfen_meta_df$Datum)+1) {

  Sys.sleep(60)
  # Abbruchbedingung: 6x3600 Sekunden vergangen?
  if (now() > ts+(6*3600)) {
    msg("KEINE NEUEN DATEN GEFUNDEN")
    stop("Timeout")
  }
  # Nochmal versuchen: XLSX neu lesen
  msg("Gelesenes Datum: ",max(impfen_meta_df$Datum)," - nochmal versuchen...")
  # Tabelle mit den (Meta-)Daten und Impfzahlen lesen; Spalte Datum ist das
  # Stichdatum
  tryCatch(impfen_meta_df <- read.xlsx(rki_xlsx_url,sheet=4) %>% 
             mutate(Datum = as.Date(as.numeric(Datum),origin="1899-12-30")) %>%
             na.omit())
  # Plausibilitätsprüfung: Wenn Zahl der Geimpften der von gestern entspricht. 
  # lieber nochmal lesen. 
    
}
if (!server) beepr::beep(2)
msg("Daten bis ",max(impfen_meta_df$Datum)," gelesen - überprüfen...")

# Archivkopie

pp2 <- read.csv2("daten/impfen-gestern-2.csv")
pp3 <- read.csv2("daten/impfen-gestern-3.csv")
tabelle2 <- read.xlsx(rki_xlsx_url,sheet=2)
tabelle3 <- read.xlsx(rki_xlsx_url,sheet=3)
tabelle4 <- read.xlsx(rki_xlsx_url,sheet=4)
write.csv2(tabelle2,file = "daten/impfen-gestern-2.csv",row.names=FALSE)
write.csv2(tabelle3,file = "daten/impfen-gestern-3.csv",row.names=FALSE)
write.csv2(tabelle4,file = "daten/impfen-gestern-4.csv",row.names=FALSE)

msg("Ländertabelle vom ",max(impfen_meta_df$Datum)," lesen")
# Tabelle 2 war seit dem 8.4. nicht mehr wirklich sinnvoll zu nutzen. 
# Seit dem 7.6. enthält sie die Altersaufschlüsselung )Impfquoten
# Deswegen: Gesamt-Tabelle machen mit den Impfquoten

# Plausibilität: In beiden Tabellen die gleichen Werte für Impfzahlen? 
# Sonst stopp.
#
# In Tabelle 2 sind es die Spaltennummern
# - 3 (Impfdosen)
# - 4 (Erstimpfungen)
# - 5 (Zweitimpfungen)
#
# In Tabelle 3 sind es die Spaltennummern
# - 3 (Erstimpfungen)
# - 9 (ZWeitimpfungen)

# Vergleiche Erstimpfungen
if (any((tabelle2 %>% rename(RS=1) %>% 
     mutate(RS = as.integer(RS)) %>%
     filter(!is.na(RS)) %>%
     arrange(RS) %>%
     select(erst= 4)) !=
    (tabelle3 %>% rename(RS=1) %>%
     mutate(RS = as.integer(RS)) %>%
     filter(!is.na(RS)) %>%
     arrange(RS) %>%
     select(erst = 3)))) {
  View(tabelle2)
  View(tabelle3)
  stop("!!!Abweichung Erstimpfungen")
}

# Vergleiche Zweitimpfungen
if (any((tabelle2 %>% rename(RS=1) %>% 
         mutate(RS = as.integer(RS)) %>%
         filter(!is.na(RS)) %>%
         arrange(RS) %>%
         select(zweit= 5)) !=
        (tabelle3 %>% rename(RS=1) %>%
         mutate(RS = as.integer(RS)) %>%
         filter(!is.na(RS)) %>%
         arrange(RS) %>%
         select(zweit = 9)))) {
  View(tabelle2)
  View(tabelle3)
  stop("!!!Abweichung Erstimpfungen")
}




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
#
# - biontech_zweit
# - moderna_zweit
# - az_zweit
#
# - Tabellenspalte für händische Anmerkungen


impfen_alle_df <- tabelle3 %>%
  filter(!is.na(RS) & !is.na(Bundesland)) %>%
  mutate(am = as.Date(max(impfen_meta_df$Datum))) %>%
  # Bundesländer auswählen und um Sternchen bereinigen
  filter(as.numeric(RS) %in% 1:16) %>%
  mutate(Bundesland = str_replace(Bundesland,"\\*","")) %>%
  # Sinnvolle Tabellenspalten auswählen und beibehalten
  select(am, ID=RS, Bundesland,
         personen = 3,
         durchgeimpft = 9,
         neu = 8,
         neu_zweit =14,
         biontech_erst = 4,
         moderna_erst = 5,
         az_erst = 6,
         janssen = 7,
         biontech_zweit = 10,
         moderna_zweit = 11, 
         az_zweit = 12) %>%
  inner_join(tabelle2 %>% 
               select(ID=1,
                      impfdosen = 3, 
                      quote_erst = 6,
                      quote_erst_u18 = 7,
                      quote_erst_18_60 =8, 
                      quote_erst_ue60 = 9,
                      quote_zweit = 10,
                      quote_zweit_u18 = 11,
                      quote_zweit_18_60 = 12,
                      quote_zweit_ue60 =13) %>%
               filter(as.numeric(ID) %in% 1:16),by="ID") %>%
  # alle Spalten außer am, ID und Bundesland in Zahlen umwandeln 
  mutate_at(4:23,as.numeric)
               

# ---- Hessen isolieren ----
  msg("Hessen-Daten bauen")
  impf_df <- impfen_alle_df %>%
    # nur Hessen
    filter(as.numeric(ID) == 6) %>%
    select(-ID,-Bundesland) %>%
    # Impfquoten selbst berechnen, genauer
    mutate(quote_erst = personen/hessen*100,
           quote_zweit = durchgeimpft/hessen*100)
  
  # Tagestabelle schreiben
  write_sheet(impf_df,ss=aaa_id,sheet="ImpfenTagestabelle")


# Impfdaten archivieren für Vergleich

# ---- Vergleichskarte D generieren ----
write_sheet(impfen_alle_df,aaa_id,sheet = "ImpfzahlenNational")
  
msg("Deutschland-Karte aktualisieren...")
#dw_data_to_chart(impfen_alle_df,chart_id="6PRAe")
dw_publish_chart("6PRAe") # Basisdaten-Seite

# Janssen-Neuimpfungen berechnen - Differenz zum kumulativen Stand gestern, 
# anders geht nicht. 

# Alle bis Stand gestern
tmp <- (read_sheet(aaa_id,sheet = "ArchivImpfzahlen") %>% filter(am < today()-1))$janssen 

# Unterste Zeile
tmp <- tmp[length(tmp)]
tmp <- ifelse(is.na(tmp),0,tmp)
janssen_neu <- impf_df$janssen - tmp


# ---- Basisdaten-Seite anpassen und aktualisieren ----
msg("Impfzahlen und Immunisierungsquote")
# Geimpft mit Quote und Datum (Zeile 8)
range_write(aaa_id,as.data.frame(paste0("Geimpft (",
                                        format.Date(impf_df$am,"%d.%m."),")")),
            range="Basisdaten!A8",col_names=FALSE,reformat=FALSE)

range_write(aaa_id, as.data.frame(paste0(
  # kumulativ: Erstgeimpfte (also RKI-Zahl plus Janssen)
  base::format(impf_df$personen,big.mark=".",decimal.mark = ","),
  # Differenz: Erstgeimpfte neu plus Janssen-Geimpfte neu
  " (", base::format(impf_df$quote_erst,
                big.mark = ".", decimal.mark = ",", nsmall =0,digits=4),
  "%)")),
  range="Basisdaten!B8", col_names = FALSE, reformat=FALSE)

# Neu dazugekommen
range_write(aaa_id,as.data.frame("Erst-/Durchimpfungen"),
            range="Basisdaten!A9",col_names=FALSE,reformat=FALSE)

range_write(aaa_id, as.data.frame(paste0("+",
  # neu erstgeimpft
  base::format(impf_df$neu,big.mark=".",decimal.mark = ","),
  # neu durchgeimpft (enhätl auch Janssen)
  " / +", base::format(impf_df$neu_zweit,
                     big.mark = ".", decimal.mark = ",", nsmall =0))),
  range="Basisdaten!B9", col_names = FALSE, reformat=FALSE)
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
  format.Date(impf_df$am,"%d.%m."),
  ")")),
  range="Impfzahlen!B2", col_names = FALSE, reformat=FALSE)

# Durchgeimpfte - Zeile 3
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$durchgeimpft,big.mark=".",decimal.mark = ","),
  " (",format(impf_df$quote_zweit,big.mark=".",decimal.mark = ",",digits=3),"%)"
  )),
  range="Impfzahlen!A3", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0("sind <strong>durchgeimpft</strong>.")),
  range="Impfzahlen!B3", col_names = FALSE, reformat=FALSE)

# Neue Impfungen - Zeile 4
range_write(aaa_id,as.data.frame(paste0(
  "+",format(impf_df$neu,big.mark="."),
  " / +",format(impf_df$neu_zweit,big.mark = "."))),
  range="Impfzahlen!A4", col_names = FALSE, reformat=FALSE)


range_write(aaa_id,as.data.frame(paste0(
  "sind am ",
  format.Date(impf_df$am,"%d.%m."),
  "<strong> dazugekommen</strong>.")),
  range="Impfzahlen!B4", col_names = FALSE, reformat=FALSE)


#Impfquote Ü60 - Zeile 5
range_write(aaa_id,as.data.frame(paste0(
  format(impf_df$quote_erst_ue60,decimal.mark=","),
  "% / ",format(impf_df$quote_zweit_ue60,decimal.mark = ","),"%")),
  range="Impfzahlen!A5", col_names = FALSE, reformat=FALSE)

range_write(aaa_id,as.data.frame(paste0("der <strong>besonders gefährdeten Menschen über 60",
                                        "</strong>",
                                        " sind inzwischen erst-/durchgeimpft.")),
  range="Impfzahlen!B5", col_names = FALSE, reformat=FALSE)


faelle_df <- read_sheet(aaa_id,sheet="Fallzahl4Wochen")
impf_tabelle <- read_sheet(aaa_id,sheet = "ArchivImpfzahlen") %>% filter(am <= today()-14)

hoffnung_str = paste0("<h4>",
                      format((max(impf_tabelle$personen)+max(faelle_df$gsum))/hessen*100,big.mark=".",decimal.mark = ",",digits = 2),
                "% der Menschen in Hessen sind inzwischen wahrscheinlich gegen ",
                "das Coronavirus immunisiert, weil sie eine Infektion überstanden oder ",
                "vor 14 Tagen die erste Impfung erhalten haben.</h4>")
dw_edit_chart(chart_id ="l5KKN", intro = hoffnung_str)
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
                                            format.Date(impf_df$am,"%d.%m.")))
dw_publish_chart(chart_id="BfPeh")



# ---- Quoten nach Alter ----


quoten_alter_df <- impf_df %>%
  select(quote_erst_u18,
         quote_erst_18_60,
         quote_erst_ue60,
         quote_erst_Hessen = quote_erst,
         quote_zweit_u18,
         quote_zweit_18_60,
         quote_zweit_ue60,
         quote_zweit_Hessen = quote_zweit) %>%
  # Aufbereiten in zwei Spalten "Erstgeimpft" und "Zweitgeimpft"
  pivot_longer(everything()) %>%
  mutate(erstzweit = ifelse(str_detect(name,"_erst_"),"nur erstgeimpft","durchgeimpft")) %>%
  mutate(name = str_replace(name,"quote_.+_","")) %>%
  pivot_wider(names_from=erstzweit,values_from=value) %>%
  # Spalte "Erstgeimpft" um Zweitgeimpfte verringern, um 
  # als gestapelte Säulen anzeigen zu können
  mutate(`nur erstgeimpft`= `nur erstgeimpft`-durchgeimpft)
  

  
write_sheet(quoten_alter_df,ss=aaa_id,sheet="ImpfquotenAlter")
dw_edit_chart(chart_id="vgJSw",annotate = paste0("Janssen-Impfstoff wird in beiden Kategorien ",
                                                  "gezählt und führt zu leichten Verzerrungen. <br>",
" Stand: ",
                           format.Date(impf_df$am,"%d.%m.%Y")))
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
# Wennde Spass dran hast, rechne noch mal kumulativ die Impfdosen aus. 
# Ermöglicht Differenzberechnung zum Vortag (und damit u.U. Korrektur der
# Erstimpfungs- und Zweitimpfungs-Werte)
archiv_tabelle$impfdosen[archiv_tabelle$am==lastdate] <- impf_df$impfdosen
#
archiv_tabelle$quote_erst_u18[archiv_tabelle$am==lastdate] <- impf_df$quote_erst_u18
archiv_tabelle$quote_erst_18_60[archiv_tabelle$am==lastdate] <- impf_df$quote_erst_18_60
archiv_tabelle$quote_erst_ue60[archiv_tabelle$am==lastdate] <- impf_df$quote_erst_ue60
archiv_tabelle$quote_zweit_u18[archiv_tabelle$am==lastdate] <- impf_df$quote_zweit_u18
archiv_tabelle$quote_zweit_18_60[archiv_tabelle$am==lastdate] <- impf_df$quote_zweit_18_60
archiv_tabelle$quote_zweit_ue60[archiv_tabelle$am==lastdate] <- impf_df$quote_zweit_ue60

  
write_sheet(archiv_tabelle,aaa_id,sheet = "ArchivImpfzahlen")

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
