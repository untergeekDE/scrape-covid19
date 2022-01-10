##### hole-voc.R ####
# Freitags morgens die Daten zu den "Variants of Concern" von der RKI-Seite holen
# und in Grafik packen - mit Fokus auf Delta und Omikron
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 10.12.2021

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

library(teamr)

# Seite beim RKI lesen
rki_url <- "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/VOC_VOI_Tabelle.xlsx?__blob=publicationFile"

# Nur die dritte Seite lesen: Viren-Anteile prozentual nach Ländern. 
voc_df <- read.xlsx(rki_url,sheet=3,colNames = F)

# Die Seite ist ein wenig tricky konstruiert:
# - erste Zeile enthält die KW (aber nur in der jeweils ersten Spalte)
# - zweite Zeile die VOC (Alpha, Beta, ....)
# - erste Spalte: Benennung der Zeile, Bundesland
#
# Deshalb am besten alle Daten extra filtern und dann pivotieren. 

# Wochen in einen Vektor
KW <- as.numeric(voc_df[1,] %>% select(-1))

# Lücken in KW ganz stumpf füllen
for (i in 2:length(KW)) {
  # Leere Spalte? Fülle von links auf
  if (is.na(KW[i])) {
    KW[i] <- KW[i-1]
  } else {
  # Jahreswechsel? Also: kleiner als der erste Wert? 
    if (KW[i] < KW[1]) { KW[i] <- KW[i]+52 }
  }
}

# VOC auch in Vektor
VOC <- as.character(voc_df[2,] %>% select(-1))
# ...und die Prozentsätze für Hessen
Anteil <- as.numeric(voc_df %>% filter(str_detect(X1,"Hessen")) %>% select(-1))

# Dataframe konstruieren
voc_n_df <- tibble(KW,VOC,Anteil) %>%
  # Prozentwerte erzeugen
  mutate(Anteil = 100*Anteil) %>% 
  pivot_wider(names_from = "VOC", values_from="Anteil") %>% 
  mutate(andere = 100-Alpha-Beta-Gamma-Delta-Omikron) %>%
  # Ganz simpel - Wochen (die evtl. nach oben korrigiert wurden) in Stichtag - 
  # der Sonntag der jeweilgen Woche, wie üblich. 
  mutate(Stichtag = as_date("2021-01-03")+(7 * KW)) %>% 
  # Datum noch nach vorn sortieren
  relocate(Stichtag) 

# In die Datawrapper-Grafik pushen
dw_data_to_chart(voc_n_df,chart_id="I7UOO")
dw_publish_chart(chart_id="I7UOO")

# Teams-Karte schicken
cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
cc$title(paste0("Corona-Update: VOC - ",format.Date(today(),"%d.%m.%y")))
cc$text("hole-voc.R")

# Drei letzte Wochen 
n <- nrow(voc_n_df)
sec1 <- card_section$new()

sec1$text(paste0("KW ",voc_n_df$KW[n],
                 "(bis ",format.Date(voc_n_df$Stichtag[n],"%d.%m.%Y"),")"))

sec1$add_fact("Alpha (%)",format(voc_n_df$Alpha[n],
                               big.mark = ".",decimal.mark=",",nsmall=2))
sec1$add_fact("Beta (%)",format(voc_n_df$Beta[n],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec1$add_fact("Gamma (%)",format(voc_n_df$Gamma[n],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec1$add_fact("Delta (%)",format(voc_n_df$Delta[n],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec1$add_fact("Omikron (%)",format(voc_n_df$Omikron[n],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec1$add_fact("andere (%)",format(voc_n_df$andere[n],
                                 big.mark = ".",decimal.mark=",",nsmall=2))

cc$add_section(new_section = sec1)

# Intensivbetten
sec2 <- card_section$new()

sec2$text(paste0("KW ",voc_n_df$KW[n-1],
                 "(bis ",format.Date(voc_n_df$Stichtag[n-1],"%d.%m.%Y"),")"))

sec2$add_fact("Alpha (%)",format(voc_n_df$Alpha[n-1],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec2$add_fact("Beta (%)",format(voc_n_df$Beta[n-1],
                                big.mark = ".",decimal.mark=",",nsmall=2))
sec2$add_fact("Gamma (%)",format(voc_n_df$Gamma[n-1],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec2$add_fact("Delta (%)",format(voc_n_df$Delta[n-1],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec2$add_fact("Omikron (%)",format(voc_n_df$Omikron[n-1],
                                   big.mark = ".",decimal.mark=",",nsmall=2))
sec2$add_fact("andere (%)",format(voc_n_df$andere[n-1],
                                  big.mark = ".",decimal.mark=",",nsmall=2))

cc$add_section(new_section = sec2)

# Normalbetten
sec3 <- card_section$new()

sec3$text(paste0("KW ",voc_n_df$KW[n-2],
                 "(bis ",format.Date(voc_n_df$Stichtag[n-2],"%d.%m.%Y"),")"))

sec3$add_fact("Alpha (%)",format(voc_n_df$Alpha[n-2],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec3$add_fact("Beta (%)",format(voc_n_df$Beta[n-2],
                                big.mark = ".",decimal.mark=",",nsmall=2))
sec3$add_fact("Gamma (%)",format(voc_n_df$Gamma[n-2],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec3$add_fact("Delta (%)",format(voc_n_df$Delta[n-2],
                                 big.mark = ".",decimal.mark=",",nsmall=2))
sec3$add_fact("Omikron (%)",format(voc_n_df$Omikron[n-2],
                                   big.mark = ".",decimal.mark=",",nsmall=2))
sec3$add_fact("andere (%)",format(voc_n_df$andere[n-2],
                                  big.mark = ".",decimal.mark=",",nsmall=2))

cc$add_section(new_section = sec3)
# Karte vorbereiten und abschicken. 

cc$send()

