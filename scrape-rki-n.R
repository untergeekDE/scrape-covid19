###################### scrape-rki-json ###########################
#
# Scraped das RKI-Dashboard und legt Daten nach Geschlecht und Alter ab. 
# 
# Jan Eggers, zuletzt verändert 18.4. (Umstellung auf CSV)

#library(jsonlite)
library(tidyverse)
library(googlesheets4)
library(lubridate)

# Bei Aufruf ohne  Argument "server"
server <- FALSE

# Argumente werden in einem String-Vektor namens args übergeben,
# wenn ein Argument übergeben wurde, dort suchen, sonst Unterverzeichnis "src"

args = commandArgs(trailingOnly = TRUE)
if (length(args)!=0) { 
  server <- args[1] %in% c("server","nologs")
  logfile <- args[1] != "nologs"
} 

###### VERSION FÜR DEN SERVER #####
if (server) {
  setwd("/home/jan_eggers_hr_de/rscripts/") 
  if (logfile) sink(file = "logs/scrape-rki.log", append = TRUE, type = "messages")
}

heute = ymd(today())
cat("\n\n----Scraper-Job am",as.character(heute),"\n")
cat("Start: ", as.character(now()),"\n")



#---- Daten für Hessen nach Alter und Geschlecht ----
cat("Versuche Alter und Geschlecht zu lesen...\n")
rki_alt_url <- "https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv"
rki_df <- read.csv(url(rki_alt_url)) 

# Hessische Fälle mal vorfiltern; eigentlich nur ein Test
he_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>%
group_by(Altersgruppe, Geschlecht) %>% 
  summarize(AnzahlFall = sum(AnzahlFall),
            AnzahlGenesen = sum(AnzahlGenesen),
            AnzahlTodesfall = sum(AnzahlTodesfall)) 
head(he_df)

# Tabelle AnzahlFall nach Alter und Geschlecht anordnen
alter_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>%
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% # Alter unbekannt -> filtern
  mutate(Altersgruppe = paste0(str_replace_all(Altersgruppe,"A","")," Jahre")) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = AnzahlFall) %>%
  select(1,männlich = M, weiblich = W) 

# Berechne Inzidenzen für die Altersgruppen
alter_df$pop <- c(302453,
                  568195,
                  1482492,
                  2222613,
                  1306319,
                  383737)
alter_df <- alter_df %>%
  mutate(Inzidenz = (männlich+weiblich)/pop*100000) %>%
  select(1,2,3,5) # Spalte pop wieder rausnehmen


# Die Fälle, die nicht zuzuordnen sind - Alter, Geschlecht - in letzte Zeile
unbek_df <- rki_df %>% 
  filter(Bundesland == "Hessen") %>%
  filter(!str_detect(Altersgruppe,"A[0-9]") | 
           !(Geschlecht %in% c("M","W")))


alter_df[nrow(alter_df)+1,] <- NA
alter_df$Altersgruppe[nrow(alter_df)] <- "unbekannt" 
alter_df$männlich[nrow(alter_df)] <- sum(unbek_df$AnzahlFall)
  
tote_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>%
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% # Alter unbekannt -> filtern
  mutate(Altersgruppe = paste0(str_replace_all(Altersgruppe,"A","")," Jahre")) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlTodesfall = sum(AnzahlTodesfall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = AnzahlTodesfall) %>%
  select(1,männlich = M, weiblich = W)

# Anteil Todesfälle in der Altersgruppe berechnen

# Unbekannte Fälle ergänzen - lassen wir weg
# tote_df[nrow(tote_df)+1,] <- NA
# tote_df$Altersgruppe[nrow(tote_df)] <- "unbekannt" 
# tote_df$männlich[nrow(tote_df)] <- sum(unbek_df$AnzahlTodesfall)

# In ein DF umbasteln
laender_faelle_df <- rki_df %>%
  select(Bundesland,AnzahlFall) %>%
  group_by(Bundesland) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Bundesland, values_from = AnzahlFall) %>%
  mutate(datum = heute) %>%
  select(17,1:16)

# In ein DF umbasteln
laender_tote_df <- rki_df %>%
  select(Bundesland,AnzahlTodesfall) %>%
  group_by(Bundesland) %>%
  summarize(AnzahlTodesfall = sum(AnzahlTodesfall)) %>%
  pivot_wider(names_from = Bundesland, values_from = AnzahlTodesfall) %>%
  mutate(datum = heute) %>%
  select(17,1:16)


############ Google-Sheet(s) und CSVs schreiben ##############
if (server) {
  ## Authentifizierung Google-Sheets für den Server
  sheets_deauth() # Authentifizierung löschen
  sheets_auth(email="googlesheets4@scrapers-272317.iam.gserviceaccount.com",path="/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json")
} else {
  # lokale Version des Keys
  sheets_email <- "googlesheets4@scrapers-272317.iam.gserviceaccount.com"
  sheets_keypath <- "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json"
}


# Alter und Geschlecht
rki_alter_id="1RxlykWHoIZEq91M1bJNf26VPn8OWwI6VsgvAO47HM8Q"

cat("Ergänze Google-Sheet um die aktuellen Zahlen Hessen nach Alter und Geschlecht\n")
# Sheet in den richtigen Bereich schreiben
sheets_edit(rki_alter_id,alter_df,sheet = "quelldaten", range = "A1:D8", col_names = TRUE) 


options(scipen=100,           # Immer normale Kommazahlen ausgeben, Keine wissenschaftlichen Zahlen
        OutDec=","	          # Komma ist Dezimaltrennzeichen bei Ausgabe
)  
# Größte Kontrolle über Base-Funktion write.table:
write.table(alter_df, "rki-alter.csv", 
            sep = ";",            # Tab als Trennzeichen 
            dec = ",",             # Komma als Dezimaltrenner
            na = "NA",             # NA-Wert für Excel auch na ="" oder na ="#NV"
            row.names = FALSE, 
            fileEncoding = "UTF-8")

# Länder-Zahlen in Übersicht faelle
cat("Ergänze Google-Sheet um Fallzahlen nach Ländern mit Zeitstempel\n")
sheets_append(laender_faelle_df,rki_alter_id,sheet ="faelle")
cat("Ergänze Google-Sheet um Todesfälle nach Ländern mit Zeitstempel\n")
sheets_append(laender_tote_df,rki_alter_id,sheet ="tote")

cat("Schreibe Altersstruktur Todesfälle...\n")
sheets_write(tote_df,ss = rki_alter_id, sheet="tote_nach_alter")
# Beschreibt ein Sheet in dem RKI-Dokument
# Ein zweites Google Sheet zieht sich per IMPORTRANGE die Daten und rechnet sie um. 
# Aus dem zieht sich dann wieder Datawrapper alles Wesentliche
write_csv2(tote_df,"rki-tote.csv")

cat("Schreibe Zeitstempel...\n")
sheets_edit(rki_alter_id,as.data.frame(as.character(heute)),sheet="live-daten",range= "A1",col_names = FALSE)



cat("Erledigt.",as.character(now()))
