###################### scrape-rki-json ###########################
#
# Scraped das RKI-Dashboard und legt Daten nach Geschlecht und Alter ab. 
# 
# Jan Eggers, zuletzt verändert 26.4. (mit Semaphore/Logging)

#library(jsonlite)
library(tidyverse)
library(googlesheets4)
library(lubridate)
library(DatawRappr)

# Bei Aufruf ohne  Argument "server"
server <- FALSE

# ---- Logging und Update der Semaphore-Seite ----
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B7, Statuszeile in C7
  d <- data.frame(b = now(tzone= "CEST"), c = paste0(x,...))
  sheets_edit(id_msg,d,sheet="Tabellenblatt1",
              range="B7:C7",col_names = FALSE,reformat=FALSE)
  if (server) Sys.sleep(10)     # Skript ein wenig runterbremsen wegen Quoa
  if (logfile != "") {
    cat(x,...,file = logfile, append = TRUE)
  }
}




# Argumente werden in einem String-Vektor namens args übergeben,
# wenn ein Argument übergeben wurde, dort suchen, sonst Unterverzeichnis "src"

args = commandArgs(trailingOnly = TRUE)
if (length(args)!=0) { 
  server <- args[1] %in% c("server","logfile")
  if (args[1] != "logfile") logfile <- "./logs/scrape-rki.log" 
} 

# ---- Google-Credentials setzen ----

sheets_deauth() # Authentifizierung löschen
if (server) {
  setwd("/home/jan_eggers_hr_de/rscripts/") 
  sheets_auth(email="googlesheets4@scrapers-272317.iam.gserviceaccount.com", 
              path = "/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json")
} else {
  sheets_auth(email="googlesheets4@scrapers-272317.iam.gserviceaccount.com", 
              path = "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json")
}
msg("Google Credentials erfolgreich gesetzt\n")

heute = ymd(today())

msg("\n\n----Scraper-Job am",as.character(heute),"\n")
msg("Start: ", as.character(now()),"\n")



#---- Daten für Hessen nach Alter und Geschlecht ----
msg("Versuche Alter und Geschlecht zu lesen...\n")
rki_url <- "https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv"
ndr_url <- "https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/rki_api.current.csv"
rki_df <- read.csv(url(ndr_url)) 

rki_url2 <- "https://prod-hub-indexer.s3.amazonaws.com/files/dd4580c810204019a7b8eb3e0b329dd6/0/full/4326/dd4580c810204019a7b8eb3e0b329dd6_0_full_4326.csv"
rki_temp_url <- "https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data"
#rki_df <- read.csv(url(rki_temp_url)) 

msg("Daten erfolgreich von Temp-CSV beim RKI gelesen")
# Hessische Fälle mal vorfiltern; eigentlich nur ein Test
he_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>%
group_by(Altersgruppe, Geschlecht) %>% 
  summarize(AnzahlFall = sum(AnzahlFall),
            AnzahlGenesen = sum(AnzahlGenesen),
            AnzahlTodesfall = sum(AnzahlTodesfall)) 
head(he_df)

# ---- Nur die hessischen Fälle lokal ablegen ----

write_csv2((rki_df %>% filter(Bundesland == "Hessen")),"rki_df.csv")


# Tabelle AnzahlFall nach Alter und Geschlecht anordnen
alter_df <- rki_df %>%
  filter(Bundesland == "Hessen") %>%
  # Alter unbekannt? Ausfiltern. 
  filter(str_detect(Altersgruppe,"A[0-9]")) %>% 
  # Genesene und Todesfälle ausfiltern - nur aktive Fälle
  filter(Genesen != 1) %>%              #
  filter(AnzahlTodesfall == 0) %>%
  group_by(Altersgruppe, Geschlecht) %>%
  summarize(AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = AnzahlFall) %>%
  select(1,männlich = M, weiblich = W) 

# Berechne Inzidenzen für die Altersgruppen
# Quelle Bevölkerungsstatistik Hessen statistik.hessen.de
alter_df$pop <- c(302453,
                  568195,
                  1482492,
                  2222613,
                  1306319,
                  383737)

alter_df <- alter_df %>%
  mutate(Inzidenz = (männlich+weiblich)/pop*100000) %>%
  select(Altersgruppe, männlich, weiblich, Inzidenz) # Spalte pop wieder rausnehmen


# Die Fälle, die nicht zuzuordnen sind - Alter, Geschlecht - in letzte Zeile
unbek_df <- rki_df %>% 
  filter(Bundesland == "Hessen") %>%
  # Genesene und Todesfälle ausfiltern - nur aktive Fälle
  filter(Genesen != 1) %>%              #
  filter(AnzahlTodesfall == 0) %>%
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
  summarize(AnzahlTodesfall = sum(AnzahlTodesfall),
            AnzahlFall = sum(AnzahlFall)) %>%
  pivot_wider(names_from = Geschlecht, values_from = c(AnzahlTodesfall, AnzahlFall)) %>%
  mutate(inz = (AnzahlTodesfall_M+AnzahlTodesfall_W)/(AnzahlFall_W+AnzahlFall_M)*100) %>%
  select(Altersgruppe,männlich = AnzahlTodesfall_M, weiblich = AnzahlTodesfall_W, Inzidenz = inz)

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

msg("Ergänze Google-Sheet um die aktuellen Zahlen Hessen nach Alter und Geschlecht\n")
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
msg("Ergänze Google-Sheet um Fallzahlen nach Ländern mit Zeitstempel\n")
sheets_append(laender_faelle_df,rki_alter_id,sheet ="faelle")
msg("Ergänze Google-Sheet um Todesfälle nach Ländern mit Zeitstempel\n")
sheets_append(laender_tote_df,rki_alter_id,sheet ="tote")

msg("Schreibe Altersstruktur Todesfälle...\n")


sheets_write(tote_df,ss = rki_alter_id, sheet="tote_nach_alter")
tote_id <- "https://docs.google.com/spreadsheets/d/1Mxpth-kJUyA14T4MXLg2sP1KhCYazAuc1Re9sBwY_KA"
sheets_write(tote_df,ss = tote_id, sheet="Tabellenblatt1")
# Beschreibt ein Sheet in dem RKI-Dokument
# Ein zweites Google Sheet zieht sich per IMPORTRANGE die Daten und rechnet sie um. 
# Aus dem zieht sich dann wieder Datawrapper alles Wesentliche
write_csv2(tote_df,"rki-tote.csv")

msg("Schreibe Zeitstempel...\n")
sheets_edit(rki_alter_id,as.data.frame(as.character(heute)),sheet="live-daten",range= "A1",col_names = FALSE)

# ---- Zahl der Genesenen für heute nachtragen ----
id_cck="1h0bvmSjSC-7osQpt94iGre9K5o_Atfj0UnLyQbuN9l4"
id_fallzahl = "1OhMGQJXe2rbKg-kCccVNpAMc3yT2i3ubmCndf-zX0JU"
id_basisdaten <- "1m6hK7s1AnDbeAJ68GSSMH24z4lL7_23RHEI8TID24R8"

# - Kreisdaten

cck_df <- read_sheet(id_cck,sheet="daten")
msg("Kreisdaten berechnen und aktualisieren")

genesen_df <- rki_df %>% 
  filter(Bundesland == "Hessen") %>%
  mutate(IdLandkreis = paste0("0",as.character(IdLandkreis))) %>%
  select(AGS = IdLandkreis,gsum = AnzahlGenesen) %>%
  group_by(AGS) %>%
  summarize(gsum = sum(gsum))

# Tabelle 
cck_df <- cck_df %>%
  left_join(genesen_df, by = c("ags_text" = "AGS")) %>%
  mutate(AnzahlGenesen = gsum,
         AnzahlAktiv = gesamt - tote - AnzahlGenesen,
         GenesenProz = round(AnzahlGenesen/gesamt*100),
         AktivProz = round(AnzahlAktiv/gesamt*100)) %>%
  select(ags_text, kreis, gesamt, stand, 
         pop, inzidenz, tote, neu7tage,
         inz7t,AnzahlGenesen,AnzahlAktiv,notizen,
         TotProz,GenesenProz,AktivProz)

# Tabelle wieder zurückschreiben         
sheets_write(cck_df,ss = id_cck, sheet = "daten")      

# - Basisdaten-Datenpunkt

msg("Basisdaten um Prozente Genesen/Aktiv von heute ergänzen")
genesen_gesamt <- sum(genesen_df$gsum)
faelle_gesamt <- sum(cck_df$gesamt)
tote_gesamt <- sum(cck_df$tote)
# Genesene (laut RKI) (Zeile 7)
# RKI-Daten zu Beginn in rki_df eingelesen - zeitaufwändig
# Absolute Zahl und Anteil an den Fällen

sheets_edit(id_basisdaten, as.data.frame(paste0(
  as.character(round(genesen_gesamt / faelle_gesamt * 100))," %")),
  range="livedaten!B7", col_names = FALSE, reformat=FALSE)

# Aktive Fälle (= Gesamt-Tote-Genesene), nur in Prozent (Zeile 8)


sheets_edit(id_basisdaten, as.data.frame(paste0(
  as.character(round((faelle_gesamt-genesen_gesamt-tote_gesamt) / faelle_gesamt * 100))," %")),
  range="livedaten!B8", col_names = FALSE, reformat=FALSE)


# - Aktive Fälle in fallzahl_df korrigieren
# fall4w_df zurücklesen, also die 4-Wochen-Statistik 

fall4w_df <- read_sheet(id_fallzahl,sheet="livedaten")

# Sicherheitsabfrage: letzte Zeile gestriges Datum? Wenn ja, dann los. 
if(fall4w_df$datum[28] == (heute-1)) {

  sheets_edit(id_fallzahl,as.data.frame(genesen_gesamt),sheet = "livedaten", 
              range = "F29", col_names = FALSE, reformat=FALSE)
  sheets_edit(id_fallzahl,as.data.frame(faelle_gesamt-genesen_gesamt-tote_gesamt),
              sheet = "livedaten", range = "G29", col_names = FALSE, reformat=FALSE)
  bar4w_df <- t(fall4w_df %>% select(datum,tote,gsum,aktiv))
  colnames(bar4w_df) <- bar4w_df[1,]
  bar4w_df <- cbind(tibble(t = c("Tote","Genesene","Aktiv")),as.data.frame(bar4w_df[-1,]))
  sheets_write(bar4w_df,ss = id_fallzahl, sheet = "livedaten-barchart" )
}



# ---- Datawrapper-Grafiken pingen ----
msg("Pinge Datawrapper-Grafiken...")

dw_publish_chart(chart_id = "XpbpH") # id_alter
dw_publish_chart(chart_id = "JQobx") # id_tote
dw_publish_chart(chart_id = "7HWCI") # Basisdaten 


msg("Erledigt.",as.character(now()))
msg("OK!")
