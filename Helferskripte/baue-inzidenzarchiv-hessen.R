##### baue-inzidenzarchiv-hessen.R #####
# Baut aus den archivierten Dateien einmalig eine Google-Tabelle mit
# den "Briefkasten"-Inzidenzen - also den am jeweiligen Tag gemeldeten
# 
# Berücksichtigt die breakpoints, an denen das RKI auf neue 
# Bevölkerungszahlen für die Inzidenzberechnung umgestellt hat
#
# Stand: 14.1.2022


rm(list=ls())
msgTarget <- NULL


if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# Library, die gezippte TSVs des NDR lesen kann
# Achtung: bringt offenbar das eine oder andere aus lubridate doppelt mit

library(data.table)

# Hessenweite Bevölkerung für Inzidenzrechnung bis 25.8.2021/ab 26.8.2021

# Breakpoints
# Nutzt die DESTATIS-Tabelle 12411-0015, die die Bevölkerungszahlen der Kreise
# nach Jahr enthält. (Spaltenname "Stichtag" enthält die Bev-Zahlen mit Stand
# des jeweiligen Jahres.)
# Umstellung 2020 zum 8.10. - Auskunft Fr. Glasmacher vom 14.1.22
# Umstellung 2021 zum 26.8. - Archiv JE

# df enthält jeweils den Breakpoint als Datum und den jeweiligen
# Spaltennamen der passenden Daten aus der Tabelle
breakpoints <- tibble(datum = c("2020-01-01",
                                "2020-10-08",
                                "2021-08-26"),
                      stichtag = c("31.12.2018",
                                   "31.12.2019",
                                   "31.12.2020"))


# Bevölkerungstabelle lesen

bev_df <- read_delim("index/12411-0015.csv",
                     delim = ";", 
                     escape_double = FALSE, 
                     locale = locale(date_names = "de", 
                                     decimal_mark = ",", 
                                     grouping_mark = ".", 
                                     encoding = "ISO-8859-1"), 
                     trim_ws = TRUE, 
                     skip = 5) %>% 
  rename(AGS = 1, Kreis = 2) %>% 
  # in Langformat umformen
  pivot_longer(cols = -c(AGS,Kreis),
               names_to = "Stichtag",
               values_to = "pop")



start_date <- as_date("2020-04-01") # Archivdaten seit April 2020
#path <- "D:/rki-archiv-lokal/" # für lokales Archiv
# path <- "./archiv/"
path <- ifelse(server,"./archiv/","~/rki-archiv-lokal/")
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


t <- today()
inz_wt_df <- NULL

  

  
for (d in (start_date):(t)) {

  # wenn existiert
  if(file.exists(paste0(path,"rki-",as_date(d),".csv"))) {
    # lies Datensatz vom Meldetag
    rki_he_df <- read_csv2(paste0(path,"rki-",as_date(d),".csv"),col_types = col_descr) 
    if(class(rki_he_df$IdLandkreis) != "character") {
      rki_he_df$IdLandkreis <- paste0("0",rki_he_df$IdLandkreis)
    }
  } else {
    # hole aus dem NDR-Archiv
    msg("Hole und archiviere den ",as_date(d))
    fallback_url <- paste0("https://storage.googleapis.com/public.ndrdata.de/",
                           "rki_covid_19_bulk/daily/covid_19_daily_", as_date(d),
                           ".tsv.gz")
    # Mit der Library data.table, die eine All-in-one-Lösung für .tsv.gz bietet
    rki_he_df <- fread(fallback_url)  %>% # Entpackt und liest das TSV des NDR-Archivs
      filter(Bundesland == "Hessen") %>% 
      group_by(Meldedatum)
    # wenn die AGS-Spalte eine Zahl ist, mit führender Null versehen
    if(class(rki_he_df$IdLandkreis) != "character") {
      rki_he_df$IdLandkreis <- paste0("0",rki_he_df$IdLandkreis)
    }
    # Wenn 'Datenstand' ein String ist, in ein Datum umwandeln. Sonst das Datum nutzen.
    if (class(rki_he_df$Datenstand) == "character") {
      rki_he_df$Datenstand <- parse_date(rki_he_df$Datenstand[1],format = "%d.%m.%y%H, %M:%S Uhr")
    }
    # im Archiv ablegen
    write_csv2(rki_he_df,paste0(path,"rki-",as_date(d),".csv"))
    
    
  }
  # Erstelle eine Kopie der Kreise-Index-Pop mit dem an diesem 
  # Tag gültigen Bevölkerungszahlen
  bev_Stichtag <- breakpoints %>% 
    # Nutze nur die Zeilen mit Datum ab heute
    filter(as_date(d) >= as_date(datum)) %>% 
    tail(1) %>% 
    pull(stichtag)
  kreise <- bev_df %>% 
    filter(bev_Stichtag == Stichtag) %>% 
    filter(str_detect(AGS,"^06")) %>% 
    mutate(pop = as.integer(pop))
  # Gesamtbevölkerung
  hessen <- sum(kreise$pop)
    
  
  # Berechne landesweite Inzidenz
  ref7tage_df <- rki_he_df %>%
    mutate(datum = as_date(Meldedatum)) %>%
    filter(datum > as_date(d-8) & datum < as_date(d)) %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall) %>%
    summarize(AnzahlFall = sum(AnzahlFall,na.rm=T)) %>%
  # Landesweite Inzidenz berechnen
    mutate(datum = as_date(d)) %>% 
    mutate(inz7t = AnzahlFall/hessen*100000) %>% 
    mutate(hessen)
  
  # Nach Kreisen
  ref7tage_kreise_df <- rki_he_df %>%
    mutate(datum = as_date(Meldedatum))  %>%
    filter(datum > as_date(d-8) & datum < as_date(d))  %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall)   %>%
    # Führende 0 ergänzen, falls fehlt (in ein paar alten Archivdateien)
    mutate(AGS = ifelse(str_detect(AGS,"^0"),AGS,paste0("0",AGS)))  %>% 
    group_by(AGS) %>% 
    summarize(neu7tage = sum(AnzahlFall,na.rm=T)) %>% 
    left_join(kreise,by="AGS") %>% 
    mutate(inz7t = neu7tage/pop*100000) %>% 
    select(AGS,inz7t) %>% 
    mutate(datum = as_date(d)) %>% 
    pivot_wider(names_from=AGS,values_from=inz7t)
  
  # Neue Zeile ins Dataframe
  if (is.null(inz_wt_df)) {
    inz_wt_df <- ref7tage_df  
    inz_kr_df <- ref7tage_kreise_df
  } else {
    inz_wt_df <- bind_rows(inz_wt_df,ref7tage_df)
    inz_kr_df <- bind_rows(inz_kr_df,ref7tage_kreise_df)
  }
  msg("Inzidenz ",as_date(d)," berechnet")
  
}
    
# Ausgabe in Archivdatei und Google Sheet
write.xlsx(inz_wt_df,"daten/ArchivHessenGemeldet.xlsx",overwrite = TRUE)
write.xlsx(inz_kr_df,"daten/ArchivKreisInzidenzGemeldet.xlsx",overwrite=T)

archiv_kreise_df <- read_sheet(aaa_id,sheet="ArchivInzidenzGemeldet")

# Vergleiche mit archivierten Inzidenzen
for (d in inz_kr_df$datum) {
  if (inz_kr_df %>% filter(datum==as_date(d)) !=
      archiv_kreise_df %>% filter(datum==as_date(d))) msg("Differenz am ",d)
}

# AGS durch Kreisnamen ersetzen
# Könnte man auch aus der Altersschichtungs-Datei nehmen, 
# aber die sind wieder nicht identisch. Und die Kreisnamen
# braucht das Inzidenzampel-Skript, sonst hagelt es NAs. 


k <- read.xlsx("index/kreise-index-pop.xlsx") %>%
  mutate(AGS = paste0("06",str_replace(AGS,"000","")))

kreise_namen <- c("datum",k$kreis)
colnames(inz_kr_df) <- kreise_namen

write_sheet(inz_kr_df,ss=aaa_id,sheet="ArchivInzidenzGemeldet")
#write_sheet(inz_wt_df,ss=aaa_id,sheet="ArchivHessenGemeldet")
