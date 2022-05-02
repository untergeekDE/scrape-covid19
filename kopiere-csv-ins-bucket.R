############################### kopiere-csv-ins-bucket.R ###########################
# Falls das Skript auf dem Laptop lief: kopierte Daten ins Bucket und ins Archiv schieben
# 7.11.2020 je

# Init-Werte fürs Logging, das Arbeitsverzeichnis und die Keyfile-Auswahl
server <- FALSE

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")


if (server) {
  # Google-Bucket befüllen
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/KreisdatenAktuell.csv gs://d.data.gcp.cloud.hr.de/scrape-hsm.csv')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/KreisdatenAktuell.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/Basisdaten.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/rki-alter.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/rki-tote.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/hessen_rki_df.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/ArchivKreisFallzahl.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/ArchivKreisGenesen.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/ArchivKreisTote.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp daten/ArchivKreisInzidenz.csv gs://d.data.gcp.cloud.hr.de/')
# DIVI-Daten  
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_he.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_beds.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_he_beds.csv gs://d.data.gcp.cloud.hr.de/')
  system('gsutil -h "Cache-Control:no-cache, max_age=0" cp ./divi_he_kreise.csv gs://d.data.gcp.cloud.hr.de/')
  
}

