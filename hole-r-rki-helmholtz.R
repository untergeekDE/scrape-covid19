################ hole-r-rki-helmholtz.R
# 
# Einfaches Kopier-Skript. Schaut nach einem neuen CSV auf dem 
# SECIR-Repository der Helmholtz-System-Immunologoen und kopiert es in ein
# Blatt des fallzahl-id-Google-Sheets.  
#
# Kontakt bei Helmholtz: Saham Khailaie, khailaie.sahamoddin@gmail.com
# Infoseite SECIR: https://gitlab.com/simm/covid19/secir/-/tree/master
#
# CSVs werden am frühen Nachmittag aktualisiert; Skript startet 15 Uhr. 
# Wenn keine neuen Daten da, versuch es zwei Stunden lang, dann gib auf. 
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 20.7.2021

# Alles weg, was noch im Speicher rumliegt
rm(list=ls())


msgTarget <- "B10:C10"

# Library zum Finden des Arbeitsverzeichnisses
# Setzt das WD auf das Verzeichnis des gerade laufenden Skripts
pacman::p_load(this.path)
setwd(this.path::this.dir())
source("Helferskripte/server-msg-googlesheet-include.R")



# Tabelle: Sheet "rt-helmholtz" aus dem AAA-Dokument
# Vergleichsdaten vom Google Sheet: letztes gelesenes Datum
fallzahl_df <- read_sheet(aaa_id,sheet ="r_rki_helmholtz")
lastdate <- max(as.Date(fallzahl_df$datum))

msg("Helmholtz-R holen - zuletzt ",lastdate)



# ---- Lies Helmholtz-Daten Rt und schreibe in Hilfsdokument id_fallzahl ----
brics_url <- "https://gitlab.com/simm/covid19/secir/-/raw/master/img/dynamic/Rt_rawData/Hessen_Rt.csv?inline=false"
this_date <- lastdate
starttime <- now()

msg("Lies CSV vom SECIR-Gitlab")
while(lastdate == this_date) {
  brics_df <- read.csv(brics_url)
  # Manchmal enthält das Dokument Kontrollzeilen, die man daran erkennt, dass in Spalte date
  # kein gültiges Datum liegt. 
  brics_df <- brics_df %>% filter(str_detect(date,"\\d{4}-\\d\\d-\\d\\d"))
  this_date <- max(as.Date(brics_df$date))
  msg("CSV gelesen vom ",this_date," (gestern: ",lastdate,")")
  # Neues Datum?
  if (this_date == lastdate){
    # Falls Startzeit schon mehr als 2 Stunden zurück: 
    if (now() > starttime+7200)
      { 
        simpleWarning("Timeout")
        msg("Alte SECIR-Daten vom ",lastdate)
        lastdate <- this_date-1               # wir tun als ob
    }
    Sys.sleep(300)
  }
}

# Berechne gleitendes 7-Tage-Mittel
# für alle Spalten außer date
b_df <- brics_df %>%
  mutate(across(-date, ~ (.x +
             lag(.x) +
               lag(.x,n=2) +
               lag(.x,n=3) +
               lag(.x,n=4) +
               lag(.x,n=5) +
               lag(.x,n=6)) / 7))


rt_df <- brics_df %>% 
  # Gleitendes 7-Tage-Mittel für jede Spalte bilden. 
  mutate(across(-date, ~ (.x +
                            lag(.x) +
                            lag(.x,n=2) +
                            lag(.x,n=3) +
                            lag(.x,n=4) +
                            lag(.x,n=5) +
                            lag(.x,n=6)) / 7)) %>%
# Maximum, Minimum, Median in Spalten schreiben. Willenlos abgeschrieben. 
  rowwise() %>% 
  do(as.data.frame(.) %>% { 
    subs <- select(., -date)
    mutate(., Min = as.numeric(subs) %>% min,
           Max = as.numeric(subs) %>% max,
           Med = as.numeric(subs) %>% median()) 
  } ) %>%
  ungroup() %>%
  select(date,Min,Med,Max) %>%
  mutate(date = as.Date(date)) %>%
  filter(date > as.Date("2020-03-08"))

# sheet_write(rt_df,ss = aaa_id, sheet = "rt-helmholtz")

# ---- Jetzt noch die RKI-Tabelle für R dazuholen ----
# Seit Juli 2021 liegen die Daten nicht mehr als XLSX auf der 
# Webseite des RKI, sondern in einem Github-Repository. 

msg("Nach aktualisierten RKI-R-Daten suchen")

repo <- "robert-koch-institut/SARS-CoV-2-Nowcasting_und_-R-Schaetzung/"
path <- "Nowcast_R_aktuell.csv"



# Wann war der letzte Commit des Github-Files? Das als Prognosedatum prog_d.

github_api_url <- paste0("https://api.github.com/repos/",
                         repo,
                         "commits?path=",path,
                         "&page=1&per_page=1")
github_data <- read_json(github_api_url, simplifyVector = TRUE)
r_d <- as_date(github_data$commit$committer$date)
msg("R-Schätzung des RKI vom ",r_d)

path <- paste0("https://github.com/",
               repo,
               "raw/main/",
               path)


# Daten lesen und schauen, ob es schon eine Archivdatei dazu gibt - 
# wenn ja, sind wir durch, 
# wenn nein, alles aktualisieren. 

#rki_r_df <- read.xlsx("daten/Nowcasting_Zahlen.xlsx", sheet=2,detectDates = TRUE)
rki_r_df <- read_csv(path)

# Der 7-Tage-R-Wert ist in Spalten 11-13: 
# 11: Punktschätzewr
# 12: Untergrenze
# 13: Obergrenze

rki_r_df <- rki_r_df %>%
  select(datum = Datum, 
         neue_punkt_og = PS_COVID_Faelle, 
         neue_lo_og = UG_PI_COVID_Faelle, 
         neue_hi_og = OG_PI_COVID_Faelle, 
         neue_punkt = PS_COVID_Faelle_ma4, 
         neue_lo = UG_PI_COVID_Faelle_ma4,
         neue_hi = OG_PI_COVID_Faelle_ma4, 
#     in KW 28 hat das RKI den 4-Tage-R-Wert ausgemustert
#         r4t_punkt = PS_4_Tage_R_Wert, 
#         r4t_lo = UG_PI_4_Tage_R_Wert, 
#         r4t_hi = OG_PI_4_Tage_R_Wert,
         r7t_punkt = PS_7_Tage_R_Wert,
         r7t_lo = UG_PI_7_Tage_R_Wert,
         r7t_hi = OG_PI_7_Tage_R_Wert) %>%
  # alle numerisch
  mutate(datum = as_date(datum))

# RKI-Nowcast-Sheet auf Sheet pushen
# msg("Schreibe Kopie der RKI-Daten")
# sheet_write(rki_r_df,ss = aaa_id, sheet = "rt-rki")
# Archivkopie nicht nötig; hebt das RKI selbst in seinem Repository auf. 

r_df <- rki_r_df %>%
  select(datum, r7t_lo, r7t_hi) %>%
  full_join(rt_df, by = c("datum" = "date")) %>%
  select(datum,r_rki_lo = r7t_lo, r_rki_hi = r7t_hi,
         r_helmholtz_min = Min, 
         r_helmholtz_med = Med,
         r_helmholtz_max = Max)

# Auf letzte 14 Tage beschränken
r_df <- r_df[nrow(r_df)-(27:0),]


msg("Schreibe Arbeitskopie r_rki_helmholtz")
sheet_write(r_df, ss = aaa_id, sheet = "r_rki_helmholtz")

# ---- Pinge Datawrapper-Grafik R-Werte, wenn neue Zahlen ----
msg("Pinge Datawrapper-Grafik")
dw_publish_chart(chart_id = "82BUn")

if (this_date > lastdate) {
  msg("OK!")
} else {
  msg("OK (nur Ping)")
}
