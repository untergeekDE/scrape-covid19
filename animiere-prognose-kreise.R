################ animiere-prognose-kreise.R
# 
# Greift die tagesaktuellen Corona-Zahlen-/Falldatenbank des RKI ab
# und generiert daraus die 7-Tage-Inzidenzen nach (hessischem) Kreis und Tag. 
# Das wird dann um die Simulator-Prognose für die nächsten vier Wochen ergänzt - 
# und diese Datei ausgegeben. 
#
# Im nächsten Schritt lässt sich das Skript über die Datawrapper-API
# Grafiken generieren - ein Frame pro Tag. Die Parameter dafür - X und Y, 
# Start- und Enddatum - holt sich das Tool aus Defaults (später: Kommandozeile)
#
# Letzter Schritt: Frames mit gganimate() zu GIFs zusammenzubinden. 
# Dann über teamr eine Teams-Karte im Kanal corona erzeugen.
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 29.6.2021
#

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----

# Alles weg, was noch im Speicher rumliegt
# rm(list=ls())
# msgTarget <- NULL

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}


library(DatawRappr)
library(magick)

# Default-Argumente
dwchart_x <- 640
dwchart_y <- 640
heute <- today()
start_date <- heute-28
end_date <- heute+14

# Was das Skript braucht: 
# - einen API-Key für Datawrapper, über datawrapper_auth() hinterlegt
# - eine Choropleth-Karte für Hessen, die die Daten anzeigt
#   und zum Export genutzt wird: Daten in der Spalte inz7t
#   (Demo-Daten zum Einrichten unter "daten/animations-demo-daten.xlsx"
#   im Github)
# - die ID dieser Choropleth-Karte

png_id = "1haMS"  #Datawrapper-Grafik-ID



# ---- Start, Prognose lesen, Hessen-Fälle filtern ----
kreise <- read.xlsx("index/kreise-namen-index.xlsx") %>% 
  # Statistische Namen enthielten bis 
  mutate(StatName = str_replace_all(StatName,"_"," "))

repo <- "onwhenrdy/cosimhessen/"
path <- "Hessen_LKs.csv"

# Wann war der letzte Commit des Github-Files? Das als Prognosedatum prog_d.

github_api_url <- paste0("https://api.github.com/repos/",
                         repo,
                         "commits?path=",path,
                         "&page=1&per_page=1")
github_data <- read_json(github_api_url, simplifyVector = TRUE)
prog_d <- as_date(github_data$commit$committer$date)
msg("Aktuelle Prognose vom ",prog_d)

path <- paste0("https://github.com/",
               repo,
               "raw/main/",
               path)

# Daten lesen und schauen, ob es schon eine Archivdatei dazu gibt - 
  msg("Trendberechnung anpassen")

  sim_lk_df <- read_csv(path) %>%
    mutate(vom = prog_d) %>%
    mutate(var = as.factor(var)) %>%
    filter(state != "Hessen")
    
  
  # Prognose-Tabelle alle Kreise mit rt, Kreisnamen und allen prognostizierten
  # Inzidenzen ab morgen
  inz_lk_df <- sim_lk_df %>%
    # Das R in der Prognose bleibt im prognostizierten Zeitraum konstant
    filter(date > heute & date <= end_date) %>%
    # Kreisbezeichner dazu
    full_join(kreise,by=c("state" = "StatName")) %>%
    # Uns interessieren die Inzidenzen
    filter(var == "INCIDENCE") %>%
    select(Datum = date, kreis, AGS, inz7t = meansim) %>%
    pivot_wider(id_cols = c(AGS,kreis), names_from=Datum, values_from=inz7t)
  
inz_lk_ist_df <- read_sheet(aaa_id, sheet="ArchivKreisInzidenz") %>%
  filter(Datum>=start_date) %>%
  pivot_longer(cols=-Datum,names_to="kreis",values_to="inz7t") %>%
  pivot_wider(id_cols=kreis,names_from=Datum,values_from=inz7t)
  # 
# ---- Prognose-Daten ergänzen ---- 


all_sim_df <- inz_lk_df %>%
  left_join(inz_lk_ist_df, by="kreis")

# Daten mit diesem Rt ausgeben
#write_csv(all_sim_df,paste0("daten/inz7t-prognose-",prog_d,".csv"))
#write.xlsx(all_sim_df,paste0("daten/inz7t-prognose-",prog_d,".xlsx"))

p_str <- paste0("Trendberechnung vom ",format.Date(prog_d,"%d.%m.%Y"))

# ---- Datawrapper-Grafiken ausgeben ----
# Wir startenmit einem leeren png-Verzeichnis.
# Unterverzeichnis anlegen, falls nötig
if (!file.exists("png")){
  dir.create(file.path(getwd(), "png"))
} else{
  # alle alten PNG-Dateien etc. löschen
  unlink("./png/karte-*")
}

# Fortschritts-String vorbereiten

intro_str <- "<br><br>7-Tage-Inzidenz (Neufälle in einer Woche umgerechnet auf 100.000 Einwohner<br>"
notizen_str <- paste0("<strong>Wie entwickeln sich die Inzidenzen in d den ",
                      "nächsten 14 Tagen, wenn es mit dem Trend der letzten Woche ",
                      "weitergeht?</strong><br>",
            "Szenario vom ",format.Date(prog_d,"%d.%m.%Y"),
            " des CoViD-Simulators der Universität des Saarlandes")

# Schleife für alle Tage
for (d in start_date:end_date) {
 
# Dataframe mit den Kreisdaten generieren und auf Grafik pushen
tag_df <- all_sim_df %>%
      select(1,2,inz7t = as.character(as_date(d)))

tag <- as_date(d)
# Raus mit den Daten!
dw_data_to_chart(tag_df,png_id,parse_dates =TRUE)


titel_str <- "Animation: Corona-Neufälle in Hessens Kreisen"
f_str <- "[<b style='color:#5a5e5c; font-size:xx-small;'>"
for (i in start_date:end_date) {
    if(i <= d) {
      f_str <- paste0(f_str,"&#x2589;")
    }
  else {
    f_str <- paste0(f_str,"&#x2504;")
    }
  if (as_date(i) == heute) { 
    f_str <- paste0(f_str,"</b><b style='color:#908d85; font-size:xx-small;'>")
    }
  }
f_str <- paste0(f_str,"</b>]")
                    


if (d > heute) {
  dw_edit_chart(chart_id = png_id, title = titel_str,
                intro = "Inzidenzwerte laut RKI-Daten")
  
  
}

dw_edit_chart(chart_id = png_id, title = titel_str, intro = 
                paste0("<h1><b style='color:#000000; font-size:xx-large;'>",day(tag),".",month(tag),".",
                       # in der Zukunft: "TREND" 
                       ifelse(d > heute," (Trend)",""),
                       "</b></h1><br>",
              f_str,intro_str), 
              annotate = notizen_str)
png <- dw_export_chart(png_id,type = "png",unit="px",mode="rgb", scale = 1, 
                       width = dwchart_x, height = dwchart_y, plain = FALSE)
image_write(png,paste0("png/karte-",as.character(as_date(d)),".png"))
if (d == start_date) {
  gif <- png
} else {
  gif <- c(gif,png)
}
# Kurze Verzögerung (10 Frames), wenn aktuelles Datum erreicht
if (as_date(d) == heute || d==end_date) {
  if (as_date(d) == heute) {
    dw_edit_chart(chart_id = png_id, title = paste0(titel_str," (heute)"),
                intro = paste0("<h1><b style='color:#000000; font-size:xx-large;'>",day(tag),".",month(tag),".</b></h1><br>",
                         f_str,intro_str), annotate = notizen_str) }
  png <- dw_export_chart(png_id,type = "png",unit="px",mode="rgb", 
                         width = dwchart_x, height = dwchart_y, plain = FALSE)
  gif <- c(gif, png, png, png, png, png, 
           png, png, png, png, png)
}

}

# Animation generieren; zwischen zwei Frames jeweils ein Frame neu generieren

final_gif <- gif %>% 
  image_morph(frames = 1) %>% 
  image_animate(fps = 5,loop = 1, optimize=TRUE)

# GIF auf die Platte speichern 
# Kann schon mal 15min dauern
gif_fn <- paste0("png/kreise-prognose-",prog_d,".gif")
image_write(final_gif,path = gif_fn, format = "gif")

if (server) {
  # GIF aufs Google-Bucket schieben
  msg("Lokale Daten ins Google-Bucket schieben...")
  system(paste0('gsutil -h ',
                '"Cache-Control:no-cache, max_age=0" ',
                'cp ./',gif_fn,
                ' gs://d.data.gcp.cloud.hr.de/kreise-prognose.gif'))
}

# ---- Animation und Übersicht in Teams-Channel pushen ----
library(teamr)

# Wieviele Hessen?
bev_df= read_sheet(aaa_id,sheet="AltersgruppenPop")
hessen=sum(bev_df %>% select(pop))

# 7-Tage-Mittel und Fallprognose aus dem Google Doc lesen, Inzidenz berechnen
inz_he_df <- read_sheet(ss=aaa_id,sheet="NeuPrognose") %>%
  mutate(i7t = ifelse(is.na(neu7tagemittel),mean,neu7tagemittel)/hessen*7*100000) %>%
  filter(!is.na(i7t)) %>%
  mutate(datum = as_date(datum)) %>%
  select(datum,i7t)

# Webhook aus dem Environment lesen, Karte generieren
cc <- connector_card$new(hookurl = Sys.getenv("WEBHOOK_CORONA"))
cc$text(paste0("Neuer COVID-Simulator-Trend, Stand: ",prog_d))

sec <- card_section$new()
#sec$title("anagrom ataf")
sec$text("Trendberechnung des COVID-Simulators der Universität des Saarlandes")
sec$add_fact("Vor 1 Woche:",format(inz_he_df$i7t[inz_he_df$datum == today()-7],
                                   big.mark=".",
                                             decimal.mark = ","))
sec$add_fact("7-Tage-Inzidenz heute:",
             paste0("<strong>",
                    format(inz_he_df$i7t[inz_he_df$datum == today()],
                           big.mark=".",
                                  decimal.mark = ","),
                    "</strong>" ))
sec$add_fact("In 1 Woche:",format(inz_he_df$i7t[inz_he_df$datum == today()+7],
                                  big.mark=".",
                               decimal.mark = ","))
i7t_14 <- inz_he_df$i7t[inz_he_df$datum == today()+14]
sec$add_fact("In 2 Wochen:",ifelse(is.null(i7t_14),"NA",
                                   format(i7t_14,big.mark=".",decimal.mark=",")))

# Wenn du auf dem Server bist: 
# Importiere eine PNG-Version des Impffortschritts, 
# schiebe sie auf den Google-Bucket, und 
# übergib die URL an die Karte. 


if (server) {
  # Google-Bucket linken
  sec$add_fact("Animation: ",
               paste0("[Letzte 4 Wochen, Trend für nächste 2]",
               "(https://d.data.gcp.cloud.hr.de/kreise-prognose.gif)"))
  sec$add_image(sec_image="https://d.data.gcp.cloud.hr.de/kreise-prognose.gif", sec_title="Letzte 4 Wochen und Trend")
} else {
  sec$add_image(sec_image=gif_fn, sec_title="Letzte 4 Wochen und Trend")
}


# Karte vorbereiten und abschicken. 
cc$add_section(new_section = sec)
cc$send()

msg("OK!")