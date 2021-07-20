# ---- hystreet-passantenzahlen.R ----

# Ausgelagert: 1x wöchentlich die Hystreet-Zahlen aktualisieren
# Soll nur montags aufgerufen werden - dann ist die letzte Woche abrufbar.

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- "B18:C18"

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

library(hystReet) # Zugriff auf Hystreet-Passantendaten

msg("Hystreet-Daten lesen und auswerten...")

# Hystreet API Token lesen
tag0 <- ymd("2019-06-01")
if (server) {
  token <- read_lines("/home/jan_eggers_hr_de/key/.hystreettoken")
} else {
  token <- read_lines(".hystreettoken")
}
set_hystreet_token(token)

# Lies die verfügbaren Messstationen
stations <- get_hystreet_locations()
msg(nrow(stations)," Hystreet-Stationen abfragen...")

# Helper: Download data from Feb. 1st until given date for given station
get_station_data <- function(id, date){
  hd <- NULL
  while (is.null(hd)) {
    tryCatch( hd <- get_hystreet_station_data(
      hystreetId = id,
      query = list(from = "2020-02-01", to = ymd(date), resolution = "day")
    ))
    msg("Gelesen: ",hd$station," (ID: ",id,")")
    # Warte 30 Sekunden wegen möglicher API-Limits
    Sys.sleep(5)
  }
  
  ret <- hd$measurements  #data.frame
  ret <- ret %>%
    mutate(id = hd$id) %>%
    mutate(city = hd$city) %>%
    mutate(station = hd$name) %>%
    mutate(label = paste0(city, ", ", station)) %>%
    select(id, city, station, label, timestamp,
           weather_condition, temperature, min_temperature,
           pedestrians_count)
  
  return(ret)
}

# Collect data for every station available
# Filter out today. Why not collect only until yesterday?
# -> Data for last day in request seems weird
all_data <- lapply(stations$id, function(x){
  get_station_data(x, today())
}) %>% bind_rows %>%
  filter(timestamp != today())

# Pivot for datawrapper
data_by_city <- all_data %>%
  pivot_wider(id_cols = timestamp, names_from = label, values_from = c(pedestrians_count,weather_condition)) %>%
  # Woche unter Einbeziehung des Jahres
  # Rechnerische Woche, damit die Rechnung über den Jahrswechsel hinweg funktioniert
  mutate(week = 1+as.integer(ymd(timestamp)-ymd(tag0)) %/% 7)

# Summiere Städte auf, dann:
# Kalkuliere Wochenmittel und prozentuale Veränderung zu Referenzwoche

reference_week = 38 # KW38 war Mitte September

data_for_dw_weeks <- data_by_city %>%
  # Nur Wochentage!
  filter(wday(timestamp) %in% 2:7) %>% # Passanten montags bis samstags
  mutate(Frankfurt = `pedestrians_count_Frankfurt a.M., Goethestraße`+
           `pedestrians_count_Frankfurt a.M., Zeil (Mitte)` +
           `pedestrians_count_Frankfurt a.M., Große Bockenheimer Straße`,
         Frankfurt_Wetter = `weather_condition_Frankfurt a.M., Zeil (Mitte)`) %>%
  mutate(Limburg = `pedestrians_count_Limburg, Werner-Senger-Straße`,
         Limburg_Wetter = `weather_condition_Limburg, Werner-Senger-Straße`) %>%
  # Für wiesbaden nur den Zähler in Mitte; den in der Kirchgasse Nord
  # gab es erst ab Anfang April. Vermutung: Das schafft Probleme!
  mutate(Wiesbaden = `pedestrians_count_Wiesbaden, Kirchgasse (Mitte)`,
         Wiesbaden_Wetter = `weather_condition_Wiesbaden, Kirchgasse (Mitte)`) %>%
  mutate(Darmstadt = `pedestrians_count_Darmstadt, Schuchardstraße`+
           `pedestrians_count_Darmstadt, Ernst-Ludwig-Straße`,
         Darmstadt_Wetter = `weather_condition_Darmstadt, Schuchardstraße`) %>%
  mutate(`Gießen` = `pedestrians_count_Gießen, Seltersweg`,
         `Gießen_Wetter` = `weather_condition_Gießen, Seltersweg`) %>%
  # Erst mal ohne Wetter
  select(timestamp,week,
         Darmstadt, Darmstadt_Wetter,
         Frankfurt,Frankfurt_Wetter,
         `Gießen`, Gießen_Wetter,
         Limburg,Limburg_Wetter,
         Wiesbaden, Wiesbaden_Wetter) %>%
  group_by(week) %>%
  summarize(Darmstadt = sum(Darmstadt),
            Darmstadt_Wetter = first(Darmstadt_Wetter),
            Frankfurt = sum(Frankfurt),
            Frankfurt_Wetter = first(Darmstadt_Wetter),
            `Gießen` = sum(`Gießen`),
            `Gießen_Wetter` = first(`Gießen_Wetter`),
            Limburg = sum(Limburg),
            Limburg_Wetter = first(Limburg_Wetter),
            Wiesbaden = sum(Wiesbaden),
            Wiesbaden_Wetter = first(Wiesbaden_Wetter))

# Referenzwerte bestimmen
ref_week <- data_for_dw_weeks %>%
  select(Frankfurt, `Gießen`, Darmstadt, Limburg, Wiesbaden,week) %>%
  filter(week == reference_week)

data_for_dw_weeks <- data_for_dw_weeks %>%
  # laufende Woche ausfiltern
  filter((tag0+week*7) > ymd("2020-03-03")) %>%
  filter((tag0+week*7-2) < today()) %>%
  mutate(Frankfurt = round(Frankfurt/ref_week$Frankfurt*100,1)-100,
         Darmstadt = round(Darmstadt/ref_week$Darmstadt*100,1)-100,
         `Gießen` = round(`Gießen`/ref_week$`Gießen`*100,1)-100,
         Wiesbaden = round(Wiesbaden/ref_week$Wiesbaden*100,1)-100,
         Limburg = round(Limburg/ref_week$Limburg*100,1)-100) %>%
  mutate(Mittel = (Frankfurt+Darmstadt+`Gießen`+Wiesbaden+Limburg)/5) %>%
  mutate(wtext = as_date(tag0+week*7-2))


dw_data_to_chart(data_for_dw_weeks,"U89m9",parse_dates =FALSE)
dw_publish_chart(chart_id = "U89m9")

# Ergänzung Sonderauswertung
if (FALSE) {
  data_for_dw_weeks_2 <- data_for_dw_weeks %>%
    #nur 2021
    filter(week > 53) %>%
    # Samstag der Woche als Stichtag
    mutate(week = week-53) %>%
    mutate(wtext = as_date(ymd("2021-01-02")+week*7)) %>%
    select(wtext,Frankfurt,Wiesbaden)
  
  write.xlsx(data_for_dw_weeks_2,"auswertungen/passantendaten.xls")
  
  dw_data_to_chart(data_for_dw_weeks_2,"OpCnd",parse_dates =FALSE)
  dw_publish_chart(chart_id = "OpCnd")
  
  
}
