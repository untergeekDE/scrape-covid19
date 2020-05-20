# DIVI Scraper Skript
# Zieht die Daten von der DIVI-Seite - bis die ein CSV liefern
# Da die täglich um 9 Uhr aktualisiert werden, sollte das Skript um 7:10 UTC laufen. 
# 
# 23.3. Till Hafermann, hr-Datenteam
# zuletzt bearbeitet: 26.4.je

#------------------------------------------#
#       Load required packages             #
#------------------------------------------#

require(tidyverse)
require(lubridate)
require(jsonlite)
require(googlesheets4)
require(tesseract)
require(magick)

# ---- Logging und Update der Semaphore-Seite ----
id_msg <- "1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk"
logfile <- ""

msg <- function(x,...) {
  print(paste0(x,...))
  # Zeitstempel in B8, Statuszeile in C8
  sheets_edit(id_msg,as.data.frame(now(tzone = "CEST")),sheet="Tabellenblatt1",
              range="B8",col_names = FALSE,reformat=FALSE)
  sheets_edit(id_msg,as.data.frame(paste0(x,...)),sheet="Tabellenblatt1",
              range="C8",col_names = FALSE,reformat=FALSE)
  if (logfile != "") {
    cat(x,...,file = logfile, append = TRUE)
  }
}

#------------------------------------------#
#       Connect to Google Sheet            #
#------------------------------------------#

sheets_deauth() # Authentifizierung löschen
sheets_auth(email="googlesheets4@scrapers-272317.iam.gserviceaccount.com", 
           path = "/home/jan_eggers_hr_de/key/scrapers-272317-4a60db8e6863.json")
          # path = "C:/Users/Jan/Documents/PythonScripts/creds/scrapers-272317-4a60db8e6863.json")


#sheets_auth(email="divi-scraper@scrapers-till.iam.gserviceaccount.com",path="/home/till_hafermann_hr_de/rscripts/divi/scrapers-till-a6a3537e31d2.json")
# sheets_auth(email="divi-scraper@scrapers-till.iam.gserviceaccount.com",path="scrapers-till-a6a3537e31d2.json")

sheet_id <- "15hhqkeyKkwEXsd7qlt90QOfsxlQFUdoJbpT18EMlHo8"

#format timestamp
ts <- stamp("2020-01-31 15:55")

msg("Starte DIVI-Scraper... \n")

#------------------------------------------#
#         Get data from API json           #
#------------------------------------------#

msg("Lese intensivregister.de via JSON\n")
d_json <- read_json("https://www.intensivregister.de/api/public/intensivregister?page=0", simplifyVector = T)

d_tbl <- tibble(
    id = d_json[["data"]]$id,
    ik_nummer = d_json[["data"]][["krankenhausStandort"]]$ikNummer,
    name = d_json[["data"]][["krankenhausStandort"]]$bezeichnung,
    adress = str_c(d_json[["data"]][["krankenhausStandort"]]$strasse, " ", 
                   d_json[["data"]][["krankenhausStandort"]]$hausnummer, "; ", 
                   d_json[["data"]][["krankenhausStandort"]]$plz, " ",
                   d_json[["data"]][["krankenhausStandort"]]$ort),
    city = d_json[["data"]][["krankenhausStandort"]]$ort,
    state = d_json[["data"]][["krankenhausStandort"]]$bundesland,
    lat = d_json[["data"]][["krankenhausStandort"]][["position"]]$latitude,
    long = d_json[["data"]][["krankenhausStandort"]][["position"]]$longitude,
    timestamp = d_json[["data"]]$meldezeitpunkt,
    icu_low = d_json[["data"]][["bettenStatus"]]$statusLowCare,
    icu_high = d_json[["data"]][["bettenStatus"]]$statusHighCare,
    ecmo = d_json[["data"]][["bettenStatus"]]$statusECMO,
    scraped = ts(now(tzone = "CET"))
)

# Sicherheitsabfrage
if (ncol(d_tbl) != 13) simpleError("Formatänderung!")

#------------------------------------------#
#         Get data from SVG via OCR        #
#------------------------------------------#

msg("Versuche SVG via OCR zu decodieren\n")
img_path <- "https://diviexchange.z6.web.core.windows.net/laendertabelle1.svg"
# turn svg into image object via magick
img <- image_read_svg(img_path, width = 2000)

scrape_img <- function(crop_str){
  msg("Versuche SVG via OCR zu decodieren\n")
  
    # crop to one linerow of data 
    cropped = image_crop(img, crop_str)
    
    # extract data with tesseract, clean and split into list 
    raw_data = tesseract::ocr(cropped)
    raw_data = str_replace(raw_data, " %", "")
    split_data = str_split(str_trim(raw_data), " ")[[1]]
    clean_data = str_replace_all(split_data, c("\\." = "", "," = "."))
    

    # sort data into one line tibble
    ret_tbl = tibble(
        cases = as.integer(clean_data[[1]]),
        resp = as.integer(clean_data[[2]]),
        resp_rel = as.double(clean_data[[3]]),
        beds_occ = as.integer(clean_data[[4]]),
        beds_free = as.integer(clean_data[[5]]),
        beds_total = as.integer(clean_data[[6]])
    )
    # Sicherheitsprüfung
    if(ret_tbl$cases * ret_tbl$beds_total == 0) {
      msg(paste0("OCR: ",ret_tbl))
      simpleError("OCR versagt!")
    }
    return(ret_tbl)
    
}

# create list of cropping coords for each row
crop_list = seq(88,740,39) %>%
    str_c("1750x39+400+", .)

# apply scraping function to each row, bind together
scraped_svg <- lapply(crop_list, scrape_img) %>% bind_rows

#------------------------------------------#
#              clean data                  #
#------------------------------------------#

d_tbl <- d_tbl %>%
    mutate(state = str_to_title(state),
           timestamp = str_replace_all(timestamp, c("T"=" ", ":\\d\\dZ" = "")),
           icu_low = str_replace_all(icu_low, c("NICHT_VERFUEGBAR"="ausgelastet", "VERFUEGBAR"="verfügbar", "BEGRENZT"="begrenzt")),
           icu_high = str_replace_all(icu_high, c("NICHT_VERFUEGBAR"="ausgelastet", "VERFUEGBAR"="verfügbar", "BEGRENZT"="begrenzt")),
           ecmo = str_replace_all(ecmo, c("NICHT_VERFUEGBAR"="ausgelastet", "VERFUEGBAR"="verfügbar", "BEGRENZT"="begrenzt"))
    ) %>%
    mutate(icu_low = ifelse(is.na(icu_low), "k. A.", icu_low),
           icu_high = ifelse(is.na(icu_high), "k. A.", icu_high),
           ecmo = ifelse(is.na(ecmo), "k. A.", ecmo),
    )


d_beds <- bind_cols(scraped_svg, tibble(state = c("Deutschland", "Baden-Württemberg", "Bayern", "Berlin", "Brandenburg",
                                                       "Bremen", "Hamburg", "Hessen", "Mecklenburg-Vorpommern", "Niedersachsen",
                                                       "Nordrhein-Westfalen", "Rheinland-Pfalz", "Saarland", "Sachsen",
                                                       "Sachsen-Anhalt", "Schleswig-Holstein", "Thüringen"))) %>%
    select(7, 1:6) %>%
    mutate(scraped = ts(now()))


h_tbl <- d_tbl %>%
    filter(state == "Hessen")

h_beds <- d_beds %>%
    filter(state == "Hessen")



# different format for table-display in datawrapper
dw_timestamp = str_c("zuletzt abgerufen: ", day(now()), ". ", month(now(), label = T), " ", year(now()),
                     ", ", hour(now()), ":", str_pad(minute(now()), width = 2, pad = "0"), " Uhr")

# format for dw table (https://datawrapper.dwcdn.net/7VKZD/3/)
h_beds_dw <- tibble(text = c("Covid-19-Fälle in Behandlung", "davon beatmet", "betreibbare Intensivbetten", "davon belegte", "Anteil belegter Betten", dw_timestamp),
                  zahlen = c(h_beds$cases, h_beds$resp, h_beds$beds_total, h_beds$beds_occ, round(h_beds$beds_occ/h_beds$beds_total * 100, 1), NA))

# format for pie or stacked bar chart (https://datawrapper.dwcdn.net/qHKhR/4/, https://datawrapper.dwcdn.net/cwnbs/1/)
h_beds_dw2 <- tibble(Betten = c("frei", "belegt"),
                     Deutschland = c(filter(d_beds, state == "Deutschland")$beds_free, filter(d_beds, state == "Deutschland")$beds_occ),
                     Hessen = c(filter(d_beds, state == "Hessen")$beds_free, filter(d_beds, state == "Hessen")$beds_occ))

# format for pie or stacked bar chart (https://datawrapper.dwcdn.net/qHKhR/4/, https://datawrapper.dwcdn.net/cwnbs/1/)
h_beds_dw2 <- tibble(Betten = c("frei", "belegt"),
                     Deutschland = c(filter(d_beds, state == "Deutschland")$beds_free, filter(d_beds, state == "Deutschland")$beds_occ),
                     Hessen = c(filter(d_beds, state == "Hessen")$beds_free, filter(d_beds, state == "Hessen")$beds_occ))

d_beds_dw <- d_beds %>%
    select(Land = state, Betten = beds_total, `davon belegt` = beds_occ) %>%
    mutate(`in Prozent` = round(`davon belegt` / Betten * 100, 1))

h_beds_dw3 <- d_beds %>%
    filter(state == "Hessen" | state == "Deutschland") %>%
    select(` ` = state, Betten = beds_total, `davon belegt` = beds_occ) %>%
    mutate(`in Prozent` = round(`davon belegt` / Betten * 100, 1)) %>%
    arrange(desc(` `))

#------------------------------------------#
#               save data                  #
#------------------------------------------#

msg("Daten lokal sichern...")
# save locally to csv
write.csv(d_tbl, format(Sys.time(), "/home/till_hafermann_hr_de/rscripts/divi/divi_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(d_beds, format(Sys.time(), "/home/till_hafermann_hr_de/rscripts/divi/divi_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_tbl, format(Sys.time(), "/home/till_hafermann_hr_de/rscripts/divi/divi_he_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_beds, format(Sys.time(), "/home/till_hafermann_hr_de/rscripts/divi/divi_he_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)

write.csv(d_tbl, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(d_beds, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_tbl, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_he_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)
write.csv(h_beds, format(Sys.time(), "/home/jan_eggers_hr_de/rscripts/archiv/divi_he_beds_%Y%m%d_%H%M.csv"), fileEncoding = "UTF-8", row.names = F)

msg("Daten im Google-Sheet sichern...")
write_sheet(d_tbl, ss = sheet_id, sheet = "current")
write_sheet(d_beds, ss = sheet_id, sheet = "beds_current")
write_sheet(h_tbl, ss = sheet_id, sheet = "current_hessen")
write_sheet(h_beds_dw, ss = sheet_id, sheet = "beds_dw_hessen")
write_sheet(h_beds_dw2, ss = sheet_id, sheet = "beds_dw_hessen2")
write_sheet(h_beds_dw3, ss = sheet_id, sheet = "beds_dw_hessen3")
write_sheet(d_beds_dw, ss = sheet_id, sheet = "beds_dw_alle")
sheets_append(d_tbl, ss=sheet_id, sheet = "archive")
sheets_append(d_beds, ss=sheet_id, sheet = "beds_archive")
sheets_append(h_tbl, ss=sheet_id, sheet = "archive_hessen")
sheets_append(h_beds, ss=sheet_id, sheet = "beds_hessen_archive")

# ---- Alles OK, melde dich ab ----
msg("OK!")
