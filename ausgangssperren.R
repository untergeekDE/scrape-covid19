##################################### ausgangssperren.R #########################
# Schaut alle 5min auf die Tabelle der Ausgangssperren
# und aktualisiert gegebenenfalls. 
# 

# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

# Definiere Messaging, Server-Parameter, RKI-Lese-Funktion
# Im WD, bitte!

msgTarget <- "B16:C16"

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

sperren_id = "1zdR1rDOt0H6THUke_W6Hb-Lt3luzh0ZJtBPxvf3cZbI"

# Hole die Kopie der eingetragenen Daten, ohne den Zeitstempel zu verändern
sperren_df <- read_sheet(ss = sperren_id, sheet = "blindcopy") 

datum <- as_datetime(sperren_df$stand[1])

d <- as_datetime(read_sheet(sperren_id,sheet="Tabellenblatt2")$stand[1])

if (datum-d > dseconds(60)) { # Aktualisierung nötig?
  sperren_df <- sperren_df %>%
    mutate(Infos = ifelse(Infos==" ","",Infos)) %>%
    mutate(Infolink = str_replace(Infolink," ","")) %>% # Leerzeichen entfernen
    filter(Infos !="") %>%
    mutate(Link = paste0("<a href=\'",
                         ifelse(Infolink=="",Gesundheitsamt,Infolink),
                         "\' target=\'blank\'>Link</a>" )) %>%
    filter(Infos != "") %>%
    select(Kreis=kreis,Infos,Link)
  
  write_sheet(sperren_df %>% mutate(stand = datum), ss = sperren_id, sheet = "Tabellenblatt2")
  
  msg("Tabelle aktualisiert")
  # Daten in die Tabelle übertragen
  dw_data_to_chart(sperren_df,chart_id="Zsej5")
  dw_edit_chart(chart_id = "Zsej5",annotate = paste0("Stand: ",format.Date(datum, "%d.%m.%Y")))
  dw_publish_chart(chart_id= "Zsej5")
  msg("Datawrapper-Tabelle geschrieben")
} else {
  msg("OK")
}

