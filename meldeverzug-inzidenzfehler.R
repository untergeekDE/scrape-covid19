################ meldeverzug-inzidenzfehler.R
# #
# Sehr roh, sehr beta - 
# Wochenweise Auswertung der im Archiv gespeicherten Daten auf den Meldeverzug - 
# - wie stark war der durchschnittliche Meldeverzug (also die Abweichung vom Datum des
#   Tagesreports) in der jeweiligen Woche?
# - Wie stark haben die Verzögerungen die Fallzahlen (und damit die Inzidenz) beeinflusst?
#   Vergleich der in der jeweiligen Woche gemeldeten Fälle mit der "richtigen" Berechnung 
#   aus der Woche danach, die auch die inzwischen nachgemeldeten Fälle erhält
#
#
# jan.eggers@hr.de hr-Datenteam 
#
# Stand: 2.12.2020
#
# ---- Bibliotheken, Einrichtung der Message-Funktion; Server- vs. Lokal-Variante ----
# Alles weg, was noch im Speicher rumliegt
rm(list=ls())

msgTarget <- "B14:C14"

if (file.exists("./server-msg-googlesheet-include.R")) {
  source("./server-msg-googlesheet-include.R")
} else {
  source("/home/jan_eggers_hr_de/rscripts/server-msg-googlesheet-include.R")
}

# Google Sheets
aaa_id = "17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ"
risklayer_id = "1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s"

# Geht öfter mal schief - Seite ist notorisch überlastet

# Im Moment kein Vergleich mit den Risklayer-Daten
# risklayer_df <- tibble()
# while (nrow(risklayer_df) < 10) {
#   tryCatch(risklayer_df <- read_sheet(risklayer_id, sheet = "Haupt"))
# }

# Kreisnamen

kreise <- read.xlsx("index/kreise-namen-index.xlsx") %>%
  select(AGS, kreis) # nur AGS und Kreisnamen
  

# Abfragedaten ab start_date im definierten Pfad

start_date <- as_date("2020-09-01")
path <- "./archiv/"

# ---- Erste Runde: Meldeverzug in eine Tabelle schreiben. -----

verzuege_df <- NULL

for (d in start_date:today()) { # Irritierenderweise ist d eine Integer-Zahl
  # Deshalb die Woche erst mal so berechnen
  # wk <- isoweek(as_date(d))
  file_name <- paste0(path,"rki-",as_date(d),".csv")
  if(file.exists(file_name)) {
    tagesmeldung_df <- read_csv2(file_name) %>%
    # Fälle, die neu zum Vortag waren
      filter(NeuerFall == 1) %>%
      mutate(datum = ymd(Datenstand)) %>%
      mutate(delta = as_date(datum) - as_date(Meldedatum)) %>%
      select(datum, delta, n = AnzahlFall, AGS = IdLandkreis)
    # Die Fallmeldungen auspacken: 
    # Funktion rep wiederholt die jeweilige df-Zeile n mal
    tmn_df <- as.data.frame(lapply(tagesmeldung_df,rep,tagesmeldung_df$n)) %>%
      select(delta, AGS, datum) %>%
      mutate(wk = isoweek(as_date(d)))
    
      
    if (is.null(verzuege_df)) {
      verzuege_df <- tmn_df
    } else {
      verzuege_df <- rbind(verzuege_df, tmn_df)
    }
  }
  msg("Meldeverzüge ",as_date(d))
}

v_ags_df <- verzuege_df %>%
  right_join(kreise, by = "AGS") %>%
  select(-AGS, -datum) %>%
  group_by(kreis, wk) %>%
  summarize(m_delay = mean(delta)) %>%
  pivot_wider(names_from = kreis, values_from = m_delay) %>%
  ungroup() 
  
sheet_write(v_ags_df,ss = aaa_id, sheet = "Meldeverzug Woche Kreis") 
# write.xlsx(v_ags_df,"mean_meldeverzug.xlsx")
msg("Durchschnittlicher Meldeverzug pro Kreis berechnet und geschrieben")

# ---- Anteil verspätete Meldungen ----
# Auswertung aktuelle Woche: Wie hoch ist der Anteil der Fälle in den letzten 7 Tagen mit mehr als 3 Tagen Meldeverzug?

delay_ags_today_df <- verzuege_df %>%
  filter(datum == today()-1) %>%
  right_join(kreise, by = "AGS") %>% # Kreisnamen reinholen
  select(-AGS) %>%
  group_by(kreis) %>%
  count(delta > 3) %>%
  pivot_wider(names_from = 'delta > 3',values_from = n) %>%
  select(1,pünktlich = 2,verspätet = 3) %>%
  mutate(verspätet = ifelse(is.na(verspätet),0,verspätet),
         pünktlich = ifelse(is.na(pünktlich),0,pünktlich)) %>%
  mutate(quote_heute = ifelse(verspätet+pünktlich > 0, verspätet/ (pünktlich + verspätet) * 100,0)) 

q_heute_hessen = sum(delay_ags_today_df$verspätet)/
  (sum(delay_ags_today_df$pünktlich) + sum(delay_ags_today_df$verspätet) * 100)
delay_ags_today_df <- delay_ags_today_df %>% select(kreis, quote_heute)

delay_ags_df <- verzuege_df %>%
  filter(datum > today()-8) %>%
  right_join(kreise, by = "AGS") %>% # Kreisnamen reinholen
  select(-AGS) %>%
  group_by(kreis) %>%
  count(delta > 3) %>%
  pivot_wider(names_from = 'delta > 3',values_from = n) %>%
  select(1,pünktlich = 2,verspätet = 3) %>%
  mutate(verspätet = ifelse(is.na(verspätet),0,verspätet)) %>%
  mutate(quote = verspätet/ (pünktlich + verspätet) * 100)



# Summe dran
delay_ags_df <- rbind(delay_ags_df,
                      tibble(kreis = "HESSEN",
                             pünktlich = sum(delay_ags_df$pünktlich),
                             verspätet = sum(delay_ags_df$verspätet),
                            quote = sum(delay_ags_df$verspätet)/sum(c(delay_ags_df$verspätet,delay_ags_df$pünktlich))*100)) %>%
                  full_join(delay_ags_today_df, by = "kreis")
  

sheet_write(delay_ags_df,ss = aaa_id, sheet = "Datenqualität")
msg("Meldeverzug hessenweit: ",round(delay_ags_df$quote[delay_ags_df$kreis=="HESSEN"],2),"% mehr als 3 Tage verspätet")
# Die drei großen Sünder: Wiesbaden, Bergstraße, LDK

wi_df <- verzuege_df %>%
  filter(AGS == "06414") %>%
  filter(wk > 46) %>% # letzte beide Wochen
  # Histogramm: 
  count(delta)
    
write.xlsx(wi_df,"wi-histogramm.xlsx")

bergstr_df <- verzuege_df %>%
  filter(AGS == "06431") %>%
  filter(wk > 45) %>% # letzte drei Wochen
  # Histogramm: 
  count(delta)

write.xlsx(bergstr_df,"bergstr-histogramm.xlsx")

ldk_df <- verzuege_df %>%
  filter(AGS == "06532") %>%
  filter(wk > 45) %>% # letzte drei Wochen
  # Histogramm: 
  count(delta)

write.xlsx(ldk_df,"ldk-histogramm.xlsx")

ffm_df <- verzuege_df %>%
  filter(AGS == "06412") %>%
  filter(wk > 45) %>% # letzte drei Wochen
  # Histogramm: 
  count(delta)

write.xlsx(ffm_df,"ffm-histogramm.xlsx")

msg("Histogrammdaten WI, LDK, Bergstrasse, FFM lokal geschrieben")
# ---- Jetzt berechnen: Wochenweise Ungenauigkeit ----

t = today()

# Einmal den kompletten Datensatz

d <- t-70 # 10 Wochen zurück 
inz_delta_df <- NULL

while (d <= t-7) {
  
  kw <- isoweek(as_date(d))
  # 
  ref7tage_df <- read_csv2(paste0(path,"rki-",as_date(d+7),".csv")) %>%
    mutate(datum = as_date(Meldedatum)) %>%
    filter(datum > as_date(d-8) & datum < as_date(d)) %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall) %>%
    # Nach Kreis sortieren
    group_by(AGS) %>%
    #  pivot_wider(names_from = datum, values_from = AnzahlFall)
    # Summen für Fallzahl, Genesen, Todesfall bilden
    summarize(AnzahlFall = sum(AnzahlFall)) %>%
    ungroup() %>%
    select(AGS,ref7t = AnzahlFall)
  alt7tage_df <- read_csv2(paste0(path,"rki-",as_date(d),".csv")) %>%
    mutate(datum = as_date(Meldedatum)) %>%
    filter(datum > as_date(d-8) & datum < as_date(d)) %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall) %>%
    # Nach Kreis sortieren
    group_by(AGS) %>%
    #  pivot_wider(names_from = datum, values_from = AnzahlFall)
    # Summen für Fallzahl, Genesen, Todesfall bilden
    summarize(AnzahlFall = sum(AnzahlFall)) %>%
    ungroup() %>%
    select(AGS,alt7t = AnzahlFall) %>%
    full_join(ref7tage_df, by = "AGS") %>%
    mutate(vollst = (100*alt7t/ref7t)-100) %>%
    select(AGS, vollst) %>%
    rename(!!as.character(d):=vollst)
    
  
  if (is.null(inz_delta_df)) {
    inz_delta_df <- alt7tage_df  
  } else {
    inz_delta_df <- inz_delta_df %>%
      full_join(alt7tage_df, by = "AGS")
  }
  msg("Inzidenz-Fehler ",as_date(d)," berechnet")
  d <- d+7 # Wochenweise durchsteppen 
  
}

inz_delta_df <- kreise %>%
  full_join(inz_delta_df, by = "AGS") %>%
  select(-AGS)

# write.xlsx(inz_delta_df,"vollstaendigkeit-kreise-wochen.xlsx")
sheet_write(inz_delta_df, ss = aaa_id, sheet = "Abweichung Inzidenz")
msg("Abweichungen berechnet und geschreiben")

# ---- Letzte 4 Wochen: Abweichungen nach Kreis und Wochentag

t <- today()
inz_wt_df <- NULL

for (d in (t-35):(t-7)) {
  
  tag <- wday(as_date(d),label=TRUE,abbr=TRUE)
  # 
  ref7tage_df <- read_csv2(paste0(path,"rki-",as_date(d+7),".csv")) %>%
    mutate(datum = as_date(Meldedatum)) %>%
    filter(datum > as_date(d-8) & datum < as_date(d)) %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall) %>%
    # Nach Kreis sortieren
    group_by(AGS) %>%
    #  pivot_wider(names_from = datum, values_from = AnzahlFall)
    # Summen für Fallzahl, Genesen, Todesfall bilden
    summarize(AnzahlFall = sum(AnzahlFall)) %>%
    ungroup() %>%
    select(AGS,ref7t = AnzahlFall)
  alt7tage_df <- read_csv2(paste0(path,"rki-",as_date(d),".csv")) %>%
    mutate(datum = as_date(Meldedatum)) %>%
    filter(datum > as_date(d-8) & datum < as_date(d)) %>%
    # Auf die Summen filtern?
    filter(NeuerFall %in% c(0,1)) %>%
    select(AGS = IdLandkreis,AnzahlFall) %>%
    # Nach Kreis sortieren
    group_by(AGS) %>%
    #  pivot_wider(names_from = datum, values_from = AnzahlFall)
    # Summen für Fallzahl, Genesen, Todesfall bilden
    summarize(AnzahlFall = sum(AnzahlFall)) %>%
    ungroup()  %>%
    select(AGS,alt7t = AnzahlFall) %>%
    full_join(ref7tage_df, by = "AGS") %>%
    mutate(vollst = (100*alt7t/ref7t)-100) %>%
    mutate(wt = tag) %>%
    # AGS durch Namen ersetzen
    full_join(kreise, by = "AGS") %>%
    select(kreis, vollst, wt) %>%
    pivot_wider(names_from = kreis, values_from = vollst)
  

  
  if (is.null(inz_wt_df)) {
    inz_wt_df <- alt7tage_df  
  } else {
    inz_wt_df <- rbind(inz_wt_df,alt7tage_df)
  }
  msg("Inzidenz-Fehler ",as_date(d)," berechnet")
  d <- d+7 # Wochenweise durchsteppen 
  
}

inz_wt_sum_df <- inz_wt_df %>%
  pivot_longer(cols = -wt, names_to = "kreis" ,values_to = "vollst") %>%
  arrange(wt) %>%
  group_by(wt,kreis) %>%
  summarize(vollst = mean(vollst)) %>%
  pivot_wider(names_from = kreis, values_from = vollst)



# write.xlsx(inz_wt_df,"vollstaendigkeit-kreise-wochentage.xlsx")
sheet_write(inz_wt_sum_df, ss = aaa_id, sheet = "Abweichung Wochentage" )
msg("Abweichungen Wochentage berechnet und geschreiben")



msg("OK")


