# scrape-covid19
Eine Sammlung von R-Code, um Covid-19-Fälle von den Webseiten des hessischen Sozialministeriums und des Robert-Koch-Instituts (RKI) zu holen. 

## Die Scraper
* scrape-hmsi-universal.R - fragt täglich ab 14 Uhr die Seite des Hessischen Sozialministeriums ab, ergänzt mit 
* scrape-rki-n.R - besorgt die Daten des RKI (auf dem Umweg über das NDR-Data-Warehouse) 
* scrape-helmholtz.R - liest die Reproduktionswert-Daten des SECIR-Modells der Helmholtz-Gesellschaft für Hessen
* scrape-divi.R - liest morgens die Einzelmeldungen (JSON) und das Überblicks-Blatt (CSV) zur Intensivbettenauslastung

### Datenexport

## Besondere R-Pakete
(müssen in der Regel über '''devtools::install_github()''' installiert werden)
* [googlesheets4](https://github.com/tidyverse/googlesheets4) wird benötigt, um Google-Sheets schreiben zu können. Aus denen zieht sich Datawrapper die Daten zur Visualisierung. Unbedingt googlesheets4-Paket installieren, nicht googlesheets - das akzeptiert Google nicht mehr!
* [DatawRrappr](https://github.com/munichrocker/DatawRappr/) wird zum API-Zugriff auf unsere Datawrapper-Grafiken benötigt - und findet nur in einer Form statt: um eine Neupublikation (und damit Aktualisierung) von Grafiken/Tabellen/Karten anzustoßen

## Authentifizierung 
Die Scraper setzen i.d.R. ein Google-Helper-Account und einen lokalen Key dazu voraus, um auf die Google-Sheets zugreifen zu können. 

Die Authentifizierung für die Datawrapper-Zugänge muss man einmal in der R-Umgebung aufrufen - dann wird der API-Key im R-Environment gespeichert. 

## Es geht auch ohne Datawrapper-API...

Eins der schönsten Features 

### Changelog: 
* 8.5.: Balkengrafik Fälle nach Geschlecht und Altersgruppe auf aktive Fälle gefiltert
* Helmholtz-Scraper versucht 2 Stunden lang, neueres Blatt zu lesen; wenn kein neueres, Abbruch mit Fehlermeldung
* 5.5.: RKI-Zahlen werden jetzt gleich morgens aktualisiert
* Genesene gesamt Basisdaten
* Genesene und aktive Fälle gesamt Barchart
* Genesene nach Kreis
* 4.5.: RKI-Scraper ein CSV lokal ablegen lassen, das das Scraper-Skript nutzt
* Fälle -> Stacked Barchart: Genesene, Fälle, Tote
* Dubletten-Erkennung durch absolute Adressierung auffangen
* Spalten hospitalisiert aus fallzahl_df, dafür aktive Fälle; Datawrapper anpassen
* 30.4.: Helmholtz-Scraper
* 29.4.: DIVI-Zahlen in Karte, Rt-Grafik/Scraper SECIR, NDR-CSV statt RKI-CSV
* 28.4.: HMSI-Tabelle wieder umgebaut;
* Umbau DIVI-Scraper auf CSV
* 26.4.: Eine Semaphore-Seite; Scraper mit Semaphor-Code ausstatten, der den Status signalisiert
* 26.4.: Aktive Fälle als Prozentzahl
* 24.4.: Gesunden-Zahlen auf Prozentzahl reduziert; Verweis auf Schätzung RKI
* 23.4.: RKI-Scraper von Till migriert
