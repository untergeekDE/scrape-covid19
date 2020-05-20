# scrape-covid19
Eine Sammlung von R-Code, um Covid-19-Fälle von den Webseiten des hessischen Sozialministeriums und des Robert-Koch-Instituts (RKI) zu holen. 

## Die Daten

- Quelle RKI: https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0
- Quelle DIVI: https://www.divi.de/images/Dokumente/Tagesdaten_Intensivregister_CSV/
- Quelle Helmholtz: https://gitlab.com/simm/covid19/secir/-/tree/master/img/dynamic/Rt_rawData
- Quelle JHU: https://gitlab.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/

Die [Quelle "Hessisches Sozialministerium"](https://soziales.hessen.de/gesundheit/infektionsschutz/coronavirus-sars-cov-2/taegliche-uebersicht-der-bestaetigten-sars-cov-2-faelle-hessen) wurde wegen mangelnder Zuverlässigkeit und fehlender API/CSV zum 21.5. aussortiert. 

### Endpunkte 

Die Daten werden in der Regel in ein Google Sheet geschrieben.

- Fälle, Inzidenz und Dynamik nach Kreisen https://docs.google.com/spreadsheets/d/1h0bvmSjSC-7osQpt94iGre9K5o_Atfj0UnLyQbuN9l4/edit#gid=0
- Fälle, Todesfälle, Genesene, Wachstumsraten in den letzten vier Wochen https://docs.google.com/spreadsheets/d/1OhMGQJXe2rbKg-kCccVNpAMc3yT2i3ubmCndf-zX0JU/edit#gid=1805279723
- Wachstumsraten und 7-Tage-Trends im Vergleich https://docs.google.com/spreadsheets/d/1NXHj4JXfD_jaf-P3YjA7jv3BYuOwU9_WKoVcDknIG64/edit#gid=1580920358
- Fälle und Todesfälle nach Alter und Geschlecht (RKI-Daten) https://docs.google.com/spreadsheets/d/1RxlykWHoIZEq91M1bJNf26VPn8OWwI6VsgvAO47HM8Q/edit#gid=89632754

Die tägliche Übersicht des Ministeriums wird bis auf weiteres auch hier angeboten: https://d.data.gcp.cloud.hr.de/scrape-hsm.csv
  
## Die Skripte
* hessen-zahlen-aufbereiten.R - holt morgens die RKI-Daten, rechnet ein paar Auswertungen, beschreibt Tabellen und erneuert die Grafiken
* scrape-helmholtz.R - liest die Reproduktionswert-Daten des SECIR-Modells der Helmholtz-Gesellschaft für Hessen
* scrape-divi.R - liest morgens die Einzelmeldungen (JSON) und das Überblicks-Blatt (CSV) zur Intensivbettenauslastung
* scrape-jhu.R - baut aus RKI-Daten via API und JHU-Daten via Github ein Google Sheet für die logarithmische Inzidenz-Wachstumskurve

### Datenexport

Eines der schönsten Features von Datawrapper ist [die Möglichkeit, Daten dynamisch anzuzeigen](https://academy.datawrapper.de/article/60-external-data-sources) (also beim Aufruf der Grafik an den Datenstand anzupassen). Dafür muss entweder ein CSV auf einen dafür ausgelegten Server geschoben werden (wir haben experimentell einen Google-Bucket verwendet), oder ein Google-Sheet genutzt werden - das der Datawrapper-Server in regelmäßigen Stunden, im Stundenabstand in der Regel, ansteuert. 

Da eine Stunde zu lang ist für den Produktiveinsatz, wurde die Kombination gewählt: Google Sheets aktualisieren, Datawrapper-Aktualisierung über Datawrapper-API-Zugriff anstoßen. 

### Funktionskontrolle

Die Scraper schreiben ihren Status in [dieses Google-Sheet](https://docs.google.com/spreadsheets/d/1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk/edit#gid=0). 

## Besondere R-Pakete
(müssen in der Regel über '''devtools::install_github()''' installiert werden)
* [googlesheets4](https://github.com/tidyverse/googlesheets4) wird benötigt, um Google-Sheets schreiben zu können. Aus denen zieht sich Datawrapper die Daten zur Visualisierung. Unbedingt googlesheets4-Paket installieren, nicht googlesheets - das akzeptiert Google nicht mehr!
* [DatawRrappr](https://github.com/munichrocker/DatawRappr/) wird zum API-Zugriff auf unsere Datawrapper-Grafiken benötigt - und findet nur in einer Form statt: um eine Neupublikation (und damit Aktualisierung) von Grafiken/Tabellen/Karten anzustoßen

## Authentifizierung 
Die Scraper setzen i.d.R. ein Google-Helper-Account und einen lokalen Key dazu voraus, um auf die Google-Sheets zugreifen zu können. 

Die Authentifizierung für die Datawrapper-Zugänge muss man einmal in der R-Umgebung aufrufen - dann wird der API-Key im R-Environment gespeichert. 

## Es geht auch ohne Datawrapper-API...

Eins der schönsten Features von Datawrapper ist die Möglichkeit, Daten dynamisch anzeigen zu können - dass Grafiken ohne unser Zutun aktualisiert werden, wenn die Daten sich geändert haben. Dazu muss man entweder
* eine CSV-Datei auf einem dafür geeigneten Server hinterlegen
* ein via URL öffentlich einsehbares Google Sheet ([Beispieldatei](https://docs.google.com/spreadsheets/d/1OhMGQJXe2rbKg-kCccVNpAMc3yT2i3ubmCndf-zX0JU/edit#gid=1805279723)) als Quelle angeben
* eine CSV-Datei auf Github hochladen und die Github-Adresse an Datawrapper übergeben
Die zweite bzw. dritte Methode hat einen Nachteil: Um hohe Last zu vermeiden, werden die Daten auf dem Datawrapper-Server zwischengespeichert und nur in Abständen aktualisiert - in der Regel stündlich. Wem das nicht schnell genug geht - und mir geht es nicht schnell genug - der muss entweder über die Datawrapper-API oder von Hand eine Neu-Veröffentlichung der betreffenden Grafik auslösen; so macht es mein Code. Oder eben die Server-Lösung bauen - dann sind sowohl die datawRappr- als auch die googlesheets4-Library verzichtbar. 

[Details hier in der Datawrapper Academy.](https://academy.datawrapper.de/article/60-external-data-sources)

Wenn man von Hand aktualisieren will, reicht es, die entsprechende Grafik in Datawrapper zum Editieren zu öffnen. Dann zieht Datawrapper die aktuellen Daten vom Google-Sheet nach. Wenn man schon in der Grafik arbeitet, einmal den "Füge Daten hinzu"-Reiter zu gehen.

### Changelog: 
* 21.5.: Neues Skript hessen-zahlen-auswerten.R löst scrape-hsm-universal.R und scrape-rki-n.R ab
* 19.5.: Teile des scrape-hsm-universal-R-Skripts ausgelagert in scrape-jhu.R (und Umstellung auf RKI- statt HMSI-Daten)
* 15.5.: Flächendiagramm für Tote, Aktive Fälle, Neufälle, Genesene (nach Spiegel-Vorbild) 
* 14.5.: Bugfix - Aktive Fälle (RKI) nach Alter und Geschlecht werden jetzt korrekt gefiltert
* 13.5.: Basisdaten ohne Verdoppelungszeit, aber mit Trend zur Vorwoche; Neufälle (Meldedatum) nach Tagen (basierend auf RKI-Daten) als neue Grafik erzeugt und gepusht; gemeldete Neufälle pro Tag als Datum mit aus den HMSI-Daten erzeugt. 
* 11.5.: Helmholtz-Daten werden um das RKI-R ergänzt (aus einem XLS, das das RKI auf die Seite stellt)
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
