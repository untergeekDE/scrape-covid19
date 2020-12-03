# scrape-covid19
Eine Sammlung von R-Code, um Covid-19-Fälle von den Webseiten des Robert-Koch-Instituts (RKI) auszulesen und gemeinsam mit einigen anderen Quellen für hessenschau.de zu verarbeiten. 

## Die Daten

- Quelle RKI (Aktuelle Fallzahlen): https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0 und API
- Quelle RKI (für den R-Wert): https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Projekte_RKI/Nowcasting_Zahlen.xlsx?__blob=publicationFile
- Quelle DIVI: Endpunkt https://diviexchange.blob.core.windows.net/%24web/DIVI_Intensivregister_Auszug_pro_Landkreis.csv und API
- Quelle Helmholtz: https://gitlab.com/simm/covid19/secir/-/tree/master/img/dynamic/Rt_rawData
- Quelle JHU: https://gitlab.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/

Die [Quelle "Hessisches Sozialministerium"](https://soziales.hessen.de/gesundheit/infektionsschutz/coronavirus-sars-cov-2/taegliche-uebersicht-der-bestaetigten-sars-cov-2-faelle-hessen) wurde wegen mangelnder Zuverlässigkeit und fehlender API/CSV zum 21.5. aussortiert. 

### Endpunkte 

Die Daten werden in der Regel in ein Google Sheet geschrieben.

- https://docs.google.com/spreadsheets/d/17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ/edit - alle Daten

Die tägliche Übersicht der Fallzahlen, Inzidenzen, Neufälle in den letzten 7 Tagen etc. nach Kreisen wird bis auf weiteres auch hier angeboten: https://d.data.gcp.cloud.hr.de/scrape-hsm.csv
  
## Die Skripte
* hessen-zahlen-aufbereiten.R - holt morgens die RKI-Daten, rechnet ein paar Auswertungen, beschreibt Tabellen und erneuert die Grafiken
* scrape-helmholtz.R - liest die Reproduktionswert-Daten des SECIR-Modells der Helmholtz-Gesellschaft für Hessen
* divi-zahlen-aufbereiten.R - liest morgens die Einzelmeldungen (JSON) und das Überblicks-Blatt (CSV) zur Intensivbettenauslastung
* meldeverzug-inzidenz.R - schaut für die zurückliegenden Wochen nach der prozentualen Veränderung der Inzidenz durch Meldeverzug und versucht einen "Datenqualitäts-Index" - Anteil der Daten in der letzten Woche, der mehr als 3 Tage zu spät kam

### veraltet: 
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
* 2.12.: Meldeverzugs-Skript
* 26.11.: DIVI-Abfrage durch ein anderes Skript ersetzt; Veränderung Todeszahlen absolut statt prozentual
* 21.11.: Rekursive Abfrage der RKI-Daten auf Schleife umgestellt, um Speichercrash zu vermeiden
* 20.11.: Abfrage und Aktualisierung der Passanten-Daten von hystreet.com für die hessischen Stationen; Passanten-Index
* 19.11.: Einlesen der Keys, Libraries, Messaging-Funktion über GoogleSheets in eine Include-Datei namens server-msg-googlesheet-include.R verlagert
* 2.11.: Update der Neufälle in 14-Tage-Prognose und der Intensivfälle in 4-Wochen-Prognose; Gesamtübersicht 7-Tage-Inzidenzen nach Kreis
* 23.10.: Tageszahl Neufälle pro Kreis ergänzt
* 2.10.: Neue Inzidenz-Choropleth-Karte eingebaut (mit diskreten Farbabstufungen statt Gradienten); Google-Bucket-Kopie angepasst
* 16.9.: R-Wert-Datumsbereich für RKI ausgeweitet, Fehler in einer (bislang nicht publizierten) DIVI-Tabelle korrigiert: Fälle statt Meldestellen; umgebaut auf neuen Google-Key 
* 23.8.: Datenbasis für Inzidenzberechnung jetzt Bevölkerungsstatistik Stand Q1/2020; Berechnung der 7-Tage-Inzidenz nach Falldatenbank, nicht nach Meldungshistorie
* 13.7.: Bis jetzt erfolgte der Aufruf über ein Shellskript; die Kopier-Befehle wurden jetzt in das Hauptskript übernommen und das Shellskript abgeschaltet. 
* 6.7.: Zahl der Genesenen korrekt formatieren; DIVI-Daten in eine andere Tabelle einfließen lassen und korrekte Zeitstempel angeben. 
* 24.6.: Altersstruktur; Anteile der Altersgruppen an den Neufällen pro Woche, seit 11. März
* 22.6.: Im Tooltipp der Datawrapper-Choropleth-Karte zu den 7-Tage-Neufällen nach Kreis eine kleine Verlaufsgrafik integriert
* 22.6.: Basisdaten angepasst; Fokussierung auf 7-Tage-Neufälle und Wochenvergleich
* 18.6.: Anpassung des DIVI-Scrapers an eine statische URL für das tagesaktuelle Daten-CSV; kleinerer Bug beim Timeout
* 10.6.: Nachdem fast alles fast drei Wochen fast störungsfrei lief, sind Reparaturen nötig - v.a. durch Umstellung auf RKI-JSON via API
* 10.6.: Update auf googlesheets4-Library v0.2.0, die fast alle Funktionen umbenannt hat
* 10.6.: URL des DIVI-Tagesreports muss aus Seite gescraped werden, da er jetzt eine laufende Nummer enthält
* 10.6.: Timeouts für DIVI und Helmholtz - wenn 2h kein neueres (bzw. tagesaktuelles) Dokument, abbrechen
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
