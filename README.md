# scrape-covid19

Eine Sammlung von R-Code, um Covid-19-Fälle von den Webseiten des Robert-Koch-Instituts (RKI) auszulesen und gemeinsam mit einigen anderen Quellen für hessenschau.de zu verarbeiten. 

## Die Daten

- Quelle RKI: [Falldaten auf Github](https://github.com/robert-koch-institut/SARS-CoV-2_Infektionen_in_Deutschland)
- Quelle ESRI: [Data Warehouse für die RKI-Falldaten](https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0 und API)
- RKI-Tagesmeldungen, auf dem Server archiviert
- Quelle RKI: [Nowcast und 7-Tage-R auf Github](https://github.com/robert-koch-institut/SARS-CoV-2-Nowcasting_und_-R-Schaetzung)
- Quelle RKI: [Testzahlen donnerstags als Excel-Tabelle](https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.html;jsessionid=EF5B74A0B2AEE46C92BE7A6C719305A9.internet082?nn=13490888)
- Quelle RKI: Impfdaten auf Github](https://github.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland) (alte Quelle: [Impfzahlen Mo-Fr als Excel-Tabelle](https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html;jsessionid=63BB094C521A1D8D2E1767E9A97F2699.internet061))
- Quelle RKI: [Todesfälle nach Sterbedatum](https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Projekte_RKI/COVID-19_Todesfaelle.html) (statt nach Meldedatum des Todesfalls)
- Quelle DIVI: Intensivregister via [CSV]h(ttps://diviexchange.blob.core.windows.net/%24web/DIVI_Intensivregister_Auszug_pro_Landkreis.csv) und API
- Quelle Helmholtz-Zentrum für Infektologie: [R-Wert-Schätzungen für Hessen als CSV](https://gitlab.com/simm/covid19/secir/-/tree/master/img/dynamic/Rt_rawData)
- Quelle Johns-Hopkins-Universität: [Länder als CSV](https://gitlab.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/)
- Quelle: DESTATIS-Bevölkerungsdaten 2018, 2019 und 2020 (Berechnungen werden ab Meldedatum 25.8.2021 mit den Daten von 2020 berechnet, ab September 2020 mit den Daten von 2019, davor mit den Daten von 2018)

Die [Quelle "Hessisches Sozialministerium"](https://soziales.hessen.de/gesundheit/infektionsschutz/coronavirus-sars-cov-2/taegliche-uebersicht-der-bestaetigten-sars-cov-2-faelle-hessen) wurde wegen mangelnder Zuverlässigkeit und fehlender API/CSV zum 21.5. aussortiert. 

### Endpunkte 

Die Daten werden in der Regel in ein Google Sheet geschrieben.

- [Google Sheet AAA](https://docs.google.com/spreadsheets/d/17s82vieTzxblhzqNmHw814F0xWN0ruJkqnFB1OpameQ/edit) - alle Daten
- [Google Sheet Hospitalisierung](https://docs.google.com/spreadsheets/d/12S4ZSLR3H7cOd9ZsHNmxNnzKqZcbnzShMxaWUcB9Zj4/edit) - von Hand eingetragene Klinikdaten

Die tägliche Übersicht der Fallzahlen, Inzidenzen, Neufälle in den letzten 7 Tagen etc. nach Kreisen wird bis auf weiteres auch hier angeboten: https://d.data.gcp.cloud.hr.de/scrape-hsm.csv
  
## Die Skripte
* hessen-zahlen-aufbereiten.R - holt morgens die RKI-Daten, rechnet ein paar Auswertungen, beschreibt Tabellen und erneuert die Grafiken
* lies-esri-tabelle-direkt.R - Daten der Auswertung des Datendienstleisters ESRI als Fallback, falls der Gesamt-Datensatz nicht kommt
* hole-r-rki-helmholtz.R - liest die Reproduktionswert-Daten des SECIR-Modells der Helmholtz-Gesellschaft für Hessen
* divi-zahlen-aufbereiten.R - liest morgens die Einzelmeldungen (JSON) und das Überblicks-Blatt (CSV) zur Intensivbettenauslastung
* hole-rki-testzahlen.R - liest ein XLSX mit den nationalen Testzahlen von der RKI-Seite
* hole-covid-simulator.R - liest ein CSV mit der wöchentlichen Simulation des COVID-Simulators der Universität des Saarlandes für Hessen 
* hole-impfzahlen.R - liest ein XLSX mit aktuellen Impfzahlen von der RKI-Seite
* meldeverzug-inzidenz.R - schaut für die zurückliegenden Wochen nach der prozentualen Veränderung der Inzidenz durch Meldeverzug und versucht einen "Datenqualitäts-Index" - Anteil der Daten in der letzten Woche, der mehr als 3 Tage zu spät kam
* server-msg-googlesheet-include.R - enthält Code, den fast alle Skripte brauchen: die Ausgabe von Statusmeldungen über GoogleSheets, den JSON-Code zum Einlesen der RKI-Daten, Unterscheidung Server/lokal, etc. 
* berechne-bundesnotbremse.R - wertet Archivmeldungen für die vergangenen Tage aus und schaut, ob die Kreise 3 Tage über/5 Werktage über der Grenze waren, und wann ein Kreis in eine der hessischen Lockerungsstufen rutscht.

### veraltet: 
* scrape-jhu.R - baut aus RKI-Daten via API und JHU-Daten via Github ein Google Sheet für die logarithmische Inzidenz-Wachstumskurve

### Datenexport

Eines der schönsten Features von Datawrapper ist [die Möglichkeit, Daten dynamisch anzuzeigen](https://academy.datawrapper.de/article/60-external-data-sources) (also beim Aufruf der Grafik an den Datenstand anzupassen). Dafür muss entweder ein CSV auf einen dafür ausgelegten Server geschoben werden (wir haben experimentell einen Google-Bucket verwendet), oder ein Google-Sheet genutzt werden - das der Datawrapper-Server in regelmäßigen Stunden, im Stundenabstand in der Regel, ansteuert. 

Da eine Stunde zu lang ist für den Produktiveinsatz, wurde die Kombination gewählt: Google Sheets aktualisieren, Datawrapper-Aktualisierung über Datawrapper-API-Zugriff anstoßen. 

[Details hier in der Datawrapper Academy.](https://academy.datawrapper.de/article/60-external-data-sources)

### Funktionskontrolle

Die Scraper schreiben ihren Status in [dieses Google-Sheet](https://docs.google.com/spreadsheets/d/1Q5rCvvSUn6WGcsCnKwGJ0y9PwbiML-34kyxYSYX2Qjk/edit#gid=0). 

## Besondere R-Pakete
(müssen in der Regel über '''devtools::install_github()''' installiert werden)
* [googlesheets4](https://github.com/tidyverse/googlesheets4) wird benötigt, um Google-Sheets schreiben zu können. Aus denen zieht sich Datawrapper die Daten zur Visualisierung. Unbedingt googlesheets4-Paket installieren, nicht googlesheets - das akzeptiert Google nicht mehr!
* [DatawRrappr](https://github.com/munichrocker/DatawRappr/) wird zum API-Zugriff auf unsere Datawrapper-Grafiken benötigt - und findet nur in einer Form statt: um eine Neupublikation (und damit Aktualisierung) von Grafiken/Tabellen/Karten anzustoßen

## Authentifizierung 
Die Scraper setzen i.d.R. ein Google-Helper-Account und einen lokalen Key dazu voraus, um auf die Google-Sheets zugreifen zu können. 

Die Authentifizierung für die Datawrapper-Zugänge muss man einmal in der R-Umgebung aufrufen - dann wird der API-Key im R-Environment gespeichert. 

Wenn man von Hand aktualisieren will, reicht es, die entsprechende Grafik in Datawrapper zum Editieren zu öffnen. Dann zieht Datawrapper die aktuellen Daten vom Google-Sheet nach. Wenn man schon in der Grafik arbeitet, einmal den "Füge Daten hinzu"-Reiter zu gehen.

### Changelog:

* 09.2.: ESRI-CSV liefert falsche Daten; Wartezeit auf Github-Daten verlängert (und dafür gesorgt, dass erst ab 4.30 Uhr gesucht wird); read.csv durch fread() aus der data.table-Library ersetzt
* 14.1.: Inzidenz mit archivieren
* 12.1.: Impfquoten 5-11jährige (aus Github)
* 12.1.: Die ehemalige Bundesnotbremsen-Tabelle auf 350er-Hotspot-Regel bereinigt, historischen Ballast abgeworfen - neues Skript berechne-inzidenz-ampel.R übernimmt das jetzt
* 10.1.2022: Kleines Linkcheck-Skript für die Links zu den Gesundheitsämtern
* 17.12.: Wochenweise Sterbefälle nach Sterbedatum (soweit vorhanden) statt nach Meldedatum des Todesfalls wie bisher. 
* 8.12.: Prognose-Wert für Impftempo und bisher verimpfte Dosen der laufenden Woche anders anzeigen (als Wert für die ganze Woche)
* 5.12.: Impfdaten-Auswertung endlich auf den Stand der Boosterimpfungen gebracht; Ländervergleich jetzt anhand des Booster-Fortschritts
* 2.12.: Bevölkerungsdaten aufbereitet (jetzt Stand 31.12.2020 aus Destatis-Tabelle 12411-04-02-04)
* 23.9.: Python-Scraper kümmert sich jetzt um Hospitalisierung und Krankenhauszahlen vom HMSI
* 16.9.: Die (unsinnige) Kenngröße "Hospitalisierungsinzidenz" vorbereitet; Basisdaten umgestellt; neue Grafik in divi-zahlen-aufbereiten.R: Altersschichtung auf den Intensivstationen
* 9.9.: Anpassung an zusätzliche Impftabellen-Spalten
* 6.9.: Bugfix - Fälle ohne Geschlecht wurden bei der Inzidenzberechnung nach Alter nicht berücksichtigt
* 3.9.: Drittimpfungen (Auffrischungsimpfungen) integriert
* 3.9.: Altersgruppengröße aus DESTATIS-Datei 12411-4-2-4 berechnen, um besser an geänderte Daten anpassen zu können (nicht mehr aus Google Sheet ziehen)
* 26.8.: Umstellung auf aktuelle Bevölkerungsdaten (Stichtag 31.12.2020); bei Berechnung der Archiv-Inzidenzen bis 24.8.2021 mit Daten von 2019 rechnen, ab 25.8.2021 mit Daten von 2020
* 24.8.: Impfdaten vom Github werden jetzt in historische Übersichtstabelle geschrieben
* 7.8.: Impfdaten werden jetzt vom Github gelesen - und aus der XLSX-Datei auf rki.de um die Impfquoten nach Altersgruppe ergänzt, die aus den Github-Dateien nicht zu berechnen sind. 
* 22.7.: Neues hessisches Eskalationskonzept umgesetzt - historische Bundesnotbremse- und Lockerungs-Tabelle wird nach alten Regeln bis 30.6. errechnet
* 20.7.: Crontab an neue RKI-Veröffentlichungstermine für Testzahlen (donnerstags) angepasst
* 20.7.: Github-Repository des RKI als Quelle für die Falldatenbank; Plausibilitätsprüfung vereinfacht
* 20.7.: R-Wert-Skript umbenannt in hole-r-rki-helmholtz.R, angepasst an geänderte RKI-Quelle ohne 4-Tage-R
* 4.7.: RKI-Github als Datenquelle, zunächst für die R-Schätzung/Nowcasting
* 20.6.: Über die teamR-Library kleine Nachrichten in einen hr-MS-Teams-Channel absetzen, wenn neue Daten vorliegen
* 26.5.: Code, der die Testzentren-Liste des Landes übernimmt und einbaut
* 21.5.: Regelungen des Landes Hessen für Lockerungs-Stufen 1 und 2 in Code übersetzt. 
* 14.5.: Tabelle mit den gemeldeten Inzidenzen ("Briefkastenmeldungen") als Archiv anlegen, weil daraus die Notbremse bestimmt wird
* 5.5.: Bundesnotbremse korrekt aus Archivdaten berechnet
* 30.4.: ESRI-Tabelle wird für Vorab-Daten genutzt 
* 26.4.: Bundesnotbremse visualisiert (Anzahl von Tagen über der jeweiligen Grenze)
* (diverse Anpassungen bei den Impfungen) 
* 28.1.: Impfquoten und -zahlen; Prognose zur Durchimpfung der Hochrisiko-Gruppe
* 12.1.: Überblick Todesfälle je Woche; Integration der Impfzahlen in die Basisdaten
* 10.12.: Helfer-Skripte aktualisieren COVID-Simulator-Prognose und RKI-Testdaten automatisch
* 8.12.: Todesfälle-Statistik berücksichtigt jetzt auch nicht zugeordnete Fälle
* 4.12.: Prognose wird aus Google Sheet gelesen, um aktuelle 4-Wochen-Werte ergänzt und neu geschrieben. 
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
