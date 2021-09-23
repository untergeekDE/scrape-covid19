#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#
# Scraper fuer die 'Taegliche Uebersicht ueber die Indikatoren zur Pandemiebestimmung'
# des 'Hessisches Ministerium fuer Soziales und Integration'
#
#
# Hessischer Rundfunk
# Autor: Sandra Kiefer
# Datum: 23.09.2021
#
#

import re
import csv
import glob
import gspread
import os.path
import requests
import pymsteams
from bs4 import BeautifulSoup
from datetime import datetime
from datawrapper import Datawrapper
from oauth2client.service_account import ServiceAccountCredentials


regExDate = r'^\s*(3[01]|[12][0-9]|0?[1-9])\.(1[012]|0?[1-9])\.((?:19|20)\d{2})\s*$'


def getHospitalisierungsinzidenz(txt_hi):
    """
    Liefert die gesuchten Werte zur Hospitalisierungsinzidenz aus dem Text und gibt diese formatiert zurueck

    :param txt_hi:  String
                    Text/Absatz auf der Website, welcher die Zahlen zur Hospitalisierungsinzidenz beinhaltet

    :return:        dict {  'Hospitalisierungsindzidenz aktuell': number (float),
                            'Hospitalisierungsindzidenz letzte Woche': number (float) }
                    Key-Value-Paar ueber die gescrapten Daten zur Hospitalisierungsinzidenz
    """
    txt_hi = txt_hi.replace(',', '.')   # Kommas mit Punkten ersetzen (ereinfacht automatische Typung auf float)
    numbers_hi = []
    for s in txt_hi.split():
        try:
            if float(s) < 99: numbers_hi.append(float(s))
        except ValueError:
            continue
    return {'Hospitalisierungsindzidenz aktuell': numbers_hi[0], 'Hospitalisierungsindzidenz letzte Woche': numbers_hi[1]}


def getIntensivbettenauslastung(txt_ia):
    """
    Liefert die gesuchten Werte zur Intensivbettenauslastung aus dem Text und gibt diese formatiert zurueck

    :param txt_ia:  String
                    Text/Absatz auf der Website, welcher die Zahlen zur Intensivbettenauslastung beinhaltet

    :return:        dict {  'Intensivbettenauslastung Datum': date (dd.mm.YYYY),
                            'Intensivbettenauslastung aktuell': number (int),
                            'Intensivbettenauslastung bestaetigt': number (int),
                            'Intensivbettenauslastung Verdacht': number (int) }
                    Key-Value-Paar ueber die gescrapten Daten zur Intensivbettenauslastung
    """
    date_ia = None
    for s in txt_ia.split():
        if re.match(regExDate, s):
            date_ia = s
    numbers_ia = []
    for s in txt_ia.split():
        try:
            numbers_ia.append(int(s))
        except ValueError:
            continue
    return {'Intensivbettenauslastung Datum': date_ia, 'Intensivbettenauslastung aktuell': numbers_ia[0], 'Intensivbettenauslastung bestaetigt': numbers_ia[1], 'Intensivbettenauslastung Verdacht': numbers_ia[2]}


def getNormalbettenauslastung(txt_na):
    """
    Liefert die gesuchten Werte zur Normalbettenauslastung aus dem Text und gibt diese formatiert zurueck

    :param txt_na:  String
                    Text/Absatz auf der Website, welcher die Zahlen zur Normalbettenauslastung beinhaltet

    :return:        dict {  'Normalbettenauslastung Datum': date (dd.mm.YYYY),
                            'Normalbettenauslastung aktuell': number (int),
                            'Normalbettenauslastung bestaetigt': number (int),
                            'Normalbettenauslastung Verdacht': number (int) }
                    Key-Value-Paar ueber die gescrapten Daten zur Normalbettenauslastung
    """
    date_na = None
    for s in txt_na.split():
        if re.match(regExDate, s):
            date_na = s
    numbers_na = []
    for s in txt_na.split():
        try:
            numbers_na.append(int(s))
        except ValueError:
            continue
    return {'Normalbettenauslastung Datum': date_na, 'Normalbettenauslastung aktuell': numbers_na[0], 'Normalbettenauslastung bestaetigt': numbers_na[1], 'Normalbettenauslastung Verdacht': numbers_na[2]}


def getImpfstatusHospitalisierte(txt_ih):
    """
    Liefert die gesuchten Werte zum Impfstatus der Hospitalisierten aus dem Text und gibt diese formatiert zurueck

    :param txt_ih:  String
                    Text/Absatz auf der Website, welcher die Zahlen zum Impfstatus der Hospitalisierten beinhaltet

    :return:        dict {  'Hospitalisierte ungeimpft': number (float),
                            'Hospitalisierte Impfstatus unbekannt': number (float),
                            'Hospitalisierte geimpft': number (float) }
                    Key-Value-Paar ueber die gescrapten Daten zum Impfstatus der Hospitalisierten
    """
    txt_ih = txt_ih.replace(',', '.')    # Kommas mit Punkten ersetzen (ereinfacht automatische Typung auf float)
    numbers_ih = []
    for s in txt_ih.split():
        try:
            numbers_ih.append(float(s))
        except ValueError:
            continue
    return {'Hospitalisierte ungeimpft': numbers_ih[0], 'Hospitalisierte Impfstatus unbekannt': numbers_ih[2], 'Hospitalisierte geimpft': numbers_ih[1]}


def getImpfquote(txt_iq):
    """
    Liefert die gesuchten Werte zur Impfquote aus dem Text und gibt diese formatiert zurueck

    :param txt_iq:  String
                    Text/Absatz auf der Website, welcher die Zahlen zur Impfquote beinhaltet

    :return:        dict {  'Impfquote Datum': date (dd.mm.YYYY),
                            'Impfquote': number (float),
                            'Impquote nur Impffaehige': number (float) }
                    Key-Value-Paar ueber die gescrapten Daten zur Impfquote
    """
    txt_iq = txt_iq.replace(',', '.')    # Kommas mit Punkten ersetzen (ereinfacht automatische Typung auf float)
    txt_iq = txt_iq.replace(')', ' ')    # Datum hinter der Klammer entfernen, damit RegEx funktioniert
    date_iq = None
    for s in txt_iq.split():
        if re.match(regExDate, s):
            date_iq = s
    numbers_iq = []
    for s in txt_iq.split():
        try:
            numbers_iq.append(float(s))
        except ValueError:
            continue
    return {'Impquote Datum': date_iq, 'Impquote': numbers_iq[0], 'Impquote nur Impffaehige': numbers_iq[1]}


def sendTeamsMessage(data_dict):
    """
    Sendet eine Message bei Teams in die 'corona'-Gruppe vom 'hr-Datenteam'

    :param data_dict:   gesammeltes Dict ueber die Werte ['Hospitalisierungsinzidenz', 'Intensivbettenauslastung', 'Normalbettenauslastung', 'Impfstatus Hospitalisierte', 'Impfquote']
                        wird von der main erstellt (durch Aufruf der verschiedenen get...()-Funktionen und Zusammenfuegen in ein gemeinsames Dict)
    """
    # hrDatenteamCorona = pymsteams.connectorcard('https://hrhessen.webhook.office.com/webhookb2/7cea83e5-54d9-4ef1-b745-29a8c632ad00@daaae532-0750-4b35-8f3f-fdd6ba4c86f0/IncomingWebhook/0b1deea18e494a14b7f4008c7cb9644f/dbe95101-4eda-4ed0-b2ac-5e03d25c0398')
    hrDatenteamCorona = pymsteams.connectorcard('https://hrhessen.webhook.office.com/webhookb2/7cea83e5-54d9-4ef1-b745-29a8c632ad00@daaae532-0750-4b35-8f3f-fdd6ba4c86f0/IncomingWebhook/cb2b0da4990948a4abf8c75faa97e0a7/dbe95101-4eda-4ed0-b2ac-5e03d25c0398')
    hrDatenteamCorona.title('Update Leitindikatoren zur Bestimmung des Pandemiegeschehens ')
    hrDatenteamCorona.text('letzte Aktualisierung: ' + list(data_dict.values())[0] + ' Uhr, Quelle: Hessisches Ministerium fuer Soziales und Integration')

    hiSection = pymsteams.cardsection()
    hiSection.text('Hospitalisierungsinzidenz')
    hiSection.addFact('aktuell', list(data_dict.values())[1])
    hiSection.addFact('letzte Woche', list(data_dict.values())[2])
    hrDatenteamCorona.addSection(hiSection)

    iaSection = pymsteams.cardsection()
    iaSection.text('Intensivbettenauslastung ' + list(data_dict.values())[3])
    iaSection.addFact('aktuell', list(data_dict.values())[4])
    iaSection.addFact('bestaetigt', list(data_dict.values())[5])
    iaSection.addFact('Verdacht', list(data_dict.values())[6])
    hrDatenteamCorona.addSection(iaSection)

    naSection = pymsteams.cardsection()
    naSection.text('Normalbettenauslastung ' + list(data_dict.values())[7])
    naSection.addFact('aktuell', list(data_dict.values())[8])
    naSection.addFact('bestaetigt', list(data_dict.values())[9])
    naSection.addFact('Verdacht', list(data_dict.values())[10])
    hrDatenteamCorona.addSection(naSection)

    ihSection = pymsteams.cardsection()
    ihSection.text('Impfstatus Hospitalisierte')
    ihSection.addFact('ungeimpft', str(list(data_dict.values())[11]) + ' %')
    ihSection.addFact('Impfstatus unbekannt', str(list(data_dict.values())[12]) + ' %')
    ihSection.addFact('geimpft', str(list(data_dict.values())[13]) + ' %')
    hrDatenteamCorona.addSection(ihSection)

    iqSection = pymsteams.cardsection()
    iqSection.text('Impfquote ' + list(data_dict.values())[14])
    iqSection.addFact('gesamt', str(list(data_dict.values())[15]) + ' %')
    iqSection.addFact('nur Impffaehige', str(list(data_dict.values())[16]) + ' %')
    hrDatenteamCorona.addSection(iqSection)

    hrDatenteamCorona.send()

    print('Nachricht in MS-Teams Chat von "hr-Datenteam corona" geschickt')


def editDataLocations(data_dict):
    """
    Verwaltet/Aktualisiert alle vorhanden Datenschnittstellen (lokales CSV, online Google Sheets, Teams Message, Datawrapper, Newswire Bucket)

    :param data_dict:   gesammeltes Dict ueber die Werte ['Hospitalisierungsinzidenz', 'Intensivbettenauslastung', 'Normalbettenauslastung', 'Impfstatus Hospitalisierte', 'Impfquote']
                        wird von der main erstellt (durch Aufruf der verschiedenen get...()-Funktionen und Zusammenfuegen in ein gemeinsames Dict)
    """
    # aktuellesten Pfad (durch Datum) speichern
    newPath = '/home/jan_eggers_hr_de/hessen_hospitalisierungen_' + datetime.now().strftime('%Y-%m-%d') + '.csv'    # newPath = 'hessen_hospitalisierungen_' + datetime.now().strftime('%Y-%m-%d') + '.csv'
    # CSV aktualisieren
    if glob.glob('/home/jan_eggers_hr_de/hessen_hospitalisierungen_*.csv'):     # if glob.glob('hessen_hospitalisierungen_*.csv'):
        oldPath = glob.glob('/home/jan_eggers_hr_de/hessen_hospitalisierungen_*.csv')[0]    # oldPath = glob.glob('hessen_hospitalisierungen_*.csv')[0]
        # Dateiname aktualisieren
        if newPath != oldPath: os.rename(oldPath, newPath)
        # bereitsvorhandene/erstellte CSV Datei einlesen
        with open(newPath, 'r', newline='') as file:
            reader = csv.reader(file)
            data = list(reader)
        # Datum bei Abgleich vernachlaessigen (hat sich was seit letztem mal veraendert?)
        compData = data[-1].copy()
        compData.pop(0)
        compOut = list(output.values()).copy()
        compOut.pop(0)
        if all(str(x) in compData for x in compOut):
            # es hat keine Aktualisierung der Daten stattgefunden
            print('CSV ist auf dem aktuellsten Stand!')
        else:
            # CSV neu schreiben
            with open(newPath, 'w', newline='') as file:
                writer = csv.writer(file)
                # bereits vorhandene Informationen wieder speichern (Daten aus letzter CSV)
                if data[-1][0].split()[0] == datetime.now().strftime('%d.%m.%Y'):
                    # ohne die letzte Zeile (Aktualisierung)/letzte Zeile loeschen
                    writer.writerows(data[:-1])
                    editGoogleSheets(data_dict, 'UPDATE')   # Google Sheets updaten
                else:
                    # alle alten Zeilen schreiben (neuer Tag/neue Daten)
                    writer.writerows(data)
                    editGoogleSheets(data_dict, 'ADD')      # Google Sheets updaten
                # neue Daten dranhaengen
                writer.writerow(list(data_dict.values()))
                print('CSV wurde aktualisiert!')
                sendTeamsMessage(data_dict)     # Teams Message verschicken
                writeTXTforNewswire(data_dict)  # txt fuer Newswire Bucket erzeugen
    # CSV neu anlegen
    else:
        with open(newPath, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(list(data_dict.keys()))
            writer.writerow(list(data_dict.values()))
            print('CSV wurde neu angelegt!')
            editGoogleSheets(data_dict, 'ADD')  # Google Sheets updaten
            sendTeamsMessage(data_dict)         # Teams Message verschicken
            writeTXTforNewswire(data_dict)      # txt fuer Newswire Bucket erzeugen


def editGoogleSheets(data_dict, method):
    """
    Updaten der bereits vorhanden Google Sheets, welche zum Fuettern des Datawrappers benutzt werden
    ruft das Publizieren der Datawrapper Inhalte auf

    :param data_dict:   gesammeltes Dict ueber die Werte ['Hospitalisierungsinzidenz', 'Intensivbettenauslastung', 'Normalbettenauslastung', 'Impfstatus Hospitalisierte', 'Impfquote']
                        wird von der main erstellt (durch Aufruf der verschiedenen get...()-Funktionen und Zusammenfuegen in ein gemeinsames Dict)
    :param method:      String
                        zwei Moeglichkeiten = 'UPDATE' oder 'ADD' (damit man weiss ob eine Zeile drangehaengt wird oder bearbeitet werden muss)
    """
    # Verbindungsaufbau zum gewuenschten Google Sheet
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/jan_eggers_hr_de/pscripts/client_secret.json', scope)
    client = gspread.authorize(creds)

    # Corona Warnstufen in Hessen aus den vorhandenen Daten ermitteln
    warnstufe = 0
    if data_dict['Intensivbettenauslastung aktuell'] > 200 or data_dict['Hospitalisierungsindzidenz aktuell'] > 8: warnstufe = 1
    if data_dict['Intensivbettenauslastung aktuell'] > 400 or data_dict['Hospitalisierungsindzidenz aktuell'] > 15: warnstufe = 2
    # Dict mit den passenden Bezeichnungen fuer Google Sheets erstellen
    excelDict = {'Intensiv-Patienten ': data_dict['Intensivbettenauslastung aktuell'], 'Hospitalisierung-Inzidenz ': data_dict['Hospitalisierungsindzidenz aktuell'], 'Corona-Warnstufe Hessen ': warnstufe}
    # entsprechende Tabbelenseite Aufrufen (hier 'Basisdaten')
    sheet1 = client.open('AAA Covid19-Daten hessenschau.de').worksheet('Basisdaten')  # sheet1 = client.open('Temp').worksheet('Basisdaten')
    # Faelle Intensivstation
    sheet1.update_cell(6, 1, (str(list(excelDict.keys())[0]) + data_dict['Intensivbettenauslastung Datum'][:-4]))
    sheet1.update_cell(6, 2, list(excelDict.values())[0])
    # Hospitalisierungsinzidenz
    sheet1.update_cell(7, 1, (str(list(excelDict.keys())[1])))
    sheet1.update_cell(7, 2, list(excelDict.values())[1])
    # Corona Warnstufe Hessen
    sheet1.update_cell(8, 1, (str(list(excelDict.keys())[2])))
    sheet1.update_cell(8, 2, list(excelDict.values())[2])
    # Datum aktualisieren
    sheet1.update_cell(2, 1, datetime.now().strftime('%d.%m.%Y, %H:%M Uhr'))

    # entsprechende Tabbelenseite Aufrufen (hier 'Krankenhauszahlen')
    sheet2 = client.open('AAA Covid19-Daten hessenschau.de').worksheet('Krankenhauszahlen') # sheet2 = client.open('Temp').worksheet('Leitindikatoren Pandemiegeschehen')
    if method == 'ADD':
        sheet2.append_row(list(data_dict.values())[:-3])
    if method == 'UPDATE':
        sheet2.delete_rows(len(sheet2.get_all_values()))
        sheet2.append_row(list(data_dict.values())[:-3])

    print('Tabellen wurden bei Google Sheets aktualisiert!')
    publishDatawrapper()            # Datawrapper Karten mit den neuen Daten aktualisieren


def publishDatawrapper():
    """
    Veroeffentlicht die Datawrapper Inhalte neu, damit die veraenderten Werte in der Tabelle bei Google Sheets neu geladen werden
    """
    dw = Datawrapper(access_token='m5n8O1c2TOtNOt5C5mtwYHyZ2IQ76JJa1NTdHeLD2HmXUi6esiSMjeXaAv0U8H8A')
    dw.publish_chart('OXn7r')
    print('"Hessen: Aktuelle Corona-Zahlen" bei Datawrapper aktualisiert (publish)!')
    dw.publish_chart('I1p2e')
    print('"Entwicklung der schweren Corona-Faelle in Hessen" bei Datawrapper aktualisiert (publish)!')


def writeTXTforNewswire(data_dict):
    """
    Schreibt txt-Datei ins Verzeichnis und kopiert diese dann in den Newswire Bucket mit Hilfe der Shell

    :param data_dict:   gesammeltes Dict ueber die Werte ['Hospitalisierungsinzidenz', 'Intensivbettenauslastung', 'Normalbettenauslastung', 'Impfstatus Hospitalisierte', 'Impfquote']
                        wird von der main erstellt (durch Aufruf der verschiedenen get...()-Funktionen und Zusammenfuegen in ein gemeinsames Dict)
    """
    with open('/home/jan_eggers_hr_de/newswiremeldung.txt', 'w', encoding='utf-8') as f:    # with open('newswiremeldung.txt', 'w', encoding='utf-8') as f:
        f.write('Corona-Update: Klinikzahlen Hessen \n')
        f.write('Quelle: Hessisches Ministerium für Soziales und Integration \n\n')

        f.write('Update Leitindikatoren zur Bestimmung des Pandemiegeschehens \n')
        f.write('- letzte Aktualisierung: ' + str(list(data_dict.values())[0]) + ' Uhr \n\n')

        f.write('# Hospitalisierungsinzidenz \n')
        f.write('- aktuell ' + str(list(data_dict.values())[1]) + '\n')
        f.write('- letzte Woche ' + str(list(data_dict.values())[2]) + '\n\n')

        f.write('# Intensivbettenauslastung \n')
        f.write('COVID-Fälle auf hessischen Intensivstationen nach der IVENA-Sonderlage, Stand: ' + str(list(data_dict.values())[3]) + '\n')
        f.write('- aktuell ' + str(list(data_dict.values())[4]) + '\n')
        f.write('- Labor bestätigt ' + str(list(data_dict.values())[5]) + '\n')
        f.write('- Verdachtsfälle ' + str(list(data_dict.values())[6]) + '\n\n')

        f.write('# Normalbettenauslastung \n')
        f.write('COVID-Fälle in hessischen Krankenhäusern auf Normalstationen  nach der IVENA-Sonderlage, Stand: ' + str(list(data_dict.values())[7]) + '\n')
        f.write('- aktuell ' + str(list(data_dict.values())[8]) + '\n')
        f.write('- Labor bestätigt ' + str(list(data_dict.values())[9]) + '\n')
        f.write('- Verdachtsfälle ' + str(list(data_dict.values())[10]) + '\n\n')

        f.write('# Impfstatus Hospitalisierte \n')
        f.write('- ungeimpft ' + str(list(data_dict.values())[11]) + ' % \n')
        f.write('- Impfstatus unbekannt ' + str(list(data_dict.values())[12]) + ' % \n')
        f.write('- geimpft ' + str(list(data_dict.values())[13]) + ' % \n\n')

        f.write('Skript: scraper_hessen_hospitalisierung.py auf 35.207.90.86 \n')
        f.write('Redaktionelle Fragen an jan.eggers@hr.de')

    # fuehre Befehl aus um Datei an gwuenschten Ort zu kopieren
    os.system('gsutil -h "Cache-Control:no-cache, max_age=0" cp /home/jan_eggers_hr_de/newswiremeldung.txt gs://d.data.gcp.cloud.hr.de/newswiremeldung.txt')
    print('Daten wurden fuer Newswire abgelegt!')


if __name__ == "__main__":
    page = requests.get('https://soziales.hessen.de/gesundheit/corona-in-hessen/taegliche-uebersicht-ueber-die-indikatoren-zur-pandemiebestimmung')
    soup = BeautifulSoup(page.content, 'html.parser')

    div = soup.find_all('div', attrs={'class': 'he_content_body'})[0]               # relevanten HTML-Tag rausholen
    p = div.find_all('p')

    output = {'Letzte Aktualisierung': datetime.now().strftime('%d.%m.%Y %H:%M')}   # Zeitpunkt Datenabfrage
    output.update(getHospitalisierungsinzidenz(p[0].text))                          # Hospitalisierungsindzidenz
    output.update(getIntensivbettenauslastung(p[2].text))                           # Intensivbettenauslastung
    output.update(getNormalbettenauslastung(p[3].text))                             # Normalbettenauslastung
    output.update(getImpfstatusHospitalisierte(p[4].text))                          # Impfstatus Hospitalisierte
    output.update(getImpfquote(p[5].text))                                          # Impfquote

    editDataLocations(output)                                                       # Daten im CSV und Tabelle speichern

