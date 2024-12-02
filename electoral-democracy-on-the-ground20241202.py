#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Titel: Mobilisierung in der Nachbarschaft: Ein Feldexperiment zur Wahlbeteiligung bei der Europawahl 2024
Datum: 2024/12/02
Autor: Paul Gies, Manuel Diaz Garcia, Jonas Elis
"""

#Googlemaps API
import googlemaps as gm

#Importe fuer OpenStreetmaps
from geopy.geocoders import Photon as nt
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder
from OSMPythonTools.overpass import Overpass

#Pandas fuer dataframes und time zum warten zwischen Anfragen
import pandas as pd
import time


"""Setup und Suchfunktionen mit der Google Maps API"""

#API Key einlesen und Mapklienten starten, Key unter https://developers.google.com/maps?hl=de verfügbar
key = open("#####.txt", 'r').read()
map_client = gm.Client(key)

def g_api_search(search_query: str) -> pd.DataFrame:
    """"Funktion nimmt eine Suchanfrage und gibt einen df mit den unbearbeiten Outputs der Maps API aus"""
    outlist = []
    response = map_client.places(query=search_query)
    outlist.extend(response.get('results'))
    next_page_token = response.get('next_page_token') #Die Maps API gibt immer nur eine gewisse Anzahl von Ergebnissen pro Anfrage zurück, zudem einen "next_page_token", falls eine weitere Seite mit Ergebnissen vorlgiegt
    i=1

    while next_page_token: #Falls also ein next_page_token vorliegt, wird auch diese Seite erfasst, bis keiner mehr zurückgegeben wird
        time.sleep(2)
        response = map_client.places(query=search_query, page_token=next_page_token)
        outlist.extend(response.get('results'))
        next_page_token = response.get('next_page_token')
        i += 1
    
    df = pd.DataFrame(outlist)
    return df


def g_place_search(place_names: list, city_name: str) -> pd.DataFrame:
    """"Nimmt eine Liste von Ortsbezeichnungen und einen Stadtnamen, konstruiert daraus Suchanfragen und gibt diese an die vorige Funktion weiter. Die resultierenden dfs werden dann für die weitere Arbeit noch etwas umbenannt und unnötige Infos gelöscht."""
    df = pd.DataFrame()
    for pn in place_names:
        df = pd.concat([df, g_api_search(f"{pn} in {city_name}")], ignore_index=True) #Suchqueries sind in dieser Google Maps API lediglich einfache Strings (Hinweis: es gibt auch andere API Varianten -> https://developers.google.com/maps/documentation?hl=de)
    df.drop_duplicates(subset=["place_id"], keep="first", inplace=True) #Notwendig, da viele Duplikate entstehen, wenn man etwa nach "Kiosk" und "Trinkhalle" sucht
    df.drop_duplicates(subset=["formatted_address"], keep="first", inplace=True)
    df = df.rename(columns={"formatted_address" : "address"})
    df = df[['name', 'address']]
    return df


"""Ähnliches Vorgehen für Open Streetmaps, erst Setup des API-Klienten, dann Definition der Suchfunnktionen"""
nominatim = Nominatim()
locator = nt(user_agent='myGeocoder')
overpass = Overpass()

def o_api_search(pn: str, city_name: str) -> pd.DataFrame:
    """"Nimmt Ortsbezeichnung und Stadtname und gibt df mit Ergebnissen aus. Gegenstück zur Googlesuche, aber etwas komplexer, da OSM nicht immer eine Straßenadresse mit ausgibt, weshalb manchmal Koordinaten erst in Adressen rekodiert werden müssen."""
    query = overpassQueryBuilder(area=nominatim.query(city_name.lower()), elementType='node', selector=f'"shop"="{pn.lower()}"', out='body') #Die OS queries werden nicht so frei gebaut wie die Google Maps queries, sind also prinzipiell etwas eingeschränkter aber präziser
    result = overpass.query(query)
    rc = [e for e in result.elements() if 'name' in e.tags().keys()] #Alle Ergebnisse filtern, die einen Firmennamen haben. OSM liefert sonst viele falsche Treffer mit

    #Aufbereitung der Suchergebnisse, sodass am Ende ein df mit den verschiedenen Daten und Koordinaten steht
    klist = [k.tags() for k in rc]
    cords = [(k.lon(), k.lat()) for k in rc]
    kd = pd.DataFrame(klist)
    cords = pd.DataFrame(cords)
    cords.columns = ["lon", "lat"]
    kd = kd.merge(cords, left_index=True, right_index=True)

    #Aufbereitung der Adressdaten in das Format wie in Googlemaps. Falls keine Adresse vorhanden ist, adresskodierung mittels OSM
    for index, row in kd.iterrows():
        if pd.notna(row['addr:street']) and pd.notna(row['addr:housenumber']) and pd.notna(row['addr:postcode']) and pd.notna(row['addr:city']):
            kd.at[index, 'address'] = f"{row['addr:street']} {row['addr:housenumber']}, {row['addr:postcode']} {row['addr:city']}, Germany"
        else:
            kd.at[index, 'address'] = locator.reverse((row['lat'], row['lon']))

    kd = kd.drop(kd[kd.amenity == "fuel"].index) #OSM gibt viele Tankstellen als Trinkhallen aus, weshalb diese entfernt werden
    kd = kd[["name", "address"]] #Aufbereitung ins Format der GM dfs
    return kd


def o_place_search(place_names: list, city_name: str) -> pd.DataFrame:
    """"Ähnlich wie auch schon bei Google Maps, nimmt Ortsbezeichnungen und einen Stadtnamen, führt die OSM Suchfunktion aus und gibt einen df für alle Ortsbezeichnungen aus"""
    df = pd.DataFrame()
    for pn in place_names:
        #OSM ist etwas fehleranfällig, deswegen muss hier eine Routine rein, damit nicht jedes mal das Script abstürtzt
        while True:
            try:
                #df = pd.concat([df, o_api_search(pn, city_name)], ignore_index=True)
                0/0
            except Exception as e:
                print(f"{e}! Fortsetzen (1) oder wiederholen (2)?")
                des = input("1 oder 2: ")
                if des == "1":
                    break
                elif des == "2":
                    continue
                else:
                    print("Nur 1 oder 2!")
    df.drop_duplicates(subset=["name"], keep="first", inplace=True) #Ähnlich wie bereits bei Google werden Duplikate entfernt
    df = df[['name', 'address']]
    return df


"""Zusammen"""
def search(place_names: list, city_name: str, google=True, osm=True) -> pd.DataFrame:
    """"Nimmt Ortsbezeichnungen und einen Stadtnamen und gibt entweder nur für google, nur für OSM oder für beide die Ergebnisse aus"""
    if google == True and osm == True:
        df = g_place_search(place_names, city_name)
        df = pd.concat([df, o_place_search(place_names, city_name)], ignore_index=True)
    elif osm == False:
        df = o_place_search(place_names, city_name)
    else:
        df = g_place_search(place_names, city_name)
    df.drop_duplicates(subset=["name"], keep="first", inplace=True) #Duplikate entfernen, da Google Maps und OSM teils dieselben Ergebnisse liefern
    df = df.reset_index(drop=True)
    return df
    


"""Suche"""        
df = search(['Kiosk', 'Trinkhalle', 'Büdchen', 'Späti', 'Minimarkt', 'Verkaufshalle'], 'Duisburg')
df.to_excel("/Users/paulgies/Desktop/Arbeit PoWi/Sonstiges/kiosk_duisburg.xlsx")


