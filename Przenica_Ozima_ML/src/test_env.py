import os
import requests

#Mój wygenerowany klucz indywidualny API został dodany do zmiennej środowiskowej
#Projekt zostanie udostępniony do oceny z pobranymi już surowymi danymi w folderze "raw"

# Pobranie klucza API GUS
api_key = os.getenv("GUS_API_KEY")

if api_key:
    try:
        response = requests.get("https://bdl.stat.gov.pl/api/v1/subjects", headers={"X-ClientId": api_key})
        if response.status_code == 200:
            print("Połączenie z API GUS działa poprawnie!")
        else:
            print(f"Błąd połączenia z API GUS. Kod statusu: {response.status_code}")
    except Exception as e:
        print(f"Wystąpił błąd: {e}")
else:
    print("Brak klucza API GUS w zmiennych środowiskowych!")
