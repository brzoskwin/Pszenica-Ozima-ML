# Skrypt do pobierania danych pogodowych z Open-Meteo API
# Dane dzienne dla województw Polski (2016–2020) zagregowane potem do danych w ujęciu rocznym
# Zawiera obliczenia wskaźnika hydrotermicznego (Sielianinowa), dni upałów i mrozów w ujęciu rocznym
# Wyniki zapisane w data/raw

import requests
import pandas as pd
import time

# Konfiguracja API i ścieżek
BAZOWY_URL = "https://archive-api.open-meteo.com/v1/archive"
PLIK_WYJSCIOWY = "../data/raw/dane_pogodowe.csv"
OPÓŹNIENIE_ZAPYTANIA = 2  # Opóźnienie między zapytaniami (sekundy)

# Współrzędne geograficzne stolic województw (na potrzeby uproszczenia)
WOJEWODZTWA = {
    "DOLNOŚLĄSKIE": (51.1079, 17.0385),
    "KUJAWSKO-POMORSKIE": (53.0333, 18.5833),
    "LUBELSKIE": (51.2465, 22.5684),
    "LUBUSKIE": (52.4615, 15.5828),
    "ŁÓDZKIE": (51.7592, 19.4550),
    "MAŁOPOLSKIE": (50.0647, 19.9450),
    "MAZOWIECKIE": (52.2318, 21.0064),
    "OPOLSKIE": (50.6711, 17.9263),
    "PODKARPACKIE": (50.0371, 22.0049),
    "PODLASKIE": (53.1325, 23.1688),
    "POMORSKIE": (54.3520, 18.6466),
    "ŚLĄSKIE": (50.2649, 19.0238),
    "ŚWIĘTOKRZYSKIE": (50.8747, 20.9753),
    "WARMIŃSKO-MAZURSKIE": (53.7784, 20.4802),
    "WIELKOPOLSKIE": (52.4007, 16.8993),
    "ZACHODNIOPOMORSKIE": (53.4285, 14.5528),
}


# Pobiera dane dzienne z API Open-Meteo dla wybranego województwa i roku
def pobierz_dane_pogodowe(wojewodztwo, szer_geo, dl_geo, rok):
    print(f"Pobieranie danych dziennych dla {wojewodztwo}, rok {rok}...")
    parametry = {
        "latitude": szer_geo,
        "longitude": dl_geo,
        "start_date": f"{rok}-01-01",
        "end_date": f"{rok}-12-31",
        "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Europe/Warsaw",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
    }

    try:
        odpowiedź = requests.get(BAZOWY_URL, params=parametry)
        odpowiedź.raise_for_status()
        dane = odpowiedź.json()["daily"]
        return pd.DataFrame(dane).assign(wojewodztwo=wojewodztwo, rok=rok)
    except Exception as e:
        print(f"Błąd dla {wojewodztwo}, rok {rok}: {str(e)}")
        return None


# Oblicza wskaźniki agro-meteorologiczne na podstawie dziennych danych
def przelicz_wskaźniki_agro(dane_dzienne):
    # Obliczenie wskaźnika hydrotermicznego (K) na poziomie miesięcznym
    dane_dzienne["miesiąc"] = pd.to_datetime(dane_dzienne["time"]).dt.month
    # Wskaźnik hydrotermiczny
    dane_miesięczne = dane_dzienne.groupby(["wojewodztwo", "rok", "miesiąc"]).agg({
        "temperature_2m_mean": "mean",
        "precipitation_sum": "sum",
        "temperature_2m_max": "max",
        "temperature_2m_min": "min"
    }).reset_index()
    dane_miesięczne["wskaźnik_hydrotermiczny"] = (
        dane_miesięczne["precipitation_sum"] / (dane_miesięczne["temperature_2m_mean"] * 10)
    ).fillna(0).round(2)

    dane_dzienne["dni_upały"] = (dane_dzienne["temperature_2m_max"] > 30).astype(int)

    # Liczenie faktycznych dni mrozu zgodnie z metodologią klimatologiczną (IMGW):
    # IMGW definiuje "dzień mroźny" jako dzień, w którym MAKSYMALNA dobowa temperatura nie przekracza 0°C (Tmax < 0)

    dane_dzienne["dni_mrozy"] = (dane_dzienne["temperature_2m_max"] < 0).astype(int)

    # Agregacja po województwie i roku
    zagregowane = dane_dzienne.groupby(["wojewodztwo", "rok"]).agg({
        "temperature_2m_mean": "mean",
        "precipitation_sum": "sum",
        "dni_upały": "sum",
        "dni_mrozy": "sum"
    }).reset_index()

    zagregowane.rename(columns={
        "temperature_2m_mean": "średnia_temp_roczna",
        "precipitation_sum": "suma_opadów",
    }, inplace=True)

    return zagregowane

# Wyjaśnienie:
# Wcześniej zliczano dni, w których TMIN < 0°C, co dawało 60–120 dni mrozu rocznie (zawyżone, bo kilkadziesiąt razy w roku przymrozek może być tylko nocą mimo ciepłego dnia).
# Teraz liczone są wyłącznie dni, w których Tmax < 0°C (całodobowy mróz, tzw. "dzień mroźny" wg IMGW).
# Taka definicja jest standardem w klimatologii i umożliwia porównywanie wyników z oficjalnymi danymi meteorologicznymi. W polskim klimacie takie dni występują rzadziej, a ich liczba pokrywa się z raportami IMGW.



# Pobiera dane dla wszystkich województw i agreguje wskaźniki roczne
def pobierz_dane_dla_wszystkich(lata):
    wszystkie_dane_dzienne = []
    for wojewodztwo, (szer_geo, dl_geo) in WOJEWODZTWA.items():
        for rok in lata:
            dane = pobierz_dane_pogodowe(wojewodztwo, szer_geo, dl_geo, rok)
            if dane is not None:
                wszystkie_dane_dzienne.append(dane)
            time.sleep(OPÓŹNIENIE_ZAPYTANIA)

    # Łączenie wszystkich danych dziennych
    ramka_danych_dzienne = pd.concat(wszystkie_dane_dzienne, ignore_index=True)

    # Obliczanie wskaźników agro-meteorologicznych
    ramka_wskaźników = przelicz_wskaźniki_agro(ramka_danych_dzienne)

    # Zapis danych do pliku CSV
    ramka_wskaźników.to_csv(PLIK_WYJSCIOWY, index=False)
    print(f"Dane pogodowe zapisano w: {PLIK_WYJSCIOWY}")
    return ramka_wskaźników


# Wywołanie głównej funkcji
if __name__ == "__main__":
    ZAKRES_LAT = list(range(2016, 2021))  # Lata 2016-2020
    dane_pogodowe_df = pobierz_dane_dla_wszystkich(ZAKRES_LAT)
    print("\nPodgląd danych pogodowych:")
    print(dane_pogodowe_df.head())
