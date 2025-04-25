# Skrypt do pobierania danych o plonach pszenicy ozimej i nawożeniu NPK: azot (N), fosfor (P), i potas (K) z API GUS
# API wygenerowano na stronie: https://api.stat.gov.pl/Home/BdlApi
# Dane będą wykorzystane do analizy efektywności rolnictwa i modelowania plonów w zależności od warunków zewnętrznych
# Wyniki zapisane w data/raw

# Plony pszenicy ozimej:
# - Zmienna ID: 4332
# - Jednostki: decytony na hektar (dt/ha)

# Nawożenie NPK:
# - Nawozy mineralne ogółem (ID: 410946) - kilogramy na hektar (kg/ha)
# - Azotowe (N) (ID: 410951) - kilogramy na hektar (kg/ha)
# - Fosforowe (P) (ID: 410956) - kilogramy na hektar (kg/ha)
# - Potasowe (K) (ID: 410958) - kilogramy na hektar (kg/ha)

import requests
import pandas as pd
import os

# Konfiguracja klucza i ścieżek
KLUCZ_API = "72edbacd-ca75-43a1-7535-08dd7b24e507"
BAZOWY_URL = "https://bdl.stat.gov.pl/api/v1"
NAGLOWKI = {"X-ClientId": KLUCZ_API}
SCIEZKA_DANYCH = "../data/raw"
CSV_PLONY = os.path.join(SCIEZKA_DANYCH, "plony_pszenicy.csv")
CSV_NAWOZY = os.path.join(SCIEZKA_DANYCH, "nawozenie_npk.csv")
CSV_CENY_PSZENICY = os.path.join(SCIEZKA_DANYCH, "ceny_pszenicy.csv")



def pobierz_dane_roczne(id_zmiennej, rok):
    # Pobiera dane z API GUS dla wybranego roku i zmiennej
    url = f"{BAZOWY_URL}/data/by-variable/{id_zmiennej}"
    parametry = {
        "format": "json",
        "unit-level": 2,  # Poziom województw
        "year": rok
    }
    odpowiedz = requests.get(url, headers=NAGLOWKI, params=parametry)
    odpowiedz.raise_for_status()
    return odpowiedz.json()


def konwertuj_do_tabeli(dane, nazwa_kolumny):
    # Konwertuje odpowiedź JSON na tabelaryczne dane pandas DataFrame
    rekordy = []
    for element in dane.get("results", []):
        wojewodztwo = element.get("name")
        woj_id = element.get("id")
        for wartosc in element.get("values", []):
            rekordy.append({
                "id": woj_id,
                "wojewodztwo": wojewodztwo,
                "rok": wartosc["year"],
                nazwa_kolumny: wartosc["val"]
            })
    return pd.DataFrame(rekordy)


def pobierz_dane(id_zmiennej, lata, nazwa_wartosci):
    # Pobiera dane z podanego zakresu lat dla wskazanej zmiennej
    wszystkie_lata = []
    for rok in lata:
        print(f"Pobieranie {nazwa_wartosci} dla {rok}...")
        dane = pobierz_dane_roczne(id_zmiennej, rok)
        df = konwertuj_do_tabeli(dane, nazwa_wartosci)
        wszystkie_lata.append(df)
    return pd.concat(wszystkie_lata, ignore_index=True)


def pobierz_nawozenie_npk(lata):
    # Pobiera dane o nawożeniu NPK dla azotu, fosforu i potasu
    zmienne = {
        "nawozy_ogolem_kg_ha": "410946",
        "N_kg_ha": "410951",
        "P_kg_ha": "410956",
        "K_kg_ha": "410958"
    }

    ramki = []
    for nazwa, id_zm in zmienne.items():
        print(f"Pobieranie {nazwa}...")
        df = pobierz_dane(id_zm, lata, nazwa)
        ramki.append(df)

    # Scalanie danych
    polaczona = ramki[0]
    for df in ramki[1:]:
        polaczona = pd.merge(
            polaczona,
            df[["id", "wojewodztwo", "rok", df.columns[-1]]],
            on=["id", "wojewodztwo", "rok"],
            how="left"
        )
    return polaczona


# Funkcja do pobierania cen pszenicy - wykorzystuje ID 4859 widoczne w zrzucie ekranu
def pobierz_ceny_pszenicy(lata):
    # Pobiera ceny skupu pszenicy (zł/dt) z GUS dla wszystkich województw
    ID_CENY_PSZENICY = "4859"

    print("Pobieranie rocznych cen skupu pszenicy...")
    wszystkie_dane = []

    for rok in lata:
        print(f"Pobieranie cen pszenicy dla {rok}...")
        dane = pobierz_dane_roczne(ID_CENY_PSZENICY, rok)
        df = konwertuj_do_tabeli(dane, "cena_pszenicy_dt")
        wszystkie_dane.append(df)

    df_ceny = pd.concat(wszystkie_dane, ignore_index=True)

    # Konwersja ceny z zł/dt na zł/t (1 tona = 10 dt)
    df_ceny["cena_pszenicy_t"] = df_ceny["cena_pszenicy_dt"] * 10

    return df_ceny


if __name__ == "__main__":
    # Zakres lat do pobrania: 2016-2020
    LATA = list(range(2016, 2021))

    # Pobieranie danych o plonach pszenicy
    print("===== ROZPOCZYNAM POBRANIE DANYCH O PLONACH =====")
    df_plony = pobierz_dane("4332", LATA, "plony_dt_ha")
    df_plony.to_csv(CSV_PLONY, index=False)

    # Pobieranie danych o nawożeniu NPK
    print("\n===== ROZPOCZYNAM POBRANIE DANYCH O NAWOŻENIU =====")
    df_nawozy = pobierz_nawozenie_npk(LATA)
    df_nawozy.to_csv(CSV_NAWOZY, index=False)

    # Pobieranie cen pszenicy - wykorzystaj ID 4859 z tabeli GUS
    print("\n===== ROZPOCZYNAM POBRANIE CEN PSZENICY =====")
    df_ceny = pobierz_ceny_pszenicy(LATA)
    df_ceny.to_csv(CSV_CENY_PSZENICY, index=False)

    # Podgląd wyników
    print("\nPodgląd danych o plonach:")
    print(df_plony.head())
    print("\nPodgląd danych o nawożeniu:")
    print(df_nawozy.head())
    print("\nPodgląd danych o cenach pszenicy:")
    print(df_ceny.head())
