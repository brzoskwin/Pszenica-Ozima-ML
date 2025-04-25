# Skrypt do przetwarzania danych o cenach nawozów z https://gielda-rolna.com/news/pokaz/12/ceny-nawozow-sztucznych-2015-2018
# Dane zostały wprowadzone do arkusza Excel za pomocą importu ze strony Web
# Uproszczenie oraz wyczyszenie danych odbyło się za pomocą PowerQuery
# Ceny nawozów zostaną użyte do analizy wydajności nawożenia względem finalnych zbiorów pszenicy ozimej
# Wyniki zapisane w data/raw
import pandas as pd
import os

# Ścieżki do katalogu i plików
RAW_DATA_PATH = "../data/raw"
EXCEL_PATH = os.path.join(RAW_DATA_PATH, "ceny_nawozow.xlsx")
CSV_PATH = os.path.join(RAW_DATA_PATH, "ceny_nawozow.csv")
CSV_PIERWIASTKI_PATH = os.path.join(RAW_DATA_PATH, "ceny_nawozow_pierwiastki.csv")
CSV_UZUPELNIONE_PATH = os.path.join(RAW_DATA_PATH, "ceny_nawozow_pierwiastki_uzupelnione.csv")

# Mapowanie skrótów województw na pełne nazwy zgodne z GUS
WOJ_MAPPING = {
    'dol': 'DOLNOŚLĄSKIE',
    'k.pom': 'KUJAWSKO-POMORSKIE',
    'lube': 'LUBELSKIE',
    'lubs': 'LUBUSKIE',
    'łódz': 'ŁÓDZKIE',
    'mał': 'MAŁOPOLSKIE',
    'maz': 'MAZOWIECKIE',
    'opol': 'OPOLSKIE',
    'podk': 'PODKARPACKIE',
    'podl': 'PODLASKIE',
    'pom': 'POMORSKIE',
    'ślą': 'ŚLĄSKIE',
    'świę': 'ŚWIĘTOKRZYSKIE',
    'w.maz': 'WARMIŃSKO-MAZURSKIE',
    'wiel': 'WIELKOPOLSKIE',
    'zach': 'ZACHODNIOPOMORSKIE',
    'śred': 'ŚREDNIA'
}

# Parametry przeliczników nawozowych
SKLADNIKI = {
    'saletra amonowa': {'skladnik': 'N', 'procent': 0.34},
    'saletrzak': {'skladnik': 'N', 'procent': 0.27},
    'mocznik': {'skladnik': 'N', 'procent': 0.46},
    'superfosfat poj. gran': {'skladnik': 'P', 'procent': 0.18 * 0.436},
    'superfosfat wzb 40%': {'skladnik': 'P', 'procent': 0.40 * 0.436},
    'sól potasowa': {'skladnik': 'K', 'procent': 0.60 * 0.830},
    'siarczan potasu': {'skladnik': 'K', 'procent': 0.50 * 0.830}
}


def znajdz_bloki(df):
    # Zwraca indeksy początkowe bloków danych na podstawie nagłówków 'Województwo'
    return df.index[df.iloc[:, 0].astype(str).str.startswith("Województwo")].tolist()


def wytnij_bloki(df):
    # Dzieli cały arkusz na listę bloków (każdy rok × kategoria)
    bloki = []
    idxs = znajdz_bloki(df)
    idxs.append(len(df))  # Dodanie końcówki, w celu wycięcia ostatniego bloku
    for i in range(len(idxs) - 1):
        bloki.append(df.iloc[idxs[i]:idxs[i + 1]])
    return bloki


def parse_blok(blok):
    # Przetwarza pojedynczy blok:
    blok = blok.reset_index(drop=True)
    woj_row = blok.iloc[0]  # Pierwszy wiersz: nagłówki województw (skróty)
    woj_skr = woj_row[1:].tolist()
    woj_full = [WOJ_MAPPING.get(str(w).strip(), str(w).strip()) for w in woj_skr]  # Mapuje na pełne nazwy
    rok_row = blok.iloc[1]  # Drugi wiersz: "Rok" + wartości roku
    rok = int([x for x in rok_row[1:] if pd.notnull(x)][0])  # Pierwszy napotkany niepusty (powinien być taki sam dla całej tabeli)
    kategoria = None  # Przechowywana aktualnie kategoria
    data = []
    for i in range(2, len(blok)):
        first = str(blok.iloc[i, 0]).strip()
        # Jeśli wiersz zaczyna się od 'Nawozy', zmienia kategorię
        if first.lower().startswith("nawozy"):
            kategoria = blok.iloc[i, 0]
            continue
        # Jeśli to dane o nawozie (wiersz niepusty, nie "średnia")
        if kategoria and first and not first.lower().startswith("średnia"):
            for j, cena in enumerate(blok.iloc[i, 1:]):
                if pd.notnull(cena):
                    try:
                        cena_f = float(cena)
                    except:
                        continue
                    # Pomijaj uśrednienie, cena > 0
                    if woj_full[j] != 'ŚREDNIA' and cena_f > 0:
                        data.append({
                            "rok": rok,
                            "kategoria": kategoria,
                            "nawóz": first,
                            "wojewodztwo": woj_full[j],
                            "cena_pln_t": cena_f
                        })
    return data


def przetworz_excel(excel_path):
    # Przetwarzanie całego arkusza Excel: dzielenie na bloki i łączenie wszystkiego w DataFrame
    df = pd.read_excel(excel_path, header=None)
    bloki = wytnij_bloki(df)
    all_data = []
    for blok in bloki:
        all_data.extend(parse_blok(blok))
    return pd.DataFrame(all_data)


def save_to_csv(df, path):
    # Zapis DataFrame do CSV
    df.to_csv(path, index=False)
    print(f"Dane zapisano w: {path}")


def przelicz_czysty_skladnik(row):
    nazwa = row["nawóz"].strip().lower()
    if nazwa in SKLADNIKI:
        procent = SKLADNIKI[nazwa]['procent']
        skladnik = SKLADNIKI[nazwa]['skladnik']
        cena_kg = row["cena_pln_t"] / 1000
        cena_czysty = cena_kg / procent if procent > 0 else None
        return pd.Series([skladnik, cena_czysty])
    return pd.Series([None, None])

#Poniżej znajduje się metoda uzupełniające dane o lata 2019 i 2020 w celu uzyskania jednolitego przedziału czasowego dla wszystkich danych
# Użyto ekstrapolacji (zamiast interpolacji), ponieważ brakujące lata znajdują się na krańcu przedziału czasowego
# Metoda: Dla każdej kombinacji (nawóz, województwo, składnik) obliczono roczną zmianę cen z ostatnich 2 dostępnych lat i zastosowano ten sam trend dla prognozy kolejnych lat
def dopasuj_brakujace_lata(df, lata_docelowe):
    # Generuj pełną sekwencję lat dla wszystkich kombinacji
    grupy = ["nawóz", "wojewodztwo", "skladnik"]

    # Dla każdej grupy oblicza zmianę roczną i ekstrapoluj
    wynik = []
    for (naw, woj, skl), grupa in df.groupby(grupy):
        last_2 = grupa.sort_values("rok").tail(2)
        if len(last_2) < 2:
            continue

        # Oblicza średnią roczną zmianę cen
        delta = (last_2["cena_za_kg_czysty"].iloc[1] - last_2["cena_za_kg_czysty"].iloc[0])
        ostatni_rok = last_2["rok"].max()

        # Dodaje rekordy dla brakujących lat
        for rok in lata_docelowe:
            if rok <= ostatni_rok:
                continue
            prognoza = last_2["cena_za_kg_czysty"].iloc[1] + delta * (rok - ostatni_rok)
            wynik.append({
                "rok": rok,
                "nawóz": naw,
                "wojewodztwo": woj,
                "skladnik": skl,
                "cena_za_kg_czysty": prognoza,
                "kategoria": last_2["kategoria"].iloc[0]
            })

    return pd.concat([df, pd.DataFrame(wynik)], ignore_index=True)

#Wywołanie głównej funkcji
if __name__ == "__main__":
    # Przetwarzanie danych surowych
    df = przetworz_excel(EXCEL_PATH)

    # Przeliczanie na ceny czystych składników
    df[["skladnik", "cena_za_kg_czysty"]] = df.apply(przelicz_czysty_skladnik, axis=1)
    df = df[df["skladnik"].notnull()]

    # Zapis danych podstawowych
    save_to_csv(df, CSV_PIERWIASTKI_PATH)

    # Ekstrapolacja brakujących lat
    df_uzupelnione = dopasuj_brakujace_lata(df, [2019, 2020])

    # Zapis danych końcowych
    save_to_csv(df_uzupelnione, CSV_UZUPELNIONE_PATH)

    print("Przetwarzanie zakończone pomyślnie")
    print(df_uzupelnione.tail())
