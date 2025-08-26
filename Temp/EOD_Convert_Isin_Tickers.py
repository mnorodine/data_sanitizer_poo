import os
import requests
import psycopg
import csv
import time
import urllib.parse
from config import get_pg_connection, EOD_API_KEY

EOD_SEARCH_URL = "https://eodhistoricaldata.com/api/search/"
CSV_REPORT = 'mise_a_jour_tickers_eod.csv'
BATCH_SIZE = 20


def get_ticker_from_isin(isin: str):
    params = {
        "q": isin,
        "api_token": EOD_API_KEY,
        "limit": 1
    }
    url = f"{EOD_SEARCH_URL}?{urllib.parse.urlencode(params)}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if data and isinstance(data, list) and data[0].get("code"):
            ticker = data[0]["code"]
            return ticker, "OK"
        else:
            return None, "Aucun résultat"
    except requests.exceptions.RequestException as req_err:
        return None, f"Erreur API : {req_err}"
    except Exception as e:
        return None, f"Erreur inattendue : {e}"


def main():
    if not EOD_API_KEY:
        raise ValueError("Clé API EOD manquante. Vérifie EOD_API_KEY dans les variables d'environnement.")

    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                # Requête SQL corrigée selon ta demande
                cur.execute("SELECT isin FROM tInstruments WHERE isin IS NOT NULL AND (ticker < '' OR status >= 9);")
                isins = cur.fetchall()

                if not isins:
                    print("Aucun ISIN à traiter.")
                    return

                rows_updated = 0

                with open(CSV_REPORT, mode='w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['isin', 'ticker', 'statut'])

                    update_sql = "UPDATE tInstruments SET ticker = %s WHERE isin = %s"

                    for idx, row in enumerate(isins, start=1):
                        isin = row[0]
                        ticker, statut = get_ticker_from_isin(isin)

                        if ticker:
                            try:
                                cur.execute(update_sql, (ticker, isin))
                                rows_updated += 1
                                statut = "OK"
                            except Exception as db_err:
                                statut = f"Erreur DB : {db_err}"

                        writer.writerow([isin, ticker or '', statut])
                        print(f"{rows_updated} - {isin} => {ticker or 'N/A'} [{statut}]")

                        if rows_updated % BATCH_SIZE == 0:
                            conn.commit()
                            print(f">>> {rows_updated} lignes validées.")

                        time.sleep(0.5)  # Respect limite API

                    if rows_updated % BATCH_SIZE != 0:
                        conn.commit()
                        print(f">>> Dernières modifications validées ({rows_updated} total).")

    except Exception as e:
        print("Erreur générale :", e)


if __name__ == '__main__':
    main()
