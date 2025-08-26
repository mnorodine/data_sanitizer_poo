import os
import requests
import psycopg
import csv
import time
from config import get_pg_connection, EOD_API_KEY

OPENFIGI_API_KEY = os.getenv("OPENFIGI_API_KEY", "")
OPENFIGI_URL = 'https://api.openfigi.com/v3/mapping'
CSV_REPORT = 'mise_a_jour_tickers.csv'

STATUS_SEUIL = 8
BATCH_SIZE = 20  # Commit toutes les 20 mises à jour


def get_ticker_from_isin(isin: str):
    headers = {
        'Content-Type': 'application/json',
        'X-OPENFIGI-APIKEY': OPENFIGI_API_KEY
    }
    payload = [{"idType": "ID_ISIN", "idValue": isin}]

    try:
        response = requests.post(OPENFIGI_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        if data and isinstance(data, list) and data[0].get('data'):
            ticker = data[0]['data'][0].get('ticker')
            return ticker, 'OK' if ticker else 'Ticker non trouvé'
        else:
            return None, 'Aucun résultat'
    except requests.exceptions.RequestException as req_err:
        return None, f'Erreur API : {req_err}'
    except Exception as e:
        return None, f'Erreur inattendue : {e}'


def main():
    if not OPENFIGI_API_KEY:
        raise ValueError("Clé API OpenFIGI manquante. Vérifie OPENFIGI_API_KEY dans les variables d'environnement.")

    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:

                cur.execute("SELECT isin FROM tInstruments WHERE isin IS NOT NULL AND status > %s;", (STATUS_SEUIL,))
                isins = cur.fetchall()

                if not isins:
                    print("Aucun ISIN à traiter.")
                    return

                rows_updated = 0

                with open(CSV_REPORT, mode='w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['isin', 'ticker', 'statut'])

                    update_sql = "UPDATE tInstruments SET ticker = %s WHERE isin = %s AND status > %s"

                    for idx, row in enumerate(isins, start=1):
                        isin = row[0]
                        ticker, statut = get_ticker_from_isin(isin)

                        if ticker:
                            try:
                                cur.execute(update_sql, (ticker, isin, STATUS_SEUIL))
                                rows_updated += 1
                                statut = "OK"
                            except Exception as db_err:
                                statut = f"Erreur DB : {db_err}"

                        writer.writerow([isin, ticker or '', statut])
                        print(f"{rows_updated} - {isin} => {ticker or 'N/A'} [{statut}]")

                        # Commit par lot
                        if rows_updated % BATCH_SIZE == 0:
                            conn.commit()
                            print(f">>> {rows_updated} lignes validées.")

                        time.sleep(0.5)  # Limite API

                    if rows_updated % BATCH_SIZE != 0:
                        conn.commit()
                        print(f">>> Dernières modifications validées ({rows_updated} total).")

    except Exception as e:
        print("Erreur générale :", e)


if __name__ == '__main__':
    main()
