import pandas as pd
import requests
from datetime import datetime, timezone
from config import EOD_API_KEY, get_pg_connection

EXCHANGE_CODE = "PA"  # Euronext Paris

def insert_data(df):
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                # Créer la table si elle n'existe pas
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tInstruments (
                        code TEXT PRIMARY KEY,
                        name TEXT,
                        country TEXT,
                        currency TEXT,
                        exchange TEXT,
                        type TEXT,
                        isin TEXT
                    );
                """)
                conn.commit()

                # Vider la table avant insertion
                print("🧹 Suppression du contenu existant de la table tInstruments...")
                cur.execute("DELETE FROM tInstruments;")
                conn.commit()

                # Insertion ligne par ligne avec affichage
                for _, row in df.iterrows():
                    print("📌 Enregistrement en cours d'insertion :")
                    print(row.to_dict())

                    cur.execute("""
                        INSERT INTO tInstruments (code, name, country, currency, exchange, type, isin)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (code) DO UPDATE SET
                            name = EXCLUDED.name,
                            country = EXCLUDED.country,
                            currency = EXCLUDED.currency,
                            exchange = EXCLUDED.exchange,
                            type = EXCLUDED.type,
                            isin = EXCLUDED.isin;
                    """, (
                        row.get("Code"),
                        row.get("Name"),
                        row.get("Country"),
                        row.get("Currency"),
                        row.get("Exchange"),
                        row.get("Type"),
                        row.get("Isin")
                    ))

                conn.commit()
                print("✅ Données insérées dans la table tInstruments.")

    except Exception as e:
        print(f"❌ Erreur d'insertion : {e}")

    # Sauvegarde CSV
    filename = f"datas/instruments_{EXCHANGE_CODE}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False, encoding='utf-8', sep=';')
    print(f"📄 Données également sauvegardées dans le fichier : {filename}")

def telecharger_et_enregistrer():
    if not EOD_API_KEY:
        raise ValueError("La variable d'environnement EOD_API_KEY n'est pas définie.")

    pd.set_option('display.max_rows', 100)

    print("📥 Téléchargement de la liste des instruments depuis EOD Historical Data...")

    url = f"https://eodhistoricaldata.com/api/exchange-symbol-list/{EXCHANGE_CODE}?api_token={EOD_API_KEY}&fmt=json"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)

    print(f"Nombre d'instruments téléchargés : {len(df)}")
    print(df.head(5000))  # Affiche les 5000 premières lignes

    # Colonnes à conserver
    colonnes_utiles = ["Code", "Name", "Country", "Exchange", "Currency", "Type", "Isin"]
    df = df.loc[:, df.columns.intersection(colonnes_utiles)]

    # Limiter à 5000 instruments seulement
    df = df.head(5000)

    # Ajout d'un timestamp (non inséré, mais utile en CSV ou debug)
    df["download_date"] = datetime.now(timezone.utc)

    # Insertion en base et sauvegarde
    insert_data(df)

if __name__ == "__main__":
    telecharger_et_enregistrer()
