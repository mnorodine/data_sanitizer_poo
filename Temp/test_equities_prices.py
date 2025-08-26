import psycopg
import yfinance as yf

def insert_equities_prices(ticker, isin, db_conninfo, start_date="2022-01-01"):
    # Télécharger les données
    df = yf.download(ticker, start=start_date, progress=False)

    # Préparer les données à insérer
    data_to_insert = []
    for date, row in df.iterrows():
        data_to_insert.append((
            isin,
            ticker,
            date.date(),
            row['Open'],
            row['Close'],
            row['High'],
            row['Low'],
            int(row['Volume'])
        ))

    # Connexion à la BDD et insertion
    with psycopg.connect(db_conninfo) as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO equities_prices
                (isin, symbol, price_date, open_price, close_price, high_price, low_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (isin, symbol, price_date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    close_price = EXCLUDED.close_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    volume = EXCLUDED.volume
            """, data_to_insert)
        conn.commit()

    print(f"{len(data_to_insert)} lignes insérées / mises à jour pour {ticker}")

# Exemple d’appel
if __name__ == "__main__":
    db_conninfo = "dbname=ma_db user=mon_user password=mon_pass host=localhost"
    insert_equities_prices("AAPL", "US0378331005", db_conninfo)
