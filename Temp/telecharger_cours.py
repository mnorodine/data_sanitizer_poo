import yfinance as yf
import pandas as pd
import psycopg
from config import DB_PARAMS, EOD_API_KEY, get_pg_connection
from datetime import datetime

def get_instruments():
    # Récupérer tous les codes des instruments depuis la table tInstruments
    with get_pg_connection() as conn:
        query = "SELECT code FROM public.tinstruments;"
        df = pd.read_sql_query(query, conn)
    return df['code'].tolist()

def telecharger_cours(instrument_code):
    # Télécharger les données journalières via yfinance pour l'instrument donné
    print(f"📥 Téléchargement des cours journaliers pour l'instrument : {instrument_code}")
    
    # Récupérer l'historique des cours journaliers
    instrument = yf.Ticker(instrument_code)
    data = instrument.history(period="max")  # Période maximale

    # Ajouter le code de l'instrument à chaque ligne
    data['code'] = instrument_code
    data.reset_index(inplace=True)  # Réinitialiser l'index pour avoir une colonne "Date"
    
    # Nous ne gardons que les colonnes nécessaires
    data = data[['code', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']]
    data.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 
                         'Close': 'close', 'Volume': 'volume', 'Adj Close': 'adjusted_close'}, inplace=True)

    return data

def inserer_cours(df):
    # Insérer les cours journaliers dans la table tInstruments_prices
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                # Insertion dans la table tInstruments_prices
                for _, row in df.iterrows():
                    cur.execute("""
                    INSERT INTO tInstruments_prices (code, date, open, high, low, close, volume, adjusted_close)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (code, date) DO NOTHING;
                    """, (row['code'], row['date'], row['open'], row['high'], row['low'], 
                          row['close'], row['volume'], row['adjusted_close']))
                conn.commit()

        print(f"✅ Insertion terminée pour {len(df)} lignes.")
    
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion : {e}")

def telecharger_et_enregistrer():
    # Récupérer tous les codes des instruments dans la table tInstruments
    codes_instruments = get_instruments()

    for code in codes_instruments:
        try:
            # Télécharger les cours journaliers pour cet instrument
            df_cours = telecharger_cours(code)
            
            # Insérer les cours dans la base de données
            inserer_cours(df_cours)
        
        except Exception as e:
            print(f"❌ Erreur pour l'instrument {code} : {e}")

if __name__ == "__main__":
    telecharger_et_enregistrer()
