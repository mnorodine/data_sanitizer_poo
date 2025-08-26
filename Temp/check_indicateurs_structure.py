# scripts/check_indicateurs_structure.py

import psycopg
import pandas as pd
import os
import sys
import argparse
from datetime import datetime

# Paramètres attendus dans chaque ligne d'indicateurs
COLONNES_OBLIGATOIRES = ["date", "close", "sma_10", "sma_20", "rsi_14", "macd", "macd_signal", "tenkan_sen", "kijun_sen"]
SEUIL_LIGNES_MIN = 100

def get_connexion():
    return psycopg.connect(
        dbname="pea_db",
        user="pea_user",
        host="localhost",
        autocommit=True
    )

def lister_tickers_selectionnes():
    with get_connexion() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ticker FROM instruments WHERE ia_selectionne = true ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]

def verifier_indicateurs(ticker, conn):
    try:
        df = pd.read_sql(f"SELECT * FROM indicateurs_techniques WHERE ticker = %s ORDER BY date", conn, params=(ticker,))
    except Exception as e:
        return [f"{ticker} → erreur SQL : {str(e)}"]

    erreurs = []

    if df.empty:
        erreurs.append(f"{ticker} → 0 ligne")
        return erreurs

    if len(df) < SEUIL_LIGNES_MIN:
        erreurs.append(f"{ticker} → seulement {len(df)} lignes")

    for col in COLONNES_OBLIGATOIRES:
        if col not in df.columns:
            erreurs.append(f"{ticker} → colonne manquante : {col}")

    return erreurs

def desactiver_ticker(conn, ticker):
    with conn.cursor() as cur:
        cur.execute("UPDATE instruments SET ia_selectionne = false WHERE ticker = %s", (ticker,))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-disable", action="store_true", help="Désactive automatiquement les tickers invalides dans la base")
    args = parser.parse_args()

    os.makedirs("outputs", exist_ok=True)
    log_path = "outputs/tickers_invalides_indicateurs.log"

    tickers_invalides = []
    with get_connexion() as conn, open(log_path, "w") as log_file:
        tickers = lister_tickers_selectionnes()

        for ticker in tickers:
            erreurs = verifier_indicateurs(ticker, conn)

            if erreurs:
                for err in erreurs:
                    log_file.write(f"{err}\n")
                tickers_invalides.append(ticker)

                if args.auto_disable:
                    desactiver_ticker(conn, ticker)
                    log_file.write(f"{ticker} → désactivé dans instruments\n")

    print(f"📝 Rapport généré : {log_path}")
    if tickers_invalides:
        print(f"⚠️ Tickers invalides détectés : {len(tickers_invalides)}")
    else:
        print("✅ Tous les tickers actifs sont exploitables.")

if __name__ == "__main__":
    main()
