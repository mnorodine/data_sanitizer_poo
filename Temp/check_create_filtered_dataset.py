# scripts/check_create_filtered_dataset.py

import os
import sys
import traceback

# Adapter le chemin vers le projet Analyse_pea si nécessaire
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../analyse_pea")))

# Import de la fonction de traitement depuis Analyse_pea
from scripts.ia.preparer_dataset import create_filtered_dataset

# Liste des tickers à tester
TICKERS_TEST = ["SAN", "TTE", "AF", "AI", "CAC"]

# TICKERS_TEST = ["BNP", "AIRLIQUIDE", "SANOFI", "TOTAL", "AIRFRANCE", "CAC40"]

# Colonnes indispensables dans le dataset final
COLONNES_REQUISES = ["Date", "close", "ticker"]

def verifier_structure_dataframe(df, ticker):
    erreurs = []
    
    if df.index.name == "Date" or df.index.name is not None:
        df = df.reset_index()

    for col in COLONNES_REQUISES:
        if col not in df.columns:
            erreurs.append(f"{ticker} → colonne manquante : {col}")

    return erreurs, df

def main():
    os.makedirs("outputs", exist_ok=True)
    log_path = "outputs/verif_create_filtered_dataset.log"
    with open(log_path, "w") as f_log:
        for ticker in TICKERS_TEST:
            f_log.write(f"\n🔍 Vérification de {ticker}...\n")
            try:
                df = create_filtered_dataset(ticker)
                erreurs, df_checked = verifier_structure_dataframe(df, ticker)

                if erreurs:
                    for err in erreurs:
                        f_log.write(f"❌ {err}\n")
                else:
                    f_log.write("✅ Structure OK\n")

            except Exception as e:
                f_log.write(f"❌ Exception pour {ticker} : {str(e)}\n")
                f_log.write(traceback.format_exc())

    print(f"📝 Rapport généré dans : {log_path}")

if __name__ == "__main__":
    main()
