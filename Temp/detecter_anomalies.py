"""
Script pour détecter les anomalies dans les données (valeurs nulles, doublons, valeurs aberrantes).
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.db import get_connection
from utils.log_anomalies import log_anomalie

def detecter_valeurs_nulles(table, colonnes):

    with get_connection() as conn:
        with conn.cursor() as cur:
            for col in colonnes:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL;")
                count = cur.fetchone()[0]
                if count > 0:
                    log_anomalie(table, f"{count} valeurs NULL dans la colonne '{col}'")

if __name__ == "__main__":
    table = "temp_test"
    colonnes = ["valeur", "date_insertion"]
    detecter_valeurs_nulles(table, colonnes)
