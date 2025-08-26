"""
Script pour vérifier les colonnes essentielles dans les tables de la base PostgreSQL 'pea_db'.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.db import get_connection
from utils.log_anomalies import log_anomalie


def verifier_colonnes(table_name, colonnes_attendues):
    """
    Vérifie la présence des colonnes attendues dans une table.
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table_name,))
            colonnes_existantes = [row[0] for row in cur.fetchall()]
        manquantes = [col for col in colonnes_attendues if col not in colonnes_existantes]
        if manquantes:
            log_anomalie(table_name, f"Colonnes manquantes : {manquantes}")
        return manquantes

if __name__ == "__main__":
    table = "cours"
    colonnes_attendues = ["date", "close", "isin"]
    manquantes = verifier_colonnes(table, colonnes_attendues)
    print(f"Colonnes manquantes : {manquantes}")
