import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.corriger_formats import corriger_dates
from utils.db import get_connection

class TestCorrigerFormats(unittest.TestCase):
    def setUp(self):
        # Réinitialiser les données avant chaque test
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE temp_test SET date_insertion = '2024-01-01' WHERE nom = 'A';")
                cur.execute("UPDATE temp_test SET date_insertion = '2024-01-02' WHERE nom = 'B';")
                cur.execute("UPDATE temp_test SET date_insertion = 'erreur_date' WHERE nom = 'C';")
                cur.execute("UPDATE temp_test SET date_insertion = NULL WHERE nom = 'D';")
                conn.commit()

    def test_corriger_dates(self):
        corriger_dates("temp_test", "date_insertion")
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM temp_test WHERE date_insertion = '2024-01-01'")
                count_ok = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM temp_test WHERE date_insertion = 'erreur_date'")
                count_erreur = cur.fetchone()[0]
        self.assertEqual(count_ok, 1)
        self.assertEqual(count_erreur, 1)  # non corrigé

if __name__ == "__main__":
    unittest.main()
