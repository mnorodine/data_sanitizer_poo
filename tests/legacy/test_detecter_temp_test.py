import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.detecter_anomalies import detecter_valeurs_nulles

class TestDetecterTempTest(unittest.TestCase):
    def test_nulls_in_temp_test(self):
        table = "temp_test"
        colonnes = ["valeur", "date_insertion"]
        try:
            detecter_valeurs_nulles(table, colonnes)
        except Exception as e:
            self.fail(f"Erreur pendant la détection des NULLs dans temp_test : {e}")

if __name__ == '__main__':
    unittest.main()
