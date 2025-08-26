import unittest
import sys
import os

# Ajouter les chemins
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.detecter_anomalies import detecter_valeurs_nulles

class TestDetecterAnomalies(unittest.TestCase):
    def test_valeurs_nulles_connues(self):
        table = "cours"
        colonnes = ["date", "close", "volume"]
        try:
            detecter_valeurs_nulles(table, colonnes)
        except Exception as e:
            self.fail(f"Erreur lors de la détection des NULLs : {e}")

if __name__ == '__main__':
    unittest.main()
