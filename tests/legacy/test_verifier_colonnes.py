import unittest
import sys
import os

# Ajout du chemin vers les scripts
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.verifier_colonnes import verifier_colonnes

class TestVerifierColonnes(unittest.TestCase):
    def test_colonnes_connues(self):
        table = "cours"
        colonnes = ["date", "close"]  # à adapter selon ta BDD
        result = verifier_colonnes(table, colonnes)
        self.assertIsInstance(result, list)
        self.assertTrue(all(isinstance(col, str) for col in result))

if __name__ == '__main__':
    unittest.main()
