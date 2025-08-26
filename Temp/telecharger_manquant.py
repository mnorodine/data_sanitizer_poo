"""
Script pour tenter de récupérer les données manquantes via l'API EOD.
(Nécessite une clé dans la variable d'environnement EOD_API_KEY)
"""

import os

def telecharger_donnees(isin):
    api_key = os.getenv("EOD_API_KEY")
    if not api_key:
        print("Erreur : EOD_API_KEY non défini.")
        return

    # TODO : Implémenter la récupération via API
    print(f"Téléchargement simulé des données pour ISIN : {isin} avec API key {api_key[:6]}...")

if __name__ == "__main__":
    telecharger_donnees("FR0000120073")  # Exemple : Air Liquide
