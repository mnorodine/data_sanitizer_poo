import os

# Structure du projet
structure = {
    "scripts": [
        "verifier_colonnes.py",
        "detecter_anomalies.py",
        "telecharger_manquant.py",
        "corriger_formats.py"
    ],
    "utils": ["log_anomalies.py"],
    "tests": [],
    "logs": [],
    ".": ["README.md"]
}

# Création des dossiers et fichiers
for folder, files in structure.items():
    os.makedirs(folder, exist_ok=True)
    for file in files:
        path = os.path.join(folder, file)
        if not os.path.exists(path):
            with open(path, "w") as f:
                if file == "README.md":
                    f.write("""\
# Projet `data_sanitizer`

Ce projet a pour but de détecter, diagnostiquer et corriger les problèmes de **qualité des données** dans le projet `analyse_pea`.

Il permet notamment :
- de repérer les colonnes manquantes ou mal formatées (`verifier_colonnes.py`) ;
- de détecter les lignes ou valeurs suspectes (`detecter_anomalies.py`) ;
- de récupérer les données manquantes si possible (`telecharger_manquant.py`) ;
- de corriger automatiquement les formats problématiques (`corriger_formats.py`) ;
- de consigner toutes les anomalies dans des fichiers de log (`utils/log_anomalies.py`).

Ce projet est **autonome** mais conçu pour être utilisé en complément du pipeline principal.
""")
                else:
                    f.write(f"# {file}\n")
print("✅ Structure du projet `data_sanitizer/` créée.")

