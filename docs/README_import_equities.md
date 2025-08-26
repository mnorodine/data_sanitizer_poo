# import_equities.py

## 📌 Description  
Ce script permet d’importer et/ou mettre à jour des données d’actions (*equities*) dans une base de données PostgreSQL.  
Il supporte deux sources de données :  
1. **Un fichier CSV** local (utilisé en phase de développement).  
2. **L’API Web Services d’Euronext** (via une requête POST, accès payant).

L’insertion ou la mise à jour s’effectue uniquement sur **7 colonnes autorisées**, et tout autre champ est ignoré.  

---

## ⚙️ Fonctionnalités principales

- **Source CSV**
  - Lecture d’un fichier au format CSV (`;`, `,`, `|`, tabulation) avec détection automatique du délimiteur.
  - Identification et saut automatique d’une éventuelle ligne d’en-tête.
  - Filtrage des lignes invalides :
    - `isin` ou `symbol` manquant.
    - `symbol` présent dans la **liste d’exclusion** (par défaut : `["-"]`).
    - `last_trade_mic_time` manquant, vide, ou `"-"`.
  - Conversion de `last_trade_mic_time` en **date Python** (`YYYY-MM-DD`) avant insertion.
  - Affichage de statistiques détaillées :
    - Total de lignes lues, lignes ignorées par motif (`PK`, `symbol`, `date`), lignes retenues.

- **Source Euronext** *(option payante)*
  - Connexion à l’API REST (URL et jeton d’authentification via variables d’environnement).
  - Récupération des instruments pour un ou plusieurs marchés donnés.
  - Nettoyage des valeurs (`None` si vide, retrait des espaces).
  - Filtrage des lignes sans `isin`, `symbol` ou `last_trade_mic_time` valide.
  - Conversion de `last_trade_mic_time` en **date Python**.

- **Base de données**
  - Connexion via la fonction `get_pg_connection()` de `scripts.config`.
  - Colonne `last_trade_mic_time` stockée en **type `DATE`** dans PostgreSQL.
  - Insertion ou mise à jour avec la clause `ON CONFLICT` sur la clé `(isin, symbol)`.
  - Limitation stricte aux colonnes autorisées :  
    `isin`, `symbol`, `name`, `market`, `currency`, `last_trade_mic_time`, `time_zone`.

- **Mode simulation** (`--dry-run`)
  - Prépare et affiche les lignes sans les insérer en base.
  - Affiche les statistiques de parsing.

---

## 📂 Structure des données

### Colonnes autorisées
| Nom | Description |
|-----|-------------|
| `isin` | International Securities Identification Number |
| `symbol` | Symbole/ticker de l’action |
| `name` | Nom de l’instrument |
| `market` | Code marché/MIC |
| `currency` | Devise de cotation |
| `last_trade_mic_time` | Date de dernière transaction (type `DATE`) |
| `time_zone` | Fuseau horaire |

### Index de colonnes pour le CSV (`COLUMN_INDICES`)
| Clé interne | Index CSV |
|-------------|-----------|
| `name` | 0 |
| `isin` | 1 |
| `symbol` | 2 |
| `market` | 3 |
| `currency` | 4 |
| `last_trade_mic_time` | 9 |
| `time_zone` | 10 |

---

## 🚀 Utilisation

### 1. Depuis un fichier CSV (phase de développement)
```bash
python -m scripts.import_equities --source csv --csv datas/equities.csv
```

### 2. Depuis Euronext WS *(accès payant)*
```bash
export EURONEXT_WS_BASE="https://votre-endpoint"
export EURONEXT_WS_TOKEN="votre_token"
python -m scripts.import_equities --source euronext --markets XPAR XAMS
```
- `--markets` : liste optionnelle de MICs à filtrer (ex. `XPAR`, `XAMS`, `XBRU`, `XLIS`).

### 3. Mode simulation (aucune écriture en base)
```bash
python -m scripts.import_equities --source csv --dry-run
```

---

## 🔒 Restrictions & règles d’importation

- **Fichier CSV**
  - Lignes ignorées si :
    - `isin` manquant ou vide.
    - `symbol` manquant, vide, ou appartenant à `exclusion_list = ["-"]`.
    - `last_trade_mic_time` manquant, vide, ou `"-"`.
- **Euronext WS**
  - Lignes ignorées si :
    - `isin` manquant.
    - `symbol` manquant.
    - `last_trade_mic_time` invalide.

---

## 🛠 Dépendances

- Python ≥ 3.8
- Bibliothèques :
  - `requests` (pour Euronext WS)
  - `psycopg` (connexion PostgreSQL)
- Variable d’environnement (uniquement pour Euronext) :
  - `EURONEXT_WS_BASE` *(URL API)*
  - `EURONEXT_WS_TOKEN` *(token d’authentification)*

---

## 📜 Licence
Script interne — usage réservé au projet Data_Sanitizer.

## [2025-08-16] Contrat de données & post-import
- Invariants: 1 ISIN ⇒ 1 symbole canonique ⇒ 1 ticker non NULL.
- Le mapping canonique est maintenu par `migrations/2025-08-16_canonicalization.sql`.
- Après import `equities`, exécuter:
