# Couche 2 — Mise à jour des prix depuis Yahoo Finance (yfinance)

Script : `scripts/update_prices_from_yfinance_refined.py`

## Objectif
Pour chaque couple `(isin, symbol)` dans la table `equities`, trouver un ticker Yahoo Finance valide,
puis insérer/mettre à jour l’historique journalier dans `equities_prices` (UPSERT sur `(isin, symbol, price_date)`).
En parallèle, mettre à jour des attributs de gestion dans `equities` (`ticker`, `api`, `is_valid`, `cnt_1y`, `cnt_total`, `w_date`).

## Sélection des lignes
- Le script ne traite que les lignes dont `w_date` est **NULL** ou **strictement inférieure** à `CURRENT_DATE`.
- Option `--claim-before` : réserve la ligne en mettant `w_date = CURRENT_DATE` **avant** de travailler, pour éviter les doublons en parallèle.

## Résolution de ticker
- Suffixes Euronext testés dans cet ordre : `.PA`, `.AS`, `.BR`, `.LS`, `.IR`.
- Si `--allow-mi-proxy` et que `symbol` commence par un chiffre → essai de `symbol + ".MI"` (proxy Milano).
- Si `--no-strict-euronext` → essai du symbole brut (sans suffixe) en dernier recours.
- Un ticker est **valide** s’il dispose d’au moins **10** séances avec `Close` non nul sur 1 an (`period="1y"`).

## Téléchargement & upsert
- Si `--since YYYY-MM-DD` fourni : on télécharge depuis cette date (buffer -1 jour).
- Sinon, si des prix existent déjà : on reprend à partir du dernier `price_date` connu (buffer -1 jour).
- Sinon : `period="max"`.
- Les colonnes attendues : `Open, High, Low, Close, Adj Close, Volume`. Index = dates.
- UPSERT : met à jour en cas de conflit sur `(isin, symbol, price_date)`.

## Mise à jour de `equities`
- `ticker` : le ticker retenu
- `api` : `'yfinance'` si succès ; `NULL` sinon
- `is_valid` : `TRUE` si succès, `FALSE` sinon
- `cnt_1y` : nombre de séances valides sur 1 an (pour le ticker retenu)
- `cnt_total` : nombre total de lignes téléchargées dans le DataFrame de la passe courante
- `w_date` : fixé à `CURRENT_DATE` après tentative (succès ou échec), sauf si pas souhaité.

## Options
```
--since YYYY-MM-DD          Début de téléchargement
--limit N                   Limite le nombre de lignes equities à traiter
--only "SYM1,SYM2"          Restreint aux symboles listés
--sleep 0.2                 Pause entre lignes (anti-rate-limit)
--dry-run                   N’écrit pas dans equities_prices
--touch-wdate-on-dry-run    En dry-run, met quand même w_date=today et met à jour les compteurs
--claim-before              Réserve la ligne (w_date=today) avant traitement
--allow-mi-proxy            Ajoute le suffixe .MI si symbole numérique (Euronext proxy Milano)
--no-strict-euronext        Autorise un essai final avec le symbole brut
--log-level DEBUG|INFO|...  Verbosité
```

## Exemples
```bash
make update_prices UPDATE_ARGS="--sleep 0.2 --claim-before --allow-mi-proxy"

python -m scripts.update_prices_from_yfinance_refined --since 2024-01-01 --allow-mi-proxy --sleep 0.1

python -m scripts.update_prices_from_yfinance_refined --dry-run --touch-wdate-on-dry-run --only SAN --log-level DEBUG
```

## [2025-08-16] Après mise à jour des prix (layer2)
- Le trigger `trg_enforce_canonical_symbol` empêche l’insertion d’un symbole ≠ canonique.
- `adj_close` est forcé > 0 (fallback sur close_price).
- À exécuter après chaque run:
