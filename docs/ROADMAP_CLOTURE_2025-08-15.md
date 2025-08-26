# 📍 Roadmap — Projet `data_sanitizer` (clôturé le 2025-08-15)

## 📖 Glossaire (rappel)
- **is_valid** : vrai si l’action a encore une activité récente (≥ 2 séances cotées sur les 5 derniers jours calendaires).
- **is_active** : vrai si l’action a une activité significative sur un an (≥ 200 séances cotées sur les 12 derniers mois).
- **w_date** : date de « travail » pour éviter de retraiter un couple (ISIN, symbol) plusieurs fois dans la même journée.
  - Si besoin de tout rejouer : forcer `w_date` à une date < today, la passe remet `w_date = today` pour les lignes traitées.

---

## 🎯 Objectif global
Maintenir deux tables Postgres cohérentes et à jour :

- `equities` : instruments listés (import CSV pour l’instant).
- `equities_prices` : historiques de prix (yfinance).

Critères : mises à jour incrémentales, scripts idempotents, règles métier stabilisées, observabilité et validations.

---

## 📊 Tableau d’avancement (état final)

| Phase | Objectif                          | État  | Source actuelle | Notes |
|------:|-----------------------------------|:----:|-----------------|-------|
| 0     | Socle (env, schéma, règles)       | [x]  | —               | `.env`, `Makefile`, schéma migré (`last_trade_mic_time` en `DATE`) |
| 1     | Pipeline clair & targets Make     | [x]  | —               | Targets Make opérationnelles (import, update, validate, backup) |
| 2     | Import Euronext (`equities`)      | [x]  | CSV manuel      | `scripts/import_equities.py` validé (v1.2.0) |
| 3     | Import yfinance (`prices`)        | [x]  | yfinance        | `make update_prices` exécuté sur 3 536 tickers (4 974 615 lignes) |
| 4     | Validation & Flags                | [x]  | —               | `make validate_layer2 FIX=--fix-counters` OK, flags `is_valid`/`is_active` appliqués |
| 5     | Tests & Observabilité             | [~]  | —               | À poursuivre (facultatif pour clôture L2) |
| 6     | Résilience & Qualité des données  | [~]  | —               | À poursuivre (facultatif pour clôture L2) |

Légende : `[ ]` à faire · `[x]` terminé · `[~]` en cours

---

## ✅ Definition of Done (couche 2) — Statut
- `make update_prices` **OK** (exécuté en totalité).
- `make validate_layer2 FIX=--fix-counters` **OK**, **0 anomalie critique**.
- Flags `is_active`/`is_valid` **posés** via vue `v_equities_activity` et `UPDATE` idempotent.
- Procédures de relance (`w_date`) et de sauvegarde **documentées**.

➡️ **Conclusion** : La couche 2 de `data_sanitizer` est **clôturée** au 2025-08-15 ; les tables `equities` et `equities_prices` sont jugées **fiables** et **stables** pour reprise du projet **Analyse_pea**.

---

## 🧰 Runbook minimal (opérationnel)
```bash
# 1) Import liste Euronext
make import_equities

# 2) Mise à jour historique de prix (incrémental)
make update_prices

# 3) Validation & recalage des compteurs
make validate_layer2 FIX=--fix-counters

# 4) (Optionnel) Recalcul flags si besoin
psql -d pea_db -c "REFRESH MATERIALIZED VIEW CONCURRENTLY v_equities_activity;"  # si vous la matérialisez
psql -d pea_db -f scripts/sql/apply_flags.sql
```

> Remarque : si vous souhaitez matérialiser `v_equities_activity`, créez la MV et un fichier `scripts/sql/apply_flags.sql` avec l’UPDATE idempotent.

---

## 🧪 Pistes Phase 5 (Tests & Obs)
- Tests unitaires : helpers de normalisation, upsert, règles de flags.
- Métriques : nb de tickers traités/échoués, temps par job, lignes insérées/jour.
- Rapports : export CSV d’anomalies (zéro attendu après `--fix-counters`).

## 🛡️ Pistes Phase 6 (Résilience)
- Gestion jours fériés & journées sans cote.
- Stratégie fallback `.MI` (contrôlée par option).
- Garde-fous sur séries plates/volumes nuls, devise manquante.
- Alerte si `last_day` > 5 jours pour un actif `is_valid=true`.
