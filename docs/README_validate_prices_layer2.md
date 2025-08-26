# Validation Couche 2 — Contrôles de cohérence des prix

Script : `scripts/validate_prices_layer2.py`

Produits : CSV dans `reports/`.
- `validation_layer2_duplicates.csv` : doublons `(isin, symbol, price_date)`
- `validation_layer2_bad_values.csv` : valeurs négatives/anormales (`open/high/low/close <= 0`, `volume < 0`)
- `validation_layer2_gaps.csv` : jours ouvrés manquants sur ~1 an (approx. sans jours fériés)
- `validation_layer2_flat_series.csv` : séries « plates » (écart-type des 5 derniers `close` = 0)
- `validation_layer2_stale.csv` : instruments sans cotation récente (< dernier 14 jours)
- `validation_layer2_w_date_future.csv` : `w_date` dans le futur

Exécution :
```bash
make validate_layer2
# ou
python -m scripts.validate_prices_layer2
```

## [2025-08-16] Validation standardisée
- Validation automatique:
