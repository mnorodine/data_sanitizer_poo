# Préparation — Transformation POO de `data_sanitizer` (pour intégration MarketAdvisor)

> Objectif : préparer à tête reposée la refonte **POO + ports/adaptateurs** de `data_sanitizer`, en conservant la compatibilité avec les scripts/CLI existants et les vues (ex. `v_prices_compt`) consommées par **MarketAdvisor**.

---

## 0) Contexte & Principes

- **But de `data_sanitizer`** : fournir des **données fiables et stables** (tables `equities`, `equities_prices`, vues de synthèse et de contrôle).
- **But de MarketAdvisor** : consommer la vue `v_prices_compt` (et à terme des vues d’aide à la décision, ex. validation `is_delisted`).
- **Motivation POO** : réutilisation (package Python), testabilité (mocks), maintenance (responsabilités claires), évolutivité (changer de provider marché sans toucher au cœur).
- **Style architectural** : **hexagonal (ports/adaptateurs)**. Le domaine ne connaît ni Postgres ni yfinance.

---

## 1) Pré-flight (avant la session de refonte)

- ✅ Répo propre : `git status` vide, tags poussés (`git push --tags`).
- ✅ Sauvegarde rapide : `make backup-sums` (dump + checksums).
- ✅ Variables d’env : vérifier `DATABASE_URL`, `.env`, accès Postgres.
- ✅ Sanity check actuel :  
  ```bash
  python -m scripts.update_prices_from_yfinance_refined --limit 3 --dry-run
  ```

---

## 2) Arborescence cible (POO)

```
data_sanitizer/
  __init__.py
  config.py              # chargement env, DSN, options (retries/logs)
  domain/
    models.py            # Equity, PriceBar (dataclasses)
    # value_objects.py   # Isin, MarketMic (optionnel)
    # events.py          # Domain events (optionnel)
  ports/                 # Interfaces (Protocol) — faciles à mocker
    equities_repo.py     # get_targets(), mark_attempt(...), get_existing_ticker(...)
    prices_repo.py       # upsert_bars(...), recompute_counts(...), update_bounds(...)
    market_data.py       # download_history(ticker, since) -> Iterable[PriceBar]
    ticker_resolver.py   # resolve(symbol)->ticker, has_enough_history(ticker)
    views_repo.py        # lecture vues (v_prices_compt, v_*_candidates)
  adapters/
    db/
      equities_repo_pg.py
      prices_repo_pg.py
      views_repo_pg.py
    providers/
      yfinance_client.py
      ticker_resolver_default.py
  services/
    update_prices.py     # UpdatePricesService (extraction du script validé)
    validate_delisted.py # ValidationService (manuel & auto basique)
  cli/
    __main__.py          # Typer/argparse; commandes: update-prices, validate-delisted
tests/
  unit/
  integration/
```

**Approche migration douce** : conserver les scripts actuels; les transformer ensuite en **minces wrappers** qui appellent les services POO.

---

## 3) Domain Models (extraits)

```python
# data_sanitizer/domain/models.py
from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass(frozen=True)
class Equity:
    isin: str
    symbol: str
    ticker: Optional[str] = None
    is_delisted: bool = False

@dataclass(frozen=True)
class PriceBar:
    date: date
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: float
    adj_close: Optional[float]
    volume: Optional[int]
```

---

## 4) Ports (interfaces à mocker)

```python
# data_sanitizer/ports/prices_repo.py
from typing import Protocol, Sequence, Optional, Tuple
from datetime import date
from data_sanitizer.domain.models import PriceBar

class PricesRepo(Protocol):
    def last_price_date(self, isin: str, symbol: str) -> Optional[date]: ...
    def upsert_bars(self, isin: str, symbol: str, bars: Sequence[PriceBar]) -> int: ...
    def recompute_counts(self, isin: str, symbol: str) -> Tuple[int, int]: ...
    def update_bounds(self, isin: str, symbol: str) -> None: ...
```

```python
# data_sanitizer/ports/equities_repo.py
from typing import Protocol, Optional

class EquitiesRepo(Protocol):
    def get_targets(self, limit: Optional[int], only: Optional[list[str]]) -> list[tuple[str, str]]: ...
    def get_existing_ticker(self, isin: str, symbol: str) -> Optional[str]: ...
    def mark_attempt(self, isin: str, symbol: str, *, success: bool,
                     ticker: Optional[str], cnt_1y: int, cnt_total: int,
                     touch_w_date: bool = True) -> None: ...
```

```python
# data_sanitizer/ports/market_data.py
from typing import Protocol, Iterable, Optional
from datetime import date
from data_sanitizer.domain.models import PriceBar

class MarketData(Protocol):
    def download_history(self, ticker: str, since: Optional[date]) -> Iterable[PriceBar]: ...
```

```python
# data_sanitizer/ports/ticker_resolver.py
from typing import Protocol, Tuple

class TickerResolver(Protocol):
    def has_enough_history(self, ticker: str, min_days: int = 10) -> Tuple[bool, int]: ...
    def resolve(self, symbol: str) -> tuple[str | None, int]: ...
```

---

## 5) Service principal (use-case `update_prices`)

```python
# data_sanitizer/services/update_prices.py
from datetime import date
from typing import Optional
from data_sanitizer.ports.equities_repo import EquitiesRepo
from data_sanitizer.ports.prices_repo import PricesRepo
from data_sanitizer.ports.market_data import MarketData
from data_sanitizer.ports.ticker_resolver import TickerResolver

class UpdatePricesService:
    def __init__(self, equities: EquitiesRepo, prices: PricesRepo,
                 market: MarketData, resolver: TickerResolver):
        self.equities = equities
        self.prices = prices
        self.market = market
        self.resolver = resolver

    def run(self, *, since: Optional[date], limit: Optional[int],
            only: Optional[list[str]], sleep: float = 0.0) -> None:
        for isin, symbol in self.equities.get_targets(limit, only):
            ticker = self._pick_ticker(isin, symbol)
            if not ticker:
                self.equities.mark_attempt(isin, symbol, success=False, ticker=None, cnt_1y=0, cnt_total=0)
                continue
            start = since or self.prices.last_price_date(isin, symbol)
            bars = list(self.market.download_history(ticker, start))
            inserted = self.prices.upsert_bars(isin, symbol, bars)
            cnt_total, cnt_1y = self.prices.recompute_counts(isin, symbol)
            self.prices.update_bounds(isin, symbol)
            self.equities.mark_attempt(isin, symbol, success=True, ticker=ticker,
                                       cnt_1y=cnt_1y, cnt_total=cnt_total)

    def _pick_ticker(self, isin: str, symbol: str) -> Optional[str]:
        existing = self.equities.get_existing_ticker(isin, symbol)
        if existing and self.resolver.has_enough_history(existing)[0]:
            return existing
        return self.resolver.resolve(symbol)[0]
```

> L’implémentation des adaptateurs **réutilise** la logique validée (priorité au ticker existant, retries/backoff, comptage strict des insertions, MAJ `first_quote_at`/`last_quote_at`).

---

## 6) Vues SQL pour validation manuelle `is_delisted` (ébauches)

```sql
-- Candidats potentiels à "delisted" (règles basiques)
CREATE OR REPLACE VIEW v_equities_delisted_candidates AS
SELECT e.isin, e.symbol, e.ticker, e.is_delisted,
       e.first_quote_at, e.last_quote_at, e.cnt_1y, e.cnt_total
FROM equities e
WHERE (e.cnt_1y = 0 OR (e.last_quote_at IS NOT NULL AND e.last_quote_at < CURRENT_DATE - INTERVAL '365 days'))
  AND e.is_delisted = FALSE;
```

> À enrichir avec événements notoires (splits, suspensions) si disponibles.

---

## 7) Plan de travail — Prochaine session

1. **Créer le package** `data_sanitizer` (arborescence ci-dessus).
2. **Domain models** : `Equity`, `PriceBar`.
3. **Ports (Protocol)** : `EquitiesRepo`, `PricesRepo`, `MarketData`, `TickerResolver`.
4. **Adapters PG & yfinance** : extraire depuis le script existant (retries, upsert, bounds).
5. **Service** `UpdatePricesService.run()` (parité fonctionnelle avec le script actuel).
6. **CLI** (`cli/__main__.py`) : commandes `update-prices`, `validate-delisted` via Typer/argparse.
7. **Makefile** : alias vers la nouvelle CLI (sans casser les commandes actuelles).
8. **Vues SQL** : `v_equities_delisted_candidates` + (optionnel) vue(s) d’audit.
9. **Tests** : unitaires (services, ports mockés) + intégration légère (adapters).

---

## 8) Versioning & branches

- Branche de refonte : `feat/poo-core`  
- Tag cible lors du basculement CLI : **`v3.0.0`** (changement d’architecture interne; CLI maintenue).  
- Conserver les scripts historiques quelques versions pour faciliter la transition.

---

## 9) Checklists rapides

**Avant commit initial POO :**
- [ ] Arborescence créée, imports OK
- [ ] Service `UpdatePricesService` opérationnel (tests unitaires verts)
- [ ] Adapters PG/yfinance branchés (tests intégration basiques)
- [ ] CLI `update-prices` fonctionne comme le script historique

**Avant tag `v3.0.0` :**
- [ ] Makefile mis à jour (alias)
- [ ] Vues SQL créées/testées
- [ ] README/CHANGELOG mis à jour
- [ ] `make backup-sums` OK

---

## 10) Notes d’intégration MarketAdvisor

- Aujourd’hui : MarketAdvisor lit `v_prices_compt` → **rien à changer**.
- Demain : il peut importer `data_sanitizer` pour :
  - lire des vues via `ViewsRepo`,
  - exposer un bouton de **validation `is_delisted`** appelant `ValidationService`.
- Contrat : conserver les vues stables (lecture seule), la logique “métier” dans `data_sanitizer`.
