from __future__ import annotations
from datetime import date
from typing import Optional
from time import sleep as _sleep

from data_sanitizer.ports.equities_repo import EquitiesRepo
from data_sanitizer.ports.prices_repo import PricesRepo
from data_sanitizer.ports.market_data import MarketData
from data_sanitizer.ports.ticker_resolver import TickerResolver

class UpdatePricesService:
    def __init__(self, equities: EquitiesRepo, prices: PricesRepo,
                 market: MarketData, resolver: TickerResolver, *, pause_s: float = 0.0):
        self.equities = equities
        self.prices = prices
        self.market = market
        self.resolver = resolver
        self.pause_s = pause_s

    def run(
        self,
        *,
        since: Optional[date],
        limit: Optional[int],
        only: Optional[list[str]],
        sleep: float = 0.0,
        dry_run: bool = False,
        ) -> None:
        targets = list(self.equities.get_targets(limit, only))
        total = len(targets)

        for idx, (isin, symbol) in enumerate(targets, start=1):
            print(f"[{idx}/{total}] Traitement {symbol} (ISIN {isin})…", flush=True)

            ticker = self._pick_ticker(isin, symbol)
            if not ticker:
                print("   ⚠️ Aucun ticker valide trouvé → ignoré", flush=True)
                self.equities.mark_attempt(
                    isin, symbol, success=False, ticker=None, cnt_1y=0, cnt_total=0
                )
                continue

            start = since or self.prices.last_price_date(isin, symbol)
            bars = list(self.market.download_history(ticker, start))

            if dry_run:
                print(f"   ✅ Dry-run → {len(bars)} barres téléchargées", flush=True)
            else:
                self.prices.upsert_bars(isin, symbol, bars)
                cnt_total, cnt_1y = self.prices.recompute_counts(isin, symbol)
                self.prices.update_bounds(isin, symbol)
                self.equities.mark_attempt(
                    isin,
                    symbol,
                    success=True,
                    ticker=ticker,
                    cnt_1y=cnt_1y,
                    cnt_total=cnt_total,
                )
                print(
                    f"   ✅ Écriture → {len(bars)} barres insérées (total={cnt_total}, 1y={cnt_1y})",
                    flush=True,
                )

        if sleep or self.pause_s:
            _sleep(max(sleep, self.pause_s))

    def _pick_ticker(self, isin: str, symbol: str) -> Optional[str]:
        existing = self.equities.get_existing_ticker(isin, symbol)
        if existing and self.resolver.has_enough_history(existing)[0]:
            return existing
        return self.resolver.resolve(symbol)[0]
