from __future__ import annotations
from typing import Protocol, Iterable, Optional
from datetime import date
from data_sanitizer.domain.models import PriceBar

class MarketData(Protocol):
    def download_history(self, ticker: str, since: Optional[date]) -> Iterable[PriceBar]: ...
