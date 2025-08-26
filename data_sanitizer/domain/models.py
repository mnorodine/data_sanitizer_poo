from __future__ import annotations
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
