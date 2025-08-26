from __future__ import annotations
from typing import Tuple
import yfinance as yf

class DefaultTickerResolver:
    """
    Naïve resolver based on Yahoo Finance.
    """
    def has_enough_history(self, ticker: str, min_days: int = 10) -> Tuple[bool, int]:
        df = yf.Ticker(ticker).history(period="1y")
        days = len(df.index)
        return (days >= min_days, days)

    def resolve(self, symbol: str) -> tuple[str | None, int]:
        df = yf.Ticker(symbol).history(period="1y")
        days = len(df.index)
        if days > 0:
            return symbol, days
        return None, 0
