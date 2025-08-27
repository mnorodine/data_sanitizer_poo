# data_sanitizer/adapters/providers/ticker_resolver_default.py
from __future__ import annotations


import yfinance as yf


class DefaultTickerResolver:
    """
    Résolution naïve basée sur Yahoo Finance :
    - has_enough_history : vérifie la profondeur d'historique sur 1 an
    - resolve : accepte le symbole tel quel s'il a au moins 1 jour d'historique
    """

    def has_enough_history(self, ticker: str, min_days: int = 10) -> tuple[bool, int]:
        try:
            df = yf.Ticker(ticker).history(period="1y")
        except Exception:
            return False, 0
        days = int(df.index.size)
        return (days >= min_days, days)

    def resolve(self, symbol: str) -> tuple[str | None, int]:
        """
        Retourne (symbol, days) si on récupère un historique (>0 jour), sinon (None, 0).
        """
        try:
            df = yf.Ticker(symbol).history(period="1y")
        except Exception:
            return None, 0
        days = int(df.index.size)
        if days > 0:
            return symbol, days
        return None, 0
