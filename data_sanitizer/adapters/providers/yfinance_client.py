from __future__ import annotations
from typing import Iterable, Optional
from datetime import date, timedelta
import yfinance as yf
from data_sanitizer.ports.market_data import MarketData
from data_sanitizer.domain.models import PriceBar

class YFinanceClient(MarketData):
    def download_history(self, ticker: str, since: Optional[date]) -> Iterable[PriceBar]:
        start = (since - timedelta(days=2)).isoformat() if since else None
        df = yf.Ticker(ticker).history(start=start, auto_adjust=False)
        for idx, row in df.iterrows():
            def _safe(col):
                try:
                    v = row[col]
                    return None if v != v else float(v)
                except Exception:
                    return None
            yield PriceBar(
                date=idx.date(),
                open=_safe("Open"),
                high=_safe("High"),
                low=_safe("Low"),
                close=float(row["Close"]),
                adj_close=_safe("Adj Close"),
                volume=int(row["Volume"]) if "Volume" in row and row["Volume"] == row["Volume"] else None,
            )
