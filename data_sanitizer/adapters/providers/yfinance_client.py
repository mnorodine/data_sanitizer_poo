# data_sanitizer/adapters/providers/yfinance_client.py
from __future__ import annotations

from typing import Any, Callable, TypeVar

import pandas as pd  # type: ignore[import-untyped]
import yfinance as yf

T = TypeVar("T")


def _safe(fn: Callable[[], T], default: T) -> T:
    """
    Exécute fn() en rattrapant toute exception et renvoie `default` en cas d'erreur.
    """
    try:
        return fn()
    except Exception:
        return default


def get_history(
    ticker: str,
    start: str | None = None,
    end: str | None = None,
    interval: str = "1d",
) -> pd.DataFrame:
    """
    Retourne l'historique OHLCV sous forme de DataFrame (vide si indisponible).
    """
    tk = yf.Ticker(ticker)
    df = _safe(lambda: tk.history(start=start, end=end, interval=interval), pd.DataFrame())
    if isinstance(df, pd.DataFrame):
        return df
    return pd.DataFrame()


def get_info(ticker: str) -> dict[str, Any] | None:
    """
    Retourne des métadonnées (fast_info ou info) sous forme de dict, ou None.
    """
    tk = yf.Ticker(ticker)

    def _extract() -> dict[str, Any] | None:
        # fast_info peut être un dict ou un petit objet
        fi = getattr(tk, "fast_info", None)
        if fi is not None:
            if isinstance(fi, dict):
                return fi
            d = getattr(fi, "__dict__", None)
            if isinstance(d, dict):
                return d
        info = getattr(tk, "info", None)
        if isinstance(info, dict):
            return info
        return None

    return _safe(_extract, None)


def last_close(ticker: str) -> float | None:
    """
    Dernier cours de clôture (Close) si disponible, sinon None.
    """
    df = get_history(ticker, interval="1d")
    if df.empty:
        return None
    value = df["Close"].iloc[-1] if "Close" in df.columns else df.iloc[-1, 0]
    try:
        return float(value)
    except Exception:
        return None
