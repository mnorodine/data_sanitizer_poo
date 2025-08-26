
#!/usr/bin/env python3
"""
Update equities + equities_prices from Yahoo Finance (yfinance).

- Detects the best Yahoo Finance ticker for each equity (based on symbol + market suffixes).
- Upserts OHLCV daily prices into equities_prices (incremental from last stored date).
- Updates the equities row with ticker/api/is_valid/w_date/counters (cnt_1y, cnt_total).

Requirements:
  - scripts.config must expose:
      get_pg_connection() -> psycopg2 connection (or compat)
      APP_TZ: tzinfo
      YF_SLEEP_SECS: float
      EURONEXT_MIC_TO_SUFFIX: Dict[str, str]
      MARKET_TO_SUFFIX: Dict[str, str]

Notes:
  - Assumes a UNIQUE constraint on equities_prices(isin, symbol, price_date).
  - Uses ON CONFLICT DO UPDATE for idempotency.
"""
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Any

import numpy as np
import pandas as pd
import yfinance as yf
from dateutil.relativedelta import relativedelta

from scripts.config import (
    get_pg_connection,
    APP_TZ,
    YF_SLEEP_SECS,
    EURONEXT_MIC_TO_SUFFIX,
    MARKET_TO_SUFFIX,
)

# --- Logging ----------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("update_prices_from_yfinance_refactored")

API_NAME = "yfinance"

# Ordered fallback suffixes: prefer market-specific, then generic lists
FALLBACK_SUFFIXES: List[str] = list(
    dict.fromkeys(
        [s.strip(".").upper() for s in (list(EURONEXT_MIC_TO_SUFFIX.values()) + list(MARKET_TO_SUFFIX.values()))]
    )
)


# --- Data models ------------------------------------------------------------
@dataclass(frozen=True)
class Equity:
    isin: str
    symbol: str
    market: Optional[str]
    ticker: Optional[str]
    api: Optional[str]
    is_valid: Optional[bool]
    last_price_date: Optional[date]


# --- Small utils ------------------------------------------------------------
def _scalar_or_none(val: Any) -> Optional[Any]:
    """Return a Python scalar or None (handles pandas/numpy scalars & 1-item Series)."""
    if val is None:
        return None
    # 1-item Series -> extract, otherwise leave as is
    if isinstance(val, pd.Series):
        if len(val) == 1:
            val = val.iloc[0]
        else:
            # ambiguous (multiple values) -> cannot store
            return None
    # numpy scalar -> to python
    if isinstance(val, (np.generic,)):
        val = val.item()
    # NaN -> None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    return val


def _to_float(val: Any) -> Optional[float]:
    v = _scalar_or_none(val)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(val: Any) -> Optional[int]:
    v = _scalar_or_none(val)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            fv = float(v)
            if np.isnan(fv):
                return None
            return int(fv)
        except Exception:
            return None


def _normalize_yf_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df has flat columns and only OHLCV; drop rows with all-NaN OHLCV.
    Some yfinance returns MultiIndex columns or includes 'Adj Close'.
    """
    if df is None or df.empty:
        return df
    # Flatten MultiIndex by taking first level (Open/High/Low/Close/Volume)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    # Keep only known columns
    keep_cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[keep_cols].copy()
    # Drop rows where all are NaN
    df = df.dropna(how="all")
    return df


# --- DB helpers -------------------------------------------------------------
def fetch_target_equities(today: date) -> List[Equity]:
    """
    Fetch equities to (re)process: w_date < today. Also return last stored price date per pair.
    """
    sql = """
        SELECT
            e.isin, e.symbol, e.market, e.ticker, e.api, e.is_valid,
            MAX(ep.price_date) AS last_price_date
        FROM equities e
        LEFT JOIN equities_prices ep ON ep.isin = e.isin AND ep.symbol = e.symbol
        WHERE e.w_date < %s
        GROUP BY e.isin, e.symbol, e.market, e.ticker, e.api, e.is_valid
        ORDER BY e.symbol
    """
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (today,))
            rows = cur.fetchall()
    equities: List[Equity] = []
    for r in rows:
        equities.append(
            Equity(
                isin=r[0],
                symbol=r[1],
                market=r[2],
                ticker=r[3],
                api=r[4],
                is_valid=r[5],
                last_price_date=r[6],
            )
        )
    return equities


def upsert_equities_prices_batch(isin: str, symbol: str, df: pd.DataFrame) -> int:
    """Upsert a batch of prices for (isin, symbol). Returns number of rows written."""
    if df is None or df.empty:
        return 0

    # Normalize columns
    df = _normalize_yf_df(df)
    if df is None or df.empty:
        return 0

    # Normalize index to date (drop tz, keep only date component)
    # tolerate naive or tz-aware index
    try:
        if getattr(df.index, "tz", None) is not None:
            df.index = df.index.tz_convert(None)
    except Exception:
        pass
    df.index = pd.to_datetime(df.index, errors="coerce").tz_localize(None)
    df = df[~df.index.isna()].copy()
    df["price_date"] = df.index.date

    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                isin,
                symbol,
                row["price_date"],
                _to_float(row.get("Open")),
                _to_float(row.get("Close")),
                _to_float(row.get("High")),
                _to_float(row.get("Low")),
                _to_int(row.get("Volume")),
            )
        )

    sql = """
        INSERT INTO equities_prices (
            isin, symbol, price_date,
            open_price, close_price, high_price, low_price, volume
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (isin, symbol, price_date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            close_price = EXCLUDED.close_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            volume      = EXCLUDED.volume
    """
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    return len(rows)


def update_equity_row(
    isin: str,
    symbol: str,
    ticker: Optional[str],
    cnt_1y: Optional[int],
    cnt_total: Optional[int],
) -> None:
    """Update equities row with detection and counters."""
    today = datetime.now(APP_TZ).date()
    # Respect NOT NULL constraints: default to 0 when not provided
    c1 = 0 if cnt_1y is None else int(cnt_1y)
    ct = 0 if cnt_total is None else int(cnt_total)
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE equities
                SET
                    ticker = %s,
                    api = %s,
                    is_valid = %s,
                    w_date = %s,
                    cnt_1y = %s,
                    cnt_total = %s
                WHERE isin = %s AND symbol = %s
                """
                ,
                (
                    ticker,
                    API_NAME if ticker else None,
                    True if ticker else False,
                    today,
                    c1,
                    ct,
                    isin,
                    symbol,
                ),
            )
        conn.commit()


# --- yfinance helpers -------------------------------------------------------
def _count_history_stats(ticker: str) -> Tuple[int, int]:
    """Return (cnt_1y, cnt_total) based on full daily history available in yfinance."""
    try:
        h = yf.Ticker(ticker).history(period="max", interval="1d", auto_adjust=False)
        if h is None or h.empty:
            return (0, 0)
        if getattr(h.index, "tz", None) is not None:
            h.index = h.index.tz_convert(None)
        h.index = pd.to_datetime(h.index).tz_localize(None)
        today = datetime.now(APP_TZ).date()
        start_1y = today - relativedelta(years=1)
        total = int(len(h))
        # Convert to date for comparison
        idx_dates = pd.to_datetime(h.index).date
        cnt_1y = int((idx_dates >= start_1y).sum())
        return (cnt_1y, total)
    except Exception as e:
        log.debug("history stats failed for %s: %s", ticker, e)
        return (0, 0)


def _validate_symbol_on_yf(ticker: str) -> bool:
    """Quick probe to decide if ticker exists and trades (needs >= 2 rows last 5 days)."""
    try:
        probe = yf.Ticker(ticker).history(period="5d", interval="1d", auto_adjust=False)
        return probe is not None and not probe.empty and len(probe) >= 2
    except Exception as e:
        log.debug("probe failed for %s: %s", ticker, e)
        return False


def _candidate_suffixes(market: Optional[str]) -> List[str]:
    ordered: List[str] = []
    # Prefer exact market suffix first if known
    if market:
        m = market.strip().upper()
        # Try EURONEXT MIC mapping first, then MARKET mapping
        for d in (EURONEXT_MIC_TO_SUFFIX, MARKET_TO_SUFFIX):
            suf = d.get(m)
            if suf:
                suf = suf.strip(".").upper()
                if suf not in ordered:
                    ordered.append(suf)
    # Then the global fallback list
    for s in FALLBACK_SUFFIXES:
        if s not in ordered:
            ordered.append(s)
    return ordered


def find_best_yf_ticker(symbol: str, market: Optional[str]) -> Optional[str]:
    sym = symbol.strip().upper()
    tried = set()

    # 1) Try "SYMBOL.SUFFIX" variants
    for suf in _candidate_suffixes(market):
        cand = f"{sym}.{suf}"
        if cand in tried:
            continue
        tried.add(cand)
        if _validate_symbol_on_yf(cand):
            return cand

    # 2) Fallback to raw symbol as last resort
    if sym not in tried and _validate_symbol_on_yf(sym):
        return sym

    return None


def download_incremental_history(ticker: str, last_price_date: Optional[date]) -> pd.DataFrame:
    """Download daily price history starting from last_price_date+1 (if provided)."""
    kwargs = dict(interval="1d", auto_adjust=False, progress=False)
    if last_price_date:
        start = last_price_date + timedelta(days=1)
        # yfinance expects string dates
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"), **kwargs)
    else:
        df = yf.download(ticker, period="max", **kwargs)
    # Normalize columns immediately
    df = _normalize_yf_df(df)
    return df if df is not None else pd.DataFrame()


# --- Main -------------------------------------------------------------------
def main() -> None:
    today = datetime.now(APP_TZ).date()
    equities = fetch_target_equities(today)
    if not equities:
        log.info("Aucune action à traiter (w_date < %s).", today)
        return

    log.info("%d actions à analyser / mettre à jour (yfinance)", len(equities))

    processed = 0
    for idx, eq in enumerate(equities, start=1):
        symbol = eq.symbol.strip().upper()

        # Determine or reuse a valid ticker
        if eq.ticker and eq.api == API_NAME and (eq.is_valid is True):
            ticker: Optional[str] = eq.ticker
        else:
            ticker = find_best_yf_ticker(symbol, eq.market)

        if not ticker:
            # Mark row as invalid for yfinance and move on (respect NOT NULL cnt_*)
            update_equity_row(eq.isin, eq.symbol, None, 0, 0)
            log.info("[%d/%d] ❌ %s : aucun ticker yfinance valide trouvé", idx, len(equities), symbol)
            time.sleep(YF_SLEEP_SECS)
            continue

        # Count stats on full history (used to populate cnt_1y / cnt_total)
        cnt_1y, cnt_total = _count_history_stats(ticker)

        # Incremental download + upsert prices
        try:
            hist = download_incremental_history(ticker, eq.last_price_date)
            n = upsert_equities_prices_batch(eq.isin, eq.symbol, hist)
            processed += 1
            update_equity_row(eq.isin, eq.symbol, ticker, cnt_1y, cnt_total)
            log.info(
                "[%d/%d] ✅ %s → %s | +%d lignes | cnt_1y=%s cnt_total=%s",
                idx,
                len(equities),
                symbol,
                ticker,
                n,
                cnt_1y,
                cnt_total,
            )
        except Exception as e:
            # Do not crash the run on a single failure; mark equity as invalid for now
            log.error("[%d/%d] ⚠️ %s → %s : échec MAJ (%s)", idx, len(equities), symbol, ticker, e)
            update_equity_row(eq.isin, eq.symbol, None, 0, 0)

        time.sleep(YF_SLEEP_SECS)

    log.info("Terminé. %d/%d lignes equities traitées.", processed, len(equities))


if __name__ == "__main__":
    main()
