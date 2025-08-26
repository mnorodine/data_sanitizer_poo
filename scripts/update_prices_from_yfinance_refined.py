#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Couche 2 — Mise à jour des prix depuis Yahoo Finance (yfinance).
Voir README_update_prices_from_yfinance.md pour les détails et les options.
"""
import argparse
import datetime as dt
import logging
import os
import re
import time
from typing import Iterable, List, Optional, Tuple, Set

import pandas as pd
import yfinance as yf
import psycopg
from psycopg.rows import dict_row, tuple_row

# ---------------------------------------------------------------------------
# Helpers de normalisation/conversion
# ---------------------------------------------------------------------------

def _none_if_nan(v):
    try:
        return None if pd.isna(v) else v
    except Exception:
        return None

def _to_float(v) -> Optional[float]:
    v = _none_if_nan(v)
    try:
        return None if v is None else float(v)
    except Exception:
        return None

def _normalize_ohlc(df: 'pd.DataFrame') -> 'pd.DataFrame':
    """
    Aplati les colonnes yfinance éventuelles (MultiIndex), dédoublonne,
    garantit un index datetime si possible.
    """
    if df is None:
        return df
    # yfinance peut renvoyer des colonnes MultiIndex (("Open",""), ...)
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [
            (c[0] if isinstance(c, tuple) and len(c) > 0 and c[0] else (c[-1] if isinstance(c, tuple) else str(c)))
            for c in df.columns
        ]
    # dédoublonne d'éventuelles colonnes répétées
    try:
        df = df.loc[:, ~df.columns.duplicated(keep="first")]
    except Exception:
        pass
    # garantit un index datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'Date' in df.columns:
            df = df.set_index('Date')
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        pass
    # tri par date
    try:
        df = df.sort_index()
    except Exception:
        pass
    return df

# ---------------------------------------------------------------------------
# Stats & bornes
# ---------------------------------------------------------------------------

def recompute_counts(conn, isin: str, symbol: str) -> tuple[int, int]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("""
            SELECT
              COUNT(*) AS cnt_total_calc,
              COUNT(*) FILTER (
                WHERE price_date >= CURRENT_DATE - INTERVAL '366 days'
              ) AS cnt_1y_calc
            FROM equities_prices
            WHERE isin = %s AND symbol = %s
        """, (isin, symbol))
        cnt_total_calc, cnt_1y_calc = cur.fetchone()

    cnt_1y_calc = min(cnt_1y_calc, cnt_total_calc)
    return cnt_total_calc, cnt_1y_calc

def get_table_columns(conn, table: str, schema: str = "public") -> Set[str]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            (schema, table),
        )
        return {r[0] for r in cur.fetchall()}

def update_quote_bounds(conn, isin: str, symbol: str):
    """
    Met à jour first_quote_at / last_quote_at (et optionnellement last_trade_mic_time comme proxy).
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("""
            SELECT MIN(price_date)::date, MAX(price_date)::date
            FROM equities_prices
            WHERE isin=%s AND symbol=%s
        """, (isin, symbol))
        row = cur.fetchone()
        if not row:
            return
        first_d, last_d = row[0], row[1]
    if not first_d or not last_d:
        return

    eq_cols = get_table_columns(conn, "equities")
    sets = []
    params = []
    if "first_quote_at" in eq_cols:
        sets.append("first_quote_at = %s")
        params.append(first_d)
    if "last_quote_at" in eq_cols:
        sets.append("last_quote_at = %s")
        params.append(last_d)
    if "last_trade_mic_time" in eq_cols:
        # Proxy : aligne la date (00:00). Commente si tu préfères conserver uniquement la valeur Euronext.
        sets.append("last_trade_mic_time = COALESCE(last_trade_mic_time, %s)")
        params.append(dt.datetime.combine(last_d, dt.time(0, 0)))

    if not sets:
        return

    sql = f"UPDATE equities SET {', '.join(sets)} WHERE isin=%s AND symbol=%s"
    params.extend([isin, symbol])
    with conn.cursor() as cur2:
        cur2.execute(sql, params)

# ---------------------------------------------------------------------------
# Connexion DB
# ---------------------------------------------------------------------------

def _connect():
    """
    Essaie d'utiliser scripts.config.get_connection() si présent,
    sinon DATABASE_URL (DSN psycopg) depuis l'environnement.
    """
    try:
        from scripts.config import get_connection  # type: ignore
        return get_connection()
    except Exception:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("Aucune connexion DB. Fournir scripts.config.get_connection() ou DATABASE_URL.")
        return psycopg.connect(dsn, row_factory=dict_row)

# ---------------------------------------------------------------------------
# Ticker resolution
# ---------------------------------------------------------------------------

EURONEXT_SUFFIXES = [".PA", ".AS", ".BR", ".LS", ".IR"]

def is_numeric_symbol(symbol: str) -> bool:
    return bool(re.match(r"^\d", symbol or ""))

def build_candidates(symbol: str, allow_mi_proxy: bool, strict_euronext: bool) -> List[str]:
    cands = [f"{symbol}{suf}" for suf in EURONEXT_SUFFIXES]
    if allow_mi_proxy and is_numeric_symbol(symbol):
        cands.append(f"{symbol}.MI")
    if not strict_euronext:
        # tentative finale : symbole brut
        cands.append(symbol)
    # remove duplicates preserving order
    seen, out = set(), []
    for t in cands:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def has_enough_history(ticker: str, min_days: int = 10) -> Tuple[bool, int]:
    try:
        hist = yf.Ticker(ticker).history(period="1y", interval="1d", actions=True)
        if hist is None or hist.empty:
            return False, 0
        cnt = int(hist["Close"].dropna().shape[0])
        return cnt >= min_days, cnt
    except Exception as e:
        # Réduire le bruit pendant les sondages de suffixes
        logging.debug(
            'HTTP Error 404:\nERROR: %s: possibly delisted; no price data found  (period=1y) (Yahoo error = %s)',
            ticker,
            getattr(e, "args", [""])[0] or "\"No data found, symbol may be delisted\"",
        )
        return False, 0

def resolve_ticker(symbol: str, allow_mi_proxy: bool, strict_euronext: bool) -> Tuple[Optional[str], int]:
    for cand in build_candidates(symbol, allow_mi_proxy, strict_euronext):
        ok, cnt = has_enough_history(cand)
        if ok:
            return cand, cnt
    return None, 0

def get_existing_ticker(conn, isin: str, symbol: str) -> Optional[str]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT ticker FROM equities WHERE isin=%s AND symbol=%s", (isin, symbol))
        row = cur.fetchone()
        return row[0] if row and row[0] else None

# ---------------------------------------------------------------------------
# Prices download (avec retries) & upsert
# ---------------------------------------------------------------------------

def last_price_date(conn, isin: str, symbol: str) -> Optional[dt.date]:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            SELECT MAX(price_date)::date
            FROM equities_prices
            WHERE isin=%s AND symbol=%s
            """,
            (isin, symbol),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None

def download_history(ticker: str,
                     start: Optional[dt.date],
                     retries: int = 3,
                     delay: float = 5.0) -> pd.DataFrame:
    """
    Télécharge l'historique avec quelques retries en cas d'erreur réseau (timeouts, etc.).
    Backoff progressif: delay * attempt (5s, 10s, 15s par défaut).
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            if start:
                # petit tampon d'un jour
                start_str = (start - dt.timedelta(days=1)).isoformat()
                df = yf.download(ticker, start=start_str, interval="1d",
                                 auto_adjust=False, actions=True, progress=False)
            else:
                df = yf.download(ticker, period="max", interval="1d",
                                 auto_adjust=False, actions=True, progress=False)
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            return df
        except Exception as e:
            last_err = e
            logging.warning("⚠️ %s : tentative %d/%d échouée (%s)", ticker, attempt, retries, e)
            time.sleep(delay * attempt)  # backoff progressif
    # si toutes les tentatives échouent
    raise last_err

def upsert_prices(conn, isin: str, symbol: str, df: 'pd.DataFrame') -> int:
    """
    Insère / met à jour equities_prices pour (isin, symbol) avec les données de df.
    Schéma cible:
      isin, symbol, price_date, open_price, close_price, high_price, low_price, volume
    Optionnellement, la colonne adj_close peut exister — gérée dynamiquement.

    Retourne UNIQUEMENT le nombre réel de dates nouvellement insérées
    (les updates ne sont pas comptées).
    """
    if df is None or len(df) == 0:
        return 0

    cols = get_table_columns(conn, "equities_prices")
    has_adj = "adj_close" in cols

    df = _normalize_ohlc(df)

    # Passe l'index en colonne pour itertuples()
    if isinstance(df.index, pd.DatetimeIndex):
        pdf = df.reset_index().rename(columns={'index': 'Date'})
    else:
        pdf = df.copy()

    def pick(colnames):
        for c in colnames:
            if c in pdf.columns:
                return c
        return None

    c_date   = pick(['Date', 'date'])
    c_open   = pick(['Open', 'open'])
    c_high   = pick(['High', 'high'])
    c_low    = pick(['Low', 'low'])
    c_close  = pick(['Close', 'close'])
    c_adj    = pick(['Adj Close', 'AdjClose', 'Adj_Close', 'adj_close', 'adjClose'])
    c_volume = pick(['Volume', 'volume'])

    if c_date is None or c_close is None:
        return 0

    rows: List[Tuple] = []
    dates_in_rows: List[dt.date] = []

    for row in pdf.itertuples(index=False):
        dtv = getattr(row, c_date, None)
        if dtv is None:
            continue
        try:
            date_val = pd.to_datetime(dtv).date()
        except Exception:
            continue

        open_v   = _to_float(getattr(row, c_open,   None)) if c_open   else None
        high_v   = _to_float(getattr(row, c_high,   None)) if c_high   else None
        low_v    = _to_float(getattr(row, c_low,    None)) if c_low    else None
        close_v  = _to_float(getattr(row, c_close,  None)) if c_close  else None
        adj_v    = _to_float(getattr(row, c_adj,    None)) if (has_adj and c_adj) else None
        vol_v    = getattr(row, c_volume, None) if c_volume else None
        try:
            vol_v = None if vol_v is None or pd.isna(vol_v) else int(vol_v)
        except Exception:
            vol_v = None

        if close_v is None:
            # on ignore les lignes sans close
            continue

        if has_adj:
            rows.append((isin, symbol, date_val, open_v, high_v, low_v, close_v, adj_v, vol_v))
        else:
            rows.append((isin, symbol, date_val, open_v, high_v, low_v, close_v, vol_v))
        dates_in_rows.append(date_val)

    if not rows:
        return 0

    # Construction dynamique du SQL selon les colonnes disponibles
    base_cols = ["isin", "symbol", "price_date", "open_price", "high_price", "low_price", "close_price"]
    if has_adj:
        base_cols.append("adj_close")
    base_cols.append("volume")

    placeholders = ", ".join(["%s"] * len(base_cols))
    col_list = ", ".join(base_cols)

    # SET update pour DO UPDATE
    set_parts = [
        "open_price  = EXCLUDED.open_price",
        "high_price  = EXCLUDED.high_price",
        "low_price   = EXCLUDED.low_price",
        "close_price = EXCLUDED.close_price",
        "volume      = EXCLUDED.volume",
    ]
    if has_adj:
        set_parts.insert(4, "adj_close   = EXCLUDED.adj_close")
    set_clause = ",\n                ".join(set_parts)

    sql = f"""
        INSERT INTO equities_prices
            ({col_list})
        VALUES
            ({placeholders})
        ON CONFLICT (isin, symbol, price_date) DO UPDATE SET
            {set_clause}
    """

    # Récupère les dates existantes avant l’upsert pour compter les vraies insertions
    with conn.cursor(row_factory=tuple_row) as cur0:
        cur0.execute("""
            SELECT price_date FROM equities_prices WHERE isin=%s AND symbol=%s
        """, (isin, symbol))
        existing_dates = {r[0] for r in cur0.fetchall()}
    new_dates = {d for d in dates_in_rows if d not in existing_dates}

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(new_dates)

# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def claim_row(conn, isin: str, symbol: str) -> bool:
    """Set w_date=today si w_date<today; renvoie True si 'pris'."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE equities
            SET w_date = CURRENT_DATE
            WHERE isin=%s AND symbol=%s AND (w_date IS NULL OR w_date < CURRENT_DATE)
            """,
            (isin, symbol),
        )
        return cur.rowcount > 0

def mark_attempt(conn, isin: str, symbol: str, *, success: bool, ticker: Optional[str],
                 cnt_1y: int, cnt_total: int, mark_w_date: bool=True):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE equities
            SET ticker=%s,
                api=%s,
                is_valid=%s,
                cnt_1y=%s,
                cnt_total=%s,
                w_date = CASE WHEN %s THEN CURRENT_DATE ELSE w_date END
            WHERE isin=%s AND symbol=%s
            """,
            (
                ticker,
                "yfinance" if success else None,
                success,
                cnt_1y,
                cnt_total,
                mark_w_date,
                isin, symbol,
            ),
        )

def fetch_equities(conn, *, limit: Optional[int], only: Optional[List[str]]) -> List[Tuple[str,str]]:
    base_sql = """
        SELECT isin, symbol
        FROM equities
        WHERE ((w_date IS NULL OR w_date < CURRENT_DATE) AND is_delisted = False)
    """
    params: List = []
    if only:
        placeholders = ",".join(["%s"]*len(only))
        base_sql += f" AND symbol IN ({placeholders})"
        params.extend(only)
    base_sql += " ORDER BY symbol"
    if limit:
        base_sql += " LIMIT %s"
        params.append(limit)
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(base_sql, params)
        return [(r[0], r[1]) for r in cur.fetchall()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=lambda s: dt.date.fromisoformat(s), default=None, help="YYYY-MM-DD (début)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--only", type=lambda s: [t.strip() for t in s.split(",") if t.strip()], default=None)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--touch-wdate-on-dry-run", action="store_true")
    ap.add_argument("--claim-before", action="store_true")
    ap.add_argument("--allow-mi-proxy", action="store_true")
    ap.add_argument("--no-strict-euronext", action="store_true")
    ap.add_argument("--retries", type=int, default=3, help="Retries pour download_history (défaut: 3)")
    ap.add_argument("--retry-delay", type=float, default=5.0, help="Délai de backoff de base en secondes (défaut: 5.0)")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")

    conn = _connect()
    conn.autocommit = False

    equities = fetch_equities(conn, limit=args.limit, only=args.only)
    logging.info("%d actions à analyser / mettre à jour (yfinance)", len(equities))

    strict_euronext = not args.no_strict_euronext

    processed = 0
    for i, (isin, symbol) in enumerate(equities, 1):
        try:
            if args.claim_before:
                if not claim_row(conn, isin, symbol):
                    logging.debug("[%d/%d] %s : déjà pris (w_date=today)", i, len(equities), symbol)
                    conn.commit()
                    continue
                conn.commit()

            # 1) Si un ticker existe déjà en base, on l'essaie en priorité
            existing = get_existing_ticker(conn, isin, symbol)
            ticker: Optional[str] = None
            cnt1y_probe: int = 0

            if existing:
                ok, cnt_try = has_enough_history(existing)
                if ok:
                    ticker, cnt1y_probe = existing, cnt_try
                else:
                    logging.info("[%d/%d] ℹ️ %s : ticker existant %s invalide → tentative de résolution",
                                 i, len(equities), symbol, existing)

            if not ticker:
                ticker, cnt1y_probe = resolve_ticker(symbol, args.allow_mi_proxy, strict_euronext)

            if not ticker:
                logging.info("[%d/%d] ❌ %s : aucun ticker valide trouvé", i, len(equities), symbol)
                if args.dry_run and args.touch_wdate_on_dry_run:
                    mark_attempt(conn, isin, symbol, success=False, ticker=None, cnt_1y=0, cnt_total=0, mark_w_date=True)
                elif not args.dry_run:
                    mark_attempt(conn, isin, symbol, success=False, ticker=None, cnt_1y=0, cnt_total=0, mark_w_date=True)
                conn.commit()
                if args.sleep: time.sleep(args.sleep)
                continue

            # Détermine la date de départ
            start_db = last_price_date(conn, isin, symbol)
            start = args.since or start_db

            # Téléchargement (avec retries)
            df = download_history(ticker, start, retries=args.retries, delay=args.retry_delay)
            df = _normalize_ohlc(df)
            df = df[~df.index.duplicated(keep="last")]

            # Harmonise "Adj Close" si présent
            if "Adj Close" in df.columns and "AdjClose" not in df.columns:
                df = df.rename(columns={"Adj Close": "AdjClose"})

            # Upsert
            inserted = 0
            if not args.dry_run:
                inserted = upsert_prices(conn, isin, symbol, df)

                # Recalcule les compteurs et met à jour les bornes de cotations
                cnt_total, cnt_1y = recompute_counts(conn, isin, symbol)
                update_quote_bounds(conn, isin, symbol)

                mark_attempt(conn, isin, symbol, success=True, ticker=ticker,
                             cnt_1y=cnt_1y, cnt_total=cnt_total, mark_w_date=True)
                conn.commit()

                log_cnt1y = cnt_1y
                log_cnt_total = cnt_total
            else:
                # En dry-run, on se contente des valeurs « probe » et d'un cnt_total heuristique
                df_rows = int(df.shape[0])
                if args.touch_wdate_on_dry_run:
                    mark_attempt(conn, isin, symbol, success=True, ticker=ticker,
                                 cnt_1y=cnt1y_probe, cnt_total=df_rows, mark_w_date=True)
                    conn.commit()
                log_cnt1y = cnt1y_probe
                log_cnt_total = df_rows

            logging.info("[%d/%d] ✅ %s → %s | +%d lignes | cnt_1y=%d cnt_total=%d",
                         i, len(equities), symbol, ticker, inserted, log_cnt1y, log_cnt_total)
            processed += 1

            if args.sleep: time.sleep(args.sleep)

        except Exception as e:
            logging.exception("[%d/%d] ⚠️ %s : échec MAJ (%s)", i, len(equities), symbol, e)
            try:
                conn.rollback()
            except Exception:
                pass
            if args.dry_run:
                if args.touch_wdate_on_dry_run:
                    mark_attempt(conn, isin, symbol, success=False, ticker=None, cnt_1y=0, cnt_total=0, mark_w_date=True)
                    conn.commit()
            else:
                mark_attempt(conn, isin, symbol, success=False, ticker=None, cnt_1y=0, cnt_total=0, mark_w_date=True)
                conn.commit()

    logging.info("Terminé. %d/%d lignes equities traitées.", processed, len(equities))

if __name__ == "__main__":
    main()
