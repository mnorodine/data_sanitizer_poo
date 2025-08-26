import os
import csv
import argparse
import logging
from typing import Dict, Iterable, List, Optional, Tuple
from datetime import datetime, date

# Robust import of get_pg_connection whether run as module or script
try:
    from scripts.config import get_pg_connection
except ModuleNotFoundError:  # allow direct execution without PYTHONPATH
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
    from scripts.config import get_pg_connection

import requests  # optional, used only for --source=euronext
from psycopg import sql

# ----------------------------
# Configuration & constants
# ----------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DEFAULT_CSV_PATH = "datas/equities.csv"
TABLE_NAME = "equities"

# IMPORTANT: Only these 7 columns may be written by this script.
ALLOWED_COLUMNS = ("isin", "symbol", "name", "market", "currency", "last_trade_mic_time", "time_zone")

# Indices for the CSV source (as specified by the user)
COLUMN_INDICES: Dict[str, int] = {
    "name": 0,
    "isin": 1,
    "symbol": 2,
    "market": 3,
    "currency": 4,
    "last_trade_mic_time": 9,
    "time_zone": 10,
}

UPSERT_SQL = """
INSERT INTO {table} (
    isin, symbol, name, market, currency, last_trade_mic_time, time_zone
)
VALUES (
    %(isin)s, %(symbol)s, %(name)s, %(market)s, %(currency)s, %(last_trade_mic_time)s, %(time_zone)s
)
ON CONFLICT (isin, symbol) DO UPDATE
SET
    name = EXCLUDED.name,
    market = EXCLUDED.market,
    currency = EXCLUDED.currency,
    last_trade_mic_time = EXCLUDED.last_trade_mic_time,
    time_zone = EXCLUDED.time_zone
;
"""

# ----------------------------
# Helpers
# ----------------------------
def _nz(value: Optional[str]) -> Optional[str]:
    """Normalize empty strings to None."""
    if value is None:
        return None
    v = value.strip()
    return v if v != "" else None

def _row_from_csv(cols: List[str]) -> Dict[str, Optional[str]]:
    def get(key: str) -> Optional[str]:
        idx = COLUMN_INDICES.get(key)
        if idx is None or idx >= len(cols):
            return None
        return _nz(cols[idx])
    return {
        "name": get("name"),
        "isin": get("isin"),
        "symbol": get("symbol"),
        "market": get("market"),
        "currency": get("currency"),
        "last_trade_mic_time": get("last_trade_mic_time"),
        "time_zone": get("time_zone"),
    }

def _to_date(v: Optional[str]) -> Optional[date]:
    """Convert string to Python date.
    - returns None for None/''/'-'
    - accepts YYYY-MM-DD; tolerates ISO datetime by slicing first 10 chars
    - also tries %d/%m/%Y and %Y/%m/%d
    """
    if v is None:
        return None
    v = v.strip()
    if v == "" or v == "-":
        return None
    candidate = v[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue
    return None

def _detect_delimiter(csv_path: str) -> str:
    """Try to detect delimiter; fall back to ';' then ','."""
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            sample = f.read(4096)
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
        return dialect.delimiter
    except Exception:
        # heuristic: prefer ';' if present in sample
        sample = sample if 'sample' in locals() else ''
        if ';' in sample and ',' not in sample:
            return ';'
        return ';' if ';' in sample else ','

def read_csv_rows(csv_path: str) -> List[Dict[str, Optional[str]]]:
    rows: List[Dict[str, Optional[str]]] = []
    exclusion_list = ["-"]  # Symboles à exclure

    delimiter = _detect_delimiter(csv_path)
    logging.info("Detected CSV delimiter: %r", delimiter)

    stats = {"rows": 0, "skip_header": 0, "skip_pk": 0, "skip_symbol": 0, "skip_date": 0, "ok": 0}

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for line_no, cols in enumerate(reader, start=1):
            stats["rows"] += 1
            # Skip header if it looks like one (heuristic)
            if line_no == 1 and any(("isin" in (c or "").lower()) or ("symbol" in (c or "").lower()) for c in cols):
                stats["skip_header"] += 1
                continue
            row = _row_from_csv(cols)
            # Require keys for the primary key (isin, symbol)
            if not row["isin"] or not row["symbol"]:
                stats["skip_pk"] += 1
                continue
            # Exclude symbols present in the exclusion list
            if row["symbol"] in exclusion_list:
                stats["skip_symbol"] += 1
                continue
            # Exclude/convert last_trade_mic_time
            dt = _to_date(row.get("last_trade_mic_time"))
            if dt is None:
                stats["skip_date"] += 1
                continue
            row["last_trade_mic_time"] = dt

            rows.append(row)
            stats["ok"] += 1

    logging.info(
        "CSV stats: total=%d, header_skipped=%d, skip_pk=%d, skip_symbol=%d, skip_date=%d, ok=%d",
        stats["rows"], stats["skip_header"], stats["skip_pk"], stats["skip_symbol"], stats["ok"], stats["ok"]
    )
    return rows

# ----------------------------
# Euronext Web Services (contractual API)
# ----------------------------
def _euronext_headers() -> Dict[str, str]:
    token = os.getenv("EURONEXT_WS_TOKEN")
    if not token:
        raise RuntimeError("EURONEXT_WS_TOKEN is not set in environment.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _euronext_base() -> str:
    base = os.getenv("EURONEXT_WS_BASE")
    if not base:
        raise RuntimeError("EURONEXT_WS_BASE is not set in environment (e.g., https://<your-endpoint>).")
    return base.rstrip("/")

def fetch_from_euronext(markets: List[str]) -> List[Dict[str, Optional[str]]]:
    """
    Fetch a list of instruments from Euronext Web Services.
    This is a generic example that you MUST adapt to your contracted endpoint and payload.
    It returns ONLY the 7 allowed columns.
    """
    base = _euronext_base()
    headers = _euronext_headers()

    # Example path & payload — replace with your specific product's endpoint/contract.
    url = f"{base}/reference-data/instruments/search"
    payload = {"markets": markets} if markets else {}

    logging.info("Querying Euronext WS: %s (markets=%s)", url, markets or "*")

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Euronext WS error {resp.status_code}: {resp.text[:300]}")

    data = resp.json()
    items = data.get("instruments") or data.get("data") or data

    rows: List[Dict[str, Optional[str]]] = []
    for it in items:
        isin = _nz(it.get("isin"))
        symbol = _nz(it.get("symbol") or it.get("ticker"))
        if not isin or not symbol:
            continue
        dt = _to_date(it.get("lastTradingTime") or it.get("last_trade_time"))
        if dt is None:
            # Exclude if no valid date (column is DATE in DB)
            continue
        rows.append({
            "name": _nz(it.get("instrumentName") or it.get("name")),
            "isin": isin,
            "symbol": symbol,
            "market": _nz(it.get("mic") or it.get("market")),
            "currency": _nz(it.get("tradingCurrency") or it.get("currency")),
            "last_trade_mic_time": dt,  # Python date
            "time_zone": _nz(it.get("timeZone") or it.get("timezone")),
        })
    return rows

# ----------------------------
# Database
# ----------------------------
def upsert_rows(rows: Iterable[Dict[str, Optional[str]]], dry_run: bool = False) -> Tuple[int, int]:
    """
    Insert or update rows strictly limited to the 7 allowed columns.
    Returns (inserted_or_updated_count, skipped_count).
    """
    rows = list(rows)
    if not rows:
        return (0, 0)

    # Ensure we do not carry any extra keys
    sanitized: List[Dict[str, Optional[str]]] = []
    for r in rows:
        rr = {k: r.get(k) for k in ALLOWED_COLUMNS}
        # Defensive conversion of last_trade_mic_time to date
        if not isinstance(rr.get("last_trade_mic_time"), date):
            rr["last_trade_mic_time"] = _to_date(rr.get("last_trade_mic_time"))  # type: ignore
        if rr["last_trade_mic_time"] is None:
            continue
        sanitized.append(rr)

    if dry_run:
        logging.info("Dry-run: %d rows prepared (no database writes).", len(sanitized))
        return (len(sanitized), 0)

    query = sql.SQL(UPSERT_SQL).format(table=sql.Identifier(TABLE_NAME))
    count = 0
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            for r in sanitized:
                cur.execute(query, r)
                count += 1
        conn.commit()
    return (count, 0)

# ----------------------------
# CLI
# ----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Import/refresh equities (strict 7-column upsert).")
    p.add_argument("--source", choices=["csv", "euronext"], default="csv", help="Data source to use.")
    p.add_argument("--csv", dest="csv_path", default=DEFAULT_CSV_PATH, help="CSV path (for --source=csv).")
    p.add_argument("--markets", nargs="*", default=[], help="Markets/MICs to filter on (for --source=euronext).")
    p.add_argument("--dry-run", action="store_true", help="Parse & prepare but do not write to DB.")
    return p

def main():
    args = build_parser().parse_args()

    if args.source == "csv":
        csv_path = os.path.abspath(args.csv_path)
        logging.info("Loading CSV: %s", csv_path)
        rows = read_csv_rows(csv_path)
    else:
        rows = fetch_from_euronext(args.markets)

    logging.info("Prepared %d row(s) for upsert.", len(rows))
    n_ok, _ = upsert_rows(rows, dry_run=args.dry_run)
    if args.dry_run:
        logging.info("DRY RUN complete. Rows ready: %d", n_ok)
    else:
        logging.info("Upsert complete. Rows written: %d", n_ok)

if __name__ == "__main__":
    main()
