from __future__ import annotations

from typing import Optional, Sequence, Tuple
from datetime import date
from psycopg import sql

from data_sanitizer.ports.prices_repo import PricesRepo
from data_sanitizer.domain.models import PriceBar
from .common import get_pg
import os

class PricesRepoPg(PricesRepo):
    def __init__(self):
        # Lire les variables d'env à l'initialisation (après éventuel chargement .env)
        self.read_view   = os.getenv("DS_PRICE_READ_VIEW", "equities_prices")   # ex: v_prices_compat
        self.date_col    = os.getenv("DS_PRICE_DATE_COL", "date")               # ex: price_date
        self.write_table = os.getenv("DS_PRICE_WRITE_TABLE", "equities_prices") # ex: equity_prices

    # ---- Reads ----

    def last_price_date(self, isin: str, symbol: str) -> Optional[date]:
        with get_pg() as conn:
            with conn.cursor() as cur:
                q = sql.SQL(
                    "SELECT MAX({d}) FROM {v} WHERE isin=%s AND symbol=%s"
                ).format(
                    d=sql.Identifier(self.date_col),
                    v=sql.Identifier(self.read_view),
                )
                cur.execute(q, (isin, symbol))
                row = cur.fetchone()
                return row[0] if row and row[0] else None

    # ---- Writes ----
    # On reste en DELETE+INSERT (compatible même sans contrainte unique explicite côté DB).
    # On passera à ON CONFLICT une fois la chaîne validée.

    def upsert_bars(self, isin: str, symbol: str, bars: Sequence[PriceBar]) -> int:
        if not bars:
            return 0
        inserted = 0
        with get_pg() as conn:
            with conn.cursor() as cur:
                for b in bars:
                    q = sql.SQL(
                        "INSERT INTO {t} "
                        "(isin, symbol, {d}, open_price, high_price, low_price, close_price, adj_close, volume) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT (isin, symbol, {d}) DO UPDATE SET "
                        "open_price = EXCLUDED.open_price, "
                        "high_price = EXCLUDED.high_price, "
                        "low_price  = EXCLUDED.low_price, "
                        "close_price= EXCLUDED.close_price, "
                        "adj_close  = EXCLUDED.adj_close, "
                        "volume     = EXCLUDED.volume"
                    ).format(
                        t=sql.Identifier(self.write_table),
                        d=sql.Identifier(self.date_col),
                    )
                    cur.execute(
                        q, (isin, symbol, b.date, b.open, b.high, b.low, b.close, b.adj_close, b.volume)
                    )
                    inserted += 1
        return inserted

    # ---- Maintenance helpers ----

    def recompute_counts(self, isin: str, symbol: str) -> Tuple[int, int]:
        with get_pg() as conn:
            with conn.cursor() as cur:
                q = sql.SQL(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE {d} >= CURRENT_DATE - INTERVAL '365 days') "
                    "FROM {v} WHERE isin=%s AND symbol=%s"
                ).format(
                    d=sql.Identifier(self.date_col),
                    v=sql.Identifier(self.read_view),
                )
                cur.execute(q, (isin, symbol))
                row = cur.fetchone()
                cnt_total = int(row[0] or 0)
                cnt_1y = int(row[1] or 0)

                cur.execute(
                    "UPDATE equities SET cnt_total=%s, cnt_1y=%s WHERE isin=%s AND symbol=%s",
                    (cnt_total, cnt_1y, isin, symbol),
                )
                return cnt_total, cnt_1y

    def update_bounds(self, isin: str, symbol: str) -> None:
        with get_pg() as conn:
            with conn.cursor() as cur:
                q = sql.SQL(
                    "UPDATE equities e "
                    "SET first_quote_at = s.min_d, last_quote_at = s.max_d "
                    "FROM (SELECT MIN({d}) AS min_d, MAX({d}) AS max_d FROM {v} WHERE isin=%s AND symbol=%s) s "
                    "WHERE e.isin=%s AND e.symbol=%s"
                ).format(
                    d=sql.Identifier(self.date_col),
                    v=sql.Identifier(self.read_view),
                )
                cur.execute(q, (isin, symbol, isin, symbol))
