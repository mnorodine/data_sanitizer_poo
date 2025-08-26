from __future__ import annotations

from typing import Iterable, Optional, Tuple
from psycopg import sql
from .common import get_pg

class EquitiesRepoPg:
    """
    Adapter DB pour la table equities.
    - Sélectionne les cibles (is_valid & is_active)
    - Récupère un ticker existant (si présent)
    - Marque la tentative (maj cnt_1y, cnt_total, ticker, w_date si dispo, last_checked_at)
    """

    def _equities_columns(self) -> set[str]:
        with get_pg() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name='equities'
                """)
                return {r[0] for r in cur.fetchall()}

    # --- Sélecteurs de cibles ---
    def fetch_targets(self, limit: Optional[int] = None, only: Optional[Iterable[str]] = None) -> Iterable[Tuple[str,str]]:
        """
        Retourne (isin, symbol) à traiter : is_valid & is_active vrais (ou NULL -> true),
        optionnellement filtré par liste 'only', et limité par 'limit'.
        """
        with get_pg() as conn:
            with conn.cursor() as cur:
                base = "SELECT isin, symbol FROM equities WHERE COALESCE(is_valid, true) AND COALESCE(is_active, true)"
                params = []
                if only:
                    syms = list(only)
                    placeholders = ",".join(["%s"] * len(syms))
                    base += f" AND symbol IN ({placeholders})"
                    params.extend(syms)
                base += " ORDER BY symbol"
                if limit:
                    base += " LIMIT %s"
                    params.append(limit)
                cur.execute(base, tuple(params))
                for r in cur.fetchall():
                    yield (r[0], r[1])

    # Alias attendu par UpdatePricesService
    def get_targets(self, limit: Optional[int] = None, only: Optional[Iterable[str]] = None) -> Iterable[Tuple[str,str]]:
        return self.fetch_targets(limit=limit, only=only)

    # --- Lecture ticker existant ---
    def get_existing_ticker(self, isin: str, symbol: str) -> Optional[str]:
        """
        Renvoie le ticker déjà connu pour (isin, symbol) s'il existe, sinon None.
        """
        with get_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ticker FROM equities WHERE isin=%s AND symbol=%s",
                    (isin, symbol),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return row[0] or None

    # --- Marquage de tentative / méta ---
    def mark_attempt(
        self,
        isin: str,
        symbol: str,
        success: bool,
        ticker: Optional[str],
        cnt_1y: int,
        cnt_total: int,
        touch_w_date: bool = True,
    ) -> None:
        cols = self._equities_columns()
        set_clauses = [
            sql.SQL("ticker = COALESCE(%s, ticker)"),
            sql.SQL("cnt_1y = %s"),
            sql.SQL("cnt_total = %s"),
            sql.SQL("last_checked_at = NOW()"),
        ]
        params = [ticker, cnt_1y, cnt_total]

        if touch_w_date and "w_date" in cols:
            set_clauses.append(sql.SQL("w_date = CURRENT_DATE"))

        q = sql.SQL("UPDATE equities SET {sets} WHERE isin=%s AND symbol=%s").format(
            sets=sql.SQL(", ").join(set_clauses)
        )
        params.extend([isin, symbol])
        with get_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(q, tuple(params))
