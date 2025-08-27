from __future__ import annotations

from typing import Any, Sequence

import psycopg


class EquitiesRepoPg:
    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self.conn = conn

    def find_symbols(self, only: list[str] | None = None) -> list[str]:
        """
        Retourne la liste des symboles à traiter.
        """
        query = "SELECT symbol FROM equities"
        params: tuple[Any, ...] = ()
        if only:
            placeholders = ",".join(["%s"] * len(only))
            query += f" WHERE symbol IN ({placeholders})"
            params = tuple(only)

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows: list[Sequence[Any]] = cur.fetchall()

        return [str(r[0]) for r in rows]

    def mark_attempt(
        self,
        isin: str | None,
        symbol: str | None,
        *,
        success: bool,
        ticker: str | None,
        cnt_1y: int,
        cnt_total: int,
    ) -> None:
        """
        Journalise une tentative d’import.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO import_attempts
                    (isin, symbol, success, ticker, cnt_1y, cnt_total, ts)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                (isin, symbol, success, ticker, cnt_1y, cnt_total),
            )
        self.conn.commit()
