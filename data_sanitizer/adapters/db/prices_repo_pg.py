from __future__ import annotations

from typing import Any, Iterable, Sequence, Tuple

import psycopg


class PricesRepoPg:
    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self.conn = conn

    def ensure_schema(self) -> None:
        """
        Crée la table si besoin. Aucun retour.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS prices (
                    isin  text NOT NULL,
                    ts    timestamptz NOT NULL,
                    close numeric NOT NULL,
                    PRIMARY KEY (isin, ts)
                )
                """
            )
        self.conn.commit()

    def upsert_many(self, isin: str, rows: Iterable[Tuple[str, float]]) -> None:
        """
        Insère ou met à jour un lot (ts ISO, close).
        """
        with self.conn.cursor() as cur:
            for ts, close in rows:
                cur.execute(
                    """
                    INSERT INTO prices (isin, ts, close)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (isin, ts)
                    DO UPDATE SET close = EXCLUDED.close
                    """,
                    (isin, ts, close),
                )
        self.conn.commit()

    def last_price(self, isin: str) -> tuple[float, Any] | None:
        """
        Dernier (close, ts) pour un ISIN ou None si absent.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT close, ts
                FROM prices
                WHERE isin = %s
                ORDER BY ts DESC
                LIMIT 1
                """,
                (isin,),
            )
            row: Sequence[Any] | None = cur.fetchone()

        if row is None:
            return None

        price, ts = row[0], row[1]
        return float(price), ts
