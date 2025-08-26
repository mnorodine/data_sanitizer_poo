#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_prices_layer2.py — QA couche 2

Ajouts:
- Recalcule cnt_total / cnt_1y / first_quote_at / last_quote_at depuis equities_prices
- Détecte cnt_1y_calc > cnt_total_calc
- Compare avec les champs de equities et écrit deux CSV
- Option --fix-counters pour mettre à jour equities

Usage:
  python -m scripts.validate_prices_layer2 \
      --dsn postgresql://pea_user@localhost/pea_db \
      --outdir reports \
      --fix-counters

Si --dsn est omis, essaie $DATABASE_URL puis
postgresql://pea_user@localhost/pea_db
"""

from __future__ import annotations
import os
import csv
import math
import argparse
from pathlib import Path
from datetime import date, timedelta

import psycopg
from psycopg.rows import dict_row


# -----------------------------
# Connexion
# -----------------------------
def get_conn(dsn: str | None) -> psycopg.Connection:
    if dsn:
        return psycopg.connect(dsn, row_factory=dict_row)
    env = os.getenv("DATABASE_URL")
    if env:
        return psycopg.connect(env, row_factory=dict_row)
    # fallback par défaut
    return psycopg.connect("postgresql://pea_user@localhost/pea_db", row_factory=dict_row)


# -----------------------------
# Utils CSV
# -----------------------------
def write_csv(outpath: Path, rows: list[dict], fieldnames: list[str]):
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with outpath.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


# -----------------------------
# Vérifications existantes (exemples minimaux)
# -----------------------------
def check_duplicates(conn) -> list[dict]:
    q = """
    SELECT isin, symbol, price_date, COUNT(*) AS n
    FROM equities_prices
    GROUP BY isin, symbol, price_date
    HAVING COUNT(*) > 1
    ORDER BY isin, symbol, price_date;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return list(cur.fetchall())


def check_bad_values(conn) -> list[dict]:
    q = """
    SELECT isin, symbol, price_date, open_price, high_price, low_price, close_price, volume
    FROM equities_prices
    WHERE
      (open_price  IS NOT NULL AND open_price  <= 0) OR
      (close_price IS NOT NULL AND close_price <= 0) OR
      (high_price  IS NOT NULL AND high_price  <= 0) OR
      (low_price   IS NOT NULL AND low_price   <= 0) OR
      (volume      IS NOT NULL AND volume      <  0)
    ORDER BY isin, symbol, price_date;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return list(cur.fetchall())


def check_flat_series(conn) -> list[dict]:
    # série plate sur 5 dernières clôtures
    q = """
    WITH last5 AS (
      SELECT isin, symbol, price_date, close_price,
             ROW_NUMBER() OVER (PARTITION BY isin, symbol ORDER BY price_date DESC) AS rn
      FROM equities_prices
    )
    , grouped AS (
      SELECT isin, symbol,
             COUNT(*) FILTER (WHERE rn <= 5 AND close_price IS NOT NULL) AS n5,
             MIN(close_price) FILTER (WHERE rn <= 5) AS mn,
             MAX(close_price) FILTER (WHERE rn <= 5) AS mx
      FROM last5
      GROUP BY isin, symbol
    )
    SELECT isin, symbol, n5, mn, mx
    FROM grouped
    WHERE n5 = 5 AND mn = mx
    ORDER BY isin, symbol;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return list(cur.fetchall())


def check_stale(conn, days: int = 14) -> list[dict]:
    q = """
    SELECT e.isin, e.symbol,
           MAX(p.price_date) AS last_price_date,
           CURRENT_DATE - MAX(p.price_date) AS days_since
    FROM equities e
    LEFT JOIN equities_prices p USING (isin, symbol)
    GROUP BY e.isin, e.symbol
    HAVING MAX(p.price_date) IS NULL OR CURRENT_DATE - MAX(p.price_date) >= %(days)s
    ORDER BY days_since DESC NULLS LAST, isin, symbol;
    """
    with conn.cursor() as cur:
        cur.execute(q, {"days": days})
        return list(cur.fetchall())


def check_w_date_future(conn) -> list[dict]:
    q = """
    SELECT isin, symbol, w_date
    FROM equities
    WHERE w_date IS NOT NULL AND w_date > CURRENT_DATE
    ORDER BY w_date DESC, isin, symbol;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return list(cur.fetchall())


# -----------------------------
# NOUVELLES vérifs / compteurs
# -----------------------------
def compute_counters(conn) -> list[dict]:
    """
    Recalcule cnt_total / cnt_1y / first_quote_at / last_quote_at depuis equities_prices.
    Retourne aussi les compteurs actuels stockés dans equities pour comparaison.
    """
    q = """
    WITH per AS (
      SELECT
        ep.isin,
        ep.symbol,
        COUNT(*)::int                            AS cnt_total_calc,
        COUNT(*) FILTER (
          WHERE ep.price_date >= (CURRENT_DATE - INTERVAL '365 days')
        )::int                                   AS cnt_1y_calc,
        MIN(ep.price_date)                       AS first_quote_at_calc,
        MAX(ep.price_date)                       AS last_quote_at_calc
      FROM equities_prices ep
      GROUP BY ep.isin, ep.symbol
    )
    SELECT
      e.isin, e.symbol,
      COALESCE(p.cnt_total_calc, 0)              AS cnt_total_calc,
      COALESCE(p.cnt_1y_calc, 0)                 AS cnt_1y_calc,
      p.first_quote_at_calc,
      p.last_quote_at_calc,
      e.cnt_total                                AS cnt_total_db,
      e.cnt_1y                                   AS cnt_1y_db,
      e.first_quote_at                           AS first_quote_at_db,
      e.last_quote_at                            AS last_quote_at_db
    FROM equities e
    LEFT JOIN per p ON p.isin = e.isin AND p.symbol = e.symbol
    ORDER BY e.isin, e.symbol;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return list(cur.fetchall())


def fix_counters(conn) -> int:
    """
    Mets à jour equities avec les compteurs recalculés (lorsqu'ils diffèrent).
    Retourne le nombre de lignes mises à jour.
    """
    q = """
    WITH per AS (
      SELECT
        ep.isin,
        ep.symbol,
        COUNT(*)::int                            AS cnt_total_calc,
        COUNT(*) FILTER (
          WHERE ep.price_date >= (CURRENT_DATE - INTERVAL '365 days')
        )::int                                   AS cnt_1y_calc,
        MIN(ep.price_date)                       AS first_quote_at_calc,
        MAX(ep.price_date)                       AS last_quote_at_calc
      FROM equities_prices ep
      GROUP BY ep.isin, ep.symbol
    ),
    diff AS (
      SELECT
        e.isin, e.symbol,
        COALESCE(p.cnt_total_calc, 0)   AS cnt_total_calc,
        COALESCE(p.cnt_1y_calc, 0)      AS cnt_1y_calc,
        p.first_quote_at_calc,
        p.last_quote_at_calc
      FROM equities e
      LEFT JOIN per p ON p.isin = e.isin AND p.symbol = e.symbol
      WHERE
        COALESCE(e.cnt_total, 0)        IS DISTINCT FROM COALESCE(p.cnt_total_calc, 0)
        OR COALESCE(e.cnt_1y, 0)        IS DISTINCT FROM COALESCE(p.cnt_1y_calc, 0)
        OR e.first_quote_at             IS DISTINCT FROM p.first_quote_at_calc
        OR e.last_quote_at              IS DISTINCT FROM p.last_quote_at_calc
    )
    UPDATE equities e
    SET
      cnt_total      = d.cnt_total_calc,
      cnt_1y         = d.cnt_1y_calc,
      first_quote_at = d.first_quote_at_calc,
      last_quote_at  = d.last_quote_at_calc
    FROM diff d
    WHERE e.isin = d.isin AND e.symbol = d.symbol;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.rowcount


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Validation couche 2 (equities_prices)")
    ap.add_argument("--dsn", help="DSN PostgreSQL", default=None)
    ap.add_argument("--outdir", default="reports", help="Dossier de sortie CSV")
    ap.add_argument("--fix-counters", action="store_true", help="Mettre à jour equities avec les compteurs recalculés")
    ap.add_argument("--stale-days", type=int, default=14, help="Jours calendaires pour 'stale'")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    conn = get_conn(args.dsn)
    try:
        # 1) Doublons
        dups = check_duplicates(conn)
        write_csv(
            outdir / "validation_layer2_duplicates.csv",
            dups,
            ["isin", "symbol", "price_date", "n"],
        )

        # 2) Valeurs aberrantes
        bads = check_bad_values(conn)
        write_csv(
            outdir / "validation_layer2_bad_values.csv",
            bads,
            ["isin", "symbol", "price_date", "open_price", "high_price", "low_price", "close_price", "volume"],
        )

        # 3) Séries plates
        flats = check_flat_series(conn)
        write_csv(
            outdir / "validation_layer2_flat_series.csv",
            flats,
            ["isin", "symbol", "n5", "mn", "mx"],
        )

        # 4) Stale
        stale = check_stale(conn, days=args.stale_days)
        write_csv(
            outdir / "validation_layer2_stale.csv",
            stale,
            ["isin", "symbol", "last_price_date", "days_since"],
        )

        # 5) w_date futur
        wfut = check_w_date_future(conn)
        write_csv(
            outdir / "validation_layer2_w_date_future.csv",
            wfut,
            ["isin", "symbol", "w_date"],
        )

        # 6) NOUVEAU: compteurs
        counters = compute_counters(conn)

        # 6.a) Anomalies intrinsèques: cnt_1y_calc > cnt_total_calc
        anomaly = [
            r for r in counters
            if (r["cnt_1y_calc"] or 0) > (r["cnt_total_calc"] or 0)
        ]
        write_csv(
            outdir / "validation_layer2_counters_anomaly.csv",
            anomaly,
            [
                "isin", "symbol",
                "cnt_total_calc", "cnt_1y_calc",
                "first_quote_at_calc", "last_quote_at_calc",
            ],
        )

        # 6.b) Mismatch DB vs calc
        mismatch = []
        for r in counters:
            if (
                (r["cnt_total_db"] or 0) != (r["cnt_total_calc"] or 0)
                or (r["cnt_1y_db"] or 0) != (r["cnt_1y_calc"] or 0)
                or r["first_quote_at_db"] != r["first_quote_at_calc"]
                or r["last_quote_at_db"] != r["last_quote_at_calc"]
            ):
                mismatch.append({
                    "isin": r["isin"],
                    "symbol": r["symbol"],
                    "cnt_total_db": r["cnt_total_db"],
                    "cnt_total_calc": r["cnt_total_calc"],
                    "cnt_1y_db": r["cnt_1y_db"],
                    "cnt_1y_calc": r["cnt_1y_calc"],
                    "first_quote_at_db": r["first_quote_at_db"],
                    "first_quote_at_calc": r["first_quote_at_calc"],
                    "last_quote_at_db": r["last_quote_at_db"],
                    "last_quote_at_calc": r["last_quote_at_calc"],
                })

        write_csv(
            outdir / "validation_layer2_counters_mismatch.csv",
            mismatch,
            [
                "isin", "symbol",
                "cnt_total_db", "cnt_total_calc",
                "cnt_1y_db", "cnt_1y_calc",
                "first_quote_at_db", "first_quote_at_calc",
                "last_quote_at_db", "last_quote_at_calc",
            ],
        )

        # 6.c) Optionnel: fixer les compteurs
        if args.fix_counters:
            updated = fix_counters(conn)
            conn.commit()
            print(f"[OK] Compteurs mis à jour dans equities: {updated} lignes modifiées.")
        else:
            conn.rollback()  # pas de side-effects si on n'a rien voulu modifier
            print("[INFO] Aucune mise à jour (utilise --fix-counters pour écrire dans equities).")

        print(f"[OK] Rapports écrits dans {outdir.resolve()}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
