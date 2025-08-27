# data_sanitizer/adapters/db/common.py
from __future__ import annotations

import contextlib
from typing import Iterator

import psycopg

from data_sanitizer.config import get_settings


@contextlib.contextmanager
def get_pg() -> Iterator[psycopg.Connection]:
    """
    Ouvre une connexion Postgres à partir de DATABASE_URL (chargé via .env automatiquement).
    """
    settings = get_settings()
    try:
        conn = psycopg.connect(settings.database_url)
    except Exception as e:  # connexion impossible → message clair
        raise RuntimeError(
            "Connexion Postgres impossible. Vérifie DATABASE_URL dans ton .env "
            f"(valeur actuelle: {settings.database_url!r}). Détail: {e}"
        ) from e
    try:
        yield conn
    finally:
        with contextlib.suppress(Exception):
            conn.close()
