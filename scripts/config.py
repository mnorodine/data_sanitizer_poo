# scripts/config.py
from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import psycopg



# Charge les variables d'environnement depuis .env (si présent)
load_dotenv()


# =========
# Constantes "projet"
# =========
# Seuil pour considérer un instrument "actif" (nb de séances cotées sur 365j)
ACTIVE_MIN_CNT_1Y: int = int(os.getenv("ACTIVE_MIN_CNT_1Y", "200"))
#  MIN_DATA_POINTS = int(os.getenv("MIN_DATA_POINTS", "200"))


# Fenêtre de validation "5d" (jours calendaires)
VALID_WINDOW_DAYS: int = int(os.getenv("VALID_WINDOW_DAYS", "5"))
VALID_MIN_QUOTES_5D: int = int(os.getenv("VALID_MIN_QUOTES_5D", "2"))

# Paramètres yfinance (si utilisés ailleurs)
YF_SLEEP_SECS: float = float(os.getenv("YF_SLEEP_SECS", "2.0"))
YF_MAX_RETRIES: int = int(os.getenv("YF_MAX_RETRIES", "3"))
YF_TIMEOUT_SECS: int = int(os.getenv("YF_TIMEOUT_SECS", "20"))

# Fuseau « métier »
APP_TZ_NAME: str = os.getenv("APP_TZ", "Europe/Paris")
APP_TZ = ZoneInfo(APP_TZ_NAME)


# =========
# DB config
# =========
@dataclass(frozen=True)
class DBConfig:
    # Option 1 : DSN complet (ex: postgresql://user:pass@host:5432/dbname?options=...)
    dsn: Optional[str] = os.getenv("PG_DSN") or os.getenv("DATABASE_URL")

    # Option 2 : Connexion TCP (mdp requis en général)
    host: Optional[str] = os.getenv("PGHOST")
    port: str = os.getenv("PGPORT", "5432")
    dbname: str = os.getenv("PGDATABASE", "postgres")
    user: Optional[str] = os.getenv("PGUSER")  # None -> user système
    password: Optional[str] = os.getenv("PGPASSWORD")  # None si absent

    # Par défaut on laisse psycopg3 gérer les transactions via les context managers
    autocommit: bool = False


DBCONF = DBConfig()


def get_pg_connection() -> psycopg.Connection:
    """
    Retourne une connection psycopg3 configurée selon, par ordre de priorité :
      1) PG_DSN / DATABASE_URL (DSN complet)
      2) Variables PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD (TCP)
      3) Socket Unix sans host (auth 'peer' possible), si PGHOST est absent

    -> Permet d'éviter l'erreur "fe_sendauth: no password supplied" quand on force
       TCP sans fournir de mot de passe, alors qu'une auth locale 'peer' suffirait.
    """
    # 1) DSN complet
    if DBCONF.dsn:
        return psycopg.connect(DBCONF.dsn)

    # 2) Connexion TCP explicite si PGHOST est défini
    if DBCONF.host:
        return psycopg.connect(
            host=DBCONF.host,
            port=DBCONF.port,
            dbname=DBCONF.dbname,
            user=DBCONF.user or os.getenv("USER"),
            password=DBCONF.password,
        )

    # 3) Socket Unix (ne PAS passer host) — utile en local Linux (auth peer)
    return psycopg.connect(
        dbname=DBCONF.dbname,
        user=DBCONF.user or os.getenv("USER"),
    )


@contextmanager
def pg_conn_ctx():
    """
    Context manager pratique si l'appelant préfère contrôler l'ouverture/fermeture
    explicitement, tout en gardant une sémantique claire.

    Exemple :
        with pg_conn_ctx() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    conn = get_pg_connection()
    try:
        yield conn
    finally:
        # Si on utilisait explicitement `with get_pg_connection() as conn:`,
        # psycopg gère déjà la fermeture à la sortie du bloc. Ici on s'assure
        # de fermer si on passe par ce wrapper.
        try:
            conn.close()
        except Exception:
            pass


# =========
# Helpers génériques
# =========
def now_paris() -> datetime:
    """Date/heure courante en Europe/Paris (timezone-aware)."""
    return datetime.now(tz=APP_TZ)


def env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    # constantes
    "ACTIVE_MIN_CNT_1Y",
    "VALID_WINDOW_DAYS",
    "VALID_MIN_QUOTES_5D",
    "YF_SLEEP_SECS",
    "YF_MAX_RETRIES",
    "YF_TIMEOUT_SECS",
    "APP_TZ_NAME",
    "APP_TZ",
    # DB
    "DBCONF",
    "get_pg_connection",
    "pg_conn_ctx",
    # helpers
    "now_paris",
    "env_bool",
]


# scripts/config.py

# Suffixes Yahoo Finance par MIC Euronext (et assimilés)
EURONEXT_MIC_TO_SUFFIX = {
    "XPAR": ".PA",  # Paris
    "XAMS": ".AS",  # Amsterdam
    "XBRU": ".BR",  # Brussels
    "XLIS": ".LS",  # Lisbon
    "XDUB": ".IR",  # Dublin
    "XMIL": ".MI",  # Milan
    "XOSL": ".OL",  # Oslo (Oslo Børs)
}

# (facultatif) Mapping par libellé de marché si tu en as besoin ailleurs
MARKET_TO_SUFFIX = {
    "Paris": ".PA",
    "Amsterdam": ".AS",
    "Brussels": ".BR",
    "Lisbon": ".LS",
    "Dublin": ".IR",
    "Milan": ".MI",
    "Oslo": ".OL",
}

def __mic_to_suffix(mic: str, default: str | None = None) -> str | None:
    
    """Retourne le suffixe Yahoo à partir d'un MIC Euronext."""
    if not mic:
        return default
    return EURONEXT_MIC_TO_SUFFIX.get(mic.upper(), default)
