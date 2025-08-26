from __future__ import annotations

from contextlib import contextmanager
import psycopg
from data_sanitizer.config import get_settings

@contextmanager
def get_pg():
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
