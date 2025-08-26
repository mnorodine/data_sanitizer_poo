# data_sanitizer/config.py
from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv   # 👈

# Charge automatiquement .env (dans le répertoire courant ou au-dessus)
load_dotenv()

@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://pea_user@127.0.0.1:5432/pea_db")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    request_pause_s: float = float(os.getenv("REQUEST_PAUSE_S", "0.6"))
    yfinance_timeout_s: int = int(os.getenv("YF_TIMEOUT_S", "10"))

def get_settings() -> Settings:
    return Settings()
