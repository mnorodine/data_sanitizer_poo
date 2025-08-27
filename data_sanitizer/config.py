# data_sanitizer/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv, find_dotenv

# Charge automatiquement le .env s'il existe (une seule fois par process)
# override=False → n’écrase pas des variables déjà présentes dans l’environnement
load_dotenv(find_dotenv(usecwd=True), override=False)


@dataclass(frozen=True)
class Settings:
    database_url: str
    ds_price_read_view: str
    ds_price_write_table: str
    ds_price_date_col: str
    log_level: str
    request_pause_s: float
    yf_timeout_s: float

    @staticmethod
    def from_env() -> "Settings":
        def _get(name: str, default: Optional[str] = None, required: bool = False) -> str:
            v = os.getenv(name, default)
            if required and (v is None or v == ""):
                raise RuntimeError(
                    f"Variable d'environnement manquante: {name}. "
                    "Crée/complète ton fichier .env à la racine du projet."
                )
            return v  # type: ignore[return-value]

        return Settings(
            database_url=_get("DATABASE_URL", required=True),
            ds_price_read_view=_get("DS_PRICE_READ_VIEW", "v_prices_compat"),
            ds_price_write_table=_get("DS_PRICE_WRITE_TABLE", "equity_prices"),
            ds_price_date_col=_get("DS_PRICE_DATE_COL", "price_date"),
            log_level=_get("LOG_LEVEL", "INFO"),
            request_pause_s=float(_get("REQUEST_PAUSE_S", "0.6")),
            yf_timeout_s=float(_get("YF_TIMEOUT_S", "10")),
        )


# Helper simple pour tout le code
def get_settings() -> Settings:
    return Settings.from_env()
