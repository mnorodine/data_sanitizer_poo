# data_sanitizer/adapters/providers/euronext.py
from __future__ import annotations

from typing import Any

import requests

BASE_URL: str = "https://live.euronext.com"  # ajuste au besoin


def _headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": "data-sanitizer/3",
    }


def _json_or_raise(resp: requests.Response) -> dict[str, Any]:
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    return data


def search(session: requests.Session, keyword: str) -> dict[str, Any]:
    """
    Recherche d’instruments par mot-clé. Renvoie le JSON brut.
    """
    url = f"{BASE_URL}/api/search"
    r = session.get(url, headers=_headers(), params={"keyword": keyword}, timeout=30)
    return _json_or_raise(r)


def instrument(session: requests.Session, isin: str) -> dict[str, Any]:
    """
    Détails d’un instrument par ISIN. Renvoie le JSON brut.
    """
    url = f"{BASE_URL}/api/instrument/{isin}"
    r = session.get(url, headers=_headers(), timeout=30)
    return _json_or_raise(r)
