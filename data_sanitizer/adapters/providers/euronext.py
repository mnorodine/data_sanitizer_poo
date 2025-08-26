# providers/euronext.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Generator, Iterable, Optional

import requests


@dataclass
class EuronextInstrument:
    """Représentation normalisée d’un instrument Euronext (pour table `equities`)."""
    isin: str
    ticker: str                  # symbole court (sans suffixe .PA) si dispo
    mic: str                     # XPAR, XAMS, XBRU, XETI, ...
    name: str
    currency: str
    listed: bool = True
    delisted_at: Optional[date] = None
    segment: Optional[str] = None
    data_source: str = "euronext"  # trace d’origine


# -------------------------
# Config & helpers
# -------------------------

DEFAULT_TIMEOUT = (10, 30)  # (connect, read)
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 2.0

EURONEXT_BASE = "https://live.euronext.com"  # point d’entrée public (scraping léger)
# NOTE: selon tes besoins, tu pourras remplacer par une API/dump officiel si dispo.


def _request_json(url: str, params: Optional[Dict] = None,
                  retries: int = DEFAULT_RETRIES) -> Dict:
    """GET JSON avec retry exponentiel (very light)"""
    delay = 1.0
    last_exc = None
    for _ in range(max(1, retries)):
        try:
            resp = requests.get(url, params=params or {}, timeout=DEFAULT_TIMEOUT, headers={
                "User-Agent": "data_sanitizer/euronext-importer"
            })
            resp.raise_for_status()
            # certaines pages retournent du HTML : à adapter si besoin
            ct = resp.headers.get("Content-Type", "")
            if "application/json" not in ct:
                # renvoyer {} et laisser le caller gérer
                return {}
            return resp.json()
        except Exception as e:
            last_exc = e
            time.sleep(delay)
            delay *= DEFAULT_BACKOFF
    # on remonte l’exception après les tentatives
    raise last_exc  # type: ignore[misc]


def _normalize_record(raw: Dict) -> Optional[EuronextInstrument]:
    """
    Convertit un enregistrement brut Euronext -> EuronextInstrument.

    Cette fonction est à ADAPTER selon le format réellement collecté (API/dump/HTML).
    Le but est d’obtenir un objet cohérent pour l’upsert dans `equities`.
    """
    # --- Exemple de mapping hypothétique (à adapter) ---
    isin = (raw.get("isin") or raw.get("ISIN") or "").strip()
    if not isin:
        return None

    name = (raw.get("name") or raw.get("instrumentName") or "").strip()
    symbol = (raw.get("symbol") or raw.get("mnemonic") or "").strip()  # ex: ORA
    mic = (raw.get("mic") or raw.get("market") or "").strip().upper()  # ex: XPAR
    currency = (raw.get("currency") or raw.get("tradingCurrency") or "EUR").strip().upper()

    listed = bool(raw.get("listed", True))
    delisted_at = raw.get("delisted_at")
    if isinstance(delisted_at, str):
        try:
            delisted_at = datetime.fromisoformat(delisted_at).date()
        except Exception:
            delisted_at = None

    segment = (raw.get("segment") or raw.get("marketSegment") or None)

    return EuronextInstrument(
        isin=isin,
        ticker=symbol,
        mic=mic,
        name=name,
        currency=currency,
        listed=listed,
        delisted_at=delisted_at,
        segment=segment,
        data_source="euronext",
    )


# -------------------------
# API publique du provider
# -------------------------

def list_instruments(since: Optional[date] = None,
                     mic: Optional[str] = None,
                     limit: Optional[int] = None) -> Generator[Dict, None, None]:
    """
    Génère des enregistrements normalisés (dict) pour upsert dans `equities`.

    - `since` : si tu disposes d’une manière de filtrer côté source (date de maj), applique-la.
    - `mic`   : filtre marché (XPAR, XBRU, XAMS, ...).
    - `limit` : borne pour tests/échantillons.

    Yield de dicts (clés prêtes pour l’upsert) :
      {
        "isin": "...", "ticker": "ORA", "mic": "XPAR", "name": "...", "currency": "EUR",
        "data_source":"euronext", "is_listed":true/false, "status_reason": "delisted"?
      }
    """
    # 1) Récupération : à adapter (API officielle, dump CSV, scraping léger…)
    #    -> Ici on montre le squelette : tu brancheras ta vraie collecte.
    records: Iterable[Dict] = []

    # EXEMPLE (placeholder): si tu disposes d’un dump local JSON
    # import json, pathlib
    # data_path = pathlib.Path("data/euronext_dump.json")
    # if data_path.exists():
    #     records = json.loads(data_path.read_text())

    # 2) Normalisation + filtres
    count = 0
    for raw in records:
        inst = _normalize_record(raw)
        if not inst:
            continue
        if mic and inst.mic and inst.mic != mic.upper():
            continue

        out = {
            "isin": inst.isin,
            "ticker": inst.ticker,        # symbole sans suffixe
            "mic": inst.mic,
            "name": inst.name,
            "currency": inst.currency,
            "data_source": inst.data_source,
            # champs utiles côté equities (si tu crées is_listed)
            "is_listed": bool(inst.listed),
            "status_reason": None if inst.listed else "delisted",
        }
        yield out

        count += 1
        if limit and count >= limit:
            break


# -------------------------
# Petit CLI de test (optionnel)
# -------------------------

if __name__ == "__main__":
    # Test rapide : affiche les 5 premiers d’XPAR si disponible dans la source branchée
    for i, rec in enumerate(list_instruments(mic="XPAR", limit=5), start=1):
        print(f"{i:02d}", rec)
