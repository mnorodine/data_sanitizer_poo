import time
import logging
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Dict
import psycopg
import pandas as pd
import yfinance as yf

from scripts.config import get_pg_connection, ACTIVE_MIN_CNT_1Y, YF_SLEEP_SECS, now_paris

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

YEARS_BACK = [1, 2, 4, 8, 16, 32]

# --- helper en haut du fichier ---
from datetime import datetime, timedelta
from scripts.config import APP_TZ, VALID_WINDOW_DAYS


# --- Normalisation de l'historique yfinance ---
import pandas as pd


def normalize_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise l'historique yfinance pour un seul ticker :
    - Aplati les colonnes MultiIndex en colonnes simples (Open/High/Low/Close/Adj Close/Volume).
    - Garantit un DatetimeIndex naïf (sans timezone), trié.
    - Force les types numériques, ajoute 'Adj Close' si absent (copie de 'Close').
    - Supprime les lignes entièrement vides sur (Open, High, Low, Close, Volume).
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # 0) Colonnes MultiIndex ? (ex: ('Open','ACCB.BR'), ('Close','ACCB.BR'), ...)
    if isinstance(out.columns, pd.MultiIndex):
        # Si une seule valeur au 2e niveau (un seul ticker), on enlève ce niveau.
        lvl1_unique = out.columns.get_level_values(-1).unique()
        if len(lvl1_unique) == 1:
            out.columns = out.columns.droplevel(-1)
        else:
            # Sélectionne la première valeur de niveau -1 (par sécurité)
            first = lvl1_unique[0]
            out = out.xs(first, axis=1, level=-1)

    # 1) Index -> DatetimeIndex naïf, trié
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()]
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_convert("UTC").tz_localize(None)
    out.sort_index(inplace=True)

    # 2) Normaliser noms de colonnes vers la convention yfinance standard
    rename_map = {
        "open": "Open", "high": "High", "low": "Low", "close": "Close",
        "adj close": "Adj Close", "adj_close": "Adj Close", "adjclose": "Adj Close",
        "volume": "Volume",
    }
    out.rename(columns={c: rename_map.get(str(c).lower(), c) for c in out.columns}, inplace=True)

    # 3) Ajoute 'Adj Close' si absent (copie 'Close')
    if "Adj Close" not in out.columns and "Close" in out.columns:
        out["Adj Close"] = out["Close"]

    # 4) Conversions de types (sélectionne uniquement si la colonne existe et est 1D)
    for c in ["Open", "High", "Low", "Close", "Adj Close"]:
        if c in out.columns and not isinstance(out[c], pd.DataFrame):
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if "Volume" in out.columns and not isinstance(out["Volume"], pd.DataFrame):
        out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce", downcast="integer")

    # 5) Supprimer lignes totalement vides (sur colonnes clés présentes)
    base_cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in out.columns]
    if base_cols:
        out = out.loc[~out[base_cols].isna().all(axis=1)]

    return out






def normalize_hist_index_to_date(hist_df):
    df = hist_df.copy()
    # 1) retire la timezone éventuelle
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(None)
    # 2) compare en 'date' (pas datetime)
    df["date"] = df.index.date
    return df

    # --- après chaque téléchargement yfinance ---
    hist = yf.download(ticker, period="max", progress=False)  # exemple
    df = normalize_hist_index_to_date(hist)

    today = datetime.now(APP_TZ).date()
    window_start = today - timedelta(days=VALID_WINDOW_DAYS)

    cnt5d = df[df["date"] >= window_start]["Close"].notna().count()
    # ... utilise cnt5d pour is_valid

def compute_counts(dates: pd.Series, today: date) -> Dict[int, int]:
    counts = {}
    # ✅ Convertir en date Python pur
    # Garantir un objet date (si on nous passe un datetime par inadvertance)
    if hasattr(today, "date"):
        today = today.date()

        
    dates_pydate = pd.to_datetime(dates).dt.date
    for i, years in enumerate(YEARS_BACK):
        start = today - relativedelta(years=years)
        counts[i] = int((pd.Series(dates_pydate) >= start).sum())
    return counts

def insert_equities_prices_batch(cur, isin: str, symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = []
    for dt, row in df.iterrows():
        price_date = pd.to_datetime(dt).date()
        rows.append((
            isin, symbol, price_date,
            float(row.get("Open")) if pd.notna(row.get("Open")) else None,
            float(row.get("Close")) if pd.notna(row.get("Close")) else None,
            float(row.get("High")) if pd.notna(row.get("High")) else None,
            float(row.get("Low"))  if pd.notna(row.get("Low")) else None,
            int(row.get("Volume")) if pd.notna(row.get("Volume")) else None,
        ))
    cur.executemany("""
        INSERT INTO equities_prices (
            isin, symbol, price_date,
            open_price, close_price, high_price, low_price, volume
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (isin, symbol, price_date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            close_price = EXCLUDED.close_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            volume      = EXCLUDED.volume
    """, rows)
    return len(rows)

def validate_equities():


    today = now_paris().date()
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT isin, symbol, ticker
                  FROM equities
                 WHERE ticker IS NOT NULL
                 ORDER BY isin, symbol
            """)
            equities = cur.fetchall()

        
        total = len(equities)
        logging.info(f"🔍 {total} actions à valider (1 appel yfinance par ticker).")


        for i, (isin, symbol, ticker) in enumerate(equities, start=1):


            logging.info(f"[{i}/{total}] Téléchargement: {ticker}")
            hist = yf.download(ticker, period="max", progress=False, auto_adjust=False)
            hist = normalize_history(hist)

            
            try:

                #logging.info(f"[{i}/{total}] Téléchargement: {ticker}")
                #hist = yf.download(ticker, period="max", progress=False, auto_adjust=False)

            
                #hist = normalize_history(hist)

            
                if hist.empty:
                    is_valid = False
                    cnt_1y = 0
                    cnt_total = 0
                
                else:
                    # convert index to naive dates
                    hist = hist.copy()
                    hist.index = pd.to_datetime(hist.index).tz_localize(None)
                    cnts = compute_counts(hist.index.to_series(), today)
                    is_valid = cnts[0] >= ACTIVE_MIN_CNT_1Y

                
                # Update equities counts + valid
                with get_pg_connection() as conn2:
                    with conn2.cursor() as cur2:
                        cur2.execute("""
                            UPDATE equities SET
                                is_valid = %s, cnt_1y = %s
                            WHERE isin = %s AND symbol = %s
                        """, ( is_valid, cnts.get(0, 0), cnts.get(5, 0), isin, symbol ))

                        # Insert price history (always insert if we have data)
                        inserted = 0
                        if not hist.empty:
                            inserted = insert_equities_prices_batch(cur2, isin, symbol, hist)

                    conn2.commit()
                
                logging.info(f"[{i}/{total}] {ticker} → valid={is_valid}, insérés={inserted}")

            except Exception as e:
                logging.error(f"[{i}/{total}] Erreur {ticker} ({isin},{symbol}): {e}")

            time.sleep(YF_SLEEP_SECS)

    logging.info("✅ Validation terminée.")

if __name__ == "__main__":
    validate_equities()
