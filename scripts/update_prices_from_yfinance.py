import time
import logging
import pandas as pd
from typing import List, Tuple, Optional
from dateutil.relativedelta import relativedelta
import yfinance as yf
from scripts.config import get_pg_connection, APP_TZ, YF_SLEEP_SECS, EURONEXT_MIC_TO_SUFFIX, MARKET_TO_SUFFIX


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
API_NAME = "yfinance"

FALLBACK_SUFFIXES = list({*EURONEXT_MIC_TO_SUFFIX.values(), *MARKET_TO_SUFFIX.values()})

from datetime import datetime
from typing import List, Tuple, Optional

cnt__1y = 0
cnt__total = 0

hist = None



def insert_equities_prices_batch(isin: str, symbol: str, df: pd.DataFrame) -> int:
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
    with get_pg_connection() as conn:
        
        with conn.cursor() as cur:

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







def get_target_equities() -> List[Tuple[str, str, Optional[str], Optional[str]]]:
    """
    Retourne (isin, symbol, market, last_trade_mic_time) pour les lignes sans ticker/api,
    avec date w_date antérieure à aujourd'hui.
    """
    
    today = datetime.now(APP_TZ).date()

    with get_pg_connection() as conn:
        
        with conn.cursor() as cur:
            
            cur.execute("""
                SELECT isin, symbol, market
                FROM equities WHERE w_date < %s
                ORDER BY symbol
            """, (today,))
            
            return cur.fetchall()


def normalize_hist_index_to_date(hist_df):
    df = hist_df.copy()
    # 1) retire la timezone éventuelle
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(None)
    # 2) compare en 'date' (pas datetime)
    df["date"] = df.index.date
    return df



def try_yf_ticker(ticker: str) -> bool:
    global cnt__1y
    global cnt__total
    global hist

    try:
        
        data = yf.Ticker(ticker).history(period="5d")
        
        if not data.empty:
            today = datetime.now(APP_TZ).date()
            hist = yf.download(ticker, period="max", progress=False)
            df = normalize_hist_index_to_date(hist)
         

            hist = hist.copy()
            hist.index = pd.to_datetime(hist.index).tz_localize(None)
            

            dates_pydate = pd.to_datetime(hist.index.to_series()).dt.date
            start = today - relativedelta(years=1)
            cnt__1y = int((pd.Series(dates_pydate) >= start).sum())
            cnt__total = len(df)

            # On valide le Ticker si on a au moins 2 cotations 
            return len(data) >= 2
    
    except Exception as e:
        logging.debug(f"[debug] yfinance error for {ticker}: {e}")
        
    return False



def find_best_ticker(symbol: str, market: Optional[str]) -> Optional[str]:
    tried = set()
    # 1) Try all known Euronext suffixes
    for s in FALLBACK_SUFFIXES:
        cand = f"{symbol}.{s.strip('.').upper()}"
        if cand in tried:
            continue
        if try_yf_ticker(cand):
            return cand

    # 2) As a last resort, raw symbol
    if try_yf_ticker(symbol):
        return symbol

    return None

def update_equity_ticker(isin: str, symbol: str, ticker: str):
    today = datetime.now(APP_TZ).date()

    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE equities
                SET ticker = %s, api = %s, is_valid = True, w_date = %s, cnt_1y=%s, cnt_total=%s
            WHERE isin = %s AND symbol = %s
            """, (ticker, API_NAME, today, cnt__1y, cnt__total, isin, symbol))

        conn.commit()






def main():
    global hist
    
    equities = get_target_equities()
    

    logging.info(f"{len(equities)} actions à analyser (détection yfinance)")
    k = 0
    
    for i, (isin, symbol, market) in enumerate(equities, start=1):
        best = find_best_ticker(symbol.strip().upper(), market)
        
        if best:
            k += 1      
                  
            update_equity_ticker(isin, symbol, best)
            insert_equities_prices_batch(isin, symbol, hist)
            logging.info(f"[{k}/{i} -> {len(equities)}] ✅ {symbol} → {best}")
        else:
            update_equity_ticker(isin, symbol, None)
            
            logging.info(f"[{i}/{len(equities)}] ❌ {symbol} : aucun ticker yfinance valide trouvé")
        time.sleep(YF_SLEEP_SECS)

if __name__ == "__main__":
    main()
