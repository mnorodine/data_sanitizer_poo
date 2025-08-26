import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.db import get_connection
from utils.log_anomalies import log_anomalie

def corriger_dates(table, colonne):
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"""
                    UPDATE {table}
                    SET {colonne} = TO_DATE({colonne}, 'YYYY-MM-DD')
                    WHERE {colonne} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$';
                """)
                conn.commit()
            except Exception as e:
                log_anomalie(table, f"Erreur correction format date : {e}")
