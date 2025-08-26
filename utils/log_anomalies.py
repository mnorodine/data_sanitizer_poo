import os
from datetime import datetime

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def log_anomalie(table, message):
    date_str = datetime.now().strftime("%Y-%m-%d")
    with open(os.path.join(LOG_DIR, f"anomalies_{date_str}.log"), "a") as f:
        f.write(f"[{datetime.now()}] [{table}] {message}\n")
