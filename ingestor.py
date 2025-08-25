import sqlite3, requests, time, pandas as pd
from datetime import datetime, timezone

URL = "https://scada.mtregional.org/olympicpark"
DB_PATH = "C:\park_water_tracker\water_log.db"
POLL_SECS = 15

conn = sqlite3.connect(fr"{DB_PATH}", isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("""
             Create table if not exists water_log (
                event_ts_utc TEXT,
                p_date TEXT,
                flow_2in REAL,
                flow_8in REAL,
                tank_level REAL,
                total_2in REAL,
                total_8in REAL
             )
             """)

def fetch_row():
    r = requests.get(URL, timeout=10)
    r.raise_for_status()
    j = r.json()
    now = datetime.utcnow().isoformat(timespec='seconds')
    pdate = now[:10]
    return(
        now,
        pdate,
        j.get("Oly Park 2in Flow"),
        j.get("Oly Park 8in Flow"),
        j.get("Tank Level"),
        j.get("Oly Park 2in Total"),
        j.get("Oly Park 8in Total")
    )

while True:
    conn.execute("Insert into water_log values (?,?,?,?,?,?,?)", fetch_row())
    time.sleep(POLL_SECS)