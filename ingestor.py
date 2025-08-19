import time
import requests
import duckdb
from datetime import datetime, timezone
from pathlib import Path

URL = "https://scada.mtregional.org/olympicpark"
DB_PATH = r"C:\park_water_tracker\water.duckdb"   # <-- raw string
TABLE = "bronze_park_water_readings"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  event_ts_utc TIMESTAMP,
  p_date DATE,
  flow_2in DOUBLE,
  flow_8in DOUBLE,
  tank_level DOUBLE,
  total_2in DOUBLE,
  total_8in DOUBLE
);
"""

INSERT_SQL = f"""
INSERT INTO {TABLE} (
  event_ts_utc, p_date, flow_2in, flow_8in, tank_level, total_2in, total_8in
) VALUES (?, ?, ?, ?, ?, ?, ?);
"""

# Ensure folder exists
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

# One-time bootstrap
with duckdb.connect(DB_PATH) as con:
    con.execute(DDL)

def fetch():
    r = requests.get(URL, timeout=10)
    r.raise_for_status()
    j = r.json()
    now = datetime.now(timezone.utc)
    return (
        now,
        now.date(),
        j.get("Oly Park 2in Flow"),
        j.get("Oly Park 8in Flow"),
        j.get("Tank Level"),
        j.get("Oly Park 2in Total"),
        j.get("Oly Park 8in Total"),
    )

while True:
    try:
        row = fetch()
        with duckdb.connect(DB_PATH) as con:
            con.execute(INSERT_SQL, row)
            con.commit()
    except Exception as e:
        print("[ingestor warn]", e)
    time.sleep(30)
