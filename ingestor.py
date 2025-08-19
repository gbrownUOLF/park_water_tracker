import time
import requests
import duckdb
from datetime import datetime, timezone

URL = "https://scada.mtregional.org/olympicpark"
DB_PATH = "water.duckdb"
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

INSERT = f"""
INSERT INTO {TABLE} (
  event_ts_utc, p_date, flow_2in, flow_8in, tank_level, total_2in, total_8in
) VALUES (?, ?, ?, ?, ?, ?, ?);
"""

def fetch():
    r = requests.get(URL, timeout=10)
    r.raise_for_status()
    return r.json()

def normalize(j):
    return {
        "flow_2in": j.get("Oly Park 2in Flow"),
        "flow_8in": j.get("Oly Park 8in Flow"),
        "tank_level": j.get("Tank Level"),
        "total_2in": j.get("Oly Park 2in Total"),
        "total_8in": j.get("Oly Park 8in Total"),
    }

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("PRAGMA threads=4")
    con.execute(DDL)

    while True:
        try:
            j = fetch()
            vals = normalize(j)
            ts = datetime.now(timezone.utc)
            row = (
                ts,
                ts.date(),
                vals["flow_2in"],
                vals["flow_8in"],
                vals["tank_level"],
                vals["total_2in"],
                vals["total_8in"],
            )
            con.execute(INSERT, row)
            con.commit()
            # print(f"Inserted at {ts.isoformat()}")
        except Exception as e:
            print(f"[warn] {e}")
        time.sleep(30)

if __name__ == "__main__":
    main()