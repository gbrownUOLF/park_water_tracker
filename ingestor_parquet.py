# ingestor_parquet_single.py  — append to ONE parquet file
import time
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path

URL = "https://scada.mtregional.org/olympicpark"
PARQUET_PATH = Path(r"C:\park_water_tracker\landing\water_log.parquet")
POLL_SECS = 15

# Define a fixed schema so appends are consistent
schema = pa.schema([
    ("event_ts_utc", pa.timestamp("us")),  # store UTC as naive timestamp
    ("p_date",       pa.date32()),
    ("flow_2in",     pa.float64()),
    ("flow_8in",     pa.float64()),
    ("tank_level",   pa.float64()),
    ("total_2in",    pa.float64()),
    ("total_8in",    pa.float64()),
])

def fetch_row_dict():
    r = requests.get(URL, timeout=10)
    r.raise_for_status()
    j = r.json()
    now = pd.Timestamp.utcnow()  # naive UTC
    return {
        "event_ts_utc": now,
        "p_date": now.date(),
        "flow_2in": j.get("Oly Park 2in Flow"),
        "flow_8in": j.get("Oly Park 8in Flow"),
        "tank_level": j.get("Tank Level"),
        "total_2in": j.get("Oly Park 2in Total"),
        "total_8in": j.get("Oly Park 8in Total"),
    }

# Create writer if needed; otherwise open and append
writer = None
try:
    if not PARQUET_PATH.exists():
        writer = pq.ParquetWriter(PARQUET_PATH, schema)

    while True:
        row = fetch_row_dict()
        df = pd.DataFrame([row])
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        if writer is None:
            # File already exists, so open a writer in overwrite mode and append going forward
            writer = pq.ParquetWriter(PARQUET_PATH, schema, use_dictionary=True)
        writer.write_table(table)

        time.sleep(POLL_SECS)
finally:
    if writer is not None:
        writer.close()
