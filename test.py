import sqlite3
import time
import pandas as pd


DB_PATH = r"C:\park_water_tracker\water_log.db"
TABLE = "water_log"

def get_conn():
    # Use URI so we can open read-only and enable shared cache
    # Note: 'timeout' helps if a checkpoint is happening; readers are non-blocking in WAL mode.
    uri = f"file:{DB_PATH}?mode=ro&cache=shared"
    return sqlite3.connect(uri, uri=True, timeout=1.0, check_same_thread=False)

def safe_df(sql, params=None, tries=3, delay=0.3):
    con = get_conn()
    for i in range(tries):
        try:
            # pandas handles parameter substitution with sqlite3 DB-API
            return pd.read_sql_query(sql, con, params=params or [])
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(delay)

df = safe_df(
    f"""
    SELECT *
    FROM {TABLE}
    LIMIT 10
    """   # store as ISO-8601 strings; matches the writer
)

print(df)
