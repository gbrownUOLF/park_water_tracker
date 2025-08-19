import duckdb


DB_PATH = "water.duckdb"
TABLE = "bronze_park_water_readings"

def get_conn():
    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("PRAGMA threads=4")
    return con

con = get_conn()


print(con.execute(f"SELECT * FROM {TABLE}").df())