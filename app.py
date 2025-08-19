import duckdb
import pandas as pd
import streamlit as st
import time
from streamlit_autorefresh import st_autorefresh
import altair as alt
from datetime import timedelta, datetime, timezone

DB_PATH = "C:\park_water_tracker\water.duckdb"
TABLE = "bronze_park_water_readings"

st.set_page_config(page_title="Water Telemetry (PoC)", layout="wide")

# Auto-refresh the page every 5 seconds
st_autorefresh(interval=5_000, key="autorefresh")

@st.cache_resource
def get_conn():
    # Make sure DB_PATH is defined above
    return duckdb.connect(DB_PATH, read_only=True)

def safe_df(sql, params=None, tries=3, delay=0.3):
    con = get_conn()  # reuse the cached read-only connection
    for i in range(tries):
        try:
            return con.execute(sql, params or []).df()
        except Exception as e:
            if i == tries - 1:
                raise
            time.sleep(delay)


# Controls
colc1, colc2, colc3 = st.columns(3)
with colc1:
    window_minutes = st.slider("Window (minutes)", 15, 360, 60, step=5)
with colc2:
    show_totals = st.checkbox("Show cumulative totals", value=False)
with colc3:
    downsample = st.selectbox("Downsample", ["None", "1m", "5m"], index=1)

# Data
now_utc = datetime.now(timezone.utc)
start_ts = pd.Timestamp(now_utc - timedelta(minutes=window_minutes))

# Base pull (parameterized by start_ts)
df = safe_df(
    f"""
    SELECT *
    FROM {TABLE}
    WHERE event_ts_utc >= ?
    ORDER BY event_ts_utc
    """,
    [start_ts]
)

st.title("🏔️ Olympic Park – Water Telemetry (Local PoC)")

if df.empty:
    st.info("Waiting for data…")
    st.stop()

# Optional downsampling (DuckDB time_bucket)
if downsample != "None":
    bucket = {"1m": "60s", "5m": "300s"}[downsample]
    dfd = safe_df(
        f"""
        WITH b AS (
          SELECT time_bucket(INTERVAL '{bucket}', event_ts_utc) AS bucket_ts,
                 avg(flow_2in)     AS flow_2in,
                 avg(flow_8in)     AS flow_8in,
                 avg(tank_level)   AS tank_level,
                 max(total_2in)    AS total_2in,
                 max(total_8in)    AS total_8in
          FROM {TABLE}
          WHERE event_ts_utc >= ?
          GROUP BY 1
          ORDER BY 1
        )
        SELECT * FROM b
        """,
        [start_ts]
    )
    dfd = dfd.rename(columns={"bucket_ts": "event_ts_utc"})
    df = dfd

if df.empty:
    st.info("No data in the selected window yet.")
    st.stop()

# Transform for plotting
df["event_ts_utc"] = pd.to_datetime(df["event_ts_utc"])
df["total_flow"] = df["flow_2in"] + df["flow_8in"]

# KPIs (use latest row)
latest = df.iloc[-1]
k1, k2, k3, k4 = st.columns(4)
k1.metric("Tank Level", f"{latest['tank_level']:.2f}")
k2.metric("Flow 2in", f"{latest['flow_2in']:.2f}")
k3.metric("Flow 8in", f"{latest['flow_8in']:.2f}")
k4.metric("Last Update (UTC)", pd.to_datetime(latest['event_ts_utc']).strftime("%H:%M:%S"))

# Charts
tab1, tab2 = st.tabs(["Flows & Level", "Totals"])

with tab1:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Flow – 2in")
        st.line_chart(df.set_index("event_ts_utc")["flow_2in"])
    with c2:
        st.subheader("Flow – 8in")
        st.line_chart(df.set_index("event_ts_utc")["flow_8in"])
    st.subheader("Tank Level")
    st.line_chart(df.set_index("event_ts_utc")["tank_level"])

with tab2:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Total Consumption Watch")

        # Flow line
        line_flow = alt.Chart(df).mark_line().encode(
            x="event_ts_utc:T",
            y=alt.Y("total_flow:Q", scale=alt.Scale(domain=[0, 350]))
        )

        # Constant horizontal line at y=300
        line_threshold = alt.Chart(pd.DataFrame({"y": [300]})).mark_rule(
            color="red", strokeDash=[5, 5]
        ).encode(
            y="y:Q"
        )

        st.altair_chart((line_flow + line_threshold), use_container_width=True)

    with c2:
        if show_totals:
            st.subheader("Cumulative Totals (as reported)")
            st.line_chart(df.set_index("event_ts_utc")[["total_2in", "total_8in"]])
        else:
            df2 = df.copy()
            df2["total_2in_delta"] = df2["total_2in"].diff().clip(lower=0)
            df2["total_8in_delta"] = df2["total_8in"].diff().clip(lower=0)
            st.subheader("Incremental Totals (derived per interval)")
            st.line_chart(df2.set_index("event_ts_utc")[["total_2in_delta", "total_8in_delta"]])

st.caption("Auto-refreshing every 5 seconds · Data source polled every 30 seconds by ingestor.py")
