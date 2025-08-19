# app_parquet.py
import duckdb
import pandas as pd
import streamlit as st
import altair as alt
from streamlit_autorefresh import st_autorefresh
from datetime import timedelta, datetime, timezone

DATA_GLOB = r"C:/park_water_tracker/landing/*.parquet"  # forward slashes ok
st.set_page_config(page_title="Water Telemetry (Parquet PoC)", layout="wide")

# Auto-refresh every 5 seconds
st_autorefresh(interval=5_000, key="refresh")

# Controls
c1, c2, c3 = st.columns(3)
with c1:
    window_minutes = st.slider("Window (minutes)", 15, 360, 60, step=5)
with c2:
    downsample = st.selectbox("Downsample", ["None", "1m", "5m"], index=1)
with c3:
    y_max = st.number_input("Y-axis max (total_flow)", value=350, step=10)

# Read data via DuckDB-in-memory (no DB file)
con = duckdb.connect(":memory:")

start_ts = pd.Timestamp(datetime.now(timezone.utc) - timedelta(minutes=window_minutes))

if downsample == "None":
    df = con.execute(f"""
        SELECT *
        FROM read_parquet('{DATA_GLOB}')
        WHERE event_ts_utc >= ?
        ORDER BY event_ts_utc
    """, [start_ts]).df()
else:
    bucket = {"1m": "60s", "5m": "300s"}[downsample]
    df = con.execute(f"""
        WITH b AS (
          SELECT time_bucket(INTERVAL '{bucket}', event_ts_utc) AS bucket_ts,
                 avg(flow_2in)   AS flow_2in,
                 avg(flow_8in)   AS flow_8in,
                 avg(tank_level) AS tank_level,
                 max(total_2in)  AS total_2in,
                 max(total_8in)  AS total_8in
          FROM read_parquet('{DATA_GLOB}')
          WHERE event_ts_utc >= ?
          GROUP BY 1
          ORDER BY 1
        )
        SELECT * FROM b
    """, [start_ts]).df().rename(columns={"bucket_ts": "event_ts_utc"})

st.title("🏔️ Olympic Park – Water Telemetry (Parquet PoC)")

if df.empty:
    st.info("Waiting for data…")
    st.stop()

# Prepare fields
df["event_ts_utc"] = pd.to_datetime(df["event_ts_utc"])
df["total_flow"] = df["flow_2in"] + df["flow_8in"]

# KPIs
latest = df.iloc[-1]
k1, k2, k3, k4 = st.columns(4)
k1.metric("Tank Level", f"{latest['tank_level']:.2f}")
k2.metric("Flow 2in", f"{latest['flow_2in']:.2f}")
k3.metric("Flow 8in", f"{latest['flow_8in']:.2f}")
k4.metric("Last Update (UTC)", pd.to_datetime(latest['event_ts_utc']).strftime("%H:%M:%S"))

# Tabs
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

        line_flow = alt.Chart(df).mark_line().encode(
            x="event_ts_utc:T",
            y=alt.Y("total_flow:Q", scale=alt.Scale(domain=[0, y_max]))
        )
        line_threshold = alt.Chart(pd.DataFrame({"y": [300]})).mark_rule(
            color="red", strokeDash=[5,5]
        ).encode(y="y:Q")

        st.altair_chart(line_flow + line_threshold, use_container_width=True)

    with c2:
        st.subheader("Incremental Totals (derived)")
        df2 = df.copy()
        df2["total_2in_delta"] = df2["total_2in"].diff().clip(lower=0)
        df2["total_8in_delta"] = df2["total_8in"].diff().clip(lower=0)
        st.line_chart(df2.set_index("event_ts_utc")[["total_2in_delta", "total_8in_delta"]])

st.caption("Auto-refresh ~5s · Parquet rolled ~every 60s · Source polled every 30s")
