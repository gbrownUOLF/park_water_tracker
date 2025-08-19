import duckdb
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import altair as alt
from datetime import timedelta, datetime, timezone

DB_PATH = "water.duckdb"
TABLE = "bronze_park_water_readings"

st.set_page_config(page_title="Water Telemetry (PoC)", layout="wide")

# Auto-refresh the page every 5 seconds
st_autorefresh(interval=5_000, key="autorefresh")

@st.cache_resource
def get_conn():
    con = duckdb.connect(DB_PATH, read_only=True)
    con.execute("PRAGMA threads=4")
    return con

con = get_conn()

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

df = con.execute(
    f"""
    SELECT *
    FROM {TABLE}
    WHERE event_ts_utc >= ?
    ORDER BY event_ts_utc
    """,
    [start_ts]
).df()

st.title("🏔️ Olympic Park – Water Telemetry (Local PoC)")

# KPIs
if not df.empty:
    latest = df.iloc[-1]
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Tank Level", f"{latest['tank_level']:.2f}")
    k2.metric("Flow 2in", f"{latest['flow_2in']:.2f}")
    k3.metric("Flow 8in", f"{latest['flow_8in']:.2f}")
    k4.metric("Last Update (UTC)", pd.to_datetime(latest['event_ts_utc']).strftime("%H:%M:%S"))
else:
    st.info("Waiting for data…")
    st.stop()

# Optional downsampling (DuckDB time_bucket)
if downsample != "None":
    bucket = {"1m": "60s", "5m": "300s"}[downsample]
    dfd = con.execute(
        f"""
        WITH b AS (
          SELECT time_bucket(INTERVAL '{bucket}', event_ts_utc) AS bucket_ts,
                 avg(flow_2in) AS flow_2in,
                 avg(flow_8in) AS flow_8in,
                 avg(tank_level) AS tank_level,
                 max(total_2in) AS total_2in,
                 max(total_8in) AS total_8in
          FROM {TABLE}
          WHERE event_ts_utc >= ?
          GROUP BY 1
          ORDER BY 1
        )
        SELECT * FROM b
        """,
        [start_ts]
    ).df()
    df = dfd.rename(columns={"bucket_ts": "event_ts_utc"})

# Transform for plotting
df["event_ts_utc"] = pd.to_datetime(df["event_ts_utc"])
# df = df.set_index("event_ts_utc")
df["total_flow"] = df["flow_2in"] + df["flow_8in"]

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
        line_flow = alt.Chart(df).mark_line(color="steelblue").encode(
            x="event_ts_utc:T",
            y=alt.Y("total_flow:Q", scale=alt.Scale(domain=[0,350]))
        )

        # Constant horizontal line at y=300
        line_threshold = alt.Chart(pd.DataFrame({"y": [300]})).mark_rule(
            color="red", strokeDash=[5,5]
        ).encode(
            y="y"
        )

        chart = (line_flow + line_threshold).properties(
            width=600, height=300
        )

        st.altair_chart(chart, use_container_width=True)
    with c2:
        if show_totals:
            st.subheader("Cumulative Totals (as reported)")
            st.line_chart(df.set_index("event_ts_utc")[["total_2in", "total_8in"]])
        else:
            # per-interval delta of totals (approximate usage)
            df2 = df.copy()
            df2["total_2in_delta"] = df2["total_2in"].diff().clip(lower=0)
            df2["total_8in_delta"] = df2["total_8in"].diff().clip(lower=0)
            st.subheader("Incremental Totals (derived per interval)")
            st.line_chart(df2.set_index("event_ts_utc")[["total_2in_delta", "total_8in_delta"]])

st.caption("Auto-refreshing every 5 seconds · Data source polled every 30 seconds by ingestor.py")
