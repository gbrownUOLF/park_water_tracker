import sqlite3
import pandas as pd
import streamlit as st
import time
from streamlit_autorefresh import st_autorefresh
import altair as alt
from datetime import timedelta, datetime, timezone

DB_PATH = r"C:\park_water_tracker\water_log.db"
TABLE = "water_log"  

st.set_page_config(page_title="Water Telemetry (PoC)", layout="wide")

# Auto-refresh the page every 5 seconds
st_autorefresh(interval=5_000, key="autorefresh")

@st.cache_resource
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
    [start_ts.isoformat()]   # store as ISO-8601 strings; matches the writer
)

st.title("🏔️ Olympic Park – Water Telemetry (Local PoC)")

if df.empty:
    st.info("Waiting for data…")
    st.stop()

# Optional downsampling (DuckDB time_bucket)
if downsample == "1m":
    bucket_size = 60
elif downsample == "5m":
    bucket_size = 300
else:
    bucket_size = None

if bucket_size:
    dfd = safe_df(
        """
        WITH b AS (
          SELECT datetime(strftime('%s', event_ts_utc) / ? * ? , 'unixepoch') AS bucket_ts,
                 avg(flow_2in)   AS flow_2in,
                 avg(flow_8in)   AS flow_8in,
                 avg(tank_level) AS tank_level,
                 max(total_2in)  AS total_2in,
                 max(total_8in)  AS total_8in
          FROM water_log
          WHERE event_ts_utc >= ?
          GROUP BY bucket_ts
          ORDER BY bucket_ts
        )
        SELECT * FROM b
        """,
        [bucket_size, bucket_size, start_ts.isoformat()]
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
    # Selector to toggle series on/off
    series_options = ["flow_2in", "flow_8in", "total_flow"]
    selected = st.multiselect(
        "Select lines to display",
        options=series_options,
        default=series_options
    )

    # Long-form for Altair
    df_long = (
        df[["event_ts_utc", "flow_2in", "flow_8in", "total_flow"]]
        .melt("event_ts_utc", var_name="series", value_name="value")
    )
    if selected:
        df_long = df_long[df_long["series"].isin(selected)]
    else:
        # If nothing selected, show empty frame to avoid errors
        df_long = df_long.iloc[0:0]

    # Optional: legend-based toggling too
    sel = alt.selection_point(fields=["series"], bind="legend", toggle=True)

    # Lines
    lines = (
        alt.Chart(df_long)
        .mark_line()
        .encode(
            x=alt.X("event_ts_utc:T", title="Time (UTC)"),
            y=alt.Y("value:Q", title="Flow (gpm)"),
            color=alt.Color("series:N", title="Series"),
            tooltip=[
                alt.Tooltip("event_ts_utc:T", title="Time (UTC)"),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("value:Q", title="Value", format=".2f"),
            ],
            opacity=alt.condition(sel, alt.value(1.0), alt.value(0.2)),
        )
        .add_params(sel)   # still valid in v5
    )

    # Static horizontal threshold at 300
    rule = (
        alt.Chart(pd.DataFrame({"y": [300]}))
        .mark_rule(strokeDash=[5, 5], color="red")
        .encode(y="y:Q")
    )

    st.subheader("Flows (2in, 8in, Total) with Threshold")
    st.altair_chart(lines + rule, use_container_width=True)

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

st.caption("Auto-refreshing every 5 seconds · Data source polled every 15 seconds by ingestor.py")
