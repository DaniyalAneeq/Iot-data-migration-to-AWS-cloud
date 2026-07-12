import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from streamlit_autorefresh import st_autorefresh
from datetime import date

st.set_page_config(page_title="IoT Pipeline Dashboard", layout="wide", page_icon="📡")
st_autorefresh(interval=30 * 1000, key="datarefresh")

st.markdown("""
<style>
[data-testid="stMetric"] {
    background-color: #F8FAFC;
    border: 1px solid #E2E8F0;
    padding: 16px 16px 8px 16px;
    border-radius: 12px;
}
[data-testid="stMetricLabel"] { font-weight: 600; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        role=st.secrets["snowflake"]["role"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
    )


@st.cache_data(ttl=25)
def run_query(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    return df


def sql_list(values):
    if not values:
        return "'__none__'"
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


# ================= SIDEBAR =================
st.sidebar.title("📡 Pipeline Control")
st.sidebar.caption("PostgreSQL → Debezium → Kafka → Snowflake → dbt")
st.sidebar.divider()

devices_df = run_query("SELECT DISTINCT device_id FROM gold_daily_device_metrics ORDER BY device_id")
all_devices = devices_df["DEVICE_ID"].tolist() if not devices_df.empty else []

bounds_df = run_query("SELECT MIN(event_date) AS min_d, MAX(event_date) AS max_d FROM gold_daily_device_metrics")
if not bounds_df.empty and bounds_df.iloc[0]["MIN_D"] is not None:
    min_date, max_date = bounds_df.iloc[0]["MIN_D"], bounds_df.iloc[0]["MAX_D"]
else:
    min_date = max_date = date.today()

st.sidebar.subheader("Filters")
selected_devices = st.sidebar.multiselect("Devices", options=all_devices, default=all_devices)
date_range = st.sidebar.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
start_date, end_date = date_range if isinstance(date_range, tuple) and len(date_range) == 2 else (date_range, date_range)

st.sidebar.divider()
st.sidebar.caption(f"🔄 Auto-refreshes every 30s\n\nLast loaded: {pd.Timestamp.now().strftime('%H:%M:%S')}")

dev_filter = f"device_id IN ({sql_list(selected_devices)})"
date_filter = f"event_date BETWEEN '{start_date}' AND '{end_date}'"
where_clause = f"{dev_filter} AND {date_filter}"

# ================= HEADER =================
st.title("📡 IoT Real-Time Pipeline Dashboard")
st.caption("PostgreSQL → Debezium CDC → Kafka → Snowflake → dbt Medallion Architecture")

# ================= KPI ROW =================
kpi_df = run_query(f"""
    SELECT
        COUNT(DISTINCT device_id) AS total_devices,
        SUM(reading_count) AS total_readings,
        ROUND(AVG(avg_ingestion_lag_seconds), 2) AS avg_lag_seconds,
        ROUND(100.0 * SUM(on_time_readings) / NULLIF(SUM(reading_count), 0), 2) AS on_time_pct
    FROM gold_daily_device_metrics WHERE {where_clause}
""")

live_df = run_query(f"""
    SELECT COUNT(*) AS live_count
    FROM CLEAN.silver_iot_events
    WHERE {dev_filter} AND ingested_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
""")

c1, c2, c3, c4 = st.columns(4)
if not kpi_df.empty and kpi_df.iloc[0]["TOTAL_READINGS"] is not None:
    row = kpi_df.iloc[0]
    live_count = int(live_df.iloc[0]["LIVE_COUNT"]) if not live_df.empty else 0
    c1.metric("📟 Active Devices", int(row["TOTAL_DEVICES"]))
    c2.metric("🟢 Live (last 5 min)", live_count, help="Readings ingested in the last 5 minutes — proves the pipeline is streaming live, not static.")
    c3.metric("⏱️ Avg Ingestion Lag", f'{row["AVG_LAG_SECONDS"]}s', help="Time between a reading being generated and landing in Postgres.")
    c4.metric("✅ On-Time Rate", f'{row["ON_TIME_PCT"]}%', help="Share of readings that arrived within 30 seconds.")
else:
    st.info("Waiting for data matching your filters...")

st.divider()

# ================= TABS =================
tab_overview, tab_devices, tab_quality, tab_explorer = st.tabs(
    ["📊 Overview", "🗺️ Devices & Map", "🩺 Data Quality", "📋 Raw Data Explorer"]
)

# ---------- TAB 1: OVERVIEW ----------
with tab_overview:
    col_trend, col_top = st.columns([3, 2])

    with col_trend:
        st.subheader("Reading Volume Over Time")
        st.caption("Total readings per day across selected devices.")
        trend_df = run_query(f"""
            SELECT event_date, SUM(reading_count) AS total_readings
            FROM gold_daily_device_metrics WHERE {where_clause}
            GROUP BY event_date ORDER BY event_date
        """)
        if not trend_df.empty:
            fig = px.area(trend_df, x="EVENT_DATE", y="TOTAL_READINGS",
                           labels={"EVENT_DATE": "Date", "TOTAL_READINGS": "Readings"})
            fig.update_traces(line_color="#2563EB", fillcolor="rgba(37,99,235,0.15)")
            fig.update_layout(height=340, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data for this selection.")

    with col_top:
        st.subheader("Top Devices")
        st.caption("Ranked by total readings sent.")
        top_df = run_query(f"""
            SELECT device_id, SUM(reading_count) AS total_readings
            FROM gold_daily_device_metrics WHERE {where_clause}
            GROUP BY device_id ORDER BY total_readings DESC LIMIT 10
        """)
        if not top_df.empty:
            fig = px.bar(top_df.sort_values("TOTAL_READINGS"), x="TOTAL_READINGS", y="DEVICE_ID",
                          orientation="h", text="TOTAL_READINGS", color="TOTAL_READINGS",
                          color_continuous_scale="Blues",
                          labels={"TOTAL_READINGS": "Readings", "DEVICE_ID": "Device"})
            fig.update_traces(textposition="outside")
            fig.update_layout(height=340, showlegend=False, coloraxis_showscale=False, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No device data for this selection.")

# ---------- TAB 2: DEVICES & MAP ----------
with tab_devices:
    st.subheader("Device Locations")
    st.caption("Bubble size and color both reflect total readings — bigger and brighter means more active.")
    map_df = run_query(f"""
        SELECT device_id, AVG(avg_latitude) AS lat, AVG(avg_longitude) AS lon, SUM(reading_count) AS total_readings
        FROM gold_daily_device_metrics WHERE {where_clause}
        GROUP BY device_id ORDER BY total_readings DESC
    """)
    if not map_df.empty:
        fig = px.scatter_mapbox(
            map_df, lat="LAT", lon="LON", size="TOTAL_READINGS", color="TOTAL_READINGS",
            hover_name="DEVICE_ID", color_continuous_scale="Viridis", zoom=10, height=420,
            mapbox_style="open-street-map",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No location data for this selection.")

    st.subheader("Device Summary Table")
    table_df = run_query(f"""
        SELECT
            device_id, SUM(reading_count) AS readings,
            ROUND(AVG(avg_ingestion_lag_seconds), 2) AS avg_lag_s,
            ROUND(100.0 * SUM(on_time_readings) / NULLIF(SUM(reading_count), 0), 1) AS on_time_pct,
            MAX(last_reading_at) AS last_seen
        FROM gold_daily_device_metrics WHERE {where_clause}
        GROUP BY device_id ORDER BY readings DESC
    """)
    if not table_df.empty:
        st.dataframe(
            table_df,
            column_config={
                "DEVICE_ID": st.column_config.TextColumn("Device"),
                "READINGS": st.column_config.NumberColumn("Total Readings", format="%d"),
                "AVG_LAG_S": st.column_config.NumberColumn("Avg Lag (s)", format="%.1f"),
                "ON_TIME_PCT": st.column_config.ProgressColumn("On-Time %", min_value=0, max_value=100, format="%.0f%%"),
                "LAST_SEEN": st.column_config.DatetimeColumn("Last Seen", format="YYYY-MM-DD HH:mm:ss"),
            },
            hide_index=True, use_container_width=True,
        )
    else:
        st.info("No device data for this selection.")

# ---------- TAB 3: DATA QUALITY ----------
with tab_quality:
    col_pie, col_offenders = st.columns([2, 3])

    with col_pie:
        st.subheader("Overall Health")
        q_df = run_query(f"""
            SELECT SUM(on_time_readings) AS on_time, SUM(delayed_readings) AS delayed,
                   SUM(invalid_location_readings) AS invalid_location
            FROM gold_daily_device_metrics WHERE {where_clause}
        """)
        if not q_df.empty and q_df.iloc[0].sum() > 0:
            q = q_df.iloc[0]
            pie_df = pd.DataFrame({
                "Status": ["On-Time", "Delayed", "Invalid Location"],
                "Count": [q["ON_TIME"], q["DELAYED"], q["INVALID_LOCATION"]],
            })
            fig = px.pie(pie_df, names="Status", values="Count", hole=0.5, color="Status",
                         color_discrete_map={"On-Time": "#22C55E", "Delayed": "#F59E0B", "Invalid Location": "#EF4444"})
            fig.update_traces(textinfo="percent+label")
            fig.update_layout(height=360, showlegend=False, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No quality data for this selection.")

    with col_offenders:
        st.subheader("Devices With the Most Delayed Readings")
        st.caption("Sorted worst-first — these devices are the best candidates to investigate.")
        offenders_df = run_query(f"""
            SELECT device_id, SUM(delayed_readings) AS delayed,
                   SUM(invalid_location_readings) AS invalid_location,
                   ROUND(100.0 * SUM(on_time_readings) / NULLIF(SUM(reading_count), 0), 1) AS on_time_pct
            FROM gold_daily_device_metrics WHERE {where_clause}
            GROUP BY device_id HAVING SUM(delayed_readings) + SUM(invalid_location_readings) > 0
            ORDER BY delayed DESC LIMIT 10
        """)
        if not offenders_df.empty:
            st.dataframe(
                offenders_df,
                column_config={
                    "DEVICE_ID": st.column_config.TextColumn("Device"),
                    "DELAYED": st.column_config.NumberColumn("Delayed Readings"),
                    "INVALID_LOCATION": st.column_config.NumberColumn("Invalid Location"),
                    "ON_TIME_PCT": st.column_config.ProgressColumn("On-Time %", min_value=0, max_value=100, format="%.0f%%"),
                },
                hide_index=True, use_container_width=True,
            )
        else:
            st.success("No delayed or invalid readings for this selection — pipeline is fully healthy. ✅")

# ---------- TAB 4: RAW DATA EXPLORER ----------
with tab_explorer:
    st.subheader("Latest Silver-Layer Events")
    st.caption("The most recent 300 cleaned, flattened events — useful for spot-checking pipeline output.")
    explorer_df = run_query(f"""
        SELECT device_id, event_timestamp, latitude, longitude,
               ingestion_lag_seconds, data_quality_severity, cdc_operation
        FROM CLEAN.silver_iot_events       
        WHERE {dev_filter} AND CAST(event_timestamp AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY event_timestamp DESC LIMIT 300
    """)
    if not explorer_df.empty:
        icon_map = {"on_time": "🟢 On-time", "delayed": "🟡 Delayed", "invalid_location": "🔴 Invalid"}
        explorer_df["STATUS"] = explorer_df["DATA_QUALITY_SEVERITY"].map(icon_map).fillna("⚪ Unknown")
        st.dataframe(
            explorer_df[["DEVICE_ID", "EVENT_TIMESTAMP", "LATITUDE", "LONGITUDE",
                         "INGESTION_LAG_SECONDS", "STATUS", "CDC_OPERATION"]],
            column_config={
                "DEVICE_ID": st.column_config.TextColumn("Device"),
                "EVENT_TIMESTAMP": st.column_config.DatetimeColumn("Event Time", format="YYYY-MM-DD HH:mm:ss"),
                "LATITUDE": st.column_config.NumberColumn("Latitude", format="%.6f"),
                "LONGITUDE": st.column_config.NumberColumn("Longitude", format="%.6f"),
                "INGESTION_LAG_SECONDS": st.column_config.NumberColumn("Lag (s)", format="%.1f"),
                "STATUS": st.column_config.TextColumn("Quality"),
                "CDC_OPERATION": st.column_config.TextColumn("CDC Op"),
            },
            hide_index=True, use_container_width=True, height=500,
        )
    else:
        st.info("No events for this selection.")

st.divider()
st.caption(f"Data current as of {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} · Auto-refreshes every 30 seconds")
