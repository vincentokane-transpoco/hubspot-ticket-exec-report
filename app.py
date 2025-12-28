import os
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from sqlalchemy import create_engine


# ----------------------------
# Config
# ----------------------------
TZ = ZoneInfo("Europe/Dublin")
WORK_START = time(9, 0)
WORK_END = time(17, 0)

st.set_page_config(page_title="Support Ticket Executive Reporting", layout="wide")
st.title("Support Ticket Executive Reporting")

DATABASE_URL = os.environ["DATABASE_URL"]

# Make the driver explicit if you used psycopg v3
# (If your DATABASE_URL already starts with postgresql+psycopg:// this is fine too)
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"connect_timeout": 10},
)


# ----------------------------
# Business hours calculator (Mon–Fri 09:00–17:00, Europe/Dublin)
# ----------------------------
def business_hours_between(start: datetime, end: datetime) -> float:
    """
    Business hours between two timezone-aware datetimes.
    Counts Mon–Fri 09:00–17:00 only (no lunch subtraction).
    """
    if pd.isna(start) or pd.isna(end) or end <= start:
        return 0.0

    start = start.astimezone(TZ)
    end = end.astimezone(TZ)

    total = 0.0
    cur = start

    while cur.date() <= end.date():
        # Skip weekends
        if cur.weekday() >= 5:  # Sat/Sun
            cur = (cur.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            continue

        day_start = datetime.combine(cur.date(), WORK_START, tzinfo=TZ)
        day_end = datetime.combine(cur.date(), WORK_END, tzinfo=TZ)

        window_start = max(day_start, start)
        window_end = min(day_end, end)

        if window_end > window_start:
            total += (window_end - window_start).total_seconds() / 3600.0

        cur = (cur.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))

    return total


# ----------------------------
# Data loading
# ----------------------------
@st.cache_data(ttl=300)
def load_data():
    tickets = pd.read_sql("select * from tickets_snapshot", engine)
    owners = pd.read_sql("select * from owners", engine)
    weekly = pd.read_sql("select * from weekly_metrics", engine)

    # Parse datetimes (stored as timestamptz, but ensure UTC in pandas)
    tickets["created_at"] = pd.to_datetime(tickets.get("created_at"), utc=True, errors="coerce")
    tickets["closed_at"] = pd.to_datetime(tickets.get("closed_at"), utc=True, errors="coerce")
    tickets["updated_at"] = pd.to_datetime(tickets.get("updated_at"), utc=True, errors="coerce")

    weekly["week_start"] = pd.to_datetime(weekly.get("week_start"), errors="coerce")

    # Normalize ids/types
    if "owner_id" in tickets.columns:
        tickets["owner_id"] = tickets["owner_id"].fillna("Unassigned").astype(str)
    else:
        tickets["owner_id"] = "Unassigned"

    if "category" in tickets.columns:
        tickets["category"] = tickets["category"].fillna("Uncategorised")
    else:
        tickets["category"] = "Uncategorised"

    owners["owner_id"] = owners["owner_id"].astype(str)

    return tickets, owners, weekly


tickets, owners, weekly = load_data()


# ----------------------------
# Date range helpers (exec-friendly)
# ----------------------------
def start_of_week(dt: datetime) -> datetime:
    dt = dt.astimezone(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return dt - timedelta(days=dt.weekday())  # Monday


def last_week_range(now: datetime):
    this_week_start = start_of_week(now)
    prev_week_start = this_week_start - timedelta(days=7)
    return prev_week_start, this_week_start


def last_month_range(now: datetime):
    now = now.astimezone(TZ)
    first_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if first_this_month.month == 1:
        prev_month_start = first_this_month.replace(year=first_this_month.year - 1, month=12)
    else:
        prev_month_start = first_this_month.replace(month=first_this_month.month - 1)
    return prev_month_start, first_this_month


# ----------------------------
# Sidebar filters
# ----------------------------
st.sidebar.header("Filters")

period = st.sidebar.selectbox("Period", ["Last week", "Last month", "Custom"])
now = datetime.now(TZ)

if period == "Last week":
    start_dt, end_dt = last_week_range(now)
elif period == "Last month":
    start_dt, end_dt = last_month_range(now)
else:
    c1, c2 = st.sidebar.columns(2)
    with c1:
        sd = st.date_input("Start", value=(now.date() - timedelta(days=30)))
    with c2:
        ed = st.date_input("End", value=now.date())
    start_dt = datetime.combine(sd, datetime.min.time(), tzinfo=TZ)
    end_dt = datetime.combine(ed, datetime.min.time(), tzinfo=TZ)

st.sidebar.caption(f"{start_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d} (Europe/Dublin)")

# Category filter
categories = ["All"] + sorted(tickets["category"].unique().tolist())
sel_cat = st.sidebar.selectbox("Category", categories)

# Agent filter
owner_map = owners.set_index("owner_id")["full_name"].to_dict()
tickets["owner_name"] = tickets["owner_id"].map(owner_map).fillna("Unassigned")
agents = ["All"] + sorted(tickets["owner_name"].unique().tolist())
sel_agent = st.sidebar.selectbox("Agent", agents)

# Apply filters
t = tickets.copy()
if sel_cat != "All":
    t = t[t["category"] == sel_cat]
if sel_agent != "All":
    t = t[t["owner_name"] == sel_agent]

# Build effective close time:
# - use closed_at when present
# - else (closed stage) fallback to updated_at (hs_lastmodifieddate) as proxy
t["closed_effective_at"] = t["closed_at"]
mask_fallback = t["closed_effective_at"].isna() & (t.get("is_closed", False) == True)
t.loc[mask_fallback, "closed_effective_at"] = t.loc[mask_fallback, "updated_at"]

# Selected period window in UTC for comparisons
start_utc = start_dt.astimezone(ZoneInfo("UTC"))
end_utc = end_dt.astimezone(ZoneInfo("UTC"))

# ----------------------------
# Period slices
# ----------------------------
opened = t[(t["created_at"] >= start_utc) & (t["created_at"] < end_utc)]

closed = t[
    (t.get("is_closed", False) == True)
    & (t["closed_effective_at"].notna())
    & (t["closed_effective_at"] >= start_utc)
    & (t["closed_effective_at"] < end_utc)
]

# backlog at end of period: created before end AND not closed before end OR not in closed stage
backlog = t[
    (t["created_at"] < end_utc)
    & ((t["closed_effective_at"].isna()) | (t["closed_effective_at"] >= end_utc) | (t.get("is_closed", False) == False))
]

# ----------------------------
# Resolution time (business hours) for closed tickets in period
# ----------------------------
if len(closed) > 0:
    closed = closed.copy()
    closed["resolution_bh_hours"] = closed.apply(
        lambda r: business_hours_between(
            r["created_at"].to_pydatetime(),
            r["closed_effective_at"].to_pydatetime(),
        ),
        axis=1,
    )
    closed["resolution_bh_hours"] = pd.to_numeric(closed["resolution_bh_hours"], errors="coerce")
else:
    closed = closed.copy()
    closed["resolution_bh_hours"] = pd.Series(dtype="float")


# ----------------------------
# KPI tiles
# ----------------------------
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Opened", len(opened))
k2.metric("Closed", len(closed))
k3.metric("Backlog (end of period)", len(backlog))

if closed["resolution_bh_hours"].notna().any():
    med = float(closed["resolution_bh_hours"].median())
    p90 = float(closed["resolution_bh_hours"].quantile(0.9))
    k4.metric("Resolution time (median, bh)", f"{med:.1f}h")
    k5.metric("Resolution time (P90, bh)", f"{p90:.1f}h")
else:
    k4.metric("Resolution time (median, bh)", "—")
    k5.metric("Resolution time (P90, bh)", "—")

st.divider()


# ----------------------------
# Categories table
# ----------------------------
st.subheader("Categories")
cat_tbl = pd.DataFrame(
    {
        "Opened": opened["category"].value_counts(),
        "Closed": closed["category"].value_counts(),
        "Backlog": backlog["category"].value_counts(),
    }
).fillna(0).astype(int).sort_values(by=["Backlog", "Opened"], ascending=False)

st.dataframe(cat_tbl, use_container_width=True)

st.divider()


# ----------------------------
# Trends (weekly) – uses precomputed weekly_metrics
# ----------------------------
st.subheader("Trends (weekly)")
w = weekly.copy()

if sel_cat != "All":
    w = w[w["category"] == sel_cat]

if sel_agent != "All":
    # Map selected agent back to owner_id(s)
    owner_ids = owners.loc[owners["full_name"] == sel_agent, "owner_id"].astype(str).tolist()
    if owner_ids:
        w = w[w["owner_id"].astype(str).isin(owner_ids)]
    else:
        w = w.iloc[0:0]  # empty

trend = (
    w.groupby("week_start")
    .agg(
        opened=("opened_count", "sum"),
        closed=("closed_count", "sum"),
        backlog=("backlog_end_count", "sum"),
        median_bh=("median_bh_close_hours", "median"),
        p90_bh=("p90_bh_close_hours", "median"),
    )
    .reset_index()
    .sort_values("week_start")
)

if len(trend) == 0:
    st.info("No weekly trend data found for the selected filters.")
else:
    fig = plt.figure()
    plt.plot(trend["week_start"], trend["opened"], label="Opened")
    plt.plot(trend["week_start"], trend["closed"], label="Closed")
    plt.plot(trend["week_start"], trend["backlog"], label="Backlog")
    plt.legend()
    st.pyplot(fig, clear_figure=True)

    fig2 = plt.figure()
    plt.plot(trend["week_start"], trend["median_bh"], label="Median resolution (bh)")
    plt.plot(trend["week_start"], trend["p90_bh"], label="P90 resolution (bh)")
    plt.legend()
    st.pyplot(fig2, clear_figure=True)

st.divider()


# ----------------------------
# Agent activity (selected period)
# ----------------------------
st.subheader("Agent activity (selected period)")
agent_tbl = (
    closed.groupby("owner_name")
    .agg(
        Closed=("ticket_id", "count"),
        Median_resolution_bh=("resolution_bh_hours", "median"),
        P90_resolution_bh=("resolution_bh_hours", lambda s: s.quantile(0.9) if len(s.dropna()) else float("nan")),
    )
    .join(backlog.groupby("owner_name").size().to_frame("Assigned backlog"), how="outer")
)

agent_tbl["Closed"] = agent_tbl["Closed"].fillna(0).astype(int)
agent_tbl["Assigned backlog"] = agent_tbl["Assigned backlog"].fillna(0).astype(int)

# Format numeric cols
for col in ["Median_resolution_bh", "P90_resolution_bh"]:
    if col in agent_tbl.columns:
        agent_tbl[col] = pd.to_numeric(agent_tbl[col], errors="coerce").round(1)

agent_tbl = agent_tbl.sort_values(by=["Assigned backlog", "Closed"], ascending=False)

st.dataframe(agent_tbl, use_container_width=True)
