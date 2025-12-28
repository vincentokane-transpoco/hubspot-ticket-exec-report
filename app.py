import os
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine

from datetime import time

WORK_START = time(9, 0)
WORK_END = time(17, 0)

def business_hours_between(start: datetime, end: datetime) -> float:
    """
    Business hours between two timezone-aware datetimes in Europe/Dublin.
    Counts Mon–Fri 09:00–17:00 only.
    """
    if pd.isna(start) or pd.isna(end) or end <= start:
        return 0.0

    start = start.astimezone(TZ)
    end = end.astimezone(TZ)

    total = 0.0
    cur = start

    while cur.date() <= end.date():
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


DATABASE_URL = os.environ["DATABASE_URL"]
TZ = ZoneInfo("Europe/Dublin")

st.set_page_config(page_title="Support Ticket Executive Reporting", layout="wide")
st.title("Support Ticket Executive Reporting")

engine = create_engine(DATABASE_URL)

@st.cache_data(ttl=300)
def load():
    tickets = pd.read_sql("select * from tickets_snapshot", engine)
    owners = pd.read_sql("select * from owners", engine)
    weekly = pd.read_sql("select * from weekly_metrics", engine)
    tickets["created_at"] = pd.to_datetime(tickets["created_at"], utc=True, errors="coerce")
    tickets["closed_at"] = pd.to_datetime(tickets["closed_at"], utc=True, errors="coerce")
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])
    return tickets, owners, weekly

tickets, owners, weekly = load()

def start_of_week(dt):
    dt = dt.astimezone(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return dt - timedelta(days=dt.weekday())

def last_week_range(now):
    this_week = start_of_week(now)
    return this_week - timedelta(days=7), this_week

def last_month_range(now):
    now = now.astimezone(TZ)
    first_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if first_this_month.month == 1:
        prev_start = first_this_month.replace(year=first_this_month.year - 1, month=12)
    else:
        prev_start = first_this_month.replace(month=first_this_month.month - 1)
    return prev_start, first_this_month

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

st.sidebar.caption(f"{start_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d}")

tickets["category"] = tickets["category"].fillna("Uncategorised")
categories = ["All"] + sorted(tickets["category"].unique().tolist())
sel_cat = st.sidebar.selectbox("Category", categories)

owners["owner_id"] = owners["owner_id"].astype(str)
owner_map = owners.set_index("owner_id")["full_name"].to_dict()
tickets["owner_id"] = tickets["owner_id"].fillna("Unassigned").astype(str)
tickets["owner_name"] = tickets["owner_id"].map(owner_map).fillna("Unassigned")
agents = ["All"] + sorted(tickets["owner_name"].unique().tolist())
sel_agent = st.sidebar.selectbox("Agent", agents)

t = tickets.copy()
if sel_cat != "All":
    t = t[t["category"] == sel_cat]
if sel_agent != "All":
    t = t[t["owner_name"] == sel_agent]

start_utc = start_dt.astimezone(ZoneInfo("UTC"))
end_utc = end_dt.astimezone(ZoneInfo("UTC"))

opened = t[(t["created_at"] >= start_utc) & (t["created_at"] < end_utc)]
closed = t[(t["is_closed"] == True) & (t["closed_at"].notna()) & (t["closed_at"] >= start_utc) & (t["closed_at"] < end_utc)]
if len(closed) > 0:
    closed = closed.copy()
    closed["resolution_bh_hours"] = closed.apply(
        lambda r: business_hours_between(r["created_at"].to_pydatetime(), r["closed_at"].to_pydatetime()),
        axis=1
    )
else:
    closed = closed.copy()
    closed["resolution_bh_hours"] = pd.Series(dtype="float")

backlog = t[(t["created_at"] < end_utc) & ((t["closed_at"].isna()) | (t["closed_at"] >= end_utc) | (t["is_closed"] == False))]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Opened", len(opened))
c2.metric("Closed", len(closed))
c3.metric("Backlog (end of period)", len(backlog))

if closed["resolution_bh_hours"].notna().any():
    med = float(closed["resolution_bh_hours"].median())
    p90 = float(closed["resolution_bh_hours"].quantile(0.9))
    c4.metric("Resolution time (median, bh)", f"{med:.1f}h")
    c5.metric("Resolution time (P90, bh)", f"{p90:.1f}h")
else:
    c4.metric("Resolution time (median, bh)", "—")
    c5.metric("Resolution time (P90, bh)", "—")


st.divider()

st.subheader("Categories")
cat_tbl = pd.DataFrame({
    "Opened": opened["category"].value_counts(),
    "Closed": closed["category"].value_counts(),
    "Backlog": backlog["category"].value_counts(),
}).fillna(0).astype(int).sort_values(by=["Backlog","Opened"], ascending=False)
st.dataframe(cat_tbl, use_container_width=True)

st.divider()

st.subheader("Trends (weekly)")
w = weekly.copy()
if sel_cat != "All":
    w = w[w["category"] == sel_cat]
if sel_agent != "All":
    owner_ids = owners[owners["full_name"] == sel_agent]["owner_id"].astype(str).tolist()
    w = w[w["owner_id"].isin(owner_ids)]

trend = w.groupby("week_start").agg(
    opened=("opened_count","sum"),
    closed=("closed_count","sum"),
    backlog=("backlog_end_count","sum"),
    median_bh=("median_bh_close_hours","median"),
).reset_index().sort_values("week_start")

fig = plt.figure()
plt.plot(trend["week_start"], trend["opened"], label="Opened")
plt.plot(trend["week_start"], trend["closed"], label="Closed")
plt.plot(trend["week_start"], trend["backlog"], label="Backlog")
plt.legend()
st.pyplot(fig, clear_figure=True)

fig2 = plt.figure()
plt.plot(trend["week_start"], trend["median_bh"], label="Median business-hours to close")
plt.legend()
st.pyplot(fig2, clear_figure=True)

st.divider()

st.subheader("Agent activity (selected period)")
agent_tbl = closed.groupby("owner_name").size().to_frame("Closed").join(
    backlog.groupby("owner_name").size().to_frame("Assigned backlog"),
    how="outer"
).fillna(0).astype(int).sort_values(by=["Assigned backlog","Closed"], ascending=False)
st.dataframe(agent_tbl, use_container_width=True)
