import os
import pandas as pd
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]

TZ = ZoneInfo("Europe/Dublin")
WORK_START = time(9, 0)
WORK_END = time(17, 0)

def business_hours_between(start: datetime, end: datetime) -> float:
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

def week_start(dt: datetime) -> datetime:
    dt = dt.astimezone(TZ)
    monday = (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return monday

def build_weekly_metrics(df: pd.DataFrame, weeks_back: int = 26) -> pd.DataFrame:
    now = datetime.now(TZ)
    start = week_start(now) - timedelta(days=7 * weeks_back)
    end = week_start(now)

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["closed_at"] = pd.to_datetime(df["closed_at"], utc=True, errors="coerce")

    weeks = []
    cur = start
    while cur < end:
        weeks.append((cur, cur + timedelta(days=7)))
        cur = cur + timedelta(days=7)

    rows = []
    for ws, we in weeks:
        ws_utc = ws.astimezone(ZoneInfo("UTC"))
        we_utc = we.astimezone(ZoneInfo("UTC"))

        opened = df[(df["created_at"] >= ws_utc) & (df["created_at"] < we_utc)]
        closed = df[(df["is_closed"] == True) & (df["closed_at"].notna()) &
                    (df["closed_at"] >= ws_utc) & (df["closed_at"] < we_utc)]

        backlog = df[(df["created_at"] < we_utc) &
                     ((df["closed_at"].isna()) | (df["closed_at"] >= we_utc) | (df["is_closed"] == False))]

        if len(closed) > 0:
            bh = closed.apply(
                lambda r: business_hours_between(r["created_at"].to_pydatetime(), r["closed_at"].to_pydatetime()),
                axis=1
            )
            closed = closed.assign(bh_close_hours=bh)
        else:
            closed = closed.assign(bh_close_hours=pd.Series(dtype="float"))

        cats = pd.concat([
            opened[["category","owner_id"]].assign(opened_count=1),
            closed[["category","owner_id"]].assign(closed_count=1),
            backlog[["category","owner_id"]].assign(backlog_end_count=1),
        ], axis=0, ignore_index=True)

        grouped = cats.groupby(["category","owner_id"], dropna=False).sum(numeric_only=True).reset_index()

        if len(closed) > 0:
            stats = closed.groupby(["category","owner_id"], dropna=False)["bh_close_hours"].agg(
                median_bh_close_hours="median",
                p90_bh_close_hours=lambda s: s.quantile(0.9)
            ).reset_index()
            grouped = grouped.merge(stats, on=["category","owner_id"], how="left")
        else:
            grouped["median_bh_close_hours"] = None
            grouped["p90_bh_close_hours"] = None

        grouped.insert(0, "week_start", ws.date())
        rows.append(grouped)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    out["category"] = out["category"].fillna("Uncategorised")
    out["owner_id"] = out["owner_id"].fillna("Unassigned").astype(str)
    return out

def main():
    engine = create_engine(DATABASE_URL)
    df = pd.read_sql("select * from tickets_snapshot", engine)
    df["owner_id"] = df["owner_id"].fillna("Unassigned").astype(str)
    df["category"] = df["category"].fillna("Uncategorised")

    weekly = build_weekly_metrics(df, weeks_back=26)

    with engine.begin() as conn:
        conn.execute(text("delete from weekly_metrics"))
        weekly.to_sql("weekly_metrics", conn, if_exists="append", index=False, method="multi")

    print(f"Built weekly_metrics rows: {len(weekly)}")

if __name__ == "__main__":
    main()
