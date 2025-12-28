import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

BASE = "https://api.hubapi.com"
HEADERS = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

# Your confirmed internal property name:
CATEGORY_PROP = os.environ.get("CATEGORY_PROP", "hs_ticket_category")

TICKET_PROPS = [
    "subject",
    "createdate",
    "closedate",
    "hs_pipeline",
    "hs_pipeline_stage",
    "hubspot_owner_id",
    CATEGORY_PROP,
    "hs_lastmodifieddate",
]

def get_closed_stage_ids() -> set[str]:
    url = f"{BASE}/crm/v3/pipelines/tickets"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    closed = set()
    for p in data.get("results", []):
        for s in p.get("stages", []):
            md = s.get("metadata", {}) or {}
            if str(md.get("isClosed", "")).lower() == "true":
                closed.add(s["id"])
    return closed

def fetch_all_tickets() -> list[dict]:
    url = f"{BASE}/crm/v3/objects/tickets"
    after = None
    out = []

    while True:
        params = {"limit": 100, "properties": ",".join(TICKET_PROPS)}
        if after:
            params["after"] = after

        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()

        out.extend(payload.get("results", []))

        after = (payload.get("paging", {}) or {}).get("next", {}).get("after")
        if not after:
            break

    return out

def fetch_owners() -> list[dict]:
    url = f"{BASE}/crm/v3/owners"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

def main():
    engine = create_engine(DATABASE_URL)

    closed_stage_ids = get_closed_stage_ids()
    tickets = fetch_all_tickets()

    rows = []
    for t in tickets:
        props = t.get("properties", {}) or {}
        stage_id = props.get("hs_pipeline_stage")
        rows.append({
            "ticket_id": t["id"],
            "subject": props.get("subject"),
            "created_at": props.get("createdate"),
            "closed_at": props.get("closedate"),
            "pipeline_id": props.get("hs_pipeline"),
            "stage_id": stage_id,
            "is_closed": bool(stage_id in closed_stage_ids),
            "owner_id": props.get("hubspot_owner_id"),
            "category": props.get(CATEGORY_PROP),
            "updated_at": props.get("hs_lastmodifieddate"),
        })

    df = pd.DataFrame(rows)
    for c in ["created_at", "closed_at", "updated_at"]:
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    with engine.begin() as conn:
        conn.execute(text("delete from tickets_snapshot"))
        df.to_sql("tickets_snapshot", conn, if_exists="append", index=False, method="multi")

    owners = fetch_owners()
    odf = pd.DataFrame([{
        "owner_id": str(o.get("id")),
        "full_name": f"{o.get('firstName','')} {o.get('lastName','')}".strip(),
        "email": o.get("email"),
    } for o in owners])

    with engine.begin() as conn:
        conn.execute(text("delete from owners"))
        odf.to_sql("owners", conn, if_exists="append", index=False, method="multi")

    print(f"Loaded {len(df)} tickets and {len(odf)} owners.")

if __name__ == "__main__":
    main()
