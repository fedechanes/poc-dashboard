"""External API + front end that embeds the Databricks dashboard.

Flows:
  App -> Dashboard   : params are put on the iframe URL as dashboard filter values (f_<page>~<widget>=v)
  App -> Databricks  : every submit is INSERTed into  <catalog>.poc_dashboard.app_params
  Dashboard -> App   : the API runs the dashboard's own query on the SQL warehouse and returns the rows
  Results -> Databricks : summary INSERTed into  <catalog>.poc_dashboard.app_results

Option 2 (/external): token-based embedding for users without a Databricks login, see embed_token.py
"""

from __future__ import annotations

import uuid
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from poc.common import FARE_WIDGET, FQ, PAGE_NAME, ROOT, TRIPS_SQL, ZIP_WIDGET, client, load_state, run_sql
from poc.embed_token import mint_viewer_token

app = FastAPI(title="Databricks dashboard embedding POC")
w = client()
state = load_state(w)


def embed_url(pickup_zip: int | None = None, min_fare: float | None = None) -> str:
    q = {"o": state["workspace_id"]}
    if pickup_zip is not None:
        q[f"f_{PAGE_NAME}~{ZIP_WIDGET}"] = pickup_zip
    if min_fare is not None:
        q[f"f_{PAGE_NAME}~{FARE_WIDGET}"] = min_fare
    return f"{state['host']}/embed/dashboardsv3/{state['dashboard_id']}?{urlencode(q, safe='~')}"


class Params(BaseModel):
    pickup_zip: int = Field(10001, ge=0, le=99999)
    min_fare: float = Field(0, ge=0)
    session_id: str = Field("demo", max_length=64)


@app.get("/")
def index():
    return FileResponse(ROOT / "static" / "index.html")


@app.get("/api/config")
def config():
    return {"embed_url": embed_url(), "dashboard_url":
            f"{state['host']}/dashboardsv3/{state['dashboard_id']}/published?o={state['workspace_id']}"}


def record_and_query(source: str, session_id: str, pickup_zip: int, min_fare: float) -> dict:
    """Store params in Databricks, run the dashboard query, store + return the result."""
    rid = str(uuid.uuid4())
    try:
        # 1) App -> Databricks: persist the parameters (parameterised SQL, no string interpolation)
        run_sql(w, f"""INSERT INTO {FQ}.app_params
                       VALUES (:rid, :sid, CAST(:zip AS INT), CAST(:fare AS DOUBLE), :src, current_timestamp())""",
                {"rid": rid, "sid": session_id, "zip": str(pickup_zip), "fare": str(min_fare), "src": source})

        # 2) Dashboard -> App: same query the dashboard runs, same params
        rows = run_sql(w, TRIPS_SQL, {"pickup_zip": str(pickup_zip), "min_fare": str(min_fare)})
        total_trips = sum(int(r["trips"]) for r in rows)
        total_fare = round(sum(float(r["total_fare"]) for r in rows), 2)
        top = int(rows[0]["dropoff_zip"]) if rows and rows[0]["dropoff_zip"] else None

        # 3) Results -> Databricks
        run_sql(w, f"""INSERT INTO {FQ}.app_results
                       VALUES (:rid, CAST(:t AS BIGINT), CAST(:f AS DOUBLE), CAST(:z AS INT), current_timestamp())""",
                {"rid": rid, "t": str(total_trips), "f": str(total_fare), "z": None if top is None else str(top)})
    except RuntimeError as e:
        raise HTTPException(502, str(e)) from e
    return {"request_id": rid,
            "summary": {"total_trips": total_trips, "total_fare": total_fare, "top_dropoff_zip": top},
            "rows": rows[:10]}


@app.post("/api/params")
def submit(p: Params):
    res = record_and_query("external-app", p.session_id, p.pickup_zip, p.min_fare)
    return {**res, "embed_url": embed_url(p.pickup_zip, p.min_fare)}


@app.get("/api/history")
def history(limit: int = 10):
    """Read back what is stored in Databricks (proves the round trip)."""
    return run_sql(w, f"""
        SELECT p.request_id, p.session_id, p.pickup_zip, p.min_fare, p.source, p.created_at,
               r.total_trips, r.total_fare, r.top_dropoff_zip
        FROM {FQ}.app_params p LEFT JOIN {FQ}.app_results r USING (request_id)
        ORDER BY p.created_at DESC LIMIT {max(1, min(int(limit), 100))}""")


# ───────────────────────── Option 2: embedding for external users ─────────────────────────
# Stand-in for YOUR app's own auth: the server (not the browser) decides which data a user may see.
# In production this comes from your session/JWT; the value is locked into the Databricks token.
USERS = {"alice": 10001, "bob": 10003, "carol": 10011}


class Session(BaseModel):
    user_id: str


class FilterEvent(BaseModel):
    user_id: str
    min_fare: float = Field(0, ge=0)


def _user_zip(user_id: str) -> int:
    if user_id not in USERS:
        raise HTTPException(401, "unknown user")
    return USERS[user_id]


@app.get("/external")
def external_page():
    return FileResponse(ROOT / "static" / "external.html")


@app.post("/api/external/token")
def external_token(s: Session):
    """Mint a short-lived, dashboard-scoped token for this viewer (no Databricks login needed)."""
    zip_code = _user_zip(s.user_id)
    try:
        tok = mint_viewer_token(state["host"], state["workspace_id"], state["external_dashboard_id"],
                                external_viewer_id=f"poc-{s.user_id}",  # audit id, no PII
                                external_value=str(zip_code))           # -> __aibi_external_value
    except Exception as e:  # noqa: BLE001 - surface token-exchange problems to the POC UI
        raise HTTPException(502, f"token exchange failed: {e}") from e
    return {**tok, "instance_url": state["host"], "workspace_id": str(state["workspace_id"]),
            "dashboard_id": state["external_dashboard_id"], "pickup_zip": zip_code,
            "fare_filter_id": f"{PAGE_NAME}~{FARE_WIDGET}"}


@app.post("/api/external/event")
def external_event(e: FilterEvent):
    """Dashboard -> app/Databricks: called by the browser whenever a dashboard filter changes."""
    return record_and_query("dashboard-filter", e.user_id, _user_zip(e.user_id), e.min_fare)
