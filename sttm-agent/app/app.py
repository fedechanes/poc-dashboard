"""STTM viewer: a web app over the agent's flat `sttm_export` table, where stewards approve definitions and owners
work through the issues on their tables (My worklist).

Runs locally from the repo (uses sttm-agent/.env and sttm_state.json) or as a Databricks App (app.yaml; auth and
warehouse come from the app's service principal and its sql-warehouse resource).

  uv run python sttm-agent/app/app.py      -> http://localhost:8001  (port: DATABRICKS_APP_PORT or STTM_APP_PORT)

  STTM_EXPORT_TABLE        table to serve (default <STTM_CATALOG>.<STTM_PREFIX>_meta.sttm_export)
  DATABRICKS_WAREHOUSE_ID  warehouse to query (locally: from sttm_state.json if not set)
  STTM_JOB_ID              the agent job that "Run agent" starts (locally: from sttm_state.json; none = no button)
  STTM_STEWARDS_GROUP      group whose members may approve definitions and run the agent (default sttm-stewards;
                           empty = anyone who can open the app)
  STTM_AUDIT_HISTORY_DAYS  days of history in the "Download for audit" workbook (default 30)

Reads run as the app's service principal. Steward actions (approve, edit, "still correct", sensitivity overrides) and
issue statuses run as the signed-in user with the token Databricks Apps forwards (user authorization, `sql` scope), so
Unity Catalog checks the user's own MODIFY grant on glossary / sensitivity_overrides / issue_status and records the user
as the writer. Locally both run as your
CLI profile.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import audit
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from history import History
from pydantic import BaseModel

HERE = Path(__file__).resolve().parent
AGENT_DIR = HERE.parent


def _load_local_env() -> None:
    """Running from the repo: read sttm-agent/.env (real env vars win). Absent when deployed as an app."""
    env = AGENT_DIR / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                if v.strip():
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_local_env()

CATALOG = os.getenv("STTM_CATALOG", "workspace")
PREFIX = os.getenv("STTM_PREFIX", "sttm")
TABLE_NAME = os.getenv("STTM_EXPORT_TABLE") or f"{CATALOG}.{PREFIX}_meta.sttm_export"
TABLE = ".".join(f"`{p.strip('`')}`" for p in TABLE_NAME.split("."))
CACHE_SECONDS = int(os.getenv("STTM_APP_CACHE_SECONDS", "60"))
AUDIT_HISTORY_DAYS = int(os.getenv("STTM_AUDIT_HISTORY_DAYS", "30"))

# Filters: query parameter -> SQL column. Every filter takes several values (OR within a filter, AND across).
EQ_FILTERS = {"table": "target_table", "layer": "layer", "owner": "owner", "definition_source": "definition_source",
              "kind": "mapping_kind", "origin": "lineage_origin", "data_type": "data_type", "run": "last_changed_run",
              "sensitivity": "sensitivity", "sensitivity_source": "sensitivity_source"}
FACETS = {**EQ_FILTERS, "issue": None}

w = WorkspaceClient()


def _local_state() -> dict:
    """sttm_state.json when running from the repo (and it belongs to this workspace)."""
    state_file = AGENT_DIR / "sttm_state.json"
    if state_file.exists():
        state = json.loads(state_file.read_text())
        if state.get("host") == w.config.host.rstrip("/"):
            return state
    return {}


def _warehouse_id() -> str:
    if os.getenv("DATABRICKS_WAREHOUSE_ID"):
        return os.environ["DATABRICKS_WAREHOUSE_ID"]
    if _local_state().get("warehouse_id"):
        return _local_state()["warehouse_id"]
    whs = list(w.warehouses.list())
    if not whs:
        raise RuntimeError("No SQL warehouse: set DATABRICKS_WAREHOUSE_ID")
    return whs[0].id


WAREHOUSE_ID = _warehouse_id()
JOB_ID = int(os.getenv("STTM_JOB_ID") or _local_state().get("job_id") or 0) or None
META_SCHEMA = TABLE.rsplit(".", 1)[0]
RUNS_TABLE = TABLE.rsplit(".", 1)[0] + ".`agent_runs`"
GLOSSARY_TABLE = TABLE.rsplit(".", 1)[0] + ".`glossary`"
OWNERS_TABLE = TABLE.rsplit(".", 1)[0] + ".`owners`"
ISSUES_TABLE = TABLE.rsplit(".", 1)[0] + ".`issues`"
STATUS_TABLE = TABLE.rsplit(".", 1)[0] + ".`issue_status`"
OVERRIDES_TABLE = TABLE.rsplit(".", 1)[0] + ".`sensitivity_overrides`"
SENSITIVITY = ("pii", "pii_derived", "none")
STATUSES = ("open", "in_progress", "accepted", "wont_fix")
STEWARDS = os.getenv("STTM_STEWARDS_GROUP", "sttm-stewards")
IN_APP = bool(os.getenv("DATABRICKS_APP_PORT"))
ACTIVE = {RunLifeCycleState.PENDING, RunLifeCycleState.QUEUED, RunLifeCycleState.RUNNING,
          RunLifeCycleState.BLOCKED, RunLifeCycleState.WAITING_FOR_RETRY, RunLifeCycleState.TERMINATING}


def run_sql(sql: str, params: dict[str, str | None] | None = None, client: WorkspaceClient | None = None) -> list[dict]:
    """Statement Execution API. `client` = the signed-in user's client for writes; default the app's own identity."""
    c = client or w
    resp = c.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="50s",
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()])
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(0.5)
        resp = c.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        msg = resp.status.error.message if resp.status.error else str(resp.status.state)
        denied = any(x in msg for x in ("PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS", "does not have"))
        raise HTTPException(403 if denied else 502, f"SQL failed: {msg}")
    if not resp.manifest or not resp.manifest.schema or not resp.manifest.schema.columns:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = list(resp.result.data_array or []) if resp.result else []
    chunk = resp.result.next_chunk_index if resp.result else None
    while chunk is not None:
        part = c.statement_execution.get_statement_result_chunk_n(resp.statement_id, chunk)
        rows += part.data_array or []
        chunk = part.next_chunk_index
    return [dict(zip(cols, r, strict=True)) for r in rows]


_cache: dict[str, tuple[float, object]] = {}
_lock = threading.Lock()


def cached(key: str, fn, refresh: bool = False):
    with _lock:
        hit = _cache.get(key)
    if hit and not refresh and time.time() - hit[0] < CACHE_SECONDS:
        return hit[1]
    value = fn()
    with _lock:
        _cache[key] = (time.time(), value)
    return value


def forget(key: str) -> None:
    with _lock:
        _cache.pop(key, None)


app = FastAPI(title="STTM viewer")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(HERE / "static" / "index.html")


@app.get("/api/facets")
def facets(refresh: bool = False) -> dict:
    """Values (with counts) for every filter, the column list for 'depends on', and table-level info."""
    def load() -> dict:
        unions = " UNION ALL ".join(
            f"SELECT '{name}' AS facet, {col} AS value, count(*) AS n FROM {TABLE} GROUP BY {col}"
            for name, col in EQ_FILTERS.items())
        rows = run_sql(f"""{unions}
            UNION ALL SELECT 'issue', it, count(*) FROM {TABLE} LATERAL VIEW explode(split(issue_types, '; ')) AS it
                      WHERE issue_types <> '' GROUP BY it""")
        out: dict[str, list] = {f: [] for f in FACETS}
        for r in rows:
            if r["value"] is not None:
                out[r["facet"]].append({"value": r["value"], "n": int(r["n"])})
        for f in out:
            out[f].sort(key=lambda x: x["value"], reverse=f == "run")
        tables = run_sql(f"""SELECT target_table, layer, owner, uc_table, any_value(table_rule) AS table_rule,
                                    count(*) AS columns, count_if(has_open_issue) AS open_issues,
                                    count_if(sensitivity = 'pii') AS pii_columns,
                                    count_if(sensitivity = 'pii_derived') AS pii_derived_columns
                             FROM {TABLE} GROUP BY ALL""")
        upstream = run_sql(f"""SELECT DISTINCT c FROM {TABLE}
                               LATERAL VIEW explode(split(upstream_columns, '; ')) AS c WHERE c <> ''
                               UNION SELECT concat(target_table, '.', target_column) FROM {TABLE}""")
        stats = run_sql(f"""SELECT count(*) AS columns, count(DISTINCT target_table) AS tables,
                                   count_if(has_open_issue) AS open_issues, max(updated_at) AS updated_at,
                                   count_if(sensitivity IN ('pii', 'pii_derived')) AS personal_columns,
                                   max(last_changed_run) AS last_run FROM {TABLE}""")[0]
        # table-level edges for the lineage map: from every direct source and table-rule column
        edges = set()
        for r in run_sql(f"SELECT DISTINCT target_table, source_columns, rule_columns FROM {TABLE}"):
            for c in f"{r['source_columns'] or ''}; {r['rule_columns'] or ''}".split("; "):
                if c.strip():
                    edges.add((c.strip().rsplit(".", 1)[0], r["target_table"]))
        return {"facets": out, "tables": tables, "columns": sorted(r["c"] for r in upstream), "stats": stats,
                "edges": [{"source": s, "target": t} for s, t in sorted(edges)], "table_name": TABLE.replace("`", "")}
    return cached("facets", load, refresh)


def filters_of(request: Request) -> dict:
    """The filters in the query string, as {name: [values]} (q and depends_on: a string)."""
    qp = request.query_params
    out: dict = {k: qp.getlist(k) for k in (*EQ_FILTERS, "issue") if qp.getlist(k)}
    for k in ("q", "depends_on"):
        if (qp.get(k) or "").strip():
            out[k] = qp[k].strip()
    return out


def filtered_rows(f: dict, refresh: bool = False) -> list[dict]:
    """STTM rows for the filters. All values are bound as parameters."""
    where, params = ["true"], {}
    for name in EQ_FILTERS:
        if f.get(name):
            where.append(f"array_contains(from_json(:{name}, 'array<string>'), {EQ_FILTERS[name]})")
            params[name] = json.dumps(f[name])
    if f.get("issue"):
        # 'OPEN' = any issue except INFO, 'NONE' = no issue at all, otherwise an issue type
        conds = ["arrays_overlap(split(issue_types, '; '), from_json(:issue, 'array<string>'))"]
        if "OPEN" in f["issue"]:
            conds.append("has_open_issue")
        if "NONE" in f["issue"]:
            conds.append("issue_types = ''")
        where.append("(" + " OR ".join(conds) + ")")
        params["issue"] = json.dumps(f["issue"])
    if f.get("q"):
        where.append("""lower(concat_ws(' ', target_table, target_column, business_definition, transformation,
                         source_columns, proposed_definition, sensitivity_reason)) LIKE concat('%', lower(:q), '%')""")
        params["q"] = f["q"]
    if f.get("depends_on"):
        # a column (silver.orders.is_cancelled) or a table (bronze.orders_raw): everything downstream of it
        where.append("""exists(split(upstream_columns, '; '), c -> c = :dep OR startswith(c, concat(:dep, '.')))""")
        params["dep"] = f["depends_on"]
    sql = f"""SELECT * FROM {TABLE} WHERE {' AND '.join(where)}
              ORDER BY CASE layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END, target_table, target_column"""
    key = "rows:" + json.dumps(params, sort_keys=True)
    return cached(key, lambda: run_sql(sql, params), refresh)


@app.get("/api/sttm")
def sttm(request: Request, table: list[str] = Query(None), layer: list[str] = Query(None),
         owner: list[str] = Query(None), definition_source: list[str] = Query(None), kind: list[str] = Query(None),
         origin: list[str] = Query(None), data_type: list[str] = Query(None), run: list[str] = Query(None),
         issue: list[str] = Query(None), sensitivity: list[str] = Query(None),
         sensitivity_source: list[str] = Query(None), q: str | None = None, depends_on: str | None = None,
         refresh: bool = False) -> dict:
    """Filtered STTM rows (several values per filter: OR within a filter, AND across filters)."""
    return {"rows": filtered_rows(filters_of(request), refresh)}


# ---------------------------------------------------------------------------------------------------- stewardship

def user_client(request: Request) -> WorkspaceClient:
    """The signed-in user's identity: the forwarded user token in an app, the CLI profile locally."""
    token = request.headers.get("x-forwarded-access-token")
    if token:
        return WorkspaceClient(host=w.config.host, token=token, auth_type="pat")
    if IN_APP:
        raise HTTPException(401, "No user token: enable user authorization (sql scope) on the app")
    return w


def whoami(request: Request) -> dict:
    """{user, steward, teams}: user and group membership checked by Unity Catalog SQL functions as the user (no group
    API scope needed); the owner teams the user belongs to come from sttm_meta.owners."""
    client = user_client(request)
    tok = request.headers.get("x-forwarded-access-token") or "local"
    def load() -> dict:
        if not STEWARDS:
            r = run_sql("SELECT current_user() AS u, true AS s", client=client)[0]
        else:
            r = run_sql("SELECT current_user() AS u, is_account_group_member(:g) OR is_member(:g) AS s",
                        {"g": STEWARDS}, client=client)[0]
        teams = [t["owner"] for t in run_sql(f"""SELECT owner FROM {OWNERS_TABLE}
            WHERE exists(members, m -> lower(m) = lower(:u)) ORDER BY owner""", {"u": r["u"]})]
        return {"user": r["u"], "steward": r["s"] in (True, "true"), "group": STEWARDS or None, "teams": teams}
    return cached("me:" + str(hash(tok)), load)


def require_steward(request: Request) -> dict:
    me = whoami(request)
    if not me["steward"]:
        raise HTTPException(403, f"Only members of the {STEWARDS} group can do this.")
    return me


@app.get("/api/me")
def me(request: Request) -> dict:
    return whoami(request)


@app.get("/api/glossary")
def glossary(refresh: bool = False) -> dict:
    """Steward decisions, including the ones the agent hasn't applied yet."""
    rows = cached("glossary", lambda: run_sql(f"""SELECT column_key, definition, acknowledged_lineage_hash, note,
        updated_by, updated_at FROM {GLOSSARY_TABLE} ORDER BY column_key"""), refresh)
    return {"rows": rows}


class Decision(BaseModel):
    column: str                       # <layer>.<table>.<column>
    action: str                       # approve | edit | ack | remove
    definition: str | None = None     # for edit
    note: str | None = None


@app.post("/api/glossary")
def decide(d: Decision, request: Request) -> dict:
    """Record a steward decision as the signed-in user. The agent applies it on its next run."""
    require_steward(request)
    if d.action not in ("approve", "edit", "ack", "remove"):
        raise HTTPException(400, "action must be approve, edit, ack or remove")
    t, _, col = d.column.rpartition(".")
    cur = run_sql(f"""SELECT business_definition, lineage_hash FROM {TABLE}
                      WHERE target_table = :t AND target_column = :c""", {"t": t, "c": col})
    if not cur:
        raise HTTPException(404, f"Unknown column {d.column}")
    client = user_client(request)
    note = (d.note or "").strip()[:500] or None
    if d.action == "remove":
        run_sql(f"DELETE FROM {GLOSSARY_TABLE} WHERE column_key = :k", {"k": d.column}, client=client)
    elif d.action == "ack":   # keep whatever definition is in force; only close the REVIEW for this lineage
        run_sql(f"""MERGE INTO {GLOSSARY_TABLE} g USING (SELECT :k AS k) s ON g.column_key = s.k
            WHEN MATCHED THEN UPDATE SET acknowledged_lineage_hash = :h, note = coalesce(:n, g.note),
                 updated_by = current_user(), updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (column_key, definition, acknowledged_lineage_hash, note, updated_by, updated_at)
                 VALUES (:k, NULL, :h, :n, current_user(), current_timestamp())""",
                {"k": d.column, "h": cur[0]["lineage_hash"], "n": note}, client=client)
    else:
        text = (cur[0]["business_definition"] if d.action == "approve" else d.definition or "").strip()
        if not text:
            raise HTTPException(400, "The definition is empty")
        if len(text) > 1000:
            raise HTTPException(400, "Keep the definition under 1000 characters (it becomes the column comment)")
        run_sql(f"""MERGE INTO {GLOSSARY_TABLE} g USING (SELECT :k AS k) s ON g.column_key = s.k
            WHEN MATCHED THEN UPDATE SET definition = :d, acknowledged_lineage_hash = :h, note = :n,
                 updated_by = current_user(), updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (column_key, definition, acknowledged_lineage_hash, note, updated_by, updated_at)
                 VALUES (:k, :d, :h, :n, current_user(), current_timestamp())""",
                {"k": d.column, "d": text, "h": cur[0]["lineage_hash"], "n": note}, client=client)
    glossary(refresh=True)
    forget("history")
    return {"ok": True, "column": d.column, "action": d.action}


# ---------------------------------------------------------------------------------------------------- sensitivity

@app.get("/api/sensitivity")
def sensitivity_overrides(refresh: bool = False) -> dict:
    """Steward overrides of the personal data level, including the ones the agent hasn't applied yet."""
    rows = cached("overrides", lambda: run_sql(f"""SELECT column_key, sensitivity, note, updated_by, updated_at
        FROM {OVERRIDES_TABLE} ORDER BY column_key"""), refresh)
    return {"rows": rows}


class SensitivityDecision(BaseModel):
    column: str                       # <layer>.<table>.<column>
    sensitivity: str | None = None    # pii | pii_derived | none; None = remove the override (the agent decides again)
    note: str | None = None


@app.post("/api/sensitivity")
def set_sensitivity(d: SensitivityDecision, request: Request) -> dict:
    """Override (or stop overriding) a column's sensitivity as the signed-in user. The agent applies it on its next
    run, and columns downstream inherit it."""
    require_steward(request)
    if d.sensitivity is not None and d.sensitivity not in SENSITIVITY:
        raise HTTPException(400, f"sensitivity must be one of {', '.join(SENSITIVITY)}, or empty to remove the override")
    t, _, col = d.column.rpartition(".")
    if not run_sql(f"SELECT 1 FROM {TABLE} WHERE target_table = :t AND target_column = :c", {"t": t, "c": col}):
        raise HTTPException(404, f"Unknown column {d.column}")
    client = user_client(request)
    if d.sensitivity is None:
        run_sql(f"DELETE FROM {OVERRIDES_TABLE} WHERE column_key = :k", {"k": d.column}, client=client)
    else:
        run_sql(f"""MERGE INTO {OVERRIDES_TABLE} o USING (SELECT :k AS k) s ON o.column_key = s.k
            WHEN MATCHED THEN UPDATE SET sensitivity = :v, note = :n, updated_by = current_user(),
                 updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (column_key, sensitivity, note, updated_by, updated_at)
                 VALUES (:k, :v, :n, current_user(), current_timestamp())""",
                {"k": d.column, "v": d.sensitivity, "n": (d.note or "").strip()[:500] or None}, client=client)
    sensitivity_overrides(refresh=True)
    forget("history")
    return {"ok": True, "column": d.column, "sensitivity": d.sensitivity}


# ---------------------------------------------------------------------------------------------------- issues

def load_issues(refresh: bool = False) -> list[dict]:
    return cached("issues", lambda: run_sql(f"""
        SELECT i.column_key, i.target_table, i.owner, i.issue_type, i.fingerprint, i.issue_text, i.opened_run,
               i.opened_at, i.first_seen_at, coalesce(s.status, 'open') AS status, s.assignee, s.comment,
               s.updated_by, s.updated_at
        FROM {ISSUES_TABLE} i LEFT JOIN {STATUS_TABLE} s
          ON s.column_key = i.column_key AND s.issue_type = i.issue_type AND s.fingerprint = i.fingerprint
        WHERE i.is_open ORDER BY i.target_table, i.column_key, i.issue_type"""), refresh)


@app.get("/api/issues")
def issues(refresh: bool = False) -> dict:
    """Every open issue with its owner's status, plus the owner teams and their members (for assignees)."""
    owners = cached("owners", lambda: run_sql(f"SELECT owner, members FROM {OWNERS_TABLE} ORDER BY owner"), refresh)
    return {"rows": load_issues(refresh),
            "owners": [{"owner": o["owner"], "members": json.loads(o["members"] or "[]")} for o in owners]}


class IssueStatus(BaseModel):
    column: str                       # <layer>.<table>.<column>
    issue_type: str
    fingerprint: str
    status: str                       # open | in_progress | accepted | wont_fix
    assignee: str | None = None
    comment: str | None = None


@app.post("/api/issues/status")
def set_issue_status(d: IssueStatus, request: Request) -> dict:
    """Set the status of an open issue as the signed-in user. Stewards and the members of the owner team may."""
    if d.status not in STATUSES:
        raise HTTPException(400, f"status must be one of {', '.join(STATUSES)}")
    issue = next((i for i in load_issues(refresh=True) if (i["column_key"], i["issue_type"], i["fingerprint"])
                  == (d.column, d.issue_type, d.fingerprint)), None)
    if not issue:
        raise HTTPException(404, "This issue is no longer open (the agent stopped reporting it). Refresh the page.")
    me = whoami(request)
    if not me["steward"] and issue["owner"] not in me["teams"]:
        raise HTTPException(403, f"Only {issue['owner']} or the {STEWARDS} group can change this issue.")
    assignee = (d.assignee or "").strip()[:200] or None
    comment = (d.comment or "").strip()[:1000] or None
    run_sql(f"""MERGE INTO {STATUS_TABLE} t
        USING (SELECT :k AS column_key, :t AS issue_type, :f AS fingerprint) s
        ON t.column_key = s.column_key AND t.issue_type = s.issue_type AND t.fingerprint = s.fingerprint
        WHEN MATCHED THEN UPDATE SET status = :st, assignee = :a, comment = :c, updated_by = current_user(),
             updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (column_key, issue_type, fingerprint, status, assignee, comment, updated_by,
                                      updated_at)
             VALUES (:k, :t, :f, :st, :a, :c, current_user(), current_timestamp())""",
            {"k": d.column, "t": d.issue_type, "f": d.fingerprint, "st": d.status, "a": assignee, "c": comment},
            client=user_client(request))
    load_issues(refresh=True)
    forget("history")
    return {"ok": True}


# ---------------------------------------------------------------------------------------------------- history

history = History(lambda sql, params=None: run_sql(sql, params), META_SCHEMA, PREFIX)


@app.get("/api/history")
def get_history(refresh: bool = False) -> dict:
    """Agent runs with the columns each one changed (before/after, from the STTM change data feed), steward decisions
    (glossary CDF) and issue status changes (issue_status CDF). Read as the app's service principal."""
    return cached("history", history.load, refresh)


@app.get("/api/export.xlsx")
def export_xlsx(request: Request, refresh: bool = False) -> Response:
    """'Download for audit': the rows for the current filters, open issues and recent history as an Excel file."""
    f = filters_of(request)
    data = audit.build(rows=filtered_rows(f, refresh), issues=load_issues(refresh), hist=get_history(refresh),
                       user=whoami(request)["user"], source_table=TABLE.replace("`", ""), filters=f,
                       history_days=AUDIT_HISTORY_DAYS)
    name = f"sttm_audit_{time.strftime('%Y%m%d_%H%M', time.gmtime())}.xlsx"
    return Response(data, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"content-disposition": f'attachment; filename="{name}"', "cache-control": "no-store"})


# ---------------------------------------------------------------------------------------------------- agent runs

def _run_info(r) -> dict:
    st = r.state
    return {"run_id": r.run_id, "url": r.run_page_url, "start_time": r.start_time, "end_time": r.end_time or None,
            "life_cycle": st.life_cycle_state.value if st and st.life_cycle_state else None,
            "result": st.result_state.value if st and st.result_state else None,
            "message": st.state_message if st else None,
            "active": bool(st and st.life_cycle_state in ACTIVE)}


@app.get("/api/agent")
def agent_status(summary: bool = False) -> dict:
    """The latest agent job run, and (summary=true) what the latest agent run changed, from agent_runs."""
    if not JOB_ID:
        return {"enabled": False}
    runs = list(w.jobs.list_runs(job_id=JOB_ID, limit=1))
    out = {"enabled": True, "run": _run_info(runs[0]) if runs else None}
    if summary:
        rows = run_sql(f"""SELECT run_id, started_at, seconds, tables_processed, rows_inserted, rows_updated,
                                  rows_deactivated, llm_calls, comments_written, tags_written, open_issues,
                                  get_json_object(details, '$.requested_by') AS requested_by,
                                  get_json_object(details, '$.issues_new') AS issues_new
                           FROM {RUNS_TABLE} ORDER BY started_at DESC LIMIT 1""")
        out["summary"] = rows[0] if rows else None
    return out


@app.post("/api/agent/run")
def agent_run(request: Request) -> dict:
    """Start the agent job, unless a run is already going. Records who asked (the app's signed-in user)."""
    if not JOB_ID:
        raise HTTPException(404, "No agent job is configured for this app (run.py app-deploy creates it).")
    require_steward(request)
    runs = list(w.jobs.list_runs(job_id=JOB_ID, active_only=True, limit=1))
    if runs:
        return {"started": False, "run": _run_info(runs[0])}
    who = request.headers.get("x-forwarded-email") or request.headers.get("x-forwarded-preferred-username") or ""
    run_id = w.jobs.run_now(JOB_ID, job_parameters={"requested_by": who}).run_id
    with _lock:
        _cache.clear()   # the next reads after the run must see the new mapping and issues
    return {"started": True, "run": _run_info(w.jobs.get_run(run_id))}


if __name__ == "__main__":
    import uvicorn

    # Databricks Apps sets DATABRICKS_APP_PORT; locally default to 8001 (8000 is the dashboard POC)
    port = int(os.getenv("DATABRICKS_APP_PORT") or os.getenv("STTM_APP_PORT") or 8001)
    uvicorn.run(app, host="0.0.0.0" if os.getenv("DATABRICKS_APP_PORT") else "127.0.0.1", port=port)
