"""Downstream consumers of the medallion: who reads the bronze/silver/gold tables outside the pipeline.

Read incrementally from system.access.table_lineage (read events of our tables, event_time > watermark - lookback),
like the uc_lineage edges. Each read is attributed to one consumer, from entity_metadata first (it names Genie spaces,
apps and alerts, which come with a NULL entity_type) and then entity_type / entity_id:

  DASHBOARD (DASHBOARD_V3), LEGACY_DASHBOARD, GENIE_SPACE, ALERT, APP, SQL_QUERY, JOB, NOTEBOOK, PIPELINE
  TABLE      a table outside the medallion written from one of ours (CTAS, INSERT ... SELECT)
  ADHOC      warehouse queries with no entity at all (SQL editor, JDBC/ODBC tools, the SDK): one node per table

Left out: the pipeline hops inside the medallion (target in a medallion schema) and every pipeline that writes the
medallion in the same window (the producer, whose refreshes also show up as reads). Only this workspace's events are
read: the lineage system tables are regional and carry workspace_id, and consumers in another workspace could not be
resolved here.

Names, owners and links come from the workspace APIs and are refreshed on every run for the known consumers (a
handful of calls); a consumer that no longer exists is kept with status 'deleted', so the history of who used a
table is not lost. Rows are MERGEd only when something changed, so a run with nothing new writes nothing.

Who ran an ad hoc query (created_by) is not stored: the table says that a table is queried ad hoc and when, not by
whom.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from .config import CATALOG, LAYERS, META, SCHEMAS, load_state, run_sql

LOOKBACK = "INTERVAL 2 HOURS"   # lineage events land late: re-read an overlap; the MERGE is idempotent
MEDALLION = [SCHEMAS[layer] for layer in LAYERS]
TYPE_LABEL = {"DASHBOARD": "Dashboard", "LEGACY_DASHBOARD": "Legacy dashboard", "GENIE_SPACE": "Genie space",
              "ALERT": "SQL alert", "APP": "App", "SQL_QUERY": "SQL query", "JOB": "Job", "NOTEBOOK": "Notebook",
              "PIPELINE": "Pipeline", "TABLE": "Table", "ADHOC": "Ad hoc queries"}
# entity_metadata field -> consumer type, in priority order (a notebook run inside a job is attributed to the job)
METADATA = [("dashboard_id", "DASHBOARD"), ("legacy_dashboard_id", "LEGACY_DASHBOARD"),
            ("genie_space_id", "GENIE_SPACE"), ("alert_id", "ALERT"), ("app_id", "APP"),
            ("sql_query_id", "SQL_QUERY"), ("job_info.job_id", "JOB"), ("notebook_id", "NOTEBOOK"),
            ("dlt_pipeline_info.dlt_pipeline_id", "PIPELINE")]
ENTITY_TYPE = {"DASHBOARD_V3": "DASHBOARD", "DBSQL_DASHBOARD": "LEGACY_DASHBOARD", "DBSQL_QUERY": "SQL_QUERY",
               "JOB": "JOB", "NOTEBOOK": "NOTEBOOK", "PIPELINE": "PIPELINE"}
COLUMNS = ("source_table", "consumer_key", "consumer_type", "consumer_id", "name", "owner", "url", "status",
           "first_seen", "last_seen")


def ensure_table(w: WorkspaceClient) -> None:
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.consumers (
  source_table STRING COMMENT 'Medallion table that is read: <layer>.<table>',
  consumer_key STRING COMMENT '<consumer_type>:<consumer_id>, or ADHOC',
  consumer_type STRING COMMENT 'DASHBOARD, LEGACY_DASHBOARD, GENIE_SPACE, ALERT, APP, SQL_QUERY, JOB, NOTEBOOK, PIPELINE, TABLE or ADHOC',
  consumer_id STRING COMMENT 'Id of the dashboard, job, notebook, ... (full name for TABLE; NULL for ADHOC)',
  name STRING COMMENT 'Display name, from the workspace API',
  owner STRING COMMENT 'Owner or creator (user name), from the workspace API; NULL when the API does not say',
  url STRING COMMENT 'Link to the consumer in the workspace',
  status STRING COMMENT 'active, deleted (the API no longer finds it) or unknown (no access, or no API to resolve it)',
  first_seen TIMESTAMP, last_seen TIMESTAMP COMMENT 'First and latest read event in system.access.table_lineage',
  resolved_at TIMESTAMP COMMENT 'When name, owner and link were last changed by a lookup')
COMMENT 'Downstream consumers of the medallion tables (dashboards, jobs, notebooks, ad hoc queries...), pulled incrementally from system.access.table_lineage by the STTM agent'""")


def _short(full: str) -> str | None:
    """workspace.sttm_gold.daily_sales -> gold.daily_sales (None if not a medallion table)."""
    parts = (full or "").lower().split(".")
    if len(parts) == 3 and parts[0] == CATALOG.lower():
        for layer in LAYERS:
            if parts[1] == SCHEMAS[layer]:
                return f"{layer}.{parts[2]}"
    return None


def _consumer(r: dict) -> tuple[str, str | None]:
    md = json.loads(r["md"] or "{}")
    for path, ctype in METADATA:
        v = md
        for k in path.split("."):
            v = v.get(k) if isinstance(v, dict) else None
        if v:
            return ctype, str(v)
    if r["entity_type"] and r["entity_id"]:
        return ENTITY_TYPE.get(r["entity_type"], r["entity_type"]), r["entity_id"]
    return "ADHOC", None


def pull(w: WorkspaceClient, log, pipeline_id: str | None = None) -> tuple[int, str | None]:
    """New read events since the watermark -> consumer edges. Returns (edges seen, new watermark)."""
    from .agent import get_state   # agent imports this module
    wm = get_state(w, "consumers_watermark") or "1970-01-01T00:00:00Z"
    med = ", ".join(f"'{s}'" for s in MEDALLION)
    window = (f"workspace_id = :ws AND event_date >= to_date(CAST(:wm AS TIMESTAMP) - {LOOKBACK}) "
              f"AND event_time > CAST(:wm AS TIMESTAMP) - {LOOKBACK}")
    rows = run_sql(w, f"""
        WITH producers AS (   -- pipelines that write the medallion: their refreshes also read it, they are not consumers
          SELECT DISTINCT entity_id FROM system.access.table_lineage
          WHERE {window} AND entity_type = 'PIPELINE'
            AND target_table_catalog = :cat AND target_table_schema IN ({med}))
        SELECT source_table_full_name AS src, target_table_full_name AS tgt, entity_type, entity_id,
               to_json(entity_metadata) AS md, min(event_time) AS first_seen, max(event_time) AS last_seen
        FROM system.access.table_lineage
        WHERE {window} AND source_table_catalog = :cat AND source_table_schema IN ({med})
          AND NOT (coalesce(target_table_catalog, '') = :cat AND coalesce(target_table_schema, '') IN ({med}))
          AND NOT (coalesce(entity_type, '') = 'PIPELINE'
                   AND (entity_id IN (SELECT entity_id FROM producers) OR entity_id = :pid))
        GROUP BY ALL""", {"ws": str(w.get_workspace_id()), "wm": wm, "cat": CATALOG, "pid": pipeline_id or ""})
    edges: dict[tuple[str, str], dict] = {}

    def add(src: str, ctype: str, cid: str | None, r: dict) -> None:
        key = f"{ctype}:{cid}" if cid else ctype
        e = edges.setdefault((src, key), {"source_table": src, "consumer_key": key, "consumer_type": ctype,
                                          "consumer_id": cid, "first_seen": r["first_seen"],
                                          "last_seen": r["last_seen"]})
        e["first_seen"] = min(e["first_seen"], r["first_seen"])
        e["last_seen"] = max(e["last_seen"], r["last_seen"])

    for r in rows:
        src = _short(r["src"])
        if not src:
            continue
        ctype, cid = _consumer(r)
        if r["tgt"]:   # written into a table outside the medallion: that table is a consumer too
            add(src, "TABLE", r["tgt"].lower(), r)
            if ctype == "ADHOC":
                continue   # the CTAS itself is not a reader worth a node
        add(src, ctype, cid, r)
    # the overlap re-reads events already merged: send only edges that are new or widen first/last seen
    known = {(k["source_table"], k["consumer_key"]): k for k in run_sql(
        w, f"SELECT source_table, consumer_key, first_seen, last_seen FROM {META}.consumers")}
    ts = datetime.fromisoformat
    for k, e in list(edges.items()):
        old = known.get(k)
        if old and ts(e["last_seen"]) <= ts(old["last_seen"]) and ts(e["first_seen"]) >= ts(old["first_seen"]):
            del edges[k]
    if edges:
        run_sql(w, f"""
            MERGE INTO {META}.consumers t
            USING (SELECT inline(from_json(:p, 'array<struct<source_table:string,consumer_key:string,
                       consumer_type:string,consumer_id:string,first_seen:timestamp,last_seen:timestamp>>'))) s
            ON t.source_table = s.source_table AND t.consumer_key = s.consumer_key
            WHEN MATCHED AND (s.last_seen > t.last_seen OR s.first_seen < t.first_seen) THEN UPDATE SET
                 first_seen = least(t.first_seen, s.first_seen), last_seen = greatest(t.last_seen, s.last_seen)
            WHEN NOT MATCHED THEN INSERT (source_table, consumer_key, consumer_type, consumer_id, status,
                                          first_seen, last_seen)
                 VALUES (s.source_table, s.consumer_key, s.consumer_type, s.consumer_id, 'new',
                         s.first_seen, s.last_seen)""", {"p": json.dumps(list(edges.values()))})
    new_wm = max((r["last_seen"] for r in rows), default=None, key=ts)
    log(f"   consumers since {wm} (- lookback): {len(rows)} read event groups -> {len(edges)} new or later "
        f"table/consumer edges")
    return len(edges), new_wm


# ---------------------------------------------------------------------------------------------------- lookups

def _user_from_path(path: str | None) -> str | None:
    p = (path or "").removeprefix("/Workspace")
    return p.split("/")[2] if p.startswith("/Users/") and p.count("/") >= 3 else None


def _manager(w: WorkspaceClient, object_type: str, object_id: str) -> str | None:
    """Owner if the object has one (IS_OWNER), else the first user with a direct CAN_MANAGE (dashboards, notebooks)."""
    try:
        acl = w.permissions.get(object_type, object_id).access_control_list or []
    except DatabricksError:
        return None
    for level in ("IS_OWNER", "CAN_MANAGE"):
        for a in acl:
            who = a.user_name or a.service_principal_name
            if who and any(p.permission_level and p.permission_level.value == level and not p.inherited
                           for p in a.all_permissions or []):
                return who
    return None


def resolve(w: WorkspaceClient, ctype: str, cid: str | None, apps: dict) -> dict:
    """{name, owner, url, status} for one consumer. 'deleted' when the API says it is gone."""
    host = w.config.host.rstrip("/")
    try:
        if ctype == "ADHOC":
            return {"name": "Ad hoc queries", "owner": None, "url": None, "status": "active"}
        if ctype == "DASHBOARD":
            d = w.lakeview.get(cid)
            if d.lifecycle_state and d.lifecycle_state.value == "TRASHED":
                return {"name": d.display_name, "owner": None, "url": None, "status": "deleted"}
            return {"name": d.display_name, "url": f"{host}/sql/dashboardsv3/{cid}", "status": "active",
                    "owner": _manager(w, "dashboards", cid) or _user_from_path(d.path)}
        if ctype == "JOB":
            j = w.jobs.get(int(cid))
            return {"name": j.settings.name if j.settings else cid, "url": f"{host}/jobs/{cid}", "status": "active",
                    "owner": _manager(w, "jobs", cid) or j.creator_user_name}
        if ctype == "PIPELINE":
            p = w.pipelines.get(cid)
            return {"name": p.name, "url": f"{host}/pipelines/{cid}", "status": "active",
                    "owner": _manager(w, "pipelines", cid) or p.creator_user_name}
        if ctype == "NOTEBOOK":
            return {"name": f"Notebook {cid}", "url": f"{host}/#notebook/{cid}", "status": "active",
                    "owner": _manager(w, "notebooks", cid)}
        if ctype == "GENIE_SPACE":
            s = w.genie.get_space(cid)
            return {"name": s.title, "url": f"{host}/genie/rooms/{cid}", "status": "active",
                    "owner": _manager(w, "genie", cid)}
        if ctype == "SQL_QUERY":
            q = w.queries.get(cid)
            return {"name": q.display_name, "url": f"{host}/sql/editor/{cid}", "status": "active",
                    "owner": q.owner_user_name}
        if ctype == "ALERT":
            a = w.alerts_v2.get_alert(cid)
            return {"name": a.display_name, "url": f"{host}/sql/alerts-v2/{cid}", "status": "active",
                    "owner": a.owner_user_name}
        if ctype == "APP":
            a = apps.get(cid)
            if not a:
                return {"name": f"App {cid}", "owner": None, "url": None, "status": "deleted"}
            return {"name": a.name, "url": a.url, "status": "active", "owner": a.creator}
        if ctype == "TABLE":
            t = w.tables.get(cid)
            return {"name": cid, "url": f"{host}/explore/data/{cid.replace('.', '/')}", "status": "active",
                    "owner": t.owner}
    except DatabricksError as e:
        gone = e.error_code in ("RESOURCE_DOES_NOT_EXIST", "NOT_FOUND", "TABLE_OR_VIEW_NOT_FOUND") \
            or "does not exist" in str(e).lower() or "not found" in str(e).lower()
        return {"name": f"{TYPE_LABEL.get(ctype, ctype)} {cid}", "owner": None, "url": None,
                "status": "deleted" if gone else "unknown"}
    return {"name": f"{TYPE_LABEL.get(ctype, ctype)} {cid}", "owner": None, "url": None, "status": "unknown"}


def refresh_names(w: WorkspaceClient, log) -> int:
    """Look up every known consumer once; write only the rows whose name, owner, link or status changed."""
    rows = run_sql(w, f"SELECT DISTINCT consumer_key, consumer_type, consumer_id, name, owner, url, status "
                      f"FROM {META}.consumers")
    if not rows:
        return 0
    apps = {}
    if any(r["consumer_type"] == "APP" for r in rows):
        apps = {a.id: a for a in w.apps.list() if a.id}
    with ThreadPoolExecutor(8) as ex:
        found = dict(zip([r["consumer_key"] for r in rows],
                         ex.map(lambda r: resolve(w, r["consumer_type"], r["consumer_id"], apps), rows)))
    changed = []
    for r in rows:
        n = found[r["consumer_key"]]
        if r["status"] == "deleted" and n["status"] != "active":
            continue   # keep the name it had while it existed
        if any((r[k] or None) != (n[k] or None) for k in ("name", "owner", "url", "status")):
            changed.append({"consumer_key": r["consumer_key"], **n})
    if changed:
        run_sql(w, f"""
            MERGE INTO {META}.consumers t
            USING (SELECT inline(from_json(:p, 'array<struct<consumer_key:string,name:string,owner:string,
                                                   url:string,status:string>>'))) s
            ON t.consumer_key = s.consumer_key
            WHEN MATCHED THEN UPDATE SET name = s.name, owner = s.owner, url = s.url, status = s.status,
                                         resolved_at = current_timestamp()""", {"p": json.dumps(changed)})
    log(f"   consumers: {len(rows)} known, {len(changed)} looked up with a new name, owner, link or status")
    return len(changed)


def update(w: WorkspaceClient, log) -> dict:
    """One incremental pass: pull new read events, refresh names, advance the watermark."""
    from .agent import set_state
    ensure_table(w)
    edges, wm = pull(w, log, load_state(w, required=False).get("pipeline_id"))
    resolved = refresh_names(w, log)
    if wm:
        set_state(w, "consumers_watermark", wm)
    return {"consumer_edges_seen": edges, "consumers_resolved": resolved}
