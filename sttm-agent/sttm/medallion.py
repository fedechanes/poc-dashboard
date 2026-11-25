"""Create/update the Databricks objects for the medallion: schemas, landing volume, raw files, Lakeflow pipeline.

Safe to re-run: everything is CREATE ... IF NOT EXISTS or updated in place by id (sttm_state.json).
"""

from __future__ import annotations

import io
import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import pipelines as P
from databricks.sdk.service.workspace import ImportFormat

from . import datagen
from .config import (CATALOG, LANDING_PATH, PIPELINE_CONF, PIPELINE_DIR, PIPELINE_NAME, SCHEMAS, VOLUME,
                     load_state, run_sql, save_state)

PIPELINE_FILES = ["bronze.sql", "silver.sql", "gold.sql"]


def create_schemas(w: WorkspaceClient) -> None:
    for name, schema in SCHEMAS.items():
        run_sql(w, f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}` COMMENT 'STTM agent demo: {name}'")
    run_sql(w, f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMAS['landing']}`.`{VOLUME}` "
               "COMMENT 'Raw extracts landed by the source systems (synthetic)'")


def land_batch(w: WorkspaceClient, n: int) -> list[str]:
    """Upload batch n to the landing volume. Files are deterministic, so re-landing is a no-op for Auto Loader."""
    out = []
    for rel, (content, desc) in datagen.batch(n).items():
        w.files.upload(f"{LANDING_PATH}/{rel}", io.BytesIO(content.encode()), overwrite=True)
        out.append(f"{rel} ({desc})")
    return out


def workspace_dir(w: WorkspaceClient) -> str:
    return f"/Workspace/Users/{w.current_user.me().user_name}/sttm-agent/pipeline"


def upload_pipeline_sql(w: WorkspaceClient, overrides: dict[str, str] | None = None) -> list[str]:
    """Upload the pipeline SQL to the workspace. `overrides` replaces a file's content (used by the demo)."""
    d = workspace_dir(w)
    w.workspace.mkdirs(d)
    paths = []
    for f in PIPELINE_FILES:
        text = (overrides or {}).get(f) or (PIPELINE_DIR / f).read_text()
        w.workspace.upload(f"{d}/{f}", io.BytesIO(text.encode()), format=ImportFormat.RAW, overwrite=True)
        paths.append(f"{d}/{f}")
    return paths


def deploy_pipeline(w: WorkspaceClient) -> str:
    paths = upload_pipeline_sql(w)
    spec = dict(
        name=PIPELINE_NAME, catalog=CATALOG, schema=SCHEMAS["meta"], serverless=True, development=True,
        continuous=False, channel="CURRENT", configuration=PIPELINE_CONF,
        libraries=[P.PipelineLibrary(file=P.FileLibrary(path=p)) for p in paths],
        event_log=P.EventLogSpec(catalog=CATALOG, schema=SCHEMAS["meta"], name="pipeline_event_log"),
    )
    pid = load_state(w, required=False).get("pipeline_id")
    if pid:
        try:
            w.pipelines.get(pid)
        except NotFound:
            pid = None
    if not pid:  # adopt a pipeline with our name if state was lost
        pid = next((p.pipeline_id for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'")), None)
    if pid:
        w.pipelines.update(pipeline_id=pid, **spec)
    else:
        pid = w.pipelines.create(**spec).pipeline_id
    save_state(w, pipeline_id=pid)
    return pid


def run_pipeline(w: WorkspaceClient, pid: str, full_refresh: bool = False) -> dict:
    """Trigger an update and wait. Returns per-flow rows written, from the pipeline event log."""
    upd = w.pipelines.start_update(pid, full_refresh=full_refresh)
    t0 = time.time()
    while True:
        u = w.pipelines.get_update(pid, upd.update_id).update
        if u.state.value in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(5)
    if u.state.value != "COMPLETED":
        errs = [e.message for e in w.pipelines.list_pipeline_events(pid, max_results=50, filter="level='ERROR'")]
        raise RuntimeError(f"pipeline update {u.state.value}: " + "\n".join(errs[:5]))
    return {"update_id": upd.update_id, "seconds": int(time.time() - t0), "flows": flow_metrics(w, upd.update_id)}


def flow_metrics(w: WorkspaceClient, update_id: str) -> list[dict]:
    """Rows written and the refresh technique chosen per flow for one update, from the pipeline event log."""
    rows = run_sql(w, f"""
        SELECT origin.flow_name AS flow, event_type, details
        FROM `{CATALOG}`.`{SCHEMAS['meta']}`.pipeline_event_log
        WHERE origin.update_id = :u AND event_type IN ('flow_progress', 'planning_information')
          AND origin.flow_name IS NOT NULL AND origin.flow_name NOT LIKE 'pipelines.%'""", {"u": update_id})
    out: dict[str, dict] = {}
    for r in rows:
        d = json.loads(r["details"])
        f = out.setdefault(r["flow"], {"flow": r["flow"], "rows_written": 0, "technique": "streaming append"})
        if r["event_type"] == "flow_progress":
            m = d.get("flow_progress", {}).get("metrics", {})
            f["rows_written"] = max(f["rows_written"], int(m.get("num_output_rows") or 0),
                                    int(m.get("num_upserted_rows") or 0))
            if m.get("num_upserted_rows") is not None:
                f["technique"] = "AUTO CDC upsert"
        else:
            chosen = [t for t in d["planning_information"]["technique_information"] if t.get("is_chosen")]
            f["technique"] = (chosen[0].get("maintenance_type", "?").replace("MAINTENANCE_TYPE_", "").lower()
                              if chosen else "?")
    return sorted(out.values(), key=lambda f: f["flow"])
