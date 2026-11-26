"""Config + Databricks helpers.

Everything environment-specific comes from environment variables or `sttm-agent/.env` (gitignored).
Real environment variables win over the file.

  Databricks auth        standard unified auth, e.g. DATABRICKS_CONFIG_PROFILE=<profile>
  DATABRICKS_WAREHOUSE_ID  optional, auto-detected (first warehouse) if empty
  STTM_CATALOG           catalog for all schemas (default: workspace)
  STTM_PREFIX            schema prefix (default: sttm -> sttm_landing, sttm_bronze, ...)
  STTM_PIPELINE_NAME     Lakeflow pipeline name (default: sttm-medallion)
  STTM_PIPELINE_ID       pipeline id when there is no sttm_state.json (the agent job passes it as --pipeline-id)
  STTM_LLM_ENDPOINT      chat endpoint for drafting definitions (default: databricks-qwen3-next-80b-a3b-instruct,
                         'none' = deterministic rule-based fallback, no LLM)
  STTM_DEFAULT_OWNER     user name (email) that every owner team in glossary.toml maps to when it is first added to
                         the owners table (default: the user running the agent or the deploy)
  STTM_HISTORY_RETENTION_DAYS  days the change data feed of sttm, glossary, issues and issue_status stays readable
                         for the viewer's History (default 30; sets delta.deletedFileRetentionDuration)

Workspace-specific ids (pipeline id, warehouse id, host) live in sttm_state.json (gitignored).
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / ".env"
STATE_FILE = ROOT / "sttm_state.json"
PIPELINE_DIR = ROOT / "pipeline"
OUTPUT_DIR = ROOT / "output"
GLOSSARY_FILE = ROOT / "glossary.toml"


def load_env(path: Path = ENV_FILE) -> None:
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                if v.strip():
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()

CATALOG = os.getenv("STTM_CATALOG", "workspace")
PREFIX = os.getenv("STTM_PREFIX", "sttm")
PIPELINE_NAME = os.getenv("STTM_PIPELINE_NAME", "sttm-medallion")
LLM_ENDPOINT = os.getenv("STTM_LLM_ENDPOINT", "databricks-qwen3-next-80b-a3b-instruct")
DEFAULT_OWNER = os.getenv("STTM_DEFAULT_OWNER", "")
HISTORY_RETENTION_DAYS = int(os.getenv("STTM_HISTORY_RETENTION_DAYS", "30"))

LAYERS = ("bronze", "silver", "gold")
SCHEMAS = {name: f"{PREFIX}_{name}" for name in ("landing", "bronze", "silver", "gold", "meta")}
VOLUME = "raw"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMAS['landing']}/{VOLUME}"
META = f"`{CATALOG}`.`{SCHEMAS['meta']}`"


def fq(layer: str) -> str:
    """Fully qualified schema of a layer, e.g. workspace.sttm_silver."""
    return f"{CATALOG}.{SCHEMAS[layer]}"


def layer_of_schema(schema: str) -> str | None:
    for layer in LAYERS:
        if schema == SCHEMAS[layer]:
            return layer
    return None


# Values substituted into the pipeline SQL (${name}); the agent applies the same substitution when parsing.
PIPELINE_CONF = {"bronze": fq("bronze"), "silver": fq("silver"), "gold": fq("gold"), "landing": LANDING_PATH}


def client() -> WorkspaceClient:
    return WorkspaceClient()


def host_of(w: WorkspaceClient) -> str:
    return w.config.host.rstrip("/")


def load_state(w: WorkspaceClient, required: bool = True) -> dict:
    if not STATE_FILE.exists() and os.getenv("STTM_PIPELINE_ID"):
        # running as a Databricks job: ids come from the job parameters (run.py --pipeline-id/--warehouse-id)
        return {"pipeline_id": os.environ["STTM_PIPELINE_ID"], "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID", "")}
    if not STATE_FILE.exists():
        if required:
            raise SystemExit("sttm_state.json not found - run `uv run python sttm-agent/run.py setup` first.")
        return {}
    state = json.loads(STATE_FILE.read_text())
    if state.get("host") != host_of(w):
        if required:
            raise SystemExit(f"sttm_state.json belongs to {state.get('host')}, connected to {host_of(w)}: run setup.")
        return {}
    os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", state.get("warehouse_id", ""))
    return state


def save_state(w: WorkspaceClient, **kv) -> dict:
    state = load_state(w, required=False)
    state.update(kv, host=host_of(w))
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")
    return state


def pick_warehouse(w: WorkspaceClient) -> str:
    if os.getenv("DATABRICKS_WAREHOUSE_ID"):
        return os.environ["DATABRICKS_WAREHOUSE_ID"]
    whs = list(w.warehouses.list())
    if not whs:
        raise SystemExit("No SQL warehouse in this workspace - create one or set DATABRICKS_WAREHOUSE_ID.")
    os.environ["DATABRICKS_WAREHOUSE_ID"] = whs[0].id
    return whs[0].id


def run_sql(w: WorkspaceClient, sql: str, params: dict[str, str | None] | None = None) -> list[dict]:
    """Run SQL on the warehouse (Statement Execution API); rows as dicts (values are strings or None)."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID") or pick_warehouse(w),
        statement=sql,
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()],
        wait_timeout="50s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        msg = resp.status.error.message if resp.status.error else resp.status.state
        raise RuntimeError(f"SQL failed: {msg}\n--- statement ---\n{sql[:2000]}")
    if not resp.manifest or not resp.manifest.schema or not resp.manifest.schema.columns:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = list(resp.result.data_array or []) if resp.result else []
    # large results come in chunks
    chunk = resp.result.next_chunk_index if resp.result else None
    while chunk is not None:
        part = w.statement_execution.get_statement_result_chunk_n(resp.statement_id, chunk)
        rows += part.data_array or []
        chunk = part.next_chunk_index
    return [dict(zip(cols, r)) for r in rows]


def sql_str(value: str) -> str:
    """Quote a string literal for DDL that cannot take parameters (COMMENT ON, SET TAGS)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
