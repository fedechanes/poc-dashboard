"""Shared config + Databricks helpers for the POC.

Everything environment-specific comes from environment variables (or a local `.env` file):

  Databricks auth   standard Databricks unified auth, e.g. DATABRICKS_CONFIG_PROFILE=<profile in ~/.databrickscfg>
                    or DATABRICKS_HOST + DATABRICKS_TOKEN / DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET
  DATABRICKS_WAREHOUSE_ID     optional, auto-detected by setup.py
  POC_CATALOG / POC_SCHEMA    where the POC tables go (default: workspace / poc_dashboard)
  POC_EMBED_DOMAINS           domains allowed to embed the dashboards (default: localhost,127.0.0.1)
  DATABRICKS_SP_CLIENT_ID / DATABRICKS_SP_CLIENT_SECRET
                              service principal for option 2, created by setup.py if missing

Workspace-specific ids produced by setup.py live in poc_state.json (gitignored).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / ".env"
STATE_FILE = ROOT / "poc_state.json"


def load_env(path: Path = ENV_FILE) -> None:
    """Tiny .env loader: real environment variables win over the file."""
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                if v.strip():
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def set_env_value(key: str, value: str, path: Path = ENV_FILE) -> None:
    """Create or update KEY=value in .env (keeps other lines), file readable by the owner only."""
    lines = path.read_text().splitlines() if path.exists() else []
    lines = [ln for ln in lines if not ln.startswith(f"{key}=")] + [f"{key}={value}"]
    path.write_text("\n".join(lines) + "\n")
    os.chmod(path, 0o600)
    os.environ[key] = value


load_env()

CATALOG = os.getenv("POC_CATALOG", "workspace")
SCHEMA = os.getenv("POC_SCHEMA", "poc_dashboard")
FQ = f"`{CATALOG}`.`{SCHEMA}`"
EMBED_DOMAINS = [d.strip() for d in os.getenv("POC_EMBED_DOMAINS", "localhost,127.0.0.1").split(",") if d.strip()]

# Dashboard identifiers we control (they become the filter ids <page>~<widget>)
PAGE_NAME = "poc"
ZIP_WIDGET = "zip_filter"
FARE_WIDGET = "fare_filter"

# The one query both the dashboard and the app run, so they show the same numbers.
# samples.nyctaxi.trips ships with every Databricks workspace.
TRIPS_SQL = """
SELECT dropoff_zip,
       COUNT(*)                   AS trips,
       ROUND(AVG(fare_amount), 2) AS avg_fare,
       ROUND(SUM(fare_amount), 2) AS total_fare
FROM samples.nyctaxi.trips
WHERE pickup_zip = CAST(:pickup_zip AS INT)
  AND fare_amount >= CAST(:min_fare AS DOUBLE)
GROUP BY dropoff_zip
ORDER BY trips DESC
""".strip()

# Option 2 (external users): the pickup ZIP is NOT a viewer-editable parameter. It is baked into the
# short-lived token minted by our backend and surfaces in SQL as __aibi_external_value.
EXTERNAL_SQL = TRIPS_SQL.replace("CAST(:pickup_zip AS INT)", "CAST(__aibi_external_value AS INT)")


def client() -> WorkspaceClient:
    """Databricks unified auth: DATABRICKS_CONFIG_PROFILE, DATABRICKS_HOST/TOKEN, OAuth env vars, ..."""
    return WorkspaceClient()


def host_of(w: WorkspaceClient) -> str:
    return w.config.host.rstrip("/")


def run_sql(w: WorkspaceClient, sql: str, params: dict[str, str | None] | None = None,
            warehouse_id: str | None = None) -> list[dict]:
    """Run SQL on the warehouse (Statement Execution API) and return rows as dicts."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id or os.environ["DATABRICKS_WAREHOUSE_ID"],
        statement=sql,
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()],
        wait_timeout="50s",
    )
    # Serverless warehouse might still be waking up: poll until done.
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL failed: {resp.status.error.message if resp.status.error else resp.status.state}")
    if not resp.manifest or not resp.manifest.schema.columns:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (resp.result.data_array or [])]


def load_state(w: WorkspaceClient) -> dict:
    """Ids created by setup.py, checked against the workspace we are connected to."""
    if not STATE_FILE.exists():
        raise SystemExit("poc_state.json not found - run `uv run python setup.py` first.")
    state = json.loads(STATE_FILE.read_text())
    if state.get("host") != host_of(w):
        raise SystemExit(f"poc_state.json belongs to {state.get('host')} but you are connected to "
                         f"{host_of(w)} - run `uv run python setup.py` for this workspace.")
    os.environ.setdefault("DATABRICKS_WAREHOUSE_ID", state["warehouse_id"])
    return state
