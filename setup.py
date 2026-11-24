"""Idempotent setup of the Databricks side of the POC, for whatever workspace your auth points to.

  1. pick a SQL warehouse (DATABRICKS_WAREHOUSE_ID or auto-detect)
  2. schema + 2 Delta tables (app_params, app_results)
  3. service principal + OAuth secret for option 2 (reused from .env when valid, else created)
  4. two AI/BI dashboards, published with embedded credentials:
       - "POC - Embedded parameters"  option 1, pickup_zip + min_fare as filters
       - "POC - External embed"       option 2, pickup_zip from __aibi_external_value, shared CAN RUN with the SP
  5. embedding: checks the workspace policy and adds POC_EMBED_DOMAINS to the approved domains (merge, never replace)
  6. writes poc_state.json (ids for this workspace)

Run:  uv run python setup.py          (re-run safely at any time)
"""

from __future__ import annotations

import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.settings import (
    AibiDashboardEmbeddingAccessPolicyAccessPolicyType as Policy,
    AibiDashboardEmbeddingApprovedDomains,
    AibiDashboardEmbeddingApprovedDomainsSetting,
)
from databricks.sdk.service.sql import State

from poc.common import (
    CATALOG, EMBED_DOMAINS, EXTERNAL_SQL, FARE_WIDGET, FQ, PAGE_NAME, STATE_FILE, TRIPS_SQL, ZIP_WIDGET,
    client, host_of, run_sql, set_env_value,
)

SP_NAME = os.getenv("POC_SP_NAME", "poc-dashboard-embed")
SP_SECRET_DAYS = 90


# ─────────────────────────── dashboard definition ───────────────────────────

def param(keyword: str, default: str) -> dict:
    return {
        "displayName": keyword,
        "keyword": keyword,
        "dataType": "STRING",
        "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": default}]}},
    }


def filter_widget(name: str, title: str, keyword: str) -> dict:
    qname = f"param_{keyword}"
    return {
        "name": name,
        "queries": [{
            "name": qname,
            "query": {"datasetName": "trips", "parameters": [{"name": keyword, "keyword": keyword}],
                      "disaggregated": False},
        }],
        "spec": {
            "version": 2,
            "widgetType": "filter-text-entry",
            "encodings": {"fields": [{"parameterName": keyword, "queryName": qname}]},
            "frame": {"showTitle": True, "title": title},
        },
    }


def counter(name: str, title: str, expr: str) -> dict:
    return {
        "name": name,
        "queries": [{"name": "main_query", "query": {
            "datasetName": "trips", "fields": [{"name": "v", "expression": expr}], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": "v", "displayName": title}},
                 "frame": {"showTitle": True, "title": title}},
    }


def dashboard_json(sql: str, params: list, zip_filter: bool) -> dict:
    bar = {
        "name": "trips_by_dropoff",
        "queries": [{"name": "main_query", "query": {
            "datasetName": "trips",
            "fields": [{"name": "dropoff_zip", "expression": "`dropoff_zip`"},
                       {"name": "sum(trips)", "expression": "SUM(`trips`)"}],
            "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar",
                 "encodings": {"x": {"fieldName": "dropoff_zip", "scale": {"type": "categorical"},
                                     "displayName": "Dropoff ZIP"},
                               "y": {"fieldName": "sum(trips)", "scale": {"type": "quantitative"},
                                     "displayName": "Trips"}},
                 "frame": {"showTitle": True, "title": "Trips by dropoff ZIP"}},
    }
    filters = [{"widget": filter_widget(ZIP_WIDGET, "Pickup ZIP", "pickup_zip"),
                "position": {"x": 0, "y": 0, "width": 3, "height": 1}}] if zip_filter else []
    return {
        "datasets": [{"name": "trips", "displayName": "Trips (parameterised)", "queryLines": [sql],
                      "parameters": params}],
        "pages": [{
            "name": PAGE_NAME,
            "displayName": "POC",
            "layout": filters + [
                {"widget": filter_widget(FARE_WIDGET, "Min fare ($)", "min_fare"),
                 "position": {"x": 3, "y": 0, "width": 3, "height": 1}},
                {"widget": counter("kpi_trips", "Trips", "SUM(`trips`)"),
                 "position": {"x": 0, "y": 1, "width": 2, "height": 2}},
                {"widget": counter("kpi_fare", "Total fare ($)", "SUM(`total_fare`)"),
                 "position": {"x": 2, "y": 1, "width": 2, "height": 2}},
                {"widget": counter("kpi_zips", "Dropoff ZIPs", "COUNT(`dropoff_zip`)"),
                 "position": {"x": 4, "y": 1, "width": 2, "height": 2}},
                {"widget": bar, "position": {"x": 0, "y": 3, "width": 6, "height": 6}},
            ],
        }],
    }


# ─────────────────────────── steps ───────────────────────────

def pick_warehouse(w: WorkspaceClient) -> str:
    if os.getenv("DATABRICKS_WAREHOUSE_ID"):
        return os.environ["DATABRICKS_WAREHOUSE_ID"]
    whs = list(w.warehouses.list())
    if not whs:
        raise SystemExit("No SQL warehouse in this workspace - create one or set DATABRICKS_WAREHOUSE_ID.")
    # prefer serverless, then running ones
    whs.sort(key=lambda h: (not h.enable_serverless_compute, h.state != State.RUNNING))
    print(f"   using warehouse {whs[0].name} ({whs[0].id}); set DATABRICKS_WAREHOUSE_ID to choose another")
    return whs[0].id


def ensure_service_principal(w: WorkspaceClient) -> str:
    """Reuse the SP from .env if it exists in THIS workspace, else create one + a secret and save to .env."""
    app_id = os.getenv("DATABRICKS_SP_CLIENT_ID")
    if app_id and os.getenv("DATABRICKS_SP_CLIENT_SECRET"):
        if next(iter(w.service_principals.list(filter=f'applicationId eq "{app_id}"')), None):
            print(f"   reusing service principal {app_id} from .env")
            return app_id
        print(f"   service principal {app_id} from .env is not in this workspace - creating a new one")

    sp = next(iter(w.service_principals.list(filter=f'displayName eq "{SP_NAME}"')), None)
    if not sp:
        sp = w.service_principals.create(display_name=SP_NAME, active=True, entitlements=[
            iam.ComplexValue(value="databricks-sql-access"), iam.ComplexValue(value="workspace-access")])
    secret = w.service_principal_secrets_proxy.create(service_principal_id=int(sp.id),
                                                      lifetime=f"{SP_SECRET_DAYS * 86400}s")
    set_env_value("DATABRICKS_SP_CLIENT_ID", sp.application_id)
    set_env_value("DATABRICKS_SP_CLIENT_SECRET", secret.secret)
    print(f"   service principal {SP_NAME} ({sp.application_id}), new secret saved to .env, "
          f"expires {secret.expire_time}")
    return sp.application_id


def upsert_dashboard(w: WorkspaceClient, me: str, warehouse_id: str, dashboard_id: str | None,
                     name: str, body: dict) -> str:
    dash = Dashboard(display_name=name, warehouse_id=warehouse_id, parent_path=f"/Users/{me}",
                     serialized_dashboard=json.dumps(body))
    d = w.lakeview.update(dashboard_id, dashboard=dash) if dashboard_id else w.lakeview.create(dashboard=dash)
    # embed_credentials=True: queries run as the publisher, so viewers (and the SP) need no table grants
    w.lakeview.publish(d.dashboard_id, embed_credentials=True, warehouse_id=warehouse_id)
    return d.dashboard_id


def configure_embedding(w: WorkspaceClient) -> None:
    policy = w.settings.aibi_dashboard_embedding_access_policy.get() \
        .aibi_dashboard_embedding_access_policy.access_policy_type
    print(f"   workspace embedding policy: {policy.value}")
    if policy == Policy.DENY_ALL_DOMAINS:
        print("   !! embedding is disabled. A workspace admin must set Settings > Security > "
              "Embed dashboards to 'Allow approved domains'.")
        return
    if policy == Policy.ALLOW_ALL_DOMAINS:
        return
    current = w.settings.aibi_dashboard_embedding_approved_domains.get() \
        .aibi_dashboard_embedding_approved_domains.approved_domains or []
    merged = list(dict.fromkeys([*current, *EMBED_DOMAINS]))
    if merged != current:
        w.settings.aibi_dashboard_embedding_approved_domains.update(
            allow_missing=True,
            setting=AibiDashboardEmbeddingApprovedDomainsSetting(
                aibi_dashboard_embedding_approved_domains=AibiDashboardEmbeddingApprovedDomains(
                    approved_domains=merged)),
            field_mask="aibi_dashboard_embedding_approved_domains.approved_domains",
        )
    print(f"   approved domains: {merged}")


def main() -> None:
    w = client()
    host = host_of(w)
    me = w.current_user.me().user_name
    print(f"workspace: {host} (as {me})")

    state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
    if state and state.get("host") != host:
        print(f"   poc_state.json was for {state.get('host')} - starting fresh for this workspace")
        state = {}

    print("1/5 SQL warehouse")
    warehouse_id = pick_warehouse(w)

    print(f"2/5 schema + tables in {FQ}")
    if not any(c.name == CATALOG for c in w.catalogs.list()):
        raise SystemExit(f"Catalog '{CATALOG}' not found - set POC_CATALOG to a catalog you can write to.")
    run_sql(w, f"CREATE SCHEMA IF NOT EXISTS {FQ}", warehouse_id=warehouse_id)
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {FQ}.app_params (
        request_id STRING, session_id STRING, pickup_zip INT, min_fare DOUBLE,
        source STRING, created_at TIMESTAMP)""", warehouse_id=warehouse_id)
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {FQ}.app_results (
        request_id STRING, total_trips BIGINT, total_fare DOUBLE, top_dropoff_zip INT,
        created_at TIMESTAMP)""", warehouse_id=warehouse_id)

    print("3/5 service principal (option 2)")
    sp_app_id = ensure_service_principal(w)

    print("4/5 dashboards")
    dash_id = upsert_dashboard(
        w, me, warehouse_id, state.get("dashboard_id"), "POC - Embedded parameters",
        dashboard_json(TRIPS_SQL, [param("pickup_zip", "10001"), param("min_fare", "0")], zip_filter=True))
    ext_id = upsert_dashboard(
        w, me, warehouse_id, state.get("external_dashboard_id"), "POC - External embed",
        dashboard_json(EXTERNAL_SQL, [param("min_fare", "0")], zip_filter=False))
    w.permissions.update("dashboards", ext_id, access_control_list=[AccessControlRequest(
        service_principal_name=sp_app_id, permission_level=PermissionLevel.CAN_RUN)])

    print("5/5 embedding settings")
    configure_embedding(w)

    state = {"host": host, "workspace_id": w.get_workspace_id(), "warehouse_id": warehouse_id,
             "dashboard_id": dash_id, "external_dashboard_id": ext_id}
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")
    print("done, wrote poc_state.json")
    print(f"   option 1 dashboard: {host}/dashboardsv3/{dash_id}/published?o={state['workspace_id']}")
    print(f"   option 2 dashboard: {host}/dashboardsv3/{ext_id}/published?o={state['workspace_id']}")
    print("next: uv run uvicorn poc.api:app --port 8000   ->  http://localhost:8000/external")


if __name__ == "__main__":
    main()
