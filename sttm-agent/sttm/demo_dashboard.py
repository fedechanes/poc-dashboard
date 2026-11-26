"""A small AI/BI dashboard over the gold tables, so the medallion has a real downstream consumer to show.

`run.py demo-dashboard` creates it once (then updates it in place by the id in sttm_state.json, or finds it by name in
/Workspace/Users/<you>/sttm-agent/dashboards), publishes it, and refreshes it once with a one-time job run (a
dashboard task). The refresh runs its queries on the warehouse, which is what makes Unity Catalog record the
dashboard as a reader of gold.customer_360 and gold.daily_sales in system.access.table_lineage (entity_type
DASHBOARD_V3), about 15-20 minutes later. `run.py destroy` moves it to the trash.

Queries use bare table names; the catalog and gold schema are passed as the dataset defaults, so the dashboard
follows STTM_CATALOG / STTM_PREFIX.
"""

from __future__ import annotations

import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service import jobs as J
from databricks.sdk.service.dashboards import Dashboard, LifecycleState

from .config import CATALOG, SCHEMAS

NAME = os.getenv("STTM_DEMO_DASHBOARD_NAME", "STTM demo - Sales")


def _folder(w: WorkspaceClient) -> str:
    return f"/Workspace/Users/{w.current_user.me().user_name}/sttm-agent/dashboards"


def _text(name: str, lines: list[str], y: int, h: int = 1) -> dict:
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
            "position": {"x": 0, "y": y, "width": 12, "height": h}}


def _counter(name: str, title: str, ds: str, field: str, expr: str, fmt: dict | None, x: int) -> dict:
    value = {"fieldName": field, "displayName": title} | ({"format": fmt} if fmt else {})
    return {"widget": {
        "name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": False, "fields": [
            {"name": field, "expression": expr},
            {"name": "weekly(order_date)", "expression": 'DATE_TRUNC("WEEK", `order_date`)'}]}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": value, "period": {"fieldName": "weekly(order_date)"}},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": 2, "width": 4, "height": 3}}


EUR = {"type": "number-currency", "currencyCode": "EUR", "abbreviation": "compact",
       "decimalPlaces": {"type": "max", "places": 1}}


def serialized() -> str:
    datasets = [
        {"name": "ds_sales", "displayName": "Daily sales (gold.daily_sales)", "queryLines": [
            "SELECT order_date, category, country_code, orders, units, revenue_eur, gross_margin_eur ",
            "FROM daily_sales"]},
        {"name": "ds_customers", "displayName": "Customers (gold.customer_360)", "queryLines": [
            "SELECT customer_id, country_code, coalesce(loyalty_tier, 'None') AS loyalty_tier, order_count, ",
            "lifetime_value_eur, avg_order_value_eur, first_order_date AS order_date ",
            "FROM customer_360"]},
    ]
    layout = [
        _text("title", ["## STTM demo - Sales"], 0),
        _text("subtitle", ["A downstream consumer of the STTM medallion: reads `gold.daily_sales` and "
                           "`gold.customer_360`. Created by `run.py demo-dashboard`; the STTM viewer lists it under "
                           "*Used by*."], 1),
        _counter("kpi-revenue", "Revenue", "ds_sales", "sum(revenue_eur)", "SUM(`revenue_eur`)", EUR, 0),
        _counter("kpi-orders", "Orders", "ds_sales", "sum(orders)", "SUM(`orders`)", None, 4),
        _counter("kpi-customers", "Customers", "ds_customers", "count(customer_id)",
                 "COUNT(`customer_id`)", None, 8),
        {"widget": {
            "name": "revenue-by-week",
            "queries": [{"name": "main_query", "query": {"datasetName": "ds_sales", "disaggregated": False, "fields": [
                {"name": "weekly(order_date)", "expression": 'DATE_TRUNC("WEEK", `order_date`)'},
                {"name": "category", "expression": "`category`"},
                {"name": "sum(revenue_eur)", "expression": "SUM(`revenue_eur`)"}]}}],
            "spec": {"version": 3, "widgetType": "bar", "encodings": {
                "x": {"fieldName": "weekly(order_date)", "scale": {"type": "temporal"}, "displayName": "Week"},
                "y": {"fieldName": "sum(revenue_eur)", "scale": {"type": "quantitative"}, "displayName": "Revenue (EUR)"},
                "color": {"fieldName": "category", "scale": {"type": "categorical"}, "displayName": "Category"}},
                "frame": {"showTitle": True, "title": "Revenue by week and category"}}},
            "position": {"x": 0, "y": 5, "width": 7, "height": 6}},
        {"widget": {
            "name": "ltv-by-tier",
            "queries": [{"name": "main_query", "query": {"datasetName": "ds_customers", "disaggregated": False,
                                                         "fields": [
                {"name": "loyalty_tier", "expression": "`loyalty_tier`"},
                {"name": "avg(lifetime_value_eur)", "expression": "AVG(`lifetime_value_eur`)"}]}}],
            "spec": {"version": 3, "widgetType": "bar", "encodings": {
                "x": {"fieldName": "loyalty_tier", "displayName": "Loyalty tier", "scale": {
                    "type": "categorical",
                    "sort": {"by": "custom-order", "orderedValues": ["None", "Bronze", "Silver", "Gold"]}}},
                "y": {"fieldName": "avg(lifetime_value_eur)", "scale": {"type": "quantitative"},
                      "displayName": "Average lifetime value (EUR)"}},
                "frame": {"showTitle": True, "title": "Average lifetime value by loyalty tier"}}},
            "position": {"x": 7, "y": 5, "width": 5, "height": 6}},
    ]
    return json.dumps({
        "datasets": datasets,
        "pages": [{"name": "sales", "displayName": "Sales", "pageType": "PAGE_TYPE_CANVAS",
                   "layoutVersion": "GRID_V1", "layout": layout}],
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#FAFAF7", "dark": "#1B2221"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#121817"},
            "fontColor": {"light": "#1D2A28", "dark": "#E6ECEA"},
            "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
            "visualizationColors": ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#D55E00", "#56B4E9"],
            "widgetHeaderAlignment": "LEFT"}},
    })


def _find(w: WorkspaceClient, state: dict) -> str | None:
    """Our dashboard: the id in the state file if it is still alive, else by name in our folder."""
    did = state.get("demo_dashboard_id")
    if did:
        try:
            d = w.lakeview.get(did)
            if d.lifecycle_state != LifecycleState.TRASHED:
                return did
        except (NotFound, ResourceDoesNotExist):
            pass
    folder = _folder(w)
    for d in w.lakeview.list():
        if d.display_name == NAME and (d.path or "").rsplit("/", 1)[0] in (folder, folder.removeprefix("/Workspace")):
            return d.dashboard_id
    return None


def ensure(w: WorkspaceClient, state: dict, log=print) -> tuple[str, str]:
    """Create or update the dashboard, then publish it (embedded credentials: viewers' queries run as the publisher).
    Returns (dashboard id, published URL)."""
    folder = _folder(w)
    w.workspace.mkdirs(folder)
    want = Dashboard(display_name=NAME, parent_path=folder, serialized_dashboard=serialized(),
                     warehouse_id=state["warehouse_id"])
    gold = SCHEMAS["gold"]
    did = _find(w, state)
    if did:
        w.lakeview.update(did, Dashboard(display_name=NAME, serialized_dashboard=want.serialized_dashboard,
                                         warehouse_id=state["warehouse_id"]),
                          dataset_catalog=CATALOG, dataset_schema=gold)
        log(f"   updated dashboard '{NAME}' ({did})")
    else:
        did = w.lakeview.create(want, dataset_catalog=CATALOG, dataset_schema=gold).dashboard_id
        log(f"   created dashboard '{NAME}' ({did}) in /Workspace/Users/<you>/sttm-agent/dashboards")
    w.lakeview.publish(did, embed_credentials=True, warehouse_id=state["warehouse_id"])
    return did, f"{w.config.host.rstrip('/')}/sql/dashboardsv3/{did}/published"


def refresh(w: WorkspaceClient, dashboard_id: str, warehouse_id: str, log=print) -> str:
    """Run the dashboard's queries once (one-time job run with a dashboard task, no job is kept), so that Unity
    Catalog records the reads in its lineage system tables."""
    run = w.jobs.submit(run_name=f"{NAME}: refresh (records lineage)", tasks=[J.SubmitTask(
        task_key="refresh", dashboard_task=J.DashboardTask(dashboard_id=dashboard_id, warehouse_id=warehouse_id))]
    ).result()
    result = run.state.result_state.value if run.state and run.state.result_state else "unknown"
    log(f"   refresh run {run.run_id}: {result}")
    return result


def delete(w: WorkspaceClient, state: dict, log=print) -> None:
    did = _find(w, state)
    if did:
        w.lakeview.trash(did)
        log(f"   moved dashboard '{NAME}' to the trash")
