"""STTM agent demo: one entry point, safe to re-run.

  uv run python sttm-agent/run.py setup     schemas, volume, batch 1, pipeline (repo SQL), first agent run
  uv run python sttm-agent/run.py agent     incremental agent run   [--full] [--dry-run]
  uv run python sttm-agent/run.py pipeline  run the pipeline once
  uv run python sttm-agent/run.py demo      incremental demo: batch 2 + a pipeline SQL change, then re-run
  uv run python sttm-agent/run.py export    re-export CSV/Markdown from the STTM table
  uv run python sttm-agent/run.py demo-dashboard  create/update + publish the "STTM demo - Sales" dashboard, run it once
  uv run python sttm-agent/run.py app-deploy  deploy the agent as a job and the viewer app (app/) as a Databricks App
  uv run python sttm-agent/run.py destroy   delete the app, the job, the demo dashboard and the pipeline, drop the sttm_* schemas
"""

from __future__ import annotations

import argparse
import json
import os

from sttm import agent, demo_dashboard, medallion, viewer_app
from sttm.config import (CATALOG, META, PIPELINE_DIR, SCHEMAS, STATE_FILE, client, fq, load_state, pick_warehouse,
                         run_sql, save_state)

# The demo's pipeline change: one changed rule, two new columns (applied to the deployed copy, not the repo files).
DEMO_CHANGES = [
    ("silver.sql",
     "  order_status IN ('cancelled', 'returned')                          AS is_cancelled",
     "  order_status = 'cancelled'                                         AS is_cancelled,\n"
     "  net_amount * fx_rate_to_eur >= 250                                 AS is_large_order"),
    ("gold.sql",
     "    sum(net_amount_eur) FILTER (WHERE NOT is_cancelled)              AS lifetime_value_eur\n",
     "    sum(net_amount_eur) FILTER (WHERE NOT is_cancelled)              AS lifetime_value_eur,\n"
     "    count(DISTINCT order_id) FILTER (WHERE is_large_order AND NOT is_cancelled) AS large_order_count\n"),
    ("gold.sql",
     "  coalesce(s.lifetime_value_eur, 0)                                  AS lifetime_value_eur,\n",
     "  coalesce(s.lifetime_value_eur, 0)                                  AS lifetime_value_eur,\n"
     "  coalesce(s.large_order_count, 0)                                   AS large_order_count,\n"),
]

LEGACY_COMMENT = ("gold.customer_360", "avg_order_value_eur",
                  "Total revenue of the customer, including cancelled orders")


def show(title: str) -> None:
    print(f"\n=== {title}", flush=True)


def print_flows(res: dict) -> None:
    print(f"   pipeline update finished in {res['seconds']}s")
    for f in res["flows"]:
        print(f"     {f['flow'].split('.', 1)[-1]:38} rows written {f['rows_written']:>6}   {f['technique']}")


def print_summary(s: dict) -> None:
    keys = ["tables_processed", "rows_inserted", "rows_updated", "rows_deactivated", "rows_unchanged", "llm_calls",
            "comments_written", "tags_written", "open_issues", "issues_new", "issues_closed", "uc_edges_new", "consumer_edges_seen",
            "consumers_resolved", "sttm_version", "seconds"]
    print("   " + ", ".join(f"{k}={s.get(k)}" for k in keys if k in s))


def seed_legacy_comment(w) -> None:
    """Simulate a stale comment someone typed in Catalog Explorer, so the agent has a conflict to find."""
    table, col, text = LEGACY_COMMENT
    t = f"{CATALOG}.{SCHEMAS[table.split('.')[0]]}.{table.split('.')[1]}"
    rows = run_sql(w, f"""SELECT comment FROM `{CATALOG}`.information_schema.columns
                          WHERE table_schema = :s AND table_name = :t AND column_name = :c""",
                   {"s": t.split(".")[1], "t": t.split(".")[2], "c": col})
    if rows and rows[0]["comment"] is None:
        run_sql(w, f"COMMENT ON COLUMN {t}.{col} IS '{text}'")
        print(f"   seeded a stale human comment on {table}.{col}")


def cmd_setup(w, a) -> None:
    show("1. warehouse, schemas, landing volume")
    save_state(w, warehouse_id=pick_warehouse(w))
    medallion.create_schemas(w)
    print("   " + ", ".join(f"{CATALOG}.{s}" for s in SCHEMAS.values()))
    show("2. land raw batch 1")
    for f in medallion.land_batch(w, 1):
        print("   " + f)
    show("3. deploy the Lakeflow pipeline (repo SQL) and run it")
    pid = medallion.deploy_pipeline(w)
    print(f"   pipeline {pid}")
    print_flows(medallion.run_pipeline(w, pid))
    seed_legacy_comment(w)
    show("4. STTM agent")
    print_summary(agent.build(w, agent.Log(), full=a.full))


def cmd_agent(w, a) -> None:
    load_state(w)
    print_summary(agent.build(w, agent.Log(), full=a.full, dry_run=a.dry_run))


def cmd_pipeline(w, a) -> None:
    print_flows(medallion.run_pipeline(w, load_state(w)["pipeline_id"], full_refresh=a.full))


def cmd_export(w, a) -> None:
    load_state(w)
    agent.export(w, agent.Log())


def cmd_demo(w, a) -> None:
    state = load_state(w)
    pid = state["pipeline_id"]
    show("1. land raw batch 2 (new + updated customers, new orders, status changes, price changes)")
    for f in medallion.land_batch(w, 2):
        print("   " + f)

    show("2. change the deployed pipeline SQL")
    files = {f: (PIPELINE_DIR / f).read_text() for f in medallion.PIPELINE_FILES}
    for f, old, new in DEMO_CHANGES:
        assert old in files[f], f"demo change no longer applies to {f}"
        files[f] = files[f].replace(old, new)
        print(f"   {f}: " + " | ".join(ln.strip() for ln in new.splitlines() if ln.strip()))
    medallion.upload_pipeline_sql(w, overrides=files)

    show("3. pipeline run: only new files are ingested; tables refresh incrementally where possible")
    print_flows(medallion.run_pipeline(w, pid))
    for t in ("customers_raw", "orders_raw"):
        rows = run_sql(w, f"SELECT _source_file, count(*) AS n FROM {fq('bronze')}.{t} GROUP BY 1 ORDER BY 1")
        print(f"   bronze.{t}: " + ", ".join(f"{r['_source_file']}={r['n']}" for r in rows))
    rows = run_sql(w, f"SELECT count(*) AS n, count(DISTINCT customer_id) AS k FROM {fq('silver')}.customers")
    print(f"   silver.customers: {rows[0]['n']} rows for {rows[0]['k']} customers (AUTO CDC upserted the updates)")

    show("4. STTM agent, incremental")
    s = agent.build(w, agent.Log())
    print_summary(s)
    if s.get("sttm_version", 0) > s.get("sttm_version_before", 0):
        show(f"5. what changed in {META.replace('`', '')}.sttm (change data feed, versions "
             f"{s['sttm_version_before'] + 1}..{s['sttm_version']})")
        changes = run_sql(w, f"""
            SELECT _change_type, target_table, target_column, transformation, business_definition, issues,
                   lineage_hash, _commit_version
            FROM table_changes('{META.replace('`', '')}.sttm', {s['sttm_version_before'] + 1})
            ORDER BY target_table, target_column, _change_type""")
        pre = {(c["target_table"], c["target_column"]): c for c in changes if c["_change_type"] == "update_preimage"}
        for c in changes:
            if c["_change_type"] == "update_preimage":
                continue
            k = (c["target_table"], c["target_column"])
            what = ""
            if k in pre:
                diff = [f for f in ("transformation", "business_definition", "issues", "lineage_hash")
                        if pre[k][f] != c[f]]
                what = "changed: " + ", ".join(diff)
            print(f"   {c['_change_type']:17} {agent.short(c['target_table'])}.{c['target_column']:22} {what}")
            if k in pre and pre[k]["business_definition"] != c["business_definition"]:
                print(f"       definition was: {pre[k]['business_definition']}")
                print(f"       definition now: {c['business_definition']}")
            if c["issues"] and c["issues"] != "[]" and (k not in pre or pre[k]["issues"] != c["issues"]):
                for i in json.loads(c["issues"]):
                    if not i.startswith("INFO"):
                        print(f"       issue: {i}")

    show("6. STTM agent again with nothing changed (should touch nothing)")
    print_summary(agent.build(w, agent.Log()))


def cmd_app_deploy(w, a) -> None:
    state = load_state(w)
    app, job_id, alerts = viewer_app.deploy(w, state)
    save_state(w, app_name=app.name, job_id=job_id, alerts=alerts)
    print(f"   {app.name}: {app.url}")


def cmd_demo_dashboard(w, a) -> None:
    state = load_state(w)
    show(f"1. dashboard '{demo_dashboard.NAME}' on gold.daily_sales and gold.customer_360")
    did, url = demo_dashboard.ensure(w, state)
    save_state(w, demo_dashboard_id=did)
    print(f"   published: {url}")
    show("2. run its queries once, so Unity Catalog records the dashboard as a reader (lineage lands ~15-20 min later)")
    demo_dashboard.refresh(w, did, state["warehouse_id"])
    print("   then `run.py agent` (or Run agent in the app) lists it under Used by")


def cmd_destroy(w, a) -> None:
    state = load_state(w, required=False)
    viewer_app.delete(w, state)
    demo_dashboard.delete(w, state)
    if state.get("pipeline_id"):
        w.pipelines.delete(state["pipeline_id"])
        print(f"   deleted pipeline {state['pipeline_id']}")
    for s in reversed(list(SCHEMAS.values())):
        run_sql(w, f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{s}` CASCADE")
        print(f"   dropped {CATALOG}.{s}")
    try:
        w.workspace.delete(medallion.workspace_dir(w).rsplit("/", 1)[0], recursive=True)
    except Exception:
        pass
    STATE_FILE.unlink(missing_ok=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["setup", "agent", "pipeline", "demo", "export", "demo-dashboard", "app-deploy", "destroy"])
    p.add_argument("--full", action="store_true", help="agent: reprocess every table; pipeline: full refresh")
    p.add_argument("--dry-run", action="store_true", help="agent: show what would change, write nothing")
    # used by the agent job, which has no sttm_state.json and no local output folder
    p.add_argument("--pipeline-id", help="pipeline id (instead of sttm_state.json)")
    p.add_argument("--warehouse-id", help="SQL warehouse id (instead of sttm_state.json)")
    p.add_argument("--no-files", action="store_true", help="agent: skip output/sttm.csv and sttm.md")
    p.add_argument("--requested-by", help="agent: who asked for this run (recorded in agent_runs.details)")
    a = p.parse_args()
    for flag, env in ((a.pipeline_id, "STTM_PIPELINE_ID"), (a.warehouse_id, "DATABRICKS_WAREHOUSE_ID")):
        if flag:
            os.environ[env] = flag
    if a.requested_by:
        os.environ["STTM_REQUESTED_BY"] = a.requested_by
    if a.no_files:
        os.environ["STTM_EXPORT_FILES"] = "0"
    w = client()
    globals()[f"cmd_{a.command.replace('-', '_')}"](w, a)


if __name__ == "__main__":
    main()   # no sys.exit(): a job task reports any SystemExit, even a clean one, as failed
