"""Deploy the STTM viewer (app/) as a Databricks App, plus the agent as a job the app can start.
Safe to re-run: creates the job and the app once, then updates and redeploys them.

- The agent job runs `run.py agent` on serverless compute as the person who deploys (it writes comments and tags,
  reads system tables and calls the LLM, so it keeps the deployer's permissions).
- The app runs as its own service principal. It can use the SQL warehouse, start the agent job (CAN_MANAGE_RUN),
  and read seven tables: sttm_export, sttm (its change data feed, for History), agent_runs, glossary, owners, issues
  and issue_status. Nothing else.
- Steward and owner actions run as the signed-in user (user authorization, `sql` scope). The stewards group
  (STTM_STEWARDS_GROUP, default sttm-stewards) gets SELECT + MODIFY on glossary and issue_status; the members of each
  owner team (sttm_meta.owners) get SELECT + MODIFY on issue_status. The group itself and its members are managed
  outside this script (account console or SCIM).
- Notifications: one Databricks SQL alert per owner team (Alerts v2), subscribed by the team's notify_email. The
  alerts' own schedules stay paused; the agent job evaluates them in `alert_task`s right after each agent run.
"""

from __future__ import annotations

import io
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import apps as A
from databricks.sdk.service import compute as C
from databricks.sdk.service import jobs as J
from databricks.sdk.service import sql as S
from databricks.sdk.service.workspace import ImportFormat

from . import agent
from .config import (
    CATALOG,
    DEFAULT_OWNER,
    HISTORY_RETENTION_DAYS,
    LLM_ENDPOINT,
    META,
    PIPELINE_NAME,
    PREFIX,
    ROOT,
    SCHEMAS,
    run_sql,
    sql_str,
)

APP_NAME = os.getenv("STTM_APP_NAME", "sttm-viewer")
STEWARDS = os.getenv("STTM_STEWARDS_GROUP", "sttm-stewards")
USER_SCOPES = ["sql"]   # user authorization: steward writes run as the signed-in user
JOB_NAME = os.getenv("STTM_JOB_NAME", "sttm-agent")
APP_DIR = ROOT / "app"
APP_FILES = ["app.py", "history.py", "audit.py", "requirements.txt", "static/index.html"]
AGENT_DEPENDENCIES = ["databricks-sdk>=0.170.0", "sqlglot>=30.35.0"]
ALERT_PREFIX = "STTM new issues: "   # + owner team; alerts with this prefix in <user dir>/sttm-agent/alerts are ours


def _user_dir(w: WorkspaceClient) -> str:
    return f"/Workspace/Users/{w.current_user.me().user_name}/sttm-agent"


def _upload(w: WorkspaceClient, base: str, files: dict[str, str]) -> None:
    for d in sorted({f"{base}/{name}".rsplit("/", 1)[0] for name in files}):
        w.workspace.mkdirs(d)
    for name, text in files.items():
        w.workspace.upload(f"{base}/{name}", io.BytesIO(text.encode()), format=ImportFormat.RAW, overwrite=True)


# ---------------------------------------------------------------------------------------------------- agent job

def ensure_agent_job(w: WorkspaceClient, state: dict, alerts: dict[str, str] | None = None, log=print) -> int:
    """Upload the agent code + glossary and create/update the serverless job that runs it. Each owner team's alert
    gets a task that runs after the agent task, so owners hear about new issues right after the run."""
    base = f"{_user_dir(w)}/agent"
    files = {"run.py": (ROOT / "run.py").read_text(), "glossary.toml": (ROOT / "glossary.toml").read_text()}
    files |= {f"sttm/{p.name}": p.read_text() for p in sorted((ROOT / "sttm").glob("*.py"))}
    files |= {f"pipeline/{p.name}": p.read_text() for p in sorted((ROOT / "pipeline").glob("*.sql"))}
    # settings only, no credentials: the job authenticates as its owner on serverless compute
    files[".env"] = "".join(f"{k}={v}\n" for k, v in {
        "STTM_CATALOG": CATALOG, "STTM_PREFIX": PREFIX, "STTM_PIPELINE_NAME": PIPELINE_NAME,
        "STTM_LLM_ENDPOINT": LLM_ENDPOINT, "STTM_DEFAULT_OWNER": DEFAULT_OWNER,
        "STTM_HISTORY_RETENTION_DAYS": str(HISTORY_RETENTION_DAYS)}.items() if v)
    _upload(w, base, files)
    settings = J.JobSettings(
        name=JOB_NAME, max_concurrent_runs=1, tags={"project": "sttm-agent"},
        parameters=[J.JobParameterDefinition(name="requested_by", default="")],
        description="STTM agent: reads the deployed pipeline SQL and Unity Catalog, updates the STTM tables, "
                    "comments and tags. Started from the STTM viewer app.",
        environments=[J.JobEnvironment(environment_key="agent", spec=C.Environment(
            environment_version="3", dependencies=AGENT_DEPENDENCIES))],
        tasks=[J.Task(task_key="agent", environment_key="agent", timeout_seconds=1800,
                      spark_python_task=J.SparkPythonTask(python_file=f"{base}/run.py", parameters=[
                          "agent", "--pipeline-id", state["pipeline_id"], "--warehouse-id", state["warehouse_id"],
                          "--no-files", "--requested-by", "{{job.parameters.requested_by}}"]))]
        + [J.Task(task_key=f"notify_{_slug(team)}", depends_on=[J.TaskDependency(task_key="agent")],
                  description=f"Evaluate the SQL alert of {team}: mails its notify_email if the run opened issues",
                  alert_task=J.AlertTask(alert_id=alert_id, warehouse_id=state["warehouse_id"]))
           for team, alert_id in sorted((alerts or {}).items())])
    job_id = state.get("job_id")
    if not job_id:
        job_id = next((j.job_id for j in w.jobs.list(name=JOB_NAME)), None)
    if job_id:
        try:
            w.jobs.reset(int(job_id), settings)
            log(f"   updated job {JOB_NAME} ({job_id}); agent code in {base}")
            return int(job_id)
        except NotFound:
            pass
    job_id = w.jobs.create(**settings.as_shallow_dict()).job_id
    log(f"   created job {JOB_NAME} ({job_id}); agent code in {base}")
    return job_id


def _slug(text: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in text.lower()).strip("_")[:60]


# ---------------------------------------------------------------------------------------------------- alerts

def alert_query(team: str) -> str:
    """Issues the latest agent run opened on the team's tables (new, or back after they had gone), unless an owner
    already set them to accepted / won't fix. One row per issue, so the mail lists them."""
    return f"""SELECT i.column_key AS `column`, i.issue_type AS issue_type, i.issue_text AS issue
FROM {META}.issues i
LEFT JOIN {META}.issue_status s
  ON s.column_key = i.column_key AND s.issue_type = i.issue_type AND s.fingerprint = i.fingerprint
WHERE i.is_open AND i.owner = {sql_str(team)}
  AND i.opened_run = (SELECT max(run_id) FROM {META}.agent_runs)
  AND coalesce(s.status, 'open') NOT IN ('accepted', 'wont_fix')
ORDER BY 1, 2"""


def _alert(team: str, email: str, warehouse_id: str, parent: str, app_url: str | None) -> S.AlertV2:
    link = f'<p><a href="{app_url}/?view=worklist">Open your worklist in the STTM viewer</a></p>' if app_url else ""
    return S.AlertV2(
        display_name=ALERT_PREFIX + team, parent_path=parent, warehouse_id=warehouse_id, query_text=alert_query(team),
        evaluation=S.AlertV2Evaluation(
            source=S.AlertV2OperandColumn(name="column", aggregation=S.Aggregation.COUNT, display="new issues"),
            comparison_operator=S.ComparisonOperator.GREATER_THAN,
            threshold=S.AlertV2Operand(value=S.AlertV2OperandValue(double_value=0)),
            empty_result_state=S.AlertEvaluationState.OK,
            # retrigger: a run that opens issues while the alert is still TRIGGERED from the previous run mails again
            notification=S.AlertV2Notification(notify_on_ok=False, retrigger_seconds=60,
                                               subscriptions=[S.AlertV2Subscription(user_email=email)])),
        # evaluated by the agent job's alert tasks after each run, not on a schedule (no idle warehouse wake-ups)
        schedule=S.CronSchedule(quartz_cron_schedule="0 0 6 * * ?", timezone_id="UTC",
                                pause_status=S.SchedulePauseStatus.PAUSED),
        custom_summary=f"STTM: {{{{QUERY_RESULT_VALUE}}}} new issue(s) on tables owned by {team}",
        custom_description=(f"<p>The latest STTM agent run found new issues on tables owned by <b>{team}</b>. "
                            "Set a status (in progress, accepted, won't fix) in the viewer app.</p>"
                            f"{link}{{{{QUERY_RESULT_TABLE}}}}"))


ALERT_MASK = "display_name,query_text,warehouse_id,evaluation,schedule,custom_summary,custom_description"


def ensure_alerts(w: WorkspaceClient, state: dict, app_url: str | None, log=print) -> dict[str, str]:
    """One alert per owner team (sttm_meta.owners), created or updated in place; alerts of teams that are gone are
    moved to the trash. Returns {team: alert id}."""
    parent = f"{_user_dir(w)}/alerts"
    w.workspace.mkdirs(parent)
    teams = {r["owner"]: r["notify_email"] for r in run_sql(w, f"SELECT owner, notify_email FROM {META}.owners")
             if r["notify_email"]}
    ours = {a.display_name: a for a in w.alerts_v2.list_alerts()
            if a.display_name and a.display_name.startswith(ALERT_PREFIX) and (a.parent_path or "").rstrip("/")
            in (parent, parent.removeprefix("/Workspace"))}
    known = state.get("alerts", {})
    out = {}
    for team, email in sorted(teams.items()):
        want = _alert(team, email, state["warehouse_id"], parent, app_url)
        alert_id = known.get(team) or (ours[want.display_name].id if want.display_name in ours else None)
        if alert_id:
            try:
                w.alerts_v2.update_alert(alert_id, want, update_mask=ALERT_MASK)
                out[team] = alert_id
                continue
            except NotFound:
                pass
        out[team] = w.alerts_v2.create_alert(want).id
        log(f"   created SQL alert '{want.display_name}'")
    for name, a in ours.items():
        if a.id not in out.values():
            w.alerts_v2.trash_alert(a.id)
            log(f"   moved SQL alert '{name}' to the trash (team no longer in the owners table)")
    log(f"   {len(out)} SQL alerts, one per owner team, in /Workspace/Users/<you>/sttm-agent/alerts")
    return out


# ---------------------------------------------------------------------------------------------------- app

def _app_yaml() -> str:
    """app.yaml with this checkout's catalog/prefix, so the app reads the same tables the agent writes."""
    return (APP_DIR / "app.yaml").read_text() \
        .replace("value: workspace", f"value: {CATALOG}").replace("value: sttm-stewards", f"value: {STEWARDS}") \
        .replace("value: sttm\n", f"value: {PREFIX}\n")


def _resources(warehouse_id: str, job_id: int) -> list[A.AppResource]:
    return [
        A.AppResource(name="sql-warehouse", description="Warehouse that reads the STTM export table",
                      sql_warehouse=A.AppResourceSqlWarehouse(
                          id=warehouse_id, permission=A.AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE)),
        A.AppResource(name="agent-job", description="The STTM agent job, started by the Run agent button",
                      job=A.AppResourceJob(id=str(job_id), permission=A.AppResourceJobJobPermission.CAN_MANAGE_RUN)),
    ]


def _key(r: A.AppResource) -> tuple:
    return (r.name, r.sql_warehouse.id if r.sql_warehouse else None, r.job.id if r.job else None)


def ensure_app(w: WorkspaceClient, warehouse_id: str, job_id: int, log=print) -> A.App:
    wanted = _resources(warehouse_id, job_id)
    try:
        app = w.apps.get(APP_NAME)
    except NotFound:
        log(f"   creating app {APP_NAME} (starts its compute, a few minutes)")
        return w.apps.create(A.App(name=APP_NAME, resources=wanted, user_api_scopes=USER_SCOPES,
                                   description="Source-to-target mapping viewer (STTM agent POC)")).result()
    if {_key(r) for r in app.resources or []} != {_key(r) for r in wanted} \
            or sorted(app.user_api_scopes or []) != USER_SCOPES:
        log("   updating the app's resources (warehouse, agent job) and user authorization scopes")
        app = w.apps.update(APP_NAME, A.App(name=APP_NAME, description=app.description, resources=wanted,
                                            user_api_scopes=USER_SCOPES))
    state = app.compute_status.state if app.compute_status else None
    if state != A.ComputeState.ACTIVE:
        log(f"   app compute is {state.value if state else 'unknown'}: starting it")
        app = w.apps.start(APP_NAME).result()
    return app


# sttm: the History tab reads its change data feed (table_changes needs SELECT on the table); the other history
# tables (glossary, issues, issue_status) are read already
APP_READS = ("sttm_export", "sttm", "agent_runs", "glossary", "owners", "issues", "issue_status")


def grant_read(w: WorkspaceClient, app: A.App, log=print) -> None:
    sp = f"`{app.service_principal_client_id}`"
    meta = f"`{CATALOG}`.`{SCHEMAS['meta']}`"
    for sql in (f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO {sp}", f"GRANT USE SCHEMA ON SCHEMA {meta} TO {sp}",
                *(f"GRANT SELECT ON TABLE {meta}.{t} TO {sp}" for t in APP_READS)):
        run_sql(w, sql)
    log(f"   granted the app's service principal read access to {CATALOG}.{SCHEMAS['meta']}: {', '.join(APP_READS)}")
    if STEWARDS:
        g = f"`{STEWARDS}`"
        try:
            for sql in (f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO {g}", f"GRANT USE SCHEMA ON SCHEMA {meta} TO {g}",
                        f"GRANT SELECT, MODIFY ON TABLE {meta}.glossary TO {g}",
                        f"GRANT SELECT, MODIFY ON TABLE {meta}.issue_status TO {g}"):
                run_sql(w, sql)
            log(f"   granted {STEWARDS} SELECT + MODIFY on glossary and issue_status")
        except RuntimeError as e:
            log(f"   ! could not grant {STEWARDS} (does the group exist?): {str(e)[:160]}")
    # owners set the status of issues on their tables: each member of an owner team may write issue_status
    me = w.current_user.me().user_name
    people = sorted({m for r in run_sql(w, f"SELECT DISTINCT explode(members) AS m FROM {META}.owners")
                     for m in [r["m"]] if m and m != me})   # the deployer owns the table already
    for person in people:
        u = f"`{person}`"
        try:
            for sql in (f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO {u}", f"GRANT USE SCHEMA ON SCHEMA {meta} TO {u}",
                        f"GRANT SELECT, MODIFY ON TABLE {meta}.issue_status TO {u}"):
                run_sql(w, sql)
        except RuntimeError as e:
            log(f"   ! could not grant an owner team member: {str(e)[:160]}")
    log(f"   owner team members with MODIFY on issue_status: {len(people)} besides you")


def deploy(w: WorkspaceClient, state: dict, log=print) -> tuple[A.App, int, dict[str, str]]:
    agent.ensure_meta(w)   # the owners / issues / issue_status tables the alerts and grants refer to
    try:
        app_url = w.apps.get(APP_NAME).url
    except NotFound:
        app_url = None
    alerts = ensure_alerts(w, state, app_url, log)
    job_id = ensure_agent_job(w, state, alerts, log)
    app = ensure_app(w, state["warehouse_id"], job_id, log)
    if app.url != app_url:   # first deploy: put the app link into the alert mails
        alerts = ensure_alerts(w, state, app.url, log)
    grant_read(w, app, log)
    path = f"{_user_dir(w)}/app"
    _upload(w, path, {f: (APP_DIR / f).read_text() for f in APP_FILES} | {"app.yaml": _app_yaml()})
    log(f"   uploaded app/ to {path}; deploying")
    dep = w.apps.deploy(APP_NAME, A.AppDeployment(source_code_path=path)).result()
    status = dep.status.state.value if dep.status and dep.status.state else "unknown"
    log(f"   deployment {status}: {dep.status.message if dep.status else ''}")
    return w.apps.get(APP_NAME), job_id, alerts


def delete(w: WorkspaceClient, state: dict, log=print) -> None:
    try:
        w.apps.delete(APP_NAME)
        log(f"   deleted app {APP_NAME}")
    except NotFound:
        pass
    for team, alert_id in (state.get("alerts") or {}).items():
        try:
            w.alerts_v2.trash_alert(alert_id)
            log(f"   moved SQL alert '{ALERT_PREFIX}{team}' to the trash")
        except NotFound:
            pass
    job_id = state.get("job_id") or next((j.job_id for j in w.jobs.list(name=JOB_NAME)), None)
    if job_id:
        try:
            w.jobs.delete(int(job_id))
            log(f"   deleted job {JOB_NAME} ({job_id})")
        except NotFound:
            pass
