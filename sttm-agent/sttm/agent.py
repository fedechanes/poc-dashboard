"""The STTM agent.

One run:
  1. read the *deployed* pipeline SQL (from the pipeline's libraries in the workspace) and parse it
  2. read UC metadata (columns, types, comments, tags) and pull new UC column lineage since the watermark
  3. work out which target tables changed (hashes) and only process those + their downstream tables
  4. per target column: sources, expression, type, owner, layer, lineage path, definition, issues
     - definition priority: glossary (steward-approved, glossary table) > comment declared in pipeline SQL > human UC comment >
       previous draft (if nothing upstream changed) > new LLM draft (rule-based fallback without LLM)
     - human/declared definitions are checked against the transformation (LLM judge) when either changes
  5. MERGE into the STTM Delta table (only rows whose content hash changed are touched; CDF records them)
  6. write comments/tags back to UC, only where they differ from what is there
  7. save fingerprints + watermark, export the flat sttm_export table + CSV/Markdown
  8. track issues (sttm_meta.issues): which are new, reopened or gone in this run; log the run
  Sensitivity (personal data) per column is classified at bronze and propagated down the lineage (sensitivity.py).
"""

from __future__ import annotations

import csv
import json
import os
import re
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from . import consumers, sensitivity, sqlparse
from .config import (CATALOG, DEFAULT_OWNER, GLOSSARY_FILE, HISTORY_RETENTION_DAYS, LANDING_PATH, LAYERS, LLM_ENDPOINT, META, OUTPUT_DIR, PIPELINE_CONF,
                     SCHEMAS, layer_of_schema, load_state, run_sql, sql_str)
from .sqlparse import FILE_SOURCE, sha

LAYER_SCHEMAS = [SCHEMAS[layer] for layer in LAYERS]
TAG_KEYS = ("sttm_layer", "sttm_owner", "sttm_definition_source", "sttm_rule_hash", "sttm_comment_hash",
            "sttm_sensitivity")
WRITABLE_SOURCES = ("glossary", "llm", "rule", "inherited")      # definitions the agent may write as the column comment

STTM_COLUMNS = {   # name -> SQL type; order = table order
    "target_table": "STRING", "target_column": "STRING", "layer": "STRING", "data_type": "STRING",
    "owner": "STRING", "source_columns": "ARRAY<STRING>", "transformation": "STRING", "mapping_kind": "STRING",
    "table_rule": "STRING", "rule_columns": "ARRAY<STRING>", "business_definition": "STRING",
    "definition_source": "STRING",
    "proposed_definition": "STRING", "sensitivity": "STRING", "sensitivity_source": "STRING",
    "sensitivity_reason": "STRING", "lineage_path": "STRING", "bronze_sources": "ARRAY<STRING>",
    "lineage_origin": "STRING", "issues": "ARRAY<STRING>", "rule_hash": "STRING", "lineage_hash": "STRING",
    "row_hash": "STRING",
}


# ---------------------------------------------------------------------------------------------------- helpers

def short(name: str) -> str:
    """workspace.sttm_silver.orders.net_amount_eur -> silver.orders.net_amount_eur; files -> landing/orders.qty"""
    if name.startswith(FILE_SOURCE + ":"):
        return "landing/" + name[len(FILE_SOURCE) + 1:].replace(LANDING_PATH + "/", "")
    parts = name.split(".")
    if len(parts) >= 3 and parts[0] == CATALOG and layer_of_schema(parts[1]):
        return ".".join([layer_of_schema(parts[1])] + parts[2:])
    return name


def table_of(col: str) -> str:
    return col.rsplit(".", 1)[0]


def now_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


class Log:
    def __init__(self, verbose: bool = True):
        self.verbose = verbose

    def __call__(self, *a):
        if self.verbose:
            print(*a, flush=True)


# ---------------------------------------------------------------------------------------------------- meta tables

def ensure_meta(w: WorkspaceClient) -> None:
    cols = ",\n  ".join(f"{k} {v}" for k, v in STTM_COLUMNS.items())
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.sttm (
  {cols},
  is_active BOOLEAN, first_seen_run STRING, last_changed_run STRING, updated_at TIMESTAMP)
COMMENT 'Source-to-target mapping, one row per target column (maintained by the STTM agent)'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
    have = {r["col_name"] for r in run_sql(w, f"DESCRIBE TABLE {META}.sttm")}
    added = [f"{k} {v}" for k, v in STTM_COLUMNS.items() if k not in have]
    if added:   # columns added in a later version of the agent
        run_sql(w, f"ALTER TABLE {META}.sttm ADD COLUMNS ({', '.join(added)})")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.table_fingerprints (
  target_table STRING, sql_hash STRING, schema_hash STRING, glossary_hash STRING, uc_edges_hash STRING,
  run_id STRING, updated_at TIMESTAMP)
COMMENT 'Per target table: hashes of what the STTM was last built from (change detection)'""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.uc_lineage (
  level STRING, source STRING, target STRING, entity_type STRING, first_seen TIMESTAMP, last_seen TIMESTAMP)
COMMENT 'Distinct UC lineage edges (column and table level) for the medallion, pulled incrementally from system.access'""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.glossary (
  column_key STRING COMMENT '<layer>.<table>.<column>',
  definition STRING COMMENT 'Approved business definition; NULL = only an acknowledgment of the current definition',
  acknowledged_lineage_hash STRING COMMENT 'Upstream fingerprint the steward confirmed; a REVIEW issue stays closed while it matches',
  note STRING, updated_by STRING, updated_at TIMESTAMP)
COMMENT 'Steward-approved definitions and acknowledgments (written from the STTM viewer app, read by the agent)'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.sensitivity_overrides (
  column_key STRING COMMENT '<layer>.<table>.<column>',
  sensitivity STRING COMMENT 'pii, pii_derived or none; wins over what the agent derives, and columns downstream inherit it',
  note STRING COMMENT 'Why the steward set it',
  updated_by STRING, updated_at TIMESTAMP)
COMMENT 'Steward overrides of the sensitivity (personal data level) of a column (written from the STTM viewer app as the signed-in user, read by the agent)'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.owners (
  owner STRING COMMENT 'Owner team, as named in glossary.toml [owners]',
  members ARRAY<STRING> COMMENT 'Databricks user names (emails) of the people in the team',
  notify_email STRING COMMENT 'Where the SQL alert of the team sends new-issue notifications',
  note STRING, updated_by STRING, updated_at TIMESTAMP)
COMMENT 'Owner teams -> people. New teams are added by the agent; edit members and notify_email here'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.issues (
  column_key STRING COMMENT '<layer>.<table>.<column>',
  target_table STRING COMMENT '<layer>.<table>',
  owner STRING COMMENT 'Owner team of the table',
  issue_type STRING COMMENT 'CONFLICT, REVIEW, GAP, FIX IN PIPELINE SQL, LINEAGE (INFO is not tracked)',
  fingerprint STRING COMMENT 'Identity of the issue across runs; a new fingerprint means a new issue',
  issue_text STRING COMMENT 'Latest text of the issue',
  is_open BOOLEAN COMMENT 'The latest agent run still reports it',
  first_seen_run STRING, first_seen_at TIMESTAMP,
  opened_run STRING COMMENT 'Run that reported it as new (or again, after it had gone)',
  opened_at TIMESTAMP,
  closed_run STRING COMMENT 'Run that stopped reporting it', closed_at TIMESTAMP)
COMMENT 'Every issue the STTM agent has reported, with the run that opened and closed it (maintained by the agent)'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.issue_status (
  column_key STRING, issue_type STRING, fingerprint STRING COMMENT 'Same key as sttm_meta.issues',
  status STRING COMMENT 'open, in_progress, accepted or wont_fix',
  assignee STRING, comment STRING, updated_by STRING, updated_at TIMESTAMP)
COMMENT 'Owner decisions on agent issues (written from the STTM viewer app as the signed-in user)'
TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.agent_state (k STRING, v STRING)
COMMENT 'STTM agent watermarks'""")
    run_sql(w, f"""CREATE TABLE IF NOT EXISTS {META}.agent_runs (
  run_id STRING, started_at TIMESTAMP, seconds INT, mode STRING, tables_total INT, tables_processed INT,
  rows_inserted BIGINT, rows_updated BIGINT, rows_deactivated BIGINT, rows_unchanged BIGINT, llm_calls INT,
  comments_written INT, tags_written INT, open_issues INT, uc_edges_new BIGINT, lineage_watermark STRING,
  sttm_version BIGINT, details STRING)
COMMENT 'One row per STTM agent run'""")
    consumers.ensure_table(w)
    keep_history(w)
    seed_glossary(w)
    seed_owners(w)


HISTORY_TABLES = ("sttm", "glossary", "issue_status", "issues", "sensitivity_overrides")   # the viewer's History reads their change data feed


def keep_history(w: WorkspaceClient) -> None:
    """Keep the change data feed of the history tables readable for STTM_HISTORY_RETENTION_DAYS. VACUUM (run by
    predictive optimization on managed tables) deletes change files older than delta.deletedFileRetentionDuration
    (default 7 days), and table_changes() needs the log too (delta.logRetentionDuration, default 30 days)."""
    days = f"interval {HISTORY_RETENTION_DAYS} days"
    for t in HISTORY_TABLES:
        props = {r["key"]: r["value"] for r in run_sql(w, f"SHOW TBLPROPERTIES {META}.{t}")}
        want = {"delta.deletedFileRetentionDuration": days,
                "delta.logRetentionDuration": f"interval {max(HISTORY_RETENTION_DAYS, 30)} days"}
        if any(props.get(k) != v for k, v in want.items()):
            run_sql(w, f"ALTER TABLE {META}.{t} SET TBLPROPERTIES ("
                       + ", ".join(f"'{k}' = '{v}'" for k, v in want.items()) + ")")


def get_state(w: WorkspaceClient, k: str) -> str | None:
    rows = run_sql(w, f"SELECT v FROM {META}.agent_state WHERE k = :k", {"k": k})
    return rows[0]["v"] if rows else None


def set_state(w: WorkspaceClient, k: str, v: str) -> None:
    run_sql(w, f"""MERGE INTO {META}.agent_state t USING (SELECT :k AS k, :v AS v) s ON t.k = s.k
                   WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT *""", {"k": k, "v": v})


def table_version(w: WorkspaceClient, table: str) -> int:
    return int(run_sql(w, f"DESCRIBE HISTORY {table} LIMIT 1")[0]["version"])


# ---------------------------------------------------------------------------------------------------- inputs

def read_pipeline_sql(w: WorkspaceClient) -> dict[str, str]:
    """The SQL that is actually deployed, read from the pipeline's library files in the workspace."""
    pid = load_state(w)["pipeline_id"]
    spec = w.pipelines.get(pid).spec
    files = {}
    for lib in spec.libraries or []:
        path = lib.file.path if lib.file else None
        if path:
            with w.workspace.download(path) as f:
                files[path.rsplit("/", 1)[-1]] = f.read().decode()
    return files


def read_uc(w: WorkspaceClient) -> tuple[dict, dict, dict]:
    """columns[table] = [{column, data_type, comment}], tables[table] = comment, tags[(table, col)] = {k: v}"""
    in_list = ", ".join(f"'{s}'" for s in LAYER_SCHEMAS)
    cols = run_sql(w, f"""
        SELECT concat_ws('.', table_catalog, table_schema, table_name) AS t, column_name, full_data_type, comment
        FROM `{CATALOG}`.information_schema.columns
        WHERE table_schema IN ({in_list}) AND table_name NOT LIKE '\\_\\_%' AND table_name NOT LIKE 'event\\_log%'
        ORDER BY t, ordinal_position""")
    columns: dict[str, list[dict]] = {}
    for r in cols:
        columns.setdefault(r["t"], []).append({"column": r["column_name"].lower(), "data_type": r["full_data_type"],
                                                "comment": r["comment"]})
    tables = {f"{CATALOG}.{r['table_schema']}.{r['table_name']}": r["comment"] for r in run_sql(w, f"""
        SELECT table_schema, table_name, comment FROM `{CATALOG}`.information_schema.tables
        WHERE table_schema IN ({in_list}) AND table_name NOT LIKE '\\_\\_%'""")}
    tags: dict[tuple, dict] = {}
    for r in run_sql(w, f"""
        SELECT concat_ws('.', catalog_name, schema_name, table_name) AS t, column_name, tag_name, tag_value
        FROM `{CATALOG}`.information_schema.column_tags WHERE schema_name IN ({in_list})"""):
        tags.setdefault((r["t"], r["column_name"].lower()), {})[r["tag_name"]] = r["tag_value"]
    return columns, tables, tags


MAT_RE = re.compile(r"__materialization_mat_[0-9a-f]{8}(?:_[0-9a-f]{4}){3}_[0-9a-f]{12}_(.+)_\d+$")


def _norm_table(name: str | None, path: str | None) -> str | None:
    if name:
        parts = name.lower().split(".")
        m = MAT_RE.match(parts[-1])
        if m:   # pipelines read their sources through internal materialization tables
            parts[-1] = m.group(1)
        return ".".join(parts)
    if path:
        p = re.sub(r"^\w+:", "", path).rstrip("/")
        if p.startswith(LANDING_PATH + "/"):
            return f"{FILE_SOURCE}:{LANDING_PATH}/{p[len(LANDING_PATH) + 1:].split('/')[0]}"
    return None


LINEAGE_LOOKBACK = "INTERVAL 2 HOURS"   # system tables can land events late: re-read an overlap, MERGE is idempotent


def pull_uc_lineage(w: WorkspaceClient, log: Log) -> tuple[int, str | None]:
    """Incrementally copy new lineage events (event_time > watermark - lookback) into the local edge cache.
    Both levels: column edges from system.access.column_lineage, table edges from system.access.table_lineage."""
    wm = get_state(w, "lineage_watermark") or "1970-01-01T00:00:00Z"
    in_list = ", ".join(f"'{s}'" for s in LAYER_SCHEMAS)
    where = (f"target_table_catalog = :cat AND target_table_schema IN ({in_list}) "
             f"AND event_time > CAST(:wm AS TIMESTAMP) - {LINEAGE_LOOKBACK}")
    col_rows = run_sql(w, f"""
        SELECT source_table_full_name, source_path, source_column_name, target_table_full_name, target_column_name,
               entity_type, max(event_time) AS last_seen
        FROM system.access.column_lineage WHERE {where} AND target_column_name IS NOT NULL GROUP BY ALL""",
                       {"cat": CATALOG, "wm": wm})
    tab_rows = run_sql(w, f"""
        SELECT source_table_full_name, source_path, NULL AS source_column_name, target_table_full_name,
               NULL AS target_column_name, entity_type, max(event_time) AS last_seen
        FROM system.access.table_lineage WHERE {where} GROUP BY ALL""", {"cat": CATALOG, "wm": wm})
    edges, no_source = {}, 0
    for level, rows in (("column", col_rows), ("table", tab_rows)):
        for r in rows:
            src_t = _norm_table(r["source_table_full_name"], r["source_path"])
            tgt_t = _norm_table(r["target_table_full_name"], None)
            if not src_t or not tgt_t:
                no_source += level == "column"
                continue
            if level == "column":
                if not r["source_column_name"]:
                    continue
                key = (f"{src_t}.{r['source_column_name'].lower()}", f"{tgt_t}.{r['target_column_name'].lower()}")
            else:
                key = (src_t, tgt_t)
            e = edges.setdefault((level,) + key, {"level": level, "source": key[0], "target": key[1],
                                                 "entity_type": r["entity_type"], "last_seen": r["last_seen"]})
            e["last_seen"] = max(e["last_seen"], r["last_seen"])
    if edges:
        run_sql(w, f"""
            MERGE INTO {META}.uc_lineage t
            USING (SELECT inline(from_json(:p, 'array<struct<level:string,source:string,target:string,
                                                   entity_type:string,last_seen:timestamp>>'))) s
            ON t.level = s.level AND t.source = s.source AND t.target = s.target
            WHEN MATCHED THEN UPDATE SET last_seen = greatest(t.last_seen, s.last_seen)
            WHEN NOT MATCHED THEN INSERT (level, source, target, entity_type, first_seen, last_seen)
                 VALUES (s.level, s.source, s.target, s.entity_type, s.last_seen, s.last_seen)""",
                {"p": json.dumps(list(edges.values()))})
    new_wm = max((r["last_seen"] for r in col_rows + tab_rows), default=None)
    n_col = sum(1 for e in edges.values() if e["level"] == "column")
    log(f"   UC lineage since {wm} (- lookback): {len(col_rows)} column events ({no_source} without a source "
        f"column), {len(tab_rows)} table events -> {n_col} column edges, {len(edges) - n_col} table edges")
    return len(edges), new_wm


def read_uc_edges(w: WorkspaceClient) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """(column edges, table edges) as target -> sources."""
    cols: dict[str, set[str]] = {}
    tabs: dict[str, set[str]] = {}
    for r in run_sql(w, f"SELECT level, source, target FROM {META}.uc_lineage"):
        (cols if r["level"] == "column" else tabs).setdefault(r["target"], set()).add(r["source"])
    return cols, tabs


def _glossary_file() -> dict:
    return tomllib.loads(GLOSSARY_FILE.read_text()) if GLOSSARY_FILE.exists() else {}


def seed_glossary(w: WorkspaceClient) -> None:
    """First run only: copy the definitions of glossary.toml into the glossary table. After that the table is the
    source of truth (stewards edit it from the viewer app); later edits to the file's definitions are ignored."""
    if get_state(w, "glossary_seeded"):
        return
    defs = _glossary_file().get("definitions", {})
    if defs:
        run_sql(w, f"""MERGE INTO {META}.glossary t
            USING (SELECT key AS column_key, value AS definition FROM (SELECT explode(from_json(:p, 'map<string,string>')))) s
            ON t.column_key = s.column_key
            WHEN NOT MATCHED THEN INSERT (column_key, definition, note, updated_by, updated_at)
                 VALUES (s.column_key, s.definition, 'imported from glossary.toml', 'glossary.toml', current_timestamp())""",
                {"p": json.dumps(defs)})
    set_state(w, "glossary_seeded", "1")


def seed_owners(w: WorkspaceClient) -> None:
    """Add every owner team named in glossary.toml that the owners table doesn't have yet. For now each new team maps
    to one person (STTM_DEFAULT_OWNER, else whoever runs this); existing rows are never changed, so a team that was
    split into real members keeps them."""
    teams = sorted(set(_glossary_file().get("owners", {}).values()))
    if not teams:
        return
    person = DEFAULT_OWNER or w.current_user.me().user_name
    run_sql(w, f"""MERGE INTO {META}.owners t
        USING (SELECT explode(from_json(:teams, 'array<string>')) AS owner) s ON t.owner = s.owner
        WHEN NOT MATCHED THEN INSERT (owner, members, notify_email, note, updated_by, updated_at)
             VALUES (s.owner, array(:person), :person, 'default: every team maps to one person until it is split',
                     current_user(), current_timestamp())""", {"teams": json.dumps(teams), "person": person})


def load_glossary(w: WorkspaceClient) -> tuple[dict, dict, dict]:
    """(owners from glossary.toml, approved definitions, acknowledged lineage hashes) per <layer>.<table>.<column>."""
    rows = run_sql(w, f"SELECT column_key, definition, acknowledged_lineage_hash FROM {META}.glossary")
    return (_glossary_file().get("owners", {}),
            {r["column_key"]: r["definition"] for r in rows if r["definition"]},
            {r["column_key"]: r["acknowledged_lineage_hash"] for r in rows if r["acknowledged_lineage_hash"]})


def load_overrides(w: WorkspaceClient) -> dict[str, tuple[str, str | None]]:
    """Steward sensitivity overrides: <layer>.<table>.<column> -> (level, note)."""
    return {r["column_key"]: (r["sensitivity"], r["note"]) for r in run_sql(
        w, f"SELECT column_key, sensitivity, note FROM {META}.sensitivity_overrides WHERE sensitivity IN "
           "('pii', 'pii_derived', 'none')")}


# ---------------------------------------------------------------------------------------------------- LLM

class Drafter:
    """Drafts definitions and judges existing ones with a Foundation Model API chat endpoint (or rules)."""

    SYSTEM = (
        "You are a data steward writing a data catalogue for a retail company's lakehouse. "
        "You get one table and some of its columns with the SQL that computes each column. "
        "For task 'draft' write a business definition: 1-2 plain-English sentences (max 280 characters) that say "
        "what the value means to a business user, including unit/currency, grain, and which records are included "
        "or excluded when the SQL filters them. Do not restate SQL syntax or function names. "
        "Columns used in the SQL may be computed upstream: always resolve them through the 'upstream' expressions "
        "(e.g. if a filter uses a flag, say which statuses or values the flag covers). An upstream column with a "
        "'previous_expression' changed its logic in this release: judge the definition against the NEW expression. "
        "Only mention a row restriction if the SQL really has one (a FILTER/WHERE, an aggregate FILTER, or an "
        "INNER JOIN); a LEFT JOIN keeps every row of the left table. Keep pass-through columns short. "
        "For task 'check' decide whether the existing definition contradicts what the SQL actually computes "
        "(consider the upstream expressions given). Wording differences or missing detail are NOT contradictions; "
        "only a factual mismatch (different aggregate, filter, unit, inclusion/exclusion, or meaning) is. "
        "If it contradicts, also give a corrected definition. "
        'Reply with JSON only: {"columns": [{"column": "...", "definition": "...", "verdict": '
        '"consistent|contradicts", "reason": "..."}]} (definition required for draft and for contradicts).')

    def __init__(self, w: WorkspaceClient, endpoint: str, log: Log):
        self.w, self.endpoint, self.log = w, endpoint, log
        self.calls = 0
        self.enabled = endpoint.lower() not in ("", "none", "off")

    def run(self, table: dict, items: list[dict]) -> dict[str, dict]:
        if not items:
            return {}
        if self.enabled:
            try:
                return self._llm(table, items)
            except Exception as e:  # never fail the run on the LLM: fall back to rules
                self.log(f"   ! LLM call failed for {table['table']} ({e}); using rule-based fallback")
        return {i["column"]: self._rule(i) for i in items}

    def _llm(self, table: dict, items: list[dict]) -> dict[str, dict]:
        payload = json.dumps({**table, "columns": items}, indent=1)
        for attempt in range(3):
            self.calls += 1
            resp = self.w.serving_endpoints.query(
                name=self.endpoint, temperature=0.0, max_tokens=4000,
                messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=self.SYSTEM),
                          ChatMessage(role=ChatMessageRole.USER, content=payload)])
            text = resp.choices[0].message.content
            if isinstance(text, list):   # some models return content parts
                text = "".join(p.get("text", "") for p in text if isinstance(p, dict))
            try:
                data = json.loads(text[text.index("{"): text.rindex("}") + 1])
                out = {c["column"].lower(): c for c in data["columns"]}
                missing = [i["column"] for i in items if i["column"] not in out]
                for m in missing:
                    out[m] = self._rule(next(i for i in items if i["column"] == m))
                return out
            except (ValueError, KeyError) as e:
                if attempt == 2:
                    raise RuntimeError(f"unparseable LLM reply: {e}") from e
                time.sleep(1)
        return {}

    def classify(self, tables: list[dict]) -> dict[tuple[str, str], dict]:
        """Sensitivity of bronze columns whose names the rules can't decide. Metadata only, never data values.
        Returns {(table, column): {sensitivity, reason}}; empty without an LLM or when the call fails."""
        if not self.enabled or not tables:
            return {}
        payload = json.dumps({"tables": tables}, indent=1)
        for attempt in range(3):
            self.calls += 1
            try:
                resp = self.w.serving_endpoints.query(
                    name=self.endpoint, temperature=0.0, max_tokens=2000,
                    messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=sensitivity.LLM_SYSTEM),
                              ChatMessage(role=ChatMessageRole.USER, content=payload)])
                text = resp.choices[0].message.content
                if isinstance(text, list):
                    text = "".join(p.get("text", "") for p in text if isinstance(p, dict))
                data = json.loads(text[text.index("{"): text.rindex("}") + 1])
                return {(c["table"], c["column"].lower()): c for c in data["columns"]}
            except Exception as e:  # noqa: BLE001 - never fail the run on the LLM
                if attempt == 2:
                    self.log(f"   ! sensitivity LLM check failed ({e}); undecided names fall back to 'none'")
                    return {}
                time.sleep(1)
        return {}

    @staticmethod
    def _rule(item: dict) -> dict:
        """Deterministic fallback: template definition + keyword contradiction check."""
        name = item["column"].replace("_", " ")
        expr = item["expression"]
        if item["task"] == "check":
            d, e = item["existing_definition"].lower(), expr.upper()
            bad = (("total" in d or "sum" in d) and "SUM(" not in e) or ("average" in d and "/" not in e
                                                                          and "AVG(" not in e) \
                or ("including cancelled" in d and "NOT" in e and "CANCELLED" in e)
            return {"verdict": "contradicts" if bad else "consistent",
                    "reason": "keyword check (no LLM)", "definition": None}
        srcs = ", ".join(item["sources"]) or "no source column"
        if item["kind"] == "direct":
            text = f"{name.capitalize()}, carried over unchanged from {srcs}."
        elif item["kind"] == "constant":
            text = f"{name.capitalize()}: constant value {expr}."
        elif item["kind"] == "technical":
            text = f"Technical column: {expr}."
        else:
            text = f"{name.capitalize()}, derived from {srcs}."
        return {"definition": text, "verdict": None, "reason": "rule-based (no LLM)"}


# ---------------------------------------------------------------------------------------------------- core

def build(w: WorkspaceClient, log: Log, full: bool = False, dry_run: bool = False) -> dict:
    t0 = time.time()
    run_id = now_run_id()
    log(f"STTM agent run {run_id}{' (full)' if full else ''}{' (dry run)' if dry_run else ''}")
    ensure_meta(w)

    # 1-2. inputs
    files = read_pipeline_sql(w)
    datasets = sqlparse.parse_statements(files, PIPELINE_CONF)
    columns, table_comments, tags = read_uc(w)
    schema: dict = {}
    for t, cols in columns.items():
        c, s, n = t.split(".")
        schema.setdefault(c, {}).setdefault(s, {})[n] = {x["column"]: x["data_type"] for x in cols}
    for name, ds in datasets.items():
        sqlparse.map_columns(ds, [c["column"] for c in columns.get(name, [])], schema)
    missing = [n for n in datasets if n not in columns]
    if missing:
        log(f"   ! defined in SQL but not in UC yet (run the pipeline): {', '.join(map(short, missing))}")
    uc_new, new_wm = pull_uc_lineage(w, log) if not dry_run else (0, None)
    uc_edges, uc_tables = read_uc_edges(w)
    cons = consumers.update(w, log) if not dry_run else {}   # downstream readers; independent of the STTM rows
    owners, glossary, acks = load_glossary(w)
    overrides = load_overrides(w)
    prev = {}
    for r in run_sql(w, f"SELECT * FROM {META}.sttm WHERE is_active"):
        for k in ("source_columns", "issues", "bronze_sources", "rule_columns"):
            r[k] = json.loads(r[k]) if r[k] else []
        prev[(r["target_table"], r["target_column"])] = r
    fps = {r["target_table"]: r for r in run_sql(w, f"SELECT * FROM {META}.table_fingerprints")}

    # 3. per-column skeleton for *all* tables (parsing is cheap) -> hashes
    rows: dict[str, dict] = {}            # full target column name -> row
    for name, ds in datasets.items():
        layer = layer_of_schema(name.split(".")[1])
        for col in columns.get(name, []):
            cm = ds.columns.get(col["column"])
            if cm is None:
                continue
            tgt = f"{name}.{col['column']}"
            uc_src = uc_edges.get(tgt, set())
            sql_src = set(cm.sources)
            rows[tgt] = {
                "target_table": name, "target_column": col["column"], "layer": layer, "data_type": col["data_type"],
                "owner": owners.get(short(name)) or owners.get(layer, ""), "_sources": sorted(sql_src | uc_src),
                "_sql_src": sql_src, "_uc_src": uc_src, "transformation": cm.expression, "mapping_kind": cm.kind,
                "table_rule": ds.table_rule, "_ctx": ds.context_columns,
                "rule_hash": sha(f"{cm.expression}|{sorted(sql_src)}|{col['data_type']}|{ds.table_rule}"),
                "_flows": cm.flows, "_table_comment": table_comments.get(name) or ds.comment,
                "_uc_comment": col["comment"], "_declared": ds.declared_comments.get(col["column"]),
                "_tags": tags.get((name, col["column"]), {}),
            }

    lh_cache: dict[str, str] = {}

    def lineage_hash(tgt: str, seen=()) -> str:
        """Own rule + lineage hashes of everything upstream (sources and filter/join/dedup columns)."""
        if tgt in lh_cache:
            return lh_cache[tgt]
        r = rows.get(tgt)
        if r is None or tgt in seen:
            return sha(tgt)
        ups = sorted(set(r["_sources"]) | set(r["_ctx"]))
        h = sha(r["rule_hash"] + "|" + "|".join(lineage_hash(u, seen + (tgt,)) for u in ups))
        lh_cache[tgt] = h
        return h

    for tgt, r in rows.items():
        r["lineage_hash"] = lineage_hash(tgt)

    def path_levels(tgt: str) -> list[list[str]]:
        levels, frontier, seen = [], set(rows[tgt]["_sources"]), set()
        while frontier:
            levels.append(sorted(frontier))
            seen |= frontier
            frontier = {s for f in frontier if f in rows for s in rows[f]["_sources"]} - seen
        return levels

    for tgt, r in rows.items():
        levels = path_levels(tgt)
        r["lineage_path"] = " <- ".join([short(tgt)] + [_group(level) for level in levels])
        r["bronze_sources"] = sorted({short(c) for level in levels for c in level
                                      if c.startswith(f"{CATALOG}.{SCHEMAS['bronze']}.")})
        r["source_columns"] = [short(s) for s in r["_sources"]]
        r["rule_columns"] = sorted(short(c) for c in r["_ctx"])
        u, s = r["_uc_src"], r["_sql_src"]
        src_tables = {table_of(x) if not x.startswith(FILE_SOURCE) else x for x in s}
        confirmed = bool(src_tables) and src_tables <= uc_tables.get(r["target_table"], set())
        r["lineage_origin"] = ("sql+uc column" if u and u == s else "sql+uc column (differs)" if u and s
                               else "uc column" if u else "sql, uc table-confirmed" if confirmed
                               else "sql" if s else "none")

    # which tables need work
    gloss_by_table: dict[str, str] = {}
    for k, v in sorted(glossary.items()) + sorted((f"ack:{k}", h) for k, h in acks.items()) \
            + sorted((f"sens:{k}", f"{lv}|{note}") for k, (lv, note) in overrides.items()):
        t = k.split(":", 1)[-1].rsplit(".", 1)[0]
        gloss_by_table[t] = gloss_by_table.get(t, "") + f"{k}={v}|"
    new_fp, reasons = {}, {}
    # rows written by an agent version without sensitivity: process their tables once (backfill), no --full needed
    backfill = {t for (t, _), p in prev.items() if p.get("sensitivity") is None}
    for name, ds in datasets.items():
        if name not in columns:
            continue
        cols = columns[name]
        sch = sha(json.dumps([[c["column"], c["data_type"], c["comment"],
                               sorted(tags.get((name, c["column"]), {}).items())] for c in cols]))
        edges = sha(json.dumps([sorted((t, sorted(s)) for t, s in uc_edges.items() if table_of(t) == name),
                                sorted(uc_tables.get(name, set()))]))
        fp = {"target_table": name, "sql_hash": ds.sql_hash, "schema_hash": sch,
              "glossary_hash": sha(gloss_by_table.get(short(name), "")) + sha(json.dumps(owners, sort_keys=True))[:4],
              "uc_edges_hash": edges}
        new_fp[name] = fp
        old = fps.get(name)
        why = ["full run"] if full else ["new table"] if not old else \
            [k.replace("_hash", "") for k in ("sql_hash", "schema_hash", "glossary_hash", "uc_edges_hash")
             if old.get(k) != fp[k]]
        if not why and name in backfill:
            why = ["sensitivity backfill"]
        if why:
            reasons[name] = why
    # downstream of a changed table may change too (lineage path / impact): add them, hashes decide per row
    changed = set(reasons)
    grew = True
    while grew:
        grew = False
        for name, ds in datasets.items():
            if name in new_fp and name not in changed and ds.upstream & changed:
                changed.add(name)
                reasons[name] = [f"upstream {', '.join(sorted(short(u) for u in ds.upstream & changed))}"]
                grew = True
    log(f"   {len(new_fp)} target tables, {len(changed)} to process:")
    for n in sorted(changed, key=_layer_order):
        log(f"     {short(n):28} {', '.join(reasons[n])}")

    # 4. definitions + issues for rows of changed tables; sensitivity for every row (downstream needs upstream)
    drafter = Drafter(w, LLM_ENDPOINT, log)
    sensitivity.assign(rows, prev, overrides, drafter.classify, short, log)
    work = {t: r for t, r in rows.items() if r["target_table"] in changed}
    changed_rules = {t for t, r in rows.items()
                     if (p := prev.get((r["target_table"], r["target_column"]))) and p["rule_hash"] != r["rule_hash"]}
    by_table: dict[str, list[dict]] = {}
    for tgt, r in work.items():
        p = prev.get((r["target_table"], r["target_column"]))
        key = short(tgt)
        agent_owned = r["_uc_comment"] is not None and r["_tags"].get("sttm_comment_hash") == sha(r["_uc_comment"], 8)
        human = r["_uc_comment"] if r["_uc_comment"] and not agent_owned and r["_uc_comment"] != r["_declared"] else None
        r["issues"], r["proposed_definition"] = [], None
        r["_upstream_changed"] = sorted(short(u) for u in set(r["_sources"]) | set(r["_ctx"])
                                        if u in changed_rules)
        if key in glossary:
            r["business_definition"], r["definition_source"] = glossary[key], "glossary"
        elif r["_declared"]:
            r["business_definition"], r["definition_source"] = r["_declared"], "pipeline_sql"
        elif human:
            r["business_definition"], r["definition_source"] = human, "uc_comment"
        elif p and p["definition_source"] in ("llm", "rule") and p["lineage_hash"] == r["lineage_hash"] \
                and p["business_definition"]:
            r["business_definition"], r["definition_source"] = p["business_definition"], p["definition_source"]
        else:
            r["business_definition"], r["definition_source"] = None, None
        if r["business_definition"] is None:
            task = "draft"
        elif r["definition_source"] in ("glossary", "pipeline_sql", "uc_comment") and (
                not p or p["lineage_hash"] != r["lineage_hash"] or p["business_definition"] != r["business_definition"]
                or p.get("definition_source") != r["definition_source"]):
            task = "check"
        else:
            task = None
            if p:   # nothing changed for this column: keep the earlier verdict
                r["issues"] = [i for i in (p["issues"] or []) if i.startswith("CONFLICT")
                               or (i.startswith("REVIEW") and acks.get(key) != r["lineage_hash"])]
                r["proposed_definition"] = p["proposed_definition"]
        if task:
            by_table.setdefault(r["target_table"], []).append({
                "column": r["target_column"], "task": task, "data_type": r["data_type"], "kind": r["mapping_kind"],
                "expression": r["transformation"], "sources": r["source_columns"],
                "existing_definition": r["business_definition"],
                "upstream": [{"column": short(s), "expression": rows[s]["transformation"],
                              "table_rule": rows[s]["table_rule"],
                              **({"previous_expression": prev[_key(rows[s])]["transformation"]}
                                 if s in changed_rules else {})}
                             for s in sorted(set(r["_sources"]) | set(r["_ctx"])) if s in rows
                             and rows[s]["mapping_kind"] != "direct"][:8],
            })
    def definition_of(col: str) -> str | None:
        """Current definition of an upstream column: from this run if its table was processed, else the STTM."""
        if col in work:
            return work[col].get("business_definition")
        p = prev.get(_key(rows[col])) if col in rows else None
        return p["business_definition"] if p else None

    for name in topo_order(datasets, set(by_table)):
        ds = datasets[name]
        items = []
        for i in by_table[name]:
            r = rows[f"{name}.{i['column']}"]
            src = r["_sources"][0] if len(r["_sources"]) == 1 else None
            if i["task"] == "draft" and r["mapping_kind"] == "direct" and src in rows and definition_of(src):
                # pass-through column: inherit the upstream definition instead of asking the LLM
                r["business_definition"], r["definition_source"] = definition_of(src), "inherited"
            else:
                items.append(i)
        if not items:
            continue
        log(f"   {'LLM' if drafter.enabled else 'rules'}: {short(name)}: "
            f"{sum(i['task'] == 'draft' for i in items)} to draft, {sum(i['task'] == 'check' for i in items)} to check")
        table_ctx = {"table": short(name), "layer": rows[f"{name}.{items[0]['column']}"]["layer"],
                     "table_comment": table_comments.get(name) or ds.comment, "table_rule": ds.table_rule}
        res = drafter.run(table_ctx, items)
        drafted = []
        for i in items:
            r, o = rows[f"{name}.{i['column']}"], res.get(i["column"], {})
            if i["task"] == "draft":
                r["business_definition"] = (o.get("definition") or Drafter._rule(i)["definition"]).strip()
                r["definition_source"] = "llm" if drafter.enabled and o.get("reason") != "rule-based (no LLM)" else "rule"
                if r["definition_source"] == "llm" and r["mapping_kind"] in ("derived", "constant"):
                    drafted.append({**i, "task": "check", "existing_definition": r["business_definition"]})
            elif o.get("verdict") == "contradicts":
                r["issues"].append(f"CONFLICT: {r['definition_source']} definition contradicts the transformation: "
                                   f"{o.get('reason', '')}".strip())
                r["proposed_definition"] = o.get("definition")
        if drafted:   # verify the fresh drafts against their SQL, keep the judge's correction when it disagrees
            fixed = 0
            for i in drafted:
                o = drafter.run(table_ctx, [i]).get(i["column"], {})
                if o.get("verdict") == "contradicts" and o.get("definition"):
                    rows[f"{name}.{i['column']}"]["business_definition"] = o["definition"].strip()
                    fixed += 1
            log(f"     verified {len(drafted)} drafts, corrected {fixed}")

    for tgt, r in work.items():   # deterministic gap/lineage checks
        if r["_upstream_changed"] and r["definition_source"] in ("glossary", "pipeline_sql", "uc_comment") \
                and acks.get(short(tgt)) != r["lineage_hash"] and not any(i.startswith("REVIEW") for i in r["issues"]):
            r["issues"].append(f"REVIEW: upstream rule changed ({', '.join(r['_upstream_changed'])}); "
                               f"confirm this {r['definition_source']} definition still holds")
        if r["mapping_kind"] == "constant":
            r["issues"].append(f"GAP: no source column (constant {r['transformation']})")
        elif r["mapping_kind"] == "technical":
            r["issues"].append("INFO: technical column, no source field")
        elif not r["_sources"]:
            r["issues"].append("GAP: no source column found (SQL not parsed and no UC lineage)")
        if r["_uc_src"] and r["_sql_src"] and r["_uc_src"] != r["_sql_src"]:
            extra = sorted(short(s) for s in r["_uc_src"] - r["_sql_src"])
            miss = sorted(short(s) for s in r["_sql_src"] - r["_uc_src"])
            r["issues"].append("LINEAGE: UC and SQL lineage differ" + (f"; only in UC: {extra}" if extra else "")
                               + (f"; only in SQL: {miss}" if miss else ""))
        if r["definition_source"] == "pipeline_sql" and any(i.startswith("CONFLICT") for i in r["issues"]):
            r["issues"].append("FIX IN PIPELINE SQL: the comment is declared in the pipeline and a refresh restores it")
        r["row_hash"] = sha(json.dumps({k: r.get(k) for k in STTM_COLUMNS if k != "row_hash"}, sort_keys=True,
                                       default=str))

    # 5. MERGE (only for processed tables; rows of other tables are untouched by construction)
    payload = [{k: r.get(k) for k in STTM_COLUMNS} for r in work.values()]
    summary = {"run_id": run_id, "tables_total": len(new_fp), "tables_processed": len(changed),
               "llm_calls": drafter.calls, "uc_edges_new": uc_new, **cons}
    plan_changes = [k for k in work if not prev.get(_key(work[k]))
                    or prev[_key(work[k])]["row_hash"] != work[k]["row_hash"]]
    if dry_run:
        log(f"   dry run: {len(plan_changes)} STTM rows would change:")
        for k in plan_changes:
            log(f"     {short(k)}")
        return summary
    v_before = table_version(w, f"{META}.sttm")
    m = merge_sttm(w, payload, sorted(changed), run_id) if changed else {}
    v_after = table_version(w, f"{META}.sttm")
    summary.update(rows_inserted=int(m.get("num_inserted_rows", 0)),
                   rows_updated=int(m.get("num_updated_rows", 0)) - _deactivated(w, run_id),
                   rows_deactivated=_deactivated(w, run_id), sttm_version=v_after,
                   sttm_version_before=v_before)
    summary["rows_unchanged"] = len(rows) - summary["rows_inserted"] - summary["rows_updated"]

    # 6. write-back
    comments, tag_writes = write_back(w, work, log)
    summary.update(comments_written=comments, tags_written=tag_writes, llm_calls=drafter.calls)

    # 7. fingerprints (re-read schema so the agent's own writes don't count as a change next time), watermark, log
    if changed:
        columns2, _, tags2 = read_uc(w)
        for name in changed:
            new_fp[name]["schema_hash"] = sha(json.dumps([[c["column"], c["data_type"], c["comment"],
                                                           sorted(tags2.get((name, c["column"]), {}).items())]
                                                          for c in columns2.get(name, [])]))
        run_sql(w, f"""
            MERGE INTO {META}.table_fingerprints t
            USING (SELECT *, :run AS run_id, current_timestamp() AS updated_at FROM (SELECT inline(from_json(:p,
                   'array<struct<target_table:string,sql_hash:string,schema_hash:string,glossary_hash:string,
                                 uc_edges_hash:string>>')))) s
            ON t.target_table = s.target_table
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""",
                {"p": json.dumps([new_fp[n] for n in changed]), "run": run_id})
    if new_wm:
        set_state(w, "lineage_watermark", new_wm)
    export(w, log)
    summary.update(sync_issues(w, run_id, log))
    open_issues = run_sql(w, f"""SELECT count(*) AS n FROM {META}.sttm
                                 WHERE is_active AND exists(issues, i -> i NOT LIKE 'INFO%')""")[0]["n"]
    summary.update(open_issues=int(open_issues), seconds=int(time.time() - t0),
                   lineage_watermark=new_wm or get_state(w, "lineage_watermark"))
    run_sql(w, f"""INSERT INTO {META}.agent_runs VALUES (:run_id, current_timestamp(), :seconds, :mode,
                   :tables_total, :tables_processed, :ins, :upd, :deact, :unch, :llm, :com, :tags, :issues, :uc, :wm,
                   :ver, :details)""",
            {"run_id": run_id, "seconds": str(summary["seconds"]), "mode": "full" if full else "incremental",
             "tables_total": str(len(new_fp)), "tables_processed": str(len(changed)),
             "ins": str(summary["rows_inserted"]), "upd": str(summary["rows_updated"]),
             "deact": str(summary["rows_deactivated"]), "unch": str(summary["rows_unchanged"]),
             "llm": str(drafter.calls), "com": str(comments), "tags": str(tag_writes), "issues": str(open_issues),
             "uc": str(uc_new), "wm": summary["lineage_watermark"], "ver": str(v_after),
             "details": json.dumps({"reasons": {short(k): v for k, v in reasons.items()},
                                    "requested_by": os.getenv("STTM_REQUESTED_BY") or None,
                                    "sttm_version_before": v_before,
                                    "consumer_edges_new": cons.get("consumer_edges_seen"),
                                    "issues_new": summary["issues_new"], "issues_closed": summary["issues_closed"],
                                    "new_issues": summary["new_issue_keys"]})})
    return summary


def topo_order(datasets: dict, names: set[str]) -> list[str]:
    """Tables in dependency order (upstream first), so inherited definitions are resolved before use."""
    out, seen = [], set()

    def visit(n: str) -> None:
        if n in seen or n not in datasets:
            return
        seen.add(n)
        for u in sorted(datasets[n].upstream):
            visit(u)
        out.append(n)

    for n in sorted(names, key=_layer_order):
        visit(n)
    return [n for n in out if n in names]


def _key(r: dict) -> tuple:
    return r["target_table"], r["target_column"]


def _layer_order(name: str) -> tuple:
    layer = layer_of_schema(name.split(".")[1]) or "z"
    return (LAYERS.index(layer) if layer in LAYERS else 9, name)


def _group(cols: list[str]) -> str:
    """[silver.orders.a, silver.orders.b] -> silver.orders.{a, b}"""
    by: dict[str, list[str]] = {}
    for c in cols:
        t, col = short(c).rsplit(".", 1)
        by.setdefault(t, []).append(col)
    return " + ".join(f"{t}.{v[0]}" if len(v) == 1 else f"{t}.{{{', '.join(v)}}}" for t, v in sorted(by.items()))


def merge_sttm(w: WorkspaceClient, payload: list[dict], tables: list[str], run_id: str) -> dict:
    schema = ",".join(f"{k}:{v.lower()}" for k, v in STTM_COLUMNS.items())
    sets = ", ".join(f"t.{k} = s.{k}" for k in STTM_COLUMNS)
    rows = run_sql(w, f"""
        MERGE INTO {META}.sttm t
        USING (SELECT inline(from_json(:p, 'array<struct<{schema}>>'))) s
        ON t.target_table = s.target_table AND t.target_column = s.target_column
        WHEN MATCHED AND (t.row_hash <> s.row_hash OR NOT t.is_active) THEN UPDATE SET {sets},
             t.is_active = true, t.last_changed_run = :run, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT ({', '.join(STTM_COLUMNS)}, is_active, first_seen_run, last_changed_run, updated_at)
             VALUES ({', '.join('s.' + k for k in STTM_COLUMNS)}, true, :run, :run, current_timestamp())
        WHEN NOT MATCHED BY SOURCE AND t.is_active AND array_contains(from_json(:tables, 'array<string>'), t.target_table)
             THEN UPDATE SET t.is_active = false, t.last_changed_run = :run, t.updated_at = current_timestamp()""",
                   {"p": json.dumps(payload), "tables": json.dumps(tables), "run": run_id})
    return rows[0] if rows else {}


def _deactivated(w: WorkspaceClient, run_id: str) -> int:
    return int(run_sql(w, f"SELECT count(*) AS n FROM {META}.sttm WHERE NOT is_active AND last_changed_run = :r",
                       {"r": run_id})[0]["n"])


def _ident(table: str) -> str:
    return ".".join(f"`{p}`" for p in table.split("."))


def write_back(w: WorkspaceClient, work: dict[str, dict], log: Log) -> tuple[int, int]:
    """COMMENT ON COLUMN / SET TAGS, only where UC differs from what the STTM says."""
    stmts: list[tuple[str, str, str]] = []
    for r in work.values():
        t, c = r["target_table"], r["target_column"]
        col = f"{_ident(t)}.`{c}`"
        desired_tags = {"sttm_layer": r["layer"], "sttm_owner": r["owner"],
                        "sttm_definition_source": r["definition_source"] or "none",
                        "sttm_rule_hash": r["rule_hash"][:8], "sttm_sensitivity": r["sensitivity"] or "none"}
        if r["definition_source"] in WRITABLE_SOURCES and r["business_definition"]:
            text = r["business_definition"][:1000]
            desired_tags["sttm_comment_hash"] = sha(text, 8)
            if r["_uc_comment"] != text:
                stmts.append((t, "comment", f"COMMENT ON COLUMN {col} IS {sql_str(text)}"))
        elif "sttm_comment_hash" in r["_tags"]:
            desired_tags["sttm_comment_hash"] = r["_tags"]["sttm_comment_hash"]
        diff = {k: v for k, v in desired_tags.items() if r["_tags"].get(k) != v}
        if diff:
            pairs = ", ".join(f"{sql_str(k)} = {sql_str(v)}" for k, v in diff.items())
            stmts.append((t, "tags", f"ALTER TABLE {_ident(t)} ALTER COLUMN `{c}` SET TAGS ({pairs})"))
    if not stmts:
        log("   write-back: UC already matches, nothing to write")
        return 0, 0
    by_table: dict[str, list[str]] = {}
    for t, _, sql in stmts:
        by_table.setdefault(t, []).append(sql)

    def apply(sqls: list[str]) -> None:
        # one table at a time: concurrent ALTERs on the same table conflict; tables run in parallel
        for sql in sqls:
            for attempt in range(4):
                try:
                    run_sql(w, sql)
                    break
                except RuntimeError as e:
                    if "CONCURRENT" not in str(e) or attempt == 3:
                        raise
                    time.sleep(2 * (attempt + 1))

    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(apply, by_table.values()))
    n_c = sum(k == "comment" for _, k, _ in stmts)
    n_t = sum(k == "tags" for _, k, _ in stmts)
    log(f"   write-back: {n_c} column comments, {n_t} column tag sets")
    return n_c, n_t


# ---------------------------------------------------------------------------------------------------- export

# Flat copy of the active STTM, one row per target column, same columns as the CSV (lists joined with "; ").
# The viewer app (app/) reads only this table.
EXPORT_SCHEMA = {   # name -> (SQL type, column comment)
    "target_table": ("STRING", "Target table, <layer>.<table>"),
    "target_column": ("STRING", "Target column"),
    "layer": ("STRING", "bronze, silver or gold"),
    "data_type": ("STRING", "UC data type of the target column"),
    "owner": ("STRING", "Data owner / steward (glossary.toml [owners])"),
    "source_columns": ("STRING", "Direct source columns, '; '-separated"),
    "transformation": ("STRING", "Column expression, with intermediate CTE steps"),
    "mapping_kind": ("STRING", "direct, derived, constant or technical"),
    "table_rule": ("STRING", "Joins, filters, dedup, grain and CDC rules that apply to the whole table"),
    "rule_columns": ("STRING", "Columns the table rule reads (join keys, filters, dedup order), '; '-separated"),
    "business_definition": ("STRING", "Business definition of the column"),
    "definition_source": ("STRING", "glossary (steward-approved), pipeline_sql, uc_comment, inherited, llm or rule"),
    "proposed_definition": ("STRING", "Corrected definition proposed when the current one conflicts with the SQL"),
    "sensitivity": ("STRING", "Personal data: pii, pii_derived (hashed or masked PII) or none"),
    "sensitivity_source": ("STRING", ("rule (column name), llm (column name checked by the LLM), inherited (from "
                                      "the sources, through the lineage) or steward (override)")),
    "sensitivity_reason": ("STRING", "Why the column has this sensitivity"),
    "lineage_path": ("STRING", "Lineage back to bronze and the landed files: target <- sources <- ..."),
    "bronze_sources": ("STRING", "Bronze columns the target ultimately reads, '; '-separated"),
    "upstream_columns": ("STRING", "Every column this one depends on at any depth, through its sources and its "
                                   "table rule (silver, bronze, landing), '; '-separated"),
    "lineage_hash": ("STRING", "Fingerprint of the column's rule and everything upstream (a steward's "
                               "'still correct' is stored against it)"),
    "lineage_origin": ("STRING", "Where the lineage comes from: sql, sql+uc column, sql, uc table-confirmed, none"),
    "issues": ("STRING", "Issues found by the agent, ' | '-separated"),
    "issue_types": ("STRING", "Issue categories (GAP, CONFLICT, REVIEW, ...), '; '-separated"),
    "has_open_issue": ("BOOLEAN", "True when there is an issue other than INFO"),
    "uc_table": ("STRING", "Fully qualified UC table name"),
    "first_seen_run": ("STRING", "Agent run that first created the row"),
    "last_changed_run": ("STRING", "Agent run that last changed the row"),
    "updated_at": ("TIMESTAMP", "When the row last changed in the STTM table"),
}
EXPORT_COLS = list(EXPORT_SCHEMA)


def _issue_type(issue: str) -> str:
    return issue.split(":", 1)[0].strip()


def _export_rows(rows: list[dict]) -> list[dict]:
    """sttm rows (arrays as JSON) -> flat export rows, plus upstream_columns at any depth."""
    sources = {short(r["target_table"]) + "." + r["target_column"]:
               json.loads(r["source_columns"] or "[]") + json.loads(r["rule_columns"] or "[]") for r in rows}

    def upstream(col: str, seen: set[str]) -> set[str]:
        for s in sources.get(col, []):
            if s not in seen:
                seen.add(s)
                upstream(s, seen)
        return seen

    out = []
    for r in rows:
        tgt = short(r["target_table"])
        issues = json.loads(r["issues"] or "[]")
        types = list(dict.fromkeys(_issue_type(i) for i in issues))
        flat = {k: r.get(k) for k in EXPORT_COLS}
        flat.update(target_table=tgt, uc_table=r["target_table"].replace("`", ""),
                    source_columns="; ".join(json.loads(r["source_columns"] or "[]")),
                    bronze_sources="; ".join(json.loads(r["bronze_sources"] or "[]")),
                    rule_columns="; ".join(json.loads(r["rule_columns"] or "[]")),
                    upstream_columns="; ".join(sorted(upstream(f"{tgt}.{r['target_column']}", set()),
                                                      key=lambda c: (_layer_rank(c), c))),
                    issues=" | ".join(issues), issue_types="; ".join(types),
                    has_open_issue=any(t != "INFO" for t in types))
        out.append(flat)
    return out


def _layer_rank(col: str) -> int:
    for i, layer in enumerate(("gold", "silver", "bronze", "landing")):
        if col.startswith(layer):
            return i
    return 9


def write_export_table(w: WorkspaceClient, rows: list[dict], log: Log) -> None:
    """Replace sttm_export atomically (INSERT OVERWRITE), only when its content would change."""
    payload = json.dumps(rows, default=str)
    content = sha(json.dumps(EXPORT_SCHEMA) + payload)
    have = [c["column_name"] for c in run_sql(w, f"""SELECT column_name FROM `{CATALOG}`.information_schema.columns
        WHERE table_schema = :s AND table_name = 'sttm_export' ORDER BY ordinal_position""", {"s": SCHEMAS["meta"]})]
    if have == EXPORT_COLS and get_state(w, "export_content_hash") == content:
        log(f"   {META.replace('`', '')}.sttm_export already up to date")
        return
    if have != EXPORT_COLS:
        cols = ",\n  ".join(f"{k} {t} COMMENT {sql_str(c)}" for k, (t, c) in EXPORT_SCHEMA.items())
        run_sql(w, f"""CREATE OR REPLACE TABLE {META}.sttm_export (
  {cols})
COMMENT 'Flat copy of the active STTM (same columns as output/sttm.csv); read by the STTM viewer app'""")
    struct = ",".join(f"{k}:{t.lower()}" for k, (t, _) in EXPORT_SCHEMA.items())
    run_sql(w, f"""INSERT OVERWRITE {META}.sttm_export
                   SELECT * FROM (SELECT inline(from_json(:p, 'array<struct<{struct}>>')))""", {"p": payload})
    set_state(w, "export_content_hash", content)
    log(f"   wrote {len(rows)} rows -> {META.replace('`', '')}.sttm_export")


# ---------------------------------------------------------------------------------------------------- issues

TRACKED_ISSUES = ("CONFLICT", "REVIEW", "GAP", "FIX IN PIPELINE SQL", "LINEAGE")   # INFO is not an issue to work on
ISSUE_STRUCT = "array<struct<column_key:string,target_table:string,owner:string,issue_type:string,fingerprint:string," \
               "issue_text:string>>"


def issue_fingerprint(issue: str, definition: str | None) -> str:
    """Identity of an issue across runs, so an owner's status survives runs that report the same thing again.
    CONFLICT / FIX IN PIPELINE SQL: the definition being judged (the LLM's reason may be worded differently each time;
    a new definition is a new issue). Others are deterministic: their normalised text."""
    t = _issue_type(issue)
    if t in ("CONFLICT", "FIX IN PIPELINE SQL") and definition:
        basis = " ".join(definition.split()).lower()
    else:
        basis = " ".join(issue.split(":", 1)[-1].split()).lower()
    return sha(f"{t}|{basis}")


def current_issues(rows: list[dict]) -> list[dict]:
    """Open issues of the active STTM rows (issues as JSON arrays), one per column, type and fingerprint."""
    out: dict[tuple, dict] = {}
    for r in rows:
        key = f"{short(r['target_table'])}.{r['target_column']}"
        for i in json.loads(r["issues"] or "[]"):
            t = _issue_type(i)
            if t not in TRACKED_ISSUES:
                continue
            fp = issue_fingerprint(i, r["business_definition"])
            out[(key, t, fp)] = {"column_key": key, "target_table": key.rsplit(".", 1)[0], "owner": r["owner"],
                                 "issue_type": t, "fingerprint": fp, "issue_text": i}
    return sorted(out.values(), key=lambda x: (x["column_key"], x["issue_type"], x["fingerprint"]))


def sync_issues(w: WorkspaceClient, run_id: str, log: Log) -> dict:
    """Record which issues this run reports for the first time (or again), and which are gone. The owners' SQL alerts
    read `opened_run`; the viewer app joins owner decisions (issue_status) on the same key."""
    rows = run_sql(w, f"SELECT target_table, target_column, owner, issues, business_definition FROM {META}.sttm "
                      "WHERE is_active")
    issues = current_issues(rows)
    payload = json.dumps(issues)
    content = sha(payload)
    out = {"issues_new": 0, "issues_closed": 0, "new_issue_keys": []}
    if get_state(w, "issues_content_hash") == content:
        log("   issues: none new, none closed")
        return out
    run_sql(w, f"""
        MERGE INTO {META}.issues t
        USING (SELECT inline(from_json(:p, '{ISSUE_STRUCT}'))) s
        ON t.column_key = s.column_key AND t.issue_type = s.issue_type AND t.fingerprint = s.fingerprint
        WHEN MATCHED AND NOT t.is_open THEN UPDATE SET is_open = true, opened_run = :run,
             opened_at = current_timestamp(), closed_run = NULL, closed_at = NULL, issue_text = s.issue_text,
             owner = s.owner
        WHEN MATCHED AND (t.issue_text <> s.issue_text OR t.owner IS DISTINCT FROM s.owner) THEN
             UPDATE SET issue_text = s.issue_text, owner = s.owner
        WHEN NOT MATCHED THEN INSERT (column_key, target_table, owner, issue_type, fingerprint, issue_text, is_open,
                                      first_seen_run, first_seen_at, opened_run, opened_at)
             VALUES (s.column_key, s.target_table, s.owner, s.issue_type, s.fingerprint, s.issue_text, true,
                     :run, current_timestamp(), :run, current_timestamp())
        WHEN NOT MATCHED BY SOURCE AND t.is_open THEN
             UPDATE SET is_open = false, closed_run = :run, closed_at = current_timestamp()""",
            {"p": payload, "run": run_id})
    set_state(w, "issues_content_hash", content)
    new = run_sql(w, f"SELECT column_key, issue_type FROM {META}.issues WHERE opened_run = :r ORDER BY 1, 2",
                  {"r": run_id})
    closed = run_sql(w, f"SELECT count(*) AS n FROM {META}.issues WHERE closed_run = :r", {"r": run_id})[0]["n"]
    out.update(issues_new=len(new), issues_closed=int(closed),
               new_issue_keys=[f"{r['column_key']} {r['issue_type']}" for r in new])
    log(f"   issues: {len(new)} new, {closed} closed ({len(issues)} open)")
    for k in out["new_issue_keys"]:
        log(f"     new: {k}")
    return out


def export(w: WorkspaceClient, log: Log) -> None:
    rows = run_sql(w, f"""SELECT * FROM {META}.sttm WHERE is_active
                          ORDER BY CASE layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 ELSE 3 END,
                                   target_table, target_column""")
    flat = _export_rows(rows)
    write_export_table(w, flat, log)
    if os.getenv("STTM_EXPORT_FILES", "1") == "0":   # the agent job only refreshes the table
        return
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_DIR / "sttm.csv", "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(EXPORT_COLS)
        for r in flat:
            wr.writerow([r[k] for k in EXPORT_COLS])
    for r in rows:
        r["target_table"] = short(r["target_table"])
        for k in ("source_columns", "issues", "bronze_sources"):
            r[k] = json.loads(r[k]) if r[k] else []

    def esc(s) -> str:
        s = "; ".join(s) if isinstance(s, list) else (s or "")
        return s.replace("|", "\\|").replace("\n", " ")

    md = ["# Source-to-target mapping", "",
          f"Generated by the STTM agent from `{META.replace('`', '')}.sttm` ({len(rows)} active target columns). "
          "Lineage path: target <- direct sources <- ... <- bronze <- landed files.", ""]
    issues = [r for r in rows if any(not i.startswith("INFO") for i in r["issues"])]
    if issues:
        md += ["## Open issues", "", "| Column | Issue | Proposed definition |", "|---|---|---|"]
        md += [f"| `{r['target_table']}.{r['target_column']}` | {esc([i for i in r['issues'] if not i.startswith('INFO')])}"
               f" | {esc(r['proposed_definition'])} |" for r in issues]
        md.append("")
    for table in dict.fromkeys(r["target_table"] for r in rows):
        trs = [r for r in rows if r["target_table"] == table]
        md += [f"## `{table}`", "", f"Owner: {trs[0]['owner']}" + (f". Table rule: `{esc(trs[0]['table_rule'])}`"
                                                                   if trs[0]["table_rule"] else ""), "",
               "| Column | Type | Source | Transformation | Definition (source) | Personal data | Lineage to bronze |",
               "|---|---|---|---|---|---|---|"]
        for r in trs:
            md.append(f"| `{r['target_column']}` | {r['data_type']} | {esc(r['source_columns'])} | "
                      f"`{esc(r['transformation'])}` | {esc(r['business_definition'])} ({r['definition_source']}) | "
                      f"{r['sensitivity'] or ''} ({r['sensitivity_source'] or ''}) | "
                      f"{esc(r['lineage_path'].split(' <- ', 1)[-1] if ' <- ' in r['lineage_path'] else '')} |")
        md.append("")
    (OUTPUT_DIR / "sttm.md").write_text("\n".join(md))
    log(f"   exported {len(rows)} rows -> {OUTPUT_DIR / 'sttm.csv'}, {OUTPUT_DIR / 'sttm.md'}")
