"""Audit history for the STTM viewer, read from Delta change data feed (CDF).

- Agent runs come from `agent_runs`. A run's STTM changes are the CDF rows of `sttm` with a commit version in
  (version before the run, `sttm_version`]. The version before is `details.sttm_version_before` when the agent recorded
  it; for older runs it is the previous run's `sttm_version`, which is the same thing because only the agent writes `sttm`.
- Steward decisions are the CDF of `glossary`, owner statuses the CDF of `issue_status`, and issue openings and
  closings the CDF of `issues`. Who did it is the row's `updated_by` (`current_user()` at write time); for a deleted
  glossary row it is the `userName` of that commit in DESCRIBE HISTORY.

CDF is only kept for the table's retention window (VACUUM removes change files after
`delta.deletedFileRetentionDuration`, and predictive optimization runs VACUUM on managed tables). When the oldest
versions are gone, the history starts at the oldest version that can still be read, and `notes` says so.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

# STTM fields whose before/after the history shows: column -> label
FIELDS = {
    "business_definition": "Definition", "definition_source": "Definition source", "transformation": "Rule",
    "table_rule": "Table rule", "issues": "Issues", "proposed_definition": "Suggested definition",
    "source_columns": "Sources", "data_type": "Type", "owner": "Owner", "is_active": "Active",
}
LIST_FIELDS = ("issues", "source_columns")
MAX_FALLBACK_TRIES = 40
VERSIONED = ("sttm", "sttm_export", "glossary", "issue_status", "issues")   # latest Delta version, for the audit cover


def _lit(table: str) -> str:
    """'catalog.schema.table' as a SQL string literal for table_changes(), which takes no parameter marker for the
    table. The name comes from the app's configuration, never from a request."""
    return "'" + table.replace("`", "").replace("\\", "\\\\").replace("'", "\\'") + "'"


class History:
    def __init__(self, run_sql: Callable[..., list[dict]], meta_schema: str, prefix: str):
        """meta_schema: `cat`.`schema` (quoted); prefix: the STTM schema prefix, to turn sttm_gold -> gold."""
        self.run_sql = run_sql
        self.meta = meta_schema
        self.prefix = prefix

    def t(self, name: str) -> str:
        return f"{self.meta}.`{name}`"

    # ------------------------------------------------------------------------------------------------ CDF reads

    def versions(self, table: str, limit: int | None = None) -> list[dict]:
        """DESCRIBE HISTORY of one of our tables: version, timestamp, userName, operation (newest first)."""
        try:
            return self.run_sql(f"""SELECT version, timestamp, userName, operation
                                     FROM (DESCRIBE HISTORY {self.t(table)}{f' LIMIT {int(limit)}' if limit else ''})
                                     ORDER BY version DESC""")
        except Exception:   # noqa: BLE001 - history is a nice-to-have (who deleted a glossary row, versions)
            return []

    def changes(self, table: str, columns: str, notes: list[str]) -> list[dict]:
        """table_changes() from the oldest version still readable. Tries version 0 first; if that version (or its
        change files) is gone, walks up the versions DESCRIBE HISTORY lists."""
        sql = (f"SELECT _change_type, _commit_version, _commit_timestamp, {columns} "
               f"FROM table_changes({_lit(self.t(table))}, {{start}}) ORDER BY _commit_version")
        try:
            return self.run_sql(sql.format(start=0))
        except Exception as first:   # noqa: BLE001
            err = first
        for v in sorted(int(r["version"]) for r in self.versions(table))[1:MAX_FALLBACK_TRIES]:
            try:
                rows = self.run_sql(sql.format(start=v))
                notes.append(f"{table}: changes before version {v} are no longer in the change data feed "
                             "(past the table's retention), so its history starts there.")
                return rows
            except Exception as e:   # noqa: BLE001
                err = e
        notes.append(f"{table}: the change data feed could not be read ({str(getattr(err, 'detail', err))[:200]}).")
        return []

    # ------------------------------------------------------------------------------------------------ helpers

    def _key(self, target_table: str, column: str) -> str:
        """`workspace`.`sttm_gold`.`daily_sales` + units -> gold.daily_sales.units"""
        parts = target_table.replace("`", "").split(".")
        schema = parts[1] if len(parts) >= 3 else parts[0]
        layer = schema[len(self.prefix) + 1:] if schema.startswith(self.prefix + "_") else schema
        return f"{layer}.{parts[-1]}.{column}"

    @staticmethod
    def _val(field: str, v):
        if field in LIST_FIELDS:
            return json.loads(v) if v else []
        if field == "is_active":
            return v in (True, "true")
        return v

    @staticmethod
    def _pairs(rows: list[dict], key: Callable[[dict], tuple]) -> list[tuple[dict | None, dict | None, dict]]:
        """CDF rows -> (before, after, commit) per changed row: insert (None, row), delete (row, None), update."""
        pre = {(r["_commit_version"], *key(r)): r for r in rows if r["_change_type"] == "update_preimage"}
        out = []
        for r in rows:
            k = (r["_commit_version"], *key(r))
            ct = r["_change_type"]
            if ct == "update_postimage":
                out.append((pre.pop(k, None), r, r))
            elif ct == "insert":
                out.append((None, r, r))
            elif ct == "delete":
                out.append((r, None, r))
        return out

    # ------------------------------------------------------------------------------------------------ the history

    def load(self) -> dict:
        """Every read in parallel (each is one statement on the warehouse), then stitched together."""
        notes: list[str] = []
        sttm_cols = "target_table, target_column, " + ", ".join(FIELDS)
        jobs = {
            "runs": (self._runs,),
            "sttm": (self.changes, "sttm", sttm_cols, notes),
            "issues": (self.changes, "issues", ("column_key, issue_type, fingerprint, issue_text, is_open, opened_run, "
                                                "closed_run"), notes),
            "glossary": (self.changes, "glossary", ("column_key, definition, acknowledged_lineage_hash, note, "
                                                    "updated_by, updated_at"), notes),
            "status": (self.changes, "issue_status", ("column_key, issue_type, fingerprint, status, assignee, comment, "
                                                      "updated_by"), notes),
            **{f"v:{t}": (self.versions, t, 1) for t in VERSIONED},
        }
        with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
            futures = {k: pool.submit(*job) for k, job in jobs.items()}
            got = {k: f.result() for k, f in futures.items()}
        runs = got["runs"]
        self._attach_sttm_changes(runs, got["sttm"])
        self._attach_issue_changes(runs, got["issues"])
        return {"runs": runs, "decisions": self._glossary_events(got["glossary"]),
                "statuses": self._status_events(got["status"]), "notes": notes,
                "versions": {t: (got[f"v:{t}"][:1] or [{}])[0].get("version") for t in VERSIONED}}

    def _runs(self) -> list[dict]:
        rows = self.run_sql(f"""
            SELECT run_id, started_at, seconds, mode, tables_total, tables_processed, rows_inserted, rows_updated,
                   rows_deactivated, rows_unchanged, llm_calls, comments_written, tags_written, open_issues,
                   sttm_version, details
            FROM {self.t('agent_runs')} ORDER BY started_at""")
        out, prev = [], 0
        for r in rows:
            d = json.loads(r.pop("details") or "{}")
            after = int(r["sttm_version"]) if r["sttm_version"] is not None else prev
            before = d.get("sttm_version_before")
            before = int(before) if before is not None else prev
            r.update(requested_by=d.get("requested_by"), reasons=d.get("reasons") or {},
                     issues_new=d.get("issues_new"), issues_closed=d.get("issues_closed"),
                     version_before=before, version_after=after, changes=[], issues_opened=[], issues_resolved=[])
            for k in ("seconds", "tables_total", "tables_processed", "rows_inserted", "rows_updated",
                      "rows_deactivated", "rows_unchanged", "llm_calls", "comments_written", "tags_written",
                      "open_issues"):
                r[k] = int(r[k]) if r[k] is not None else None
            out.append(r)
            prev = max(prev, after)
        return out

    def _attach_sttm_changes(self, runs: list[dict], rows: list[dict]) -> None:
        for before, after, c in self._pairs(rows, lambda r: (r["target_table"], r["target_column"])):
            v = int(c["_commit_version"])
            run = next((r for r in runs if r["version_before"] < v <= r["version_after"]), None)
            if not run:
                continue   # written outside an agent run (e.g. a schema change); nothing to show
            src = after or before
            fields = {}
            for f in FIELDS:
                b = self._val(f, before[f]) if before else None
                a = self._val(f, after[f]) if after else None
                if before is None and f in ("is_active",):
                    continue
                if b != a:
                    fields[f] = {"before": b, "after": a}
            if before and after and fields.keys() == {"is_active"}:
                kind = "removed" if not fields["is_active"]["after"] else "restored"
            else:
                kind = "new" if before is None else "removed" if after is None else "updated"
            # an update with no shown field changed only lineage or hash columns: kept, shown as "lineage only"
            run["changes"].append({"column": self._key(src["target_table"], src["target_column"]), "kind": kind,
                                   "at": c["_commit_timestamp"], "version": v, "fields": fields})
        for r in runs:
            r["changes"].sort(key=lambda x: x["column"])

    def _attach_issue_changes(self, runs: list[dict], rows: list[dict]) -> None:
        by_id = {r["run_id"]: r for r in runs}
        for before, after, _ in self._pairs(rows, lambda r: (r["column_key"], r["issue_type"], r["fingerprint"])):
            if not after:
                continue
            was_open = bool(before) and before["is_open"] in (True, "true")
            is_open = after["is_open"] in (True, "true")
            item = {"column": after["column_key"], "issue_type": after["issue_type"], "text": after["issue_text"]}
            if is_open and not was_open and after["opened_run"] in by_id:
                by_id[after["opened_run"]]["issues_opened"].append(item)
            elif was_open and not is_open and after["closed_run"] in by_id:
                by_id[after["closed_run"]]["issues_resolved"].append(item)

    def _glossary_events(self, rows: list[dict]) -> list[dict]:
        deleted_by = None
        out = []
        for before, after, c in self._pairs(rows, lambda r: (r["column_key"],)):
            if after is None:
                if deleted_by is None:
                    deleted_by = {int(h["version"]): h.get("userName") for h in self.versions("glossary")}
                action, who = "removed", deleted_by.get(int(c["_commit_version"]))
            else:
                who = after["updated_by"]
                b_def = before["definition"] if before else None
                if who == "glossary.toml":
                    action = "seeded"
                elif after["definition"] and after["definition"] != b_def:
                    action = "definition"
                elif after["acknowledged_lineage_hash"] != (before or {}).get("acknowledged_lineage_hash"):
                    action = "confirmed"
                else:
                    action = "note"
            out.append({"kind": "decision", "column": (after or before)["column_key"], "action": action,
                        "at": c["_commit_timestamp"], "who": who,
                        "before": before["definition"] if before else None,
                        "after": after["definition"] if after else None,
                        "note": (after or {}).get("note")})
        return out

    def _status_events(self, rows: list[dict]) -> list[dict]:
        out = []
        for before, after, c in self._pairs(rows, lambda r: (r["column_key"], r["issue_type"], r["fingerprint"])):
            if after is None:
                continue
            out.append({"kind": "status", "column": after["column_key"], "issue_type": after["issue_type"],
                        "at": c["_commit_timestamp"], "who": after["updated_by"],
                        "before": before["status"] if before else "open", "after": after["status"],
                        "assignee": after["assignee"], "comment": after["comment"],
                        "assignee_before": before["assignee"] if before else None,
                        "comment_before": before["comment"] if before else None})
        return out
