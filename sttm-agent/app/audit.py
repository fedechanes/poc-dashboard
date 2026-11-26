"""'Download for audit': the STTM as an Excel workbook (openpyxl), built in the app.

Sheets: Cover (when, who, table versions, counts incl. personal data levels, filters), one sheet per table with the
export's columns (sensitivity included), Open issues with the owners' statuses, Used by (the dashboards, jobs and queries
that read those tables or tables built from them), and Recent history (agent-run changes, steward decisions and
sensitivity overrides, status changes).
"""

from __future__ import annotations

import io
import re
from datetime import UTC, datetime, timedelta, timezone

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

HEAD_FONT = Font(bold=True, color="FFFFFF")
HEAD_FILL = PatternFill("solid", fgColor="1D6B62")
TITLE_FONT = Font(bold=True, size=16)
WRAP = Alignment(wrap_text=True, vertical="top")
MAX_CELL = 32_000   # Excel's limit is 32,767 characters per cell
WIDE = {"business_definition", "transformation", "table_rule", "issues", "proposed_definition", "lineage_path",
        "upstream_columns", "source_columns", "bronze_sources", "rule_columns", "Issue", "Before", "After", "Detail"}
TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def _cell(v):
    """API value -> Excel value: booleans and timestamps typed, text cleaned of characters Excel rejects."""
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        v = "; ".join(str(x) for x in v)
    if isinstance(v, str):
        if v in ("true", "false"):
            return v == "true"
        if TS.match(v):
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        v = ILLEGAL_CHARACTERS_RE.sub("", v)
        return v if len(v) <= MAX_CELL else v[:MAX_CELL] + " …(truncated)"
    return v


def _put(ws, row: int, col: int, v):
    c = ws.cell(row=row, column=col, value=_cell(v))
    if isinstance(c.value, str) and c.value[:1] in ("=", "+", "-", "@"):
        c.data_type = "s"   # text, never a formula (SQL like "-1 * x" or a crafted definition must stay text)
    if isinstance(c.value, datetime):
        c.number_format = "yyyy-mm-dd hh:mm:ss"
    return c


def _table(ws, header: list[str], rows: list[list]) -> None:
    for j, h in enumerate(header, 1):
        c = ws.cell(row=1, column=j, value=h)
        c.font, c.fill = HEAD_FONT, HEAD_FILL
    for i, r in enumerate(rows, 2):
        for j, v in enumerate(r, 1):
            _put(ws, i, j, v).alignment = WRAP
    for j, h in enumerate(header, 1):
        ws.column_dimensions[get_column_letter(j)].width = 60 if h in WIDE else max(12, min(30, len(h) + 4))
    ws.freeze_panes = "B2" if header and header[0] in ("target_table",) else "A2"
    if rows:
        ws.auto_filter.ref = f"A1:{get_column_letter(len(header))}{len(rows) + 1}"


def _sheet_name(name: str, used: set[str]) -> str:
    base = re.sub(r"[\[\]:*?/\\]", "_", name)[:31]
    out, n = base, 2
    while out.lower() in used:
        out = f"{base[:28]}~{n}"
        n += 1
    used.add(out.lower())
    return out


def _fmt(v) -> str:
    if v is None or v == []:
        return "—"
    if isinstance(v, list):
        return "\n".join(map(str, v))
    return str(v)


def history_rows(hist: dict, columns: set[str] | None, since: datetime) -> list[list]:
    """Flat, newest first: When, What, Column, Field, Before, After, Who, Run. `columns` limits it to the columns in
    the export (filters); runs themselves are always listed."""
    out = []
    cutoff = since.isoformat().replace("+00:00", "Z")
    for r in hist["runs"]:
        if r["started_at"] < cutoff:
            continue
        changed = (r["rows_inserted"] or 0) + (r["rows_updated"] or 0) + (r["rows_deactivated"] or 0)
        out.append([r["started_at"], "Agent run", None, None, None,
                    (f"{r['mode']}: {r['tables_processed']} tables processed, {changed} columns changed, "
                     f"{len(r['issues_opened'])} issues opened, {len(r['issues_resolved'])} closed"),
                    r.get("requested_by") or "not recorded", r["run_id"]])
        for ch in r["changes"]:
            if columns is not None and ch["column"] not in columns:
                continue
            if ch["kind"] == "new":   # one line per new column: its first definition
                d = ch["fields"].get("business_definition", {})
                out.append([ch["at"], "Column new", ch["column"], "business_definition", None, _fmt(d.get("after")),
                            "agent", r["run_id"]])
                continue
            if not ch["fields"]:
                out.append([ch["at"], f"Column {ch['kind']}", ch["column"], "lineage only", None, None,
                            "agent", r["run_id"]])
            for f, d in ch["fields"].items():
                out.append([ch["at"], f"Column {ch['kind']}", ch["column"], f, _fmt(d["before"]), _fmt(d["after"]),
                            "agent", r["run_id"]])
        for verb, items in (("Issue opened", r["issues_opened"]), ("Issue closed", r["issues_resolved"])):
            for it in items:
                if columns is None or it["column"] in columns:
                    out.append([r["started_at"], verb, it["column"], it["issue_type"], None, it["text"], "agent",
                                r["run_id"]])
    for d in hist["decisions"]:
        if d["at"] >= cutoff and (columns is None or d["column"] in columns):
            out.append([d["at"], f"Steward decision: {d['action']}", d["column"], d.get("field", "glossary"), d["before"],
                        d["after"] if d["action"] != "confirmed" else "still correct (lineage confirmed)", d["who"],
                        None])
    for s in hist["statuses"]:
        if s["at"] >= cutoff and (columns is None or s["column"] in columns):
            extra = "; ".join(x for x in (f"assignee {s['assignee']}" if s["assignee"] else "",
                                          f"comment: {s['comment']}" if s["comment"] else "") if x)
            out.append([s["at"], "Issue status", s["column"], s["issue_type"], s["before"],
                        s["after"] + (f" ({extra})" if extra else ""), s["who"], None])
    out.sort(key=lambda x: x[0] or "", reverse=True)
    return out


def build(*, rows: list[dict], issues: list[dict], hist: dict, user: str, source_table: str, filters: dict,
          history_days: int, consumers: list[dict] | None = None,
          downstream: dict[str, list[str]] | None = None) -> bytes:
    now = datetime.now(UTC)
    wb = Workbook()
    cover = wb.active
    cover.title = "Cover"
    tables = sorted({r["target_table"] for r in rows})
    keys = {f"{r['target_table']}.{r['target_column']}" for r in rows}
    in_scope = [i for i in issues if i["column_key"] in keys]
    by_status: dict[str, int] = {}
    for i in in_scope:
        by_status[i["status"]] = by_status.get(i["status"], 0) + 1
    by_source: dict[str, int] = {}
    for r in rows:
        by_source[r["definition_source"] or "none"] = by_source.get(r["definition_source"] or "none", 0) + 1
    by_sens: dict[str, int] = {}
    for r in rows:
        by_sens[r.get("sensitivity") or "unknown"] = by_sens.get(r.get("sensitivity") or "unknown", 0) + 1
    # consumers of these tables, or of tables built from these columns (table-level lineage)
    reach = set(tables) | {t for k in keys for t in (downstream or {}).get(k, [])}
    used_by = [c for c in consumers or [] if set(c["tables"]) & reach]
    last = hist["runs"][-1] if hist["runs"] else None
    lines: list[tuple[str, object]] = [
        ("Generated (UTC)", now.replace(tzinfo=None, microsecond=0)),
        ("Generated by", user),
        ("Source table", source_table),
        ("Filters", "; ".join(f"{k} = {', '.join(v) if isinstance(v, list) else v}" for k, v in filters.items())
         or "none: the whole mapping"),
        ("", None),
        ("Table versions (Delta)", None),
        *((f"  {t}", v if v is not None else "not readable") for t, v in hist.get("versions", {}).items()),
        ("Latest agent run", f"{last['run_id']} ({last['started_at']})" if last else "none"),
        ("STTM version written by it", last["version_after"] if last else None),
        ("", None),
        ("Counts", None),
        ("  Columns", len(rows)),
        ("  Tables", len(tables)),
        ("  Columns that need review", sum(1 for r in rows if r.get("has_open_issue") in (True, "true"))),
        ("  Open issues", len(in_scope)),
        *((f"    status {s}", n) for s, n in sorted(by_status.items())),
        *((f"  Definitions from {s}", n) for s, n in sorted(by_source.items())),
        *((f"  Personal data: {s}", n) for s, n in sorted(by_sens.items())),
        ("  Downstream consumers", f"{sum(c['status'] != 'deleted' for c in used_by)} (sheet 'Used by')"),
        ("", None),
        ("Recent history", f"last {history_days} days (sheet 'Recent history')"),
        *(("Note", n) for n in hist.get("notes", [])),
    ]
    cover["A1"] = "Source-to-target mapping: audit export"
    cover["A1"].font = TITLE_FONT
    for i, (k, v) in enumerate(lines, 3):
        cover.cell(row=i, column=1, value=k).font = Font(bold=not k.startswith(" "))
        c = _put(cover, i, 2, v)
        c.alignment = Alignment(wrap_text=True, vertical="top", horizontal="left")
    cover.column_dimensions["A"].width = 34
    cover.column_dimensions["B"].width = 90

    used = {"cover", "open issues", "used by", "recent history"}
    header = list(rows[0].keys()) if rows else []
    for t in tables:
        ws = wb.create_sheet(_sheet_name(t, used))
        _table(ws, header, [[r[h] for h in header] for r in rows if r["target_table"] == t])

    ws = wb.create_sheet("Open issues")
    _table(ws, ["Column", "Table", "Owner", "Type", "Issue", "Status", "Assignee", "Comment", "Status set by",
                "Status set at", "Opened in run", "Opened at"],
           [[i["column_key"], i["target_table"], i["owner"], i["issue_type"], i["issue_text"], i["status"],
             i["assignee"], i["comment"], i["updated_by"], i["updated_at"], i["opened_run"], i["opened_at"]]
            for i in in_scope])

    ws = wb.create_sheet("Used by")
    _table(ws, ["Type", "Name", "Owner", "Status", "Reads (in scope)", "Reads (all)", "First read", "Last read", "Link"],
           [[c["consumer_type"], c["name"], c["owner"], c["status"], sorted(set(c["tables"]) & reach), c["tables"],
             c["first_seen"], c["last_seen"], c["url"]] for c in used_by])

    ws = wb.create_sheet("Recent history")
    _table(ws, ["When (UTC)", "What", "Column", "Field", "Before", "After", "Who", "Agent run"],
           history_rows(hist, keys if filters else None, now - timedelta(days=history_days)))

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
