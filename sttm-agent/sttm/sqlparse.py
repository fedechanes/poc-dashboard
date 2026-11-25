"""Parse Lakeflow Declarative Pipeline SQL into column-level mappings.

For every dataset the pipeline defines we get:
  - the statement text and a hash of it (change detection)
  - column comments declared in the statement (a pipeline refresh re-applies these)
  - table-level rules: dedup (QUALIFY), filters (WHERE), grain (GROUP BY), joins, AUTO CDC keys
  - per target column: the expression, the source columns it reads, and "context" columns
    (filters/joins/dedup keys that change the result without being projected)

sqlglot resolves the column lineage through CTEs, aliases and `SELECT *`, given the table schemas from UC.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage

DIALECT = "databricks"
FILE_SOURCE = "<files>"   # pseudo source table for columns read from landed files


@dataclass
class ColumnMap:
    column: str
    expression: str                       # how the column is computed (with intermediate steps)
    sources: list[str] = field(default_factory=list)   # "catalog.schema.table.column" or "<files>:<path>.<field>"
    kind: str = "derived"                 # direct | derived | constant | technical


@dataclass
class Dataset:
    name: str                             # catalog.schema.table
    kind: str                             # STREAMING TABLE | MATERIALIZED VIEW
    statement: str
    sql_hash: str
    comment: str | None = None
    declared_comments: dict[str, str] = field(default_factory=dict)
    table_rule: str = ""
    context_columns: list[str] = field(default_factory=list)
    columns: dict[str, ColumnMap] = field(default_factory=dict)
    upstream: set[str] = field(default_factory=set)
    query: str | None = None
    cdc: dict | None = None


def sha(text: str, n: int = 16) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:n]


def normalize(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def strip_comments(sql: str) -> str:
    out, i, q = [], 0, None
    while i < len(sql):
        ch = sql[i]
        if q:
            out.append(ch)
            if ch == "\\" and i + 1 < len(sql):
                out.append(sql[i + 1]); i += 1
            elif ch == q:
                q = None
        elif ch in "'\"`":
            q = ch; out.append(ch)
        elif sql.startswith("--", i):
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def split_top(text: str, sep: str = ";") -> list[str]:
    """Split on `sep` outside quotes and parentheses."""
    parts, buf, depth, q = [], [], 0, None
    for ch in text:
        if q:
            q = None if ch == q else q
        elif ch in "'\"`":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == sep and depth == 0:
            parts.append("".join(buf)); buf = []; continue
        buf.append(ch)
    parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def substitute(sql: str, conf: dict[str, str]) -> str:
    return re.sub(r"\$\{(\w+)\}", lambda m: conf.get(m.group(1), m.group(0)), sql)


def _balanced(text: str, start: int) -> int:
    """Index just after the parenthesis group opening at text[start] == '('."""
    depth, q = 0, None
    for i in range(start, len(text)):
        ch = text[i]
        if q:
            q = None if ch == q else q
        elif ch in "'\"`":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    raise ValueError("unbalanced parentheses")


def _unquote(s: str) -> str:
    return s[1:-1].replace("\\'", "'") if s and s[0] == "'" else s


CREATE_RE = re.compile(r"^CREATE\s+(?:OR\s+REFRESH\s+)?(?:TEMPORARY\s+)?(STREAMING\s+TABLE|MATERIALIZED\s+VIEW)\s+"
                       r"([\w.`]+)\s*", re.I)
FLOW_RE = re.compile(r"^CREATE\s+FLOW\s+(\w+)\s+AS\s+AUTO\s+CDC\s+INTO\s+([\w.`]+)\s+FROM\s+STREAM\s*\(\s*([\w.`]+)\s*\)"
                     r"(.*)$", re.I | re.S)
COMMENT_RE = re.compile(r"^COMMENT\s+('(?:[^'\\]|\\.)*')\s*", re.I)


def parse_statements(files: dict[str, str], conf: dict[str, str]) -> dict[str, Dataset]:
    datasets: dict[str, Dataset] = {}
    for fname in sorted(files):
        for stmt in split_top(strip_comments(substitute(files[fname], conf))):
            m = CREATE_RE.match(stmt)
            if m:
                name = m.group(2).replace("`", "").lower()
                rest = stmt[m.end():]
                ds = Dataset(name=name, kind=" ".join(m.group(1).upper().split()), statement=stmt,
                             sql_hash=sha(normalize(stmt)))
                if rest.startswith("("):
                    end = _balanced(rest, 0)
                    for item in split_top(rest[1:end - 1], ","):
                        if item.upper().startswith("CONSTRAINT"):
                            continue
                        cm = re.match(r"^`?(\w+)`?\s+.*?COMMENT\s+('(?:[^'\\]|\\.)*')", item, re.I | re.S)
                        if cm:
                            ds.declared_comments[cm.group(1).lower()] = _unquote(cm.group(2))
                    rest = rest[end:].lstrip()
                cm = COMMENT_RE.match(rest)
                if cm:
                    ds.comment = _unquote(cm.group(1))
                    rest = rest[cm.end():]
                am = re.match(r"^AS\s+", rest, re.I)
                if am:
                    ds.query = rest[am.end():]
                datasets[name] = ds
                continue
            f = FLOW_RE.match(stmt)
            if f:
                target = f.group(2).replace("`", "").lower()
                tail = f.group(4)
                keys = re.search(r"KEYS\s*\(([^)]*)\)", tail, re.I)
                seq = re.search(r"SEQUENCE\s+BY\s+([\w.]+)", tail, re.I)
                scd = re.search(r"SCD\s+TYPE\s+(\d)", tail, re.I)
                ds = datasets[target]
                ds.cdc = {"flow": f.group(1), "source": f.group(3).replace("`", "").lower(),
                          "keys": [k.strip() for k in keys.group(1).split(",")] if keys else [],
                          "sequence_by": seq.group(1) if seq else None, "scd_type": scd.group(1) if scd else "1"}
                ds.statement += ";\n" + stmt
                ds.sql_hash = sha(normalize(ds.statement))
    return datasets


def _for_sqlglot(query: str) -> str:
    q = re.sub(r"\bSTREAM\s*\(\s*([\w.`]+)\s*\)", r"\1", query, flags=re.I)
    return re.sub(r"\bSTREAM\s+(read_files\s*\()", r"\1", q, flags=re.I)


def _sql(e: exp.Expression) -> str:
    return e.sql(dialect=DIALECT)


def _table_name(t: exp.Table) -> str:
    return ".".join(p for p in (t.catalog, t.db, t.name) if p).lower()


def _table_rules(tree: exp.Expression, schema: dict) -> tuple[str, list[str]]:
    """Human-readable table-level rule + the base columns used by joins/filters/dedup (they change the result
    without being projected, so they matter for impact analysis)."""
    rules, ctx = [], set()
    w = tree.args.get("with_") or tree.args.get("with")
    with_sql = _sql(w) if w else ""
    ctes = {c.alias.lower() for c in tree.find_all(exp.CTE)}

    def resolve(sel: exp.Select, col: exp.Column) -> set[str]:
        local = {}
        for t in sel.find_all(exp.Table):
            if t.parent_select is sel:
                local[(t.alias_or_name).lower()] = _table_name(t) if t.db else t.name.lower()
        owner = col.table.lower() if col.table else None
        if owner is None:   # unqualified: the single source of this SELECT that has it
            cands = [a for a, n in local.items() if n in ctes or col.name.lower() in _schema_cols(schema, n)]
            owner = cands[0] if len(cands) == 1 else None
        target = local.get(owner) if owner else None
        if not target:
            return set()
        if target not in ctes:
            return {f"{target}.{col.name.lower()}"}
        try:   # column of a CTE: trace it back to base tables
            node = lineage(col.name, f"{with_sql} SELECT {col.name} FROM {target}", schema=schema, dialect=DIALECT)
        except Exception:
            return set()
        return {f"{_table_name(n.source)}.{n.name.split('.')[-1].lower()}" for n in node.walk()
                if isinstance(n.source, exp.Table) and not n.downstream}

    for sel in tree.find_all(exp.Select):
        for j in sel.args.get("joins") or []:
            on = j.args.get("on")
            rules.append(f"{(j.side or 'INNER').upper()} JOIN {_sql(j.this)}" + (f" ON {_sql(on)}" if on else ""))
            for c in (on.find_all(exp.Column) if on else []):
                ctx |= resolve(sel, c)
        for key, label in (("where", "FILTER"), ("qualify", "DEDUP/QUALIFY")):
            node = sel.args.get(key)
            if node:
                rules.append(f"{label} {_sql(node.this)}")
                for c in node.find_all(exp.Column):
                    ctx |= resolve(sel, c)
        g = sel.args.get("group")
        if g:
            rules.append("GRAIN GROUP BY " + ", ".join(_sql(e) for e in g.expressions))
    return "; ".join(dict.fromkeys(rules)), sorted(ctx)


def _schema_cols(schema: dict, table: str) -> dict:
    parts = table.split(".")
    if len(parts) != 3:
        return {}
    return schema.get(parts[0], {}).get(parts[1], {}).get(parts[2], {})


def map_columns(ds: Dataset, target_cols: list[str], schema: dict) -> None:
    """Fill ds.columns for the columns UC reports for the target table."""
    if ds.cdc:
        src = ds.cdc["source"]
        ds.upstream = {src}
        ds.table_rule = (f"AUTO CDC upsert from {src}: SCD type {ds.cdc['scd_type']}, KEYS ({', '.join(ds.cdc['keys'])}), "
                         f"latest by {ds.cdc['sequence_by']}")
        ds.context_columns = [f"{src}.{k}" for k in ds.cdc["keys"] + [ds.cdc["sequence_by"]]]
        for c in target_cols:
            ds.columns[c] = ColumnMap(c, c, [f"{src}.{c}"], "direct")
        return
    if not ds.query:
        return
    tree = sqlglot.parse_one(_for_sqlglot(ds.query), dialect=DIALECT)
    ds.table_rule, ds.context_columns = _table_rules(tree, schema)
    ds.upstream = {_table_name(t) for t in tree.find_all(exp.Table) if t.db}

    rf = next(tree.find_all(exp.ReadCSV), None) or _find_read_files(tree)
    if rf is not None:   # bronze: Auto Loader over files
        path = _sql(rf.this if isinstance(rf, exp.ReadCSV) else rf.expressions[0]).strip("'")
        ds.upstream = {f"{FILE_SOURCE}:{path}"}
        explicit = {p.alias_or_name.lower(): p for p in tree.selects if not isinstance(p, exp.Star)}
        for c in target_cols:
            if c in explicit:
                ds.columns[c] = ColumnMap(c, _sql(explicit[c].unalias()), [], "technical")
            elif c == "_rescued_data":
                ds.columns[c] = ColumnMap(c, "Auto Loader rescued data (fields that did not match the schema)", [],
                                          "technical")
            else:
                ds.columns[c] = ColumnMap(c, c, [f"{FILE_SOURCE}:{path}.{c}"], "direct")
        return

    sql_for_lineage = _sql(tree)
    for c in target_cols:
        try:
            node = lineage(c, sql_for_lineage, schema=schema, dialect=DIALECT)
        except Exception as e:  # unresolvable column: report, don't crash the run
            ds.columns[c] = ColumnMap(c, f"<unparsed: {e}>", [], "derived")
            continue
        sources, steps = set(), []
        for n in node.walk():
            if isinstance(n.source, exp.Table) and not n.downstream:
                sources.add(f"{_table_name(n.source)}.{n.name.split('.')[-1].lower()}")
            elif n is not node and isinstance(n.expression, exp.Alias) \
                    and not isinstance(n.expression.this, exp.Column):
                steps.append(f"{n.expression.alias} := {_sql(n.expression.this)}")
        root = node.expression.this if isinstance(node.expression, exp.Alias) else node.expression
        expr = _sql(root)
        if steps:
            expr += "  WHERE " + "; ".join(sorted(set(steps)))
        if not sources:
            kind = "constant"
        elif isinstance(root, exp.Column) and not steps:
            kind = "direct"
        else:
            kind = "derived"
        ds.columns[c] = ColumnMap(c, expr, sorted(sources), kind)


def _find_read_files(tree: exp.Expression):
    for f in tree.find_all(exp.Anonymous):
        if f.name.lower() == "read_files":
            return f
    return None
