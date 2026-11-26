"""Sensitivity (personal data) of every STTM column: `pii`, `pii_derived` or `none`.

1. Bronze columns read from landed files are classified by their **name**: rules first; names the rules can't decide
   (e.g. `product_name`, `country_cd`) go to the LLM with only metadata (table, comment, column names and types).
   No data value is ever read or sent.
2. Every other column **inherits** from its sources, following how each source's value reaches it
   (`sqlparse.ColumnMap.flows`):
     copy  value survives (cast, trim, lower, concat, arithmetic, MIN/MAX, CASE recode)     -> source level
     mask  hashed, masked or cut to a part (sha2, md5, mask, substring, split_part, ...)   -> pii becomes pii_derived
     drop  only an aggregate or a yes/no survives (COUNT/SUM/AVG, comparisons, filters)     -> none
   The most sensitive source wins. Boolean columns are flags: none. Constants: none.
3. A **steward override** (`<prefix>_meta.sensitivity_overrides`, written from the viewer app as the signed-in user)
   wins over both, and columns downstream inherit the overridden level.

The result is stored per column as `sensitivity`, `sensitivity_source` (rule, llm, inherited, steward) and
`sensitivity_reason`, and written back as the `sttm_sensitivity` column tag.
"""

from __future__ import annotations

import re
from collections.abc import Callable

LEVELS = ("none", "pii_derived", "pii")
RANK = {level: i for i, level in enumerate(LEVELS)}
SOURCES = ("rule", "llm", "inherited", "steward")
NO_LLM_ANSWER = "name not decided by the rules, and no LLM answer (STTM_LLM_ENDPOINT=none or failed)"

_B = r"(?:^|_)"   # token start
_E = r"(?:_|$)"   # token end

# Checked in this order: PII rules, then names the rules can't decide (LLM), then non-personal rules.
PII_RULES = [
    (r"e_?mail", "email address"),
    ((rf"{_B}(?:first|last|full|given|family|middle|maiden|sur)_?(?:name|nm){_E}"
      rf"|{_B}(?:fname|lname|surname|forename){_E}|^name$"), "name of a person"),
    (rf"phone|{_B}(?:mobile|msisdn|fax|tel){_E}", "phone number"),
    (rf"{_B}(?:address|addr|street|postcode|postal_?code|zip|zip_?code|zipcode){_E}", "postal address"),
    (rf"birth|{_B}dob{_E}", "date of birth"),
    (rf"{_B}(?:ssn|national_id|passport\w*|tax_id|taxpayer_id|nif|nie|dni|cuil|cuit|driver_?licen[cs]e){_E}",
     "national or tax identifier"),
    (rf"{_B}(?:iban|card_?(?:number|no|num)|credit_?card|account_?(?:number|no|num)){_E}", "bank or card number"),
    (rf"{_B}(?:ip|ip_?addr(?:ess)?|device_id|cookie_id|imei|mac_?addr(?:ess)?){_E}", "online identifier"),
    (rf"{_B}(?:cust|customer|client|user|person|member|patient|employee|contact)_?(?:id|no|num|number|key|ref){_E}",
     "identifier of a person (a pseudonymous id is still personal data under GDPR)"),
    (rf"{_B}(?:gender|sex|ethnic\w*|religion|health|diagnosis|biometric\w*){_E}", "special-category data about a person"),
]
AMBIGUOUS = rf"{_B}(?:name|nm|title|description|notes?|comments?|text|location|city|country\w*|region|geo\w*|lat|lon|" \
            rf"latitude|longitude){_E}"
NONE_RULES = [
    ((rf"{_B}(?:qty|quantity|price|amount|amt|cost|total|revenue|margin|discount|tax|fee|rate|pct|percent|count|"
      rf"units?|value|score){_E}"), "a measure (amount, price, quantity)"),
    (rf"{_B}(?:currency|status|category|tier|type|kind|flag|code|cd|level|segment){_E}|^(?:is|has)_",
     "a code or category"),
    (rf"{_B}(?:sku|product|order|item|invoice){_E}", "a product or order reference"),
    (rf"{_B}(?:ts|at|dt|date|time|timestamp|day|month|year){_E}$", "the date or time of an event"),
]

LLM_SYSTEM = (
    "You classify columns of a retail company's lakehouse for personal data (GDPR). You only get metadata: table name, "
    "table comment, and column names with data types. You never get data values, and must not ask for them. "
    "For each column listed in 'classify', answer with one level: "
    "'pii' if the value identifies a person or is about an identified person on its own or as a direct identifier "
    "(name, email, phone, address, birth date, national id, customer id); "
    "'pii_derived' if it is a hashed, masked or tokenised form of such a value; "
    "'none' if it holds no personal data (products, prices, codes, event timestamps, or a coarse attribute such as a "
    "country or a category that does not single out a person by itself). Use the other columns of the table as context. "
    'Reply with JSON only: {"columns": [{"table": "...", "column": "...", "sensitivity": "pii|pii_derived|none", '
    '"reason": "max 120 characters"}]}')


def classify_name(column: str) -> tuple[str | None, str]:
    """(level, reason) from the column name; level None = the rules can't decide (ask the LLM)."""
    c = column.lower()
    for rx, why in PII_RULES:
        if re.search(rx, c):
            return "pii", f"name rule: {why}"
    if re.search(AMBIGUOUS, c):
        return None, "name not decided by the rules"
    for rx, why in NONE_RULES:
        if re.search(rx, c):
            return "none", f"name rule: {why}"
    return None, "name not covered by the rules"


def apply_flow(flow: str, level: str) -> str:
    if flow == "drop" or level == "none":
        return "none"
    if flow == "mask":
        return "pii_derived"
    return level


def assign(rows: dict[str, dict], prev: dict[tuple, dict], overrides: dict[str, tuple[str, str | None]],
           ask_llm: Callable[[list[dict]], dict[tuple[str, str], dict]], short: Callable[[str], str],
           log: Callable[..., None]) -> int:
    """Set sensitivity, sensitivity_source and sensitivity_reason on every row (all tables: downstream rows need their
    upstream levels). Returns how many columns were sent to the LLM."""
    # 1. bronze columns from landed files: name rules, then the LLM (cached: an earlier LLM answer for the same
    #    column is reused; the column name is all it was based on)
    named: dict[str, tuple[str, str, str]] = {}
    ask: dict[str, list[dict]] = {}
    for tgt, r in rows.items():
        if r["layer"] != "bronze" or r["mapping_kind"] == "technical":
            continue
        level, why = classify_name(r["target_column"])
        p = prev.get((r["target_table"], r["target_column"]))
        if level:
            named[tgt] = (level, "rule", why)
        elif p and p.get("sensitivity_source") == "llm" and p.get("sensitivity") in RANK:
            named[tgt] = (p["sensitivity"], "llm", p.get("sensitivity_reason") or "")
        else:
            ask.setdefault(r["target_table"], []).append(r)
    asked = sum(len(v) for v in ask.values())
    if ask:
        tables = [{"table": short(t), "table_comment": rs[0].get("_table_comment"),
                   "columns": [{"column": x["target_column"], "data_type": x["data_type"]}
                               for x in rows.values() if x["target_table"] == t],
                   "classify": [x["target_column"] for x in rs]} for t, rs in sorted(ask.items())]
        got = ask_llm(tables)
        for t, rs in ask.items():
            for r in rs:
                o = got.get((short(t), r["target_column"]))
                if o and o.get("sensitivity") in RANK:
                    named[f"{t}.{r['target_column']}"] = (o["sensitivity"], "llm",
                                                          ("LLM (name only): " + str(o.get("reason") or "")).strip()[:300])
                else:
                    named[f"{t}.{r['target_column']}"] = ("none", "rule", NO_LLM_ANSWER)
        log(f"   sensitivity: {asked} bronze column name(s) checked by "
            f"{'the LLM' if any(v[1] == 'llm' for v in named.values()) else 'rules only'}")

    # 2. everything else, upstream first (memoised recursion over the lineage)
    memo: dict[str, tuple[str, str, str]] = {}

    def resolve(tgt: str, stack: tuple = ()) -> tuple[str, str, str]:
        if tgt in memo:
            return memo[tgt]
        r = rows[tgt]
        key = short(tgt)
        if key in overrides:
            level, note = overrides[key]
            out = (level, "steward", f"set by a steward{': ' + note if note else ''}")
        elif tgt in named:
            out = named[tgt]
        elif r["mapping_kind"] == "technical":
            if r["target_column"] == "_rescued_data":
                sib = [(resolve(t)[0], t) for t, x in rows.items()
                       if x["target_table"] == r["target_table"] and t != tgt and x["mapping_kind"] != "technical"]
                level, worst = max(sib, key=lambda s: RANK[s[0]], default=("none", None))
                why = ("Auto Loader rescued data can hold any field of the record that did not match the schema, so it "
                       "is as sensitive as the table's most sensitive column"
                       + (f" ({short(worst)})" if worst and level != "none" else ""))
                out = (level, "rule", why)
            else:
                out = ("none", "rule", "technical column (source file name or load time)")
        elif r["mapping_kind"] == "constant":
            out = ("none", "inherited", "constant value")
        else:
            flows = dict(r.get("_flows") or {})
            for s in r["_sources"]:   # a source only UC lineage knows: assume its value is copied
                flows.setdefault(s, "copy")
            best, carrier, dropped = "none", None, None
            for s, flow in sorted(flows.items()):
                if s not in rows or s in stack:
                    continue
                up = resolve(s, stack + (tgt,))[0]
                lvl = apply_flow(flow, up)
                if RANK[lvl] > RANK[best]:
                    best, carrier = lvl, (s, flow, up)
                elif up != "none" and lvl == "none" and dropped is None:
                    dropped = (s, up)
            boolean = (r["data_type"] or "").lower().startswith("boolean")
            if boolean and best != "none":
                dropped, best, carrier = (carrier[0], carrier[2]), "none", None
            if carrier:
                s, flow, up = carrier
                why = (f"hashed, masked or partial value of {short(s)} ({up})" if flow == "mask" and up == "pii"
                       else f"carries the value of {short(s)} ({up})")
            elif dropped:
                why = (f"only {'a yes/no flag' if boolean else 'an aggregate or a yes/no'} of {short(dropped[0])} "
                       f"({dropped[1]}): no personal value")
            else:
                why = "no personal data upstream"
            out = (best, "inherited", why)
        memo[tgt] = out
        return out

    for tgt, r in rows.items():
        r["sensitivity"], r["sensitivity_source"], r["sensitivity_reason"] = resolve(tgt)
    return asked
