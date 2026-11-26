# STTM agent on a synthetic medallion (Databricks Free Edition)

A small, reproducible proof of concept. It covers these things:

- **Medallion.** A synthetic online-retail medallion (bronze → silver → gold), built as one serverless Lakeflow Declarative Pipeline.
- **STTM.** A source-to-target mapping with one row per target column: source columns, transformation, type, business definition, owner, layer and lineage path back to bronze. It is kept as a Delta table, flattened into a second table (`sttm_export`, same columns as the CSV) and exported to [`output/sttm.csv`](output/sttm.csv) and [`output/sttm.md`](output/sttm.md).
- **Agent.** It reads the deployed pipeline SQL plus Unity Catalog (UC) metadata and lineage. It drafts missing definitions with a Foundation Model API LLM and writes them back as **column comments and tags**. It also flags gaps and conflicts.
- **Personal data.** Every column is classified `pii`, `pii_derived` or `none`: by name at bronze (rules, then the LLM for undecided names; no data values are read), then down the lineage, with steward overrides. The level goes to the STTM and to a `sttm_sensitivity` column tag ([personal data](#personal-data-sensitivity)).
- **Incremental updates.** Both the data and the metadata are refreshed incrementally. The [answer](#incremental-updates-the-answer) comes with evidence from a demo run.
- **Impact beyond gold.** The agent also reads who uses the medallion outside the pipeline (dashboards, jobs, notebooks, Genie spaces, ad hoc queries) from the lineage system tables. The app shows them in a *Used by* lane and in every impact analysis ([downstream consumers](#downstream-consumers-impact-beyond-gold)).
- **Viewer app.** A small web app over `sttm_export` with filters and impact analysis. Stewards approve definitions in it, and owners work through the issues on their tables, with an email from a SQL alert when an agent run finds new ones. A History tab shows who changed what and when, and **Download for audit** saves it all as an Excel workbook. It runs locally or as a Databricks App ([viewer app](#viewer-app)).

It lives next to the dashboard-embedding POC in this repo but shares nothing with it except `pyproject.toml`. It has its own folder, its own schemas (`<catalog>.sttm_*`) and its own pipeline.

## Run it

Requirements: Python 3.12+, [uv](https://docs.astral.sh/uv/), and a Databricks CLI profile. The workspace needs Unity
Catalog, serverless, and a writable catalog (default `workspace`).

```bash
cp sttm-agent/.env.example sttm-agent/.env        # set DATABRICKS_CONFIG_PROFILE=<your profile>
uv run python sttm-agent/run.py setup             # schemas, volume, batch 1, pipeline, first agent run (~3-5 min)
uv run python sttm-agent/run.py demo              # incremental demo (see below)
uv run python sttm-agent/run.py agent             # incremental agent run; --full to reprocess, --dry-run to preview
uv run python sttm-agent/app/app.py               # viewer app on http://localhost:8001
uv run python sttm-agent/run.py app-deploy        # the same app as a Databricks App
uv run python sttm-agent/run.py demo-dashboard    # the "STTM demo - Sales" dashboard: a consumer of gold
uv run python sttm-agent/run.py destroy           # delete the app, job, demo dashboard and pipeline; drop the sttm_* schemas
```

Every command is safe to re-run.

- **`setup`** uses `CREATE ... IF NOT EXISTS`, updates the pipeline in place by id (`sttm_state.json`, gitignored), and re-lands identical files, which Auto Loader skips.
- **`setup` also resets the deployed pipeline SQL** to the version in this repo, which undoes the demo change.
- **No workspace settings are changed.**

Configuration is read from environment variables or `sttm-agent/.env`; see [`.env.example`](.env.example). Nothing workspace-specific is in the code.

## What gets built

```
/Volumes/<cat>/sttm_landing/raw/{customers,orders,products}/   synthetic CRM / web-shop / catalogue extracts (JSON, CSV)
        │  Auto Loader (read_files on a stream): only new files per run
        ▼
sttm_bronze   customers_raw, orders_raw, products_raw          streaming tables, all strings + _source_file/_ingested_at
        ▼
sttm_silver   customers_cleansed   streaming table: casts, renames, initcap/lower, code→label CASE, expectation
              customers            AUTO CDC (SCD1) upsert keyed on customer_id, latest updated_at wins (dedup)
              products             MV: latest row per SKU (QUALIFY dedup), casts, derived margin_pct
              orders               MV: latest event per order (QUALIFY dedup), epoch→timestamp, FX to EUR, flags
        ▼
sttm_gold     customer_360         MV: customers LEFT JOIN per-customer order aggregates
              daily_sales          MV: orders ⋈ products ⋈ customers, filtered, grouped by day/category/country
sttm_meta     sttm, sttm_export, table_fingerprints, uc_lineage, agent_state, agent_runs, pipeline_event_log,
              glossary, sensitivity_overrides, owners, issues, issue_status, consumers
        ▼
Used by       dashboards, jobs, notebooks, Genie spaces, ad hoc queries... that read the medallion (from system.access)
```

| Path | Role |
|---|---|
| [`pipeline/*.sql`](pipeline) | The pipeline source (bronze/silver/gold). `${bronze}` etc. are pipeline configuration values. |
| [`glossary.toml`](glossary.toml) | Owner team per layer or table. New teams are added to the `sttm_meta.owners` table (team → people). Its `[definitions]` seed the `sttm_meta.glossary` table on the first run; after that stewards approve and edit definitions in the viewer app. |
| [`sttm/datagen.py`](sttm/datagen.py) | Deterministic synthetic batches: duplicates, mixed case, string types, status-change events. |
| [`sttm/medallion.py`](sttm/medallion.py) | Schemas, volume, file landing, pipeline deploy/run, and event-log metrics. |
| [`sttm/sqlparse.py`](sttm/sqlparse.py) | Lakeflow SQL → per-column expression and sources (sqlglot), plus table rules (joins, filters, dedup, grain, AUTO CDC). |
| [`sttm/agent.py`](sttm/agent.py) | The agent: change detection, definitions, checks, MERGE, write-back, export. |
| [`sttm/sensitivity.py`](sttm/sensitivity.py) | Personal data: name rules, the LLM check for undecided names, propagation down the lineage, steward overrides. |
| [`sttm/consumers.py`](sttm/consumers.py) | Downstream consumers: incremental pull from `system.access.table_lineage`, names, owners and links from the workspace APIs. |
| [`sttm/demo_dashboard.py`](sttm/demo_dashboard.py) | The "STTM demo - Sales" AI/BI dashboard over gold, created, published and refreshed by `run.py demo-dashboard`. |
| [`run.py`](run.py) | The CLI above, including the demo. |
| [`app/`](app) | The viewer app: FastAPI ([`app.py`](app/app.py)) and one static page ([`static/index.html`](app/static/index.html)), plus [`app.yaml`](app/app.yaml) for Databricks Apps. [`history.py`](app/history.py) reads the audit history from change data feed; [`audit.py`](app/audit.py) builds the Excel workbook. |

## The STTM table

`<cat>.sttm_meta.sttm` has one row per target column. Change data feed is enabled on it.

| Column | Content |
|---|---|
| `target_table`, `target_column`, `layer`, `data_type` | From UC `information_schema.columns`. |
| `source_columns` | Parsed from the pipeline SQL with sqlglot, through CTEs, aliases and `SELECT *`, unioned with UC column lineage when UC has it. Bronze columns point at `landing/<source>.<field>`. |
| `transformation`, `mapping_kind` | The column expression, with intermediate CTE steps (`… WHERE net_amount := …`). Kind is `direct`, `derived`, `constant` or `technical`. |
| `table_rule`, `rule_columns` | Rules that change every column of the table: joins, `WHERE` filters, `QUALIFY` dedup, `GROUP BY` grain, AUTO CDC keys and sequencing. `rule_columns` lists the columns those rules read (join keys, filter flags, dedup order), so a change to them reaches every column of the table. |
| `business_definition`, `definition_source` | See [definitions](#how-definitions-are-chosen). |
| `owner` | From `glossary.toml` (`[owners]`, by layer or by table). |
| `sensitivity`, `sensitivity_source`, `sensitivity_reason` | Personal data level (`pii`, `pii_derived`, `none`), where it comes from (`rule`, `llm`, `inherited`, `steward`) and why, for example "carries the value of silver.customers.full_name (pii)". See [personal data](#personal-data-sensitivity). |
| `lineage_path`, `bronze_sources` | Recursive walk back to bronze and the landed files, for example `gold.customer_360.lifetime_value_eur <- silver.orders.{is_cancelled, net_amount_eur} <- bronze.orders_raw.{currency, discount_amt, qty, status, unit_price} <- landing/orders.{…}`. |
| `lineage_origin` | `sql`, `sql, uc table-confirmed` (UC table lineage confirms every source table), or `sql+uc column` (UC column lineage agrees). |
| `issues` | `GAP` (no source column), `CONFLICT` (definition contradicts the SQL), `REVIEW` (an upstream rule changed under a curated definition), `FIX IN PIPELINE SQL`, `LINEAGE` (UC and SQL disagree), `INFO` (technical columns). |
| `proposed_definition` | The LLM's correction when it finds a conflict. |
| `rule_hash`, `lineage_hash`, `row_hash` | Change detection; see [incremental updates](#incremental-updates-the-answer). |
| `is_active`, `first_seen_run`, `last_changed_run`, `updated_at` | Soft delete and audit. |

### The flat export: `sttm_export`

`<cat>.sttm_meta.sttm_export` holds the active rows only, with the same columns as [`output/sttm.csv`](output/sttm.csv). Lists are joined with `"; "` (issues with `" | "`, because issue texts contain semicolons). It adds columns for filtering:

- `upstream_columns`: every column this one depends on, at any depth, through its sources and its table rule.
- `issue_types` and `has_open_issue`.
- `uc_table`: the fully qualified table name.

The agent rewrites it at the end of a run with `INSERT OVERWRITE`, so readers never see it half-written. It only does so when the content changed: a run with nothing to do leaves it alone. The viewer app reads only this table.

### How definitions are chosen

Highest priority first:

1. **The glossary** (`sttm_meta.glossary`): definitions a steward approved or wrote in the viewer app, seeded once from `glossary.toml`. The agent writes them back as the column comment.
2. **A comment declared in the pipeline SQL** (column list of `CREATE MATERIALIZED VIEW`). The agent does **not** overwrite it: a pipeline refresh re-applies declared comments, which I tested. A conflict there is flagged `FIX IN PIPELINE SQL`.
3. **A human UC comment**, meaning any comment the agent did not write. The agent recognises its own comments by the `sttm_comment_hash` tag. Human comments are kept, never overwritten; conflicts are flagged with a proposed fix.
4. **The previous draft**, reused when the column's `lineage_hash` has not changed. No LLM call.
5. **A new LLM draft** (default `databricks-qwen3-next-80b-a3b-instruct`). One call per table, JSON in and out. With `STTM_LLM_ENDPOINT=none` it falls back to rule-based templates.

Definitions from sources 1–3 are **checked** against the transformation by the same LLM, acting as a judge with the upstream expressions resolved. The check runs only when the definition or anything upstream changed.

### Personal data (sensitivity)

Three levels, as agreed: `pii` (identifies a person or is about one), `pii_derived` (hashed, masked or partial PII) and `none`. Finer categories (contact, financial, special category) are not tracked yet. The code is in [`sttm/sensitivity.py`](sttm/sensitivity.py) and [`sttm/sqlparse.py`](sttm/sqlparse.py) (`flows`).

**1. Bronze: classified by column name, never by data.** The agent reads no rows and sends no values anywhere.

| Step | What decides | Examples in this medallion |
|---|---|---|
| PII rules | email, person name (`first_nm`, `full_name`), phone, address, birth date, national or tax id, bank or card number, IP or device id, **person identifiers** (`cust_id`, `customer_id`, `user_id`), special-category words | `email_addr`, `first_nm`, `last_nm`, `cust_id` → `pii` |
| Undecided names → LLM | Names with `name`, `country`, `city`, `description`, `notes`, … that the rules can't decide. The LLM (same endpoint as the definitions) gets the table name and comment and the column names and types only, and answers `pii`, `pii_derived` or `none` with a reason. One call per run at most, and only for names it has not answered before. | `product_name` → `none` ("describes an item, not a person"), `country_cd` → `none` ("does not identify an individual") |
| Non-personal rules | measures (`qty`, `price`, `amount`), codes and categories (`status`, `currency`, `tier`), product and order references, event timestamps | `qty`, `status`, `signup_dt`, `order_ts` → `none` |
| Technical columns | `_source_file`, `_ingested_at` → `none`. `_rescued_data` can hold any field of the record that did not match the schema, so it gets the table's most sensitive level. | `customers_raw._rescued_data` → `pii` |

Without an LLM (`STTM_LLM_ENDPOINT=none`) or if the call fails, undecided names fall back to `none` with that reason, so a steward should look at them.

A customer id counts as `pii`: a pseudonymous identifier is still personal data under the GDPR (Recital 26), because it can be linked back to the person. That is why `silver.orders.customer_id` is `pii` too. A steward can override it per column if your policy says otherwise.

**2. Silver and gold: inherited down the lineage.** For each target column, the parser records how each source column's *value* reaches it, walking the sqlglot lineage through CTEs and aliases. Along one path the weakest step wins; across paths from the same source, the strongest path wins.

| How the value reaches the column | Effect | Examples |
|---|---|---|
| Copied or transformed: cast, trim, `lower`, `concat`, arithmetic, `datediff`, `MIN`/`MAX`, a simple `CASE x WHEN … THEN …` recode | Keeps the source's level | `full_name = concat_ws(' ', initcap(first_nm), initcap(last_nm))` → `pii` |
| Hashed, masked or cut to a part: `sha2`, `sha`/`sha1`, `md5`, `hash`, `xxhash64`, `crc32`, `mask`, `aes_encrypt`, `substring`/`substr`, `left`, `right`, `split_part`, `regexp_extract` | `pii` becomes `pii_derived` | `sha2(email, 256)`, `split_part(email, '@', 2)` → `pii_derived` |
| Only an aggregate or a yes/no survives: `COUNT`, `SUM`, `AVG`, `COUNT_IF`, `STDDEV`, `length`, comparisons, `IN`, `LIKE`, `IS NULL`, searched `CASE WHEN <condition>`, the `FILTER (WHERE …)` and `OVER (…)` parts | `none` | `count(DISTINCT email)`, `email IS NOT NULL` → `none` |
| Boolean columns (flags) and constants | `none` | `is_cancelled`, `report_currency` |

The most sensitive source wins. Join keys, `WHERE` filters and dedup columns (`rule_columns`) pick rows but do not carry values, so they don't count. `mapping_kind` and the parsed expression are the inputs; a source that only UC lineage knows is treated as copied. The synthetic cases above (`sha2`, `md5`, `mask`, `substring`, `split_part`, `count(DISTINCT email)`, `min(full_name)`, `avg(length(full_name))`, a searched and a simple `CASE`) were checked against the parser offline, because the medallion itself has no hashed column.

**3. Steward overrides win.** A steward can set the level of any column in the viewer app. The override is stored in `<prefix>_meta.sensitivity_overrides` (`column_key`, `sensitivity`, `note`, `updated_by`, `updated_at`, change data feed on) and written as the signed-in user, like glossary decisions. The next agent run uses it for that column, and columns downstream inherit it. It stays until a steward removes it.

I picked a separate table over a `sensitivity` field in `glossary`:
- A definition and a privacy level are independent decisions. Each needs its own `updated_by` and `updated_at`, and its own history.
- *Remove from glossary* deletes the glossary row. A privacy override must not disappear with it.
- It can get its own grants, for example a privacy-officer group with `MODIFY` on overrides only. Today the stewards group has `MODIFY` on both.

**Change detection.** Overrides count in each table's `glossary_hash`, so changing one reprocesses that table and its downstream tables. Rows written before this feature (with no `sensitivity`) make their tables run once (`sensitivity backfill`), so no `--full` run is needed after upgrading.

Measured in this workspace:
- **Backfill run:** 9 tables, 84 rows updated, 84 tag sets written, 1 LLM call for 2 names, 0 definitions changed, 75 s. The next run touched nothing: 0 tables, 0 LLM calls, 41 s.
- **Result:**
  - `email` is `pii` in bronze (`email_addr`), `silver.customers_cleansed` and `silver.customers`. The gold SQL doesn't select it.
  - `full_name` is `pii` from `first_nm`/`last_nm` through both silver tables to `gold.customer_360`.
  - Every `gold.daily_sales` column is `none`: `orders`, `units`, `revenue_eur` and `gross_margin_eur` are aggregates of non-personal columns. `customer_id` is only a join key there.
  - `customer_360.order_count` and `lifetime_value_eur` are `none`.
  - 16 columns in 6 tables are `pii`.
- **Override, locally:** setting `bronze.customers_raw.signup_dt` to `pii` ("Signup date plus country can single out a customer") changed 4 rows: `signup_dt`, `signup_date` in both silver tables, and `gold.customer_360.active_span_days`, which is `datediff(last_order_date, signup_date)`. Their tags changed in UC too. The next run was a no-op, and the override stayed.
- **Override, in the deployed app:** removing it, running the agent job, setting it again, running the job, then running the job once more gave 4 rows back to `none`, 4 rows to `pii`, then 0 changes. Each job run took 110–120 s. The stored row has the signed-in user as `updated_by`. The app's service principal has only `SELECT` on the table, and `sttm-stewards` has `SELECT` and `MODIFY`.

### Write-back

The agent writes only what differs from UC:

- **Comments:** `COMMENT ON COLUMN` for glossary, LLM and rule definitions.
- **Tags:** `ALTER TABLE … ALTER COLUMN … SET TAGS` for `sttm_layer`, `sttm_owner`, `sttm_definition_source`, `sttm_rule_hash`, `sttm_comment_hash` and `sttm_sensitivity` (`pii`, `pii_derived` or `none`).

`sttm_sensitivity` is a free-form tag: any user who can tag the column can change its value, and Unity Catalog doesn't enforce the allowed values. The production path is a **governed tag** with a **tag policy**:
- The policy limits the allowed values to `pii`, `pii_derived` and `none`.
- Only users or service principals with the `ASSIGN` permission on the tag can set it.
- [ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/) policies can then mask every column that carries the tag. In the docs' words, governed tags "are used in policy conditions to identify which data a policy should protect".

Governed tags are account-level objects ([governed tags](https://docs.databricks.com/aws/en/admin/governed-tags/), up to 1,000 per account and 500 allowed values per tag). Free Edition has "no access to the account console or account-level APIs" ([limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)). Creating one would also be an account-wide change, so I did not try it here. The docs pages I read don't name a pricing tier.

Databricks [data classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) is the platform's own scanner: an agent that tags columns with `class.*` system tags such as `class.email_address`. Unlike this agent, it evaluates the data itself, and its results can include sample values. On a paid workspace, the agent could read those tags as one more input next to its name rules.

Tag limits ([tags](https://docs.databricks.com/aws/en/database-objects/tags)): 50 tags per column, 1,000 column tags per table, keys and values up to 256 characters. Each column gets at most 6 `sttm_*` tags. "Tag data is stored as plain text and may be replicated globally", so tags hold levels, never values.

Statements for different tables run in parallel. Statements for the same table run one after another, because concurrent `ALTER`s on one pipeline table fail with `PIPELINE_FAILED_TO_UPDATE_UC_TABLE_DUE_TO_CONCURRENT_UPDATE`. Everything can be read back with:

```sql
SELECT table_name, column_name, comment FROM <cat>.information_schema.columns  WHERE table_schema LIKE 'sttm_%';
SELECT * FROM <cat>.information_schema.column_tags WHERE schema_name LIKE 'sttm_%';
```

## Incremental updates: the answer

**Yes, the STTM and the UC metadata can be refreshed incrementally, and so can the data.** "Incremental" here means unchanged columns cost nothing: no LLM call, no `MERGE` update, no DDL. Each run still re-parses the SQL and re-reads `information_schema`, which is cheap (about 20 s for a no-op run on Free Edition).

### Metadata (STTM, comments, tags)

| Layer | Mechanism |
|---|---|
| **Which tables to look at** | `sttm_meta.table_fingerprints` keeps one row per target table: hash of its normalised SQL statement (`sql_hash`), hash of its UC columns, types, comments and tags (`schema_hash`), hash of its glossary entries, and hash of its UC lineage edges. A table is processed only if one of these changed. Its downstream tables are processed too, because their lineage may have changed; hashes then decide per row. |
| **UC lineage** | Pulled incrementally from `system.access.column_lineage` and `table_lineage` with a watermark on `event_time`. Events can land late, so each pull re-reads a 2-hour overlap; the `MERGE` into `sttm_meta.uc_lineage` is idempotent. |
| **Consumers** | Read events of the medallion tables, pulled from `table_lineage` the same way (own watermark, 2-hour overlap). Only edges that are new or seen later are merged, and names are rewritten only when they changed, so a run with nothing new writes nothing to `sttm_meta.consumers`. They do not touch the STTM rows. See [downstream consumers](#downstream-consumers-impact-beyond-gold). |
| **Which rows changed** | `rule_hash` covers the column's own expression, sources, type and table rule. `lineage_hash` covers `rule_hash` plus the `lineage_hash` of every upstream column, including filter, join and dedup columns. `row_hash` covers everything that is exported. |
| **STTM table** | `MERGE … WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE … WHEN NOT MATCHED THEN INSERT … WHEN NOT MATCHED BY SOURCE (in processed tables) THEN is_active = false`. Change data feed records exactly the touched rows, so downstream consumers can read `table_changes()`. |
| **LLM** | Called only for new columns, or columns whose `lineage_hash` changed. A curated definition gets a check, not a redraft, when it or anything upstream changed. Pass-through columns inherit the upstream definition without a call. |
| **Comments and tags** | Compared with `information_schema.columns.comment` and `information_schema.column_tags`; only differences are written. The agent's own comments carry `sttm_comment_hash`, so it can tell its own text from a human edit. |
| **Impact analysis** | When a rule changes, every column whose `lineage_hash` depends on it is revisited. LLM drafts are redrafted. Curated definitions (glossary, pipeline SQL, human comment) get a `REVIEW: upstream rule changed (…)` flag. |

**Watermark caveat.** `information_schema.columns` has no `last_altered`. `information_schema.tables.last_altered` changes on every pipeline refresh, so it is useless as a change signal for pipeline tables. Hashes are the source of truth; watermarks only bound how much is read.

### Data (the medallion)

| Table | How it stays incremental (evidence from the demo) |
|---|---|
| bronze `*_raw` | Streaming tables over `STREAM read_files(...)`, which is Auto Loader. Batch 2 ingested **only the new files**: `orders_raw` holds `web_orders_001.json=1931, web_orders_002.json=500`, and that update wrote 500 rows. |
| `silver.customers_cleansed` | Streaming table reading bronze as a stream: **55 rows** written, only the new batch. |
| `silver.customers` | `AUTO CDC … KEYS (customer_id) SEQUENCE BY updated_at STORED AS SCD TYPE 1`: **55 upserts**. 25 existing customers were updated in place and 30 inserted, giving 230 rows for 230 customers. |
| silver/gold MVs | Serverless picks the refresh technique per update and logs it in the event log. In the demo: `products` ran `window_function` (incremental), `daily_sales` ran `generic_aggregate` (incremental), and `orders` and `customer_360` ran `complete_recompute`, because their definitions changed in that update. Avoid `current_date()` in the select list: a first version of `customer_360` used it and was flagged `PLAN_NOT_DETERMINISTIC` (full recompute every time). |
| change data feed | Pipeline MVs and streaming tables have it on (`is_cdf_enabled: true` in the planner output). The STTM table enables it explicitly. |

### Demo run (`run.py demo`)

The full output is in [`output/demo_run.txt`](output/demo_run.txt). Starting from the state after `setup` (batch 1, 82 STTM rows), the demo:

1. Lands **batch 2**: new and changed customers, new orders, status changes including returns, and re-priced products.
2. Changes the **deployed** pipeline SQL. One changed rule: `silver.orders.is_cancelled` no longer counts returns. Two new columns: `silver.orders.is_large_order` and `gold.customer_360.large_order_count`.
3. Runs the pipeline (numbers in the table above).
4. Runs the agent:

```
9 target tables, 4 to process:
  silver.orders                sql, schema
  silver.products              uc_edges          <- late-arriving UC table lineage; re-checked, 0 rows changed
  gold.customer_360            sql, schema
  gold.daily_sales             upstream silver.orders, silver.products
rows_inserted=2, rows_updated=12, rows_unchanged=70, llm_calls=10, comments_written=7, tags_written=7
```

The change data feed of `sttm_meta.sttm` for that run shows exactly these rows:

| Change | Columns | Why |
|---|---|---|
| insert | `silver.orders.is_large_order`, `gold.customer_360.large_order_count` | new columns, LLM-drafted |
| update | `silver.orders.is_cancelled` | transformation changed; definition redrafted ("…'cancelled'…" instead of "cancelled or returned") |
| update | `gold.daily_sales.orders/units/revenue_eur/report_currency` | filtered by `is_cancelled`; LLM drafts redrafted ("excluding only cancelled orders (returned orders are included)") |
| update | `gold.customer_360.lifetime_value_eur`, `order_count` | glossary definitions say "not cancelled **or returned**", which is now wrong. Flagged `REVIEW: upstream rule changed (silver.orders.is_cancelled)`; the glossary is not overwritten. |
| update | `gold.daily_sales.order_date/category/country_code/gross_margin_eur`, `gold.customer_360.avg_order_value_eur` | pipeline-SQL or human definitions under a changed filter: `REVIEW` flag. Existing `CONFLICT`s are kept. |

5. Runs the agent again with nothing changed: **0 tables processed, 0 rows, 0 LLM calls, 0 DDL**.

The run log is also in `sttm_meta.agent_runs`: one row per run, with counts, the reasons each table was processed, the issues the run opened (`details.new_issues`), and the versions of the `sttm` table before and after the run (`details.sttm_version_before`, `sttm_version`).

### Gaps and conflicts it finds (current [`output/sttm.md`](output/sttm.md))

- **`gold.customer_360.avg_order_value_eur`: CONFLICT.** A stale human comment ("Total revenue … including cancelled orders", seeded by `setup`) contradicts `ROUND(lifetime / order_count)` over non-cancelled orders. The agent proposes a correction and keeps the human comment.
- **`gold.daily_sales.gross_margin_eur`: CONFLICT + FIX IN PIPELINE SQL.** The comment declared in the pipeline ("Revenue before discounts") contradicts `SUM(net_amount_eur - quantity * cost_price_eur)`.
- **`gold.daily_sales.report_currency`: GAP.** No source column (constant `'EUR'`).
- **Bronze `_source_file`, `_ingested_at`, `_rescued_data`: INFO.** Technical columns with no source field.

## Viewer app

`app/` is a read-only web app over `sttm_export`. Each filter accepts several values (OR within a filter, AND across filters). The filters are kept in the URL, so a filtered view can be shared as a link.

| Filter | What it is for |
|---|---|
| **Table** (the nodes of the lineage map at the top) | The asked-for `table_name` filter. The map shows landed files → bronze → silver → gold → *Used by*, with table-level edges from the STTM and dashed read edges to the consumers. Selecting a table highlights everything upstream and downstream of it, consumers included. Selecting a landed source shows every column fed by those files. Selecting a consumer selects the tables it reads. |
| **Needs review** / **Issue type** | The review worklist: every column with an open issue, or one type (`CONFLICT`, `REVIEW`, `GAP`, `FIX IN PIPELINE SQL`, `LINEAGE`, `INFO`). |
| **Depends on** | Impact analysis: everything downstream of a column or a table, through sources and table rules. For example, `silver.orders.is_cancelled` returns 12 gold columns, because `daily_sales` filters on it. A *Used by* box above the results lists the consumers that read any of those tables, with owner and link. |
| **Definition source** | `glossary`, `pipeline_sql`, `uc_comment`, `inherited`, `llm`, `rule`. `llm` shows what nobody has curated yet. |
| **Contains personal data** (button) / **Personal data** / **Personal data from** | Columns that are `pii` or `pii_derived` (the count is on the button), one level, or how the level was decided (`rule`, `llm`, `inherited`, `steward`). |
| **Layer**, **Owner** | Who has to review what. |
| **Mapping kind** | `direct`, `derived`, `constant`, `technical`. |
| **Lineage origin** | How well the lineage is backed: SQL only, or confirmed by UC. |
| **Last changed in run** | What the latest agent run changed. |
| **Data type** | Find, for example, every `decimal` or `timestamp` column. |
| **Search** | Free text over column names, definitions, expressions and sources. |

Tables start collapsed. Each heading shows the column count, how many columns need review, and the owner. Click a heading to expand it, or use **Expand all tables**. A table expands on its own when you select it in the map or jump to one of its columns.

Columns that hold personal data have a **PII** badge (filled) or a **PII-derived** badge (dashed) next to their name. The badge's tooltip gives the reason. In the lineage map, tables with such columns carry the same badge, with the counts in the tooltip. The level is always written out, so colour is never the only signal.

Clicking a row shows:
- **Personal data**: the level, how it was decided and why. Stewards can **Override** it, with an optional note, or **Remove override**. An override shows as **Pending** until the next agent run applies it, and the masthead counts it with the other pending decisions;
- the transformation and table rule, with SQL highlighting, and the columns the rule reads;
- the lineage path, one rung per hop back to the landed files;
- **Used by**: the consumers that read this column's table or any table built from this column, with type, owner, link and last read;
- the issues, and the suggested definition with a copy button;
- the run history.

From there you can jump to another column or show everything that depends on this one. Definitions drafted by the LLM have a dashed badge, meaning nobody has confirmed them yet. **Download CSV** saves the rows on screen. The page follows the system light/dark setting. It loads Instrument Sans and IBM Plex Mono from Google Fonts and falls back to system fonts offline.

**Run it locally** with your own CLI profile (from `sttm-agent/.env`); the warehouse comes from `sttm_state.json`:

```bash
uv run python sttm-agent/app/app.py       # http://localhost:8001
```

**As a Databricks App**:

```bash
uv run python sttm-agent/run.py app-deploy        # creates the app once, then redeploys (safe to re-run)
```

The command:
1. Creates or updates the job `sttm-agent` (`STTM_JOB_NAME`).
   - It uploads the agent code and `glossary.toml` to `/Workspace/Users/<you>/sttm-agent/agent`.
   - The job runs `run.py agent` on serverless compute (environment 3, with `sqlglot`) as you, the deployer, because the agent needs your rights to write comments and tags, read system tables and call the LLM.
   - It only refreshes the `sttm_export` table, not the local CSV/Markdown files.
2. Creates the app `sttm-viewer` (`STTM_APP_NAME`) with two resources:
   - the warehouse (`sql-warehouse`, *Can use*);
   - the agent job (`agent-job`, *Can manage run*).

   [`app.yaml`](app/app.yaml) passes them to the app as `DATABRICKS_WAREHOUSE_ID` and `STTM_JOB_ID`.
3. Grants the app's service principal read access to nine tables only: `USE CATALOG` on the catalog, `USE SCHEMA` on `sttm_meta`, and `SELECT` on `sttm_export`, `sttm` (for the History tab, see [history](#history-and-download-for-audit)), `agent_runs`, `glossary`, `sensitivity_overrides`, `owners`, `issues`, `issue_status` and `consumers`. It grants the stewards group (`STTM_STEWARDS_GROUP`, default `sttm-stewards`) `SELECT` and `MODIFY` on `glossary`, `sensitivity_overrides` and `issue_status`, grants every member of an owner team `SELECT` and `MODIFY` on `issue_status`, and turns on user authorization for the app with the `sql` scope.
4. Creates or updates one SQL alert per owner team in `/Workspace/Users/<you>/sttm-agent/alerts`, and adds one alert task per team to the agent job (see [notifications](#owners-worklist-and-notifications)).
5. Uploads `app/` to `/Workspace/Users/<you>/sttm-agent/app` and deploys it. It prints the app URL. Viewers sign in with their Databricks account and need *Can use* on the app.

If the app was stopped, the command starts it again. `run.py destroy` deletes the app and the job and moves the alerts and the demo dashboard to the trash. Re-run `app-deploy` after editing `glossary.toml` or the agent code: the job runs the uploaded copy.

**Run agent button.** It starts the agent job; only one run at a time.
- While it runs, the page shows its progress. When it finishes, the page reloads the mapping and says what changed, from `agent_runs`, with a link to the run.
- The signed-in user's email (the `X-Forwarded-Email` header) is passed as the job parameter `requested_by` and stored in `agent_runs.details`. So each run records who asked for it, even though the job itself runs as the deployer.
- **Who can press it:** only members of the stewards group. The app checks `is_account_group_member()` as the signed-in user. The run itself writes comments and tags with the deployer's rights.

**Steward decisions.** Each column's detail has a *Steward decision* box with these actions:

| Action | What it stores in `sttm_meta.glossary` | What the next agent run does |
|---|---|---|
| **Approve** | The current definition, plus the column's `lineage_hash` | Source becomes `glossary`. The LLM still checks it against the SQL. |
| **Edit** / **Use suggestion** | Your text (max 1,000 characters), plus the `lineage_hash` | It becomes the column comment in Unity Catalog. |
| **Still correct** | Only the `lineage_hash` you confirmed | Closes the `REVIEW` flag. It comes back when something upstream changes again. |
| **Remove from glossary** | Deletes the row | The agent drafts a definition again. |

- Decisions are written **as the signed-in user**, with the token that Databricks Apps forwards ([user authorization](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth), `sql` scope). So Unity Catalog checks the user's own `MODIFY` grant, and `updated_by` is `current_user()`, not a value the browser sends. The table has change data feed on, so every decision is kept.
- A decision shows as **Pending** until the next agent run applies it, and the masthead counts the pending decisions.
- The workspace setting that limits app scopes (`allowedAppsUserApiScopes`) is at its default, all scopes, so nothing workspace-wide had to change.
- Measured:
  - "Still correct" on `gold.customer_360.order_count` closed its `REVIEW`.
  - Editing `avg_order_value_eur` to the suggested text resolved its `CONFLICT`, and the new definition was written as the column comment.
  - Approving the LLM draft of `gold.daily_sales.orders` made it `glossary`. The judge then flagged that the draft itself was wrong: it said returned orders are counted. Approval doesn't skip the check.
  - In the deployed app, a confirmation followed by **Run agent** took 33 s and closed the review.

Measured end to end:
1. I set a human comment on `silver.products.category` in Unity Catalog and pressed the button.
2. The run took 56–78 s, including serverless start-up. It processed `products` and its downstream table, updated 1 row, and the row switched to the UC comment.
3. I cleared the comment and pressed the button again. The agent drafted a new definition (2 LLM calls: draft and check) and wrote it back as the column comment.

Free Edition allows up to 3 apps per account, and each app stops 24 hours after it was started or deployed. See [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [app resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/resources), [app authorization](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth) and [app.yaml](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/app-runtime). The app caches query results for 60 s (`STTM_APP_CACHE_SECONDS`); **Refresh** bypasses the cache.

### Owners, worklist and notifications

**Owners are people.** `sttm_meta.owners` maps each owner team named in `glossary.toml` to its members (Databricks user names) and a notification email.
- The agent adds a team the first time it sees it in `glossary.toml`. It never changes an existing row, so you can split a team into its real members by editing the table, for example `UPDATE … SET members = array('a@…', 'b@…'), notify_email = 'team-list@…' WHERE owner = 'Finance team'`.
- For now every team maps to one person: `STTM_DEFAULT_OWNER`, or, if that is empty, the user who runs the agent or the deploy. The SDK looks the user up at run time, so no email address is in the repo.
- After editing the table, re-run `app-deploy`. It grants the new members `MODIFY` on `issue_status` and points each team's alert at the new `notify_email`.

**Issues have an identity.** At the end of each run the agent copies the open issues (every type except `INFO`) into `sttm_meta.issues`, one row per column, issue type and *fingerprint*:

| Issue type | Fingerprint built from | Why |
|---|---|---|
| `CONFLICT`, `FIX IN PIPELINE SQL` | The definition being judged | The LLM may word its reason differently on a re-check. A new definition is a new issue. |
| `REVIEW`, `GAP`, `LINEAGE` | The issue text, normalised | These texts are deterministic. |

- An issue the run reports for the first time gets `opened_run` = this run.
- An issue that disappears gets `closed_run`. If it comes back later, it is opened again.
- `agent_runs.details` records `issues_new`, `issues_closed` and the list of new issues.
- The first run after upgrading opens every current issue once.

**Status per issue.** `sttm_meta.issue_status` has the same key and holds `status` (`open`, `in_progress`, `accepted`, `wont_fix`), `assignee`, `comment`, `updated_by` and `updated_at`. Change data feed is on.
- The app writes it **as the signed-in user**, like the glossary. Unity Catalog checks the user's `MODIFY` grant, and `updated_by` is `current_user()`. The app itself only checks that the user is a steward or a member of the team that owns the table.
- Because the key includes the fingerprint, a status survives any number of agent runs that report the same issue. It resets to `open` when the issue really changes, for example when the definition behind a `CONFLICT` changes.

**My worklist** (the third button next to *All columns* and *Needs review*, or `?view=worklist`) lists the open issues on tables owned by the signed-in user's teams, grouped by table.
- Each issue has a status menu, an assignee (with the team's members as suggestions) and a comment.
- Issues the agent opened after your previous visit are marked **New**. The previous visit is kept per user in the browser's local storage, so it does not follow you to another browser.
- *Show accepted or won't fix* brings back the issues that were set aside. Stewards can switch to every team's issues.
- The column detail shows the same status controls under *What to review*, and the ledger row shows a non-open status next to the issue flags.

**Notifications.** `app-deploy` creates one [Databricks SQL alert](https://docs.databricks.com/aws/en/sql/user/alerts/) per owner team with the [Alerts API v2](https://docs.databricks.com/api/workspace/alertsv2). Each alert:
- runs a query that lists the team's issues opened by the latest agent run, leaving out issues an owner already set to `accepted` or `wont_fix`;
- triggers when that list has more than 0 rows;
- mails the team's `notify_email` (a user subscription, so no notification destination and no workspace setting is needed);
- uses a custom subject and body: `{{QUERY_RESULT_VALUE}}` new issues, a link to the worklist, and `{{QUERY_RESULT_TABLE}}` with the issues ([notification templates](https://docs.databricks.com/aws/en/sql/user/alerts/create)).

The alerts' own schedules are **paused**. Instead, the agent job has one [SQL alert task](https://docs.databricks.com/aws/en/jobs/alert) per team after the agent task. An alert task "evaluates the alert independently from any schedule configured on the alert itself", so mail goes out right after each run and the warehouse is not woken up on a timer. `retrigger_seconds` is 60, so a run that opens issues while the alert is still `TRIGGERED` from the previous run notifies again ("repeat notifications until the alert returns to OK"). Each alert runs as the deployer, who can read the `issues` table.

Measured in this workspace:
1. A wrong comment on `gold.daily_sales.units` ("distinct customers") plus a job run opened 1 `CONFLICT`. The run took 129 s. The `notify_finance_team` task ended `TRIGGERED`, and the other three teams' tasks ended `OK`.
2. The next run cleared that comment and put a wrong one on `revenue_eur`: 1 issue new, 1 closed (141 s). `notify_finance_team` ended `TRIGGERED` again, which is the case `retrigger_seconds` covers.
3. A third run cleared the second comment: 0 new, 1 closed, and all four tasks ended `OK`. The LLM redrafted both definitions, and the column comments went back to agent-written text.
4. In the deployed app, statuses set through the forwarded user token are stored with the user as `updated_by`. The app's service principal has only `SELECT` on `issue_status`.

The mails themselves could not be checked from this machine. The job output shows each alert's state, not whether the mail was delivered.

Limits:
- **Only job runs notify.** A local `run.py agent` records new issues, but nothing evaluates the alerts after it. The next job run does not report them as new either.
- **Free Edition allows 5 concurrent job tasks per account** ([limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)). The 4 alert tasks run in parallel after the agent task. With more teams they would wait for a free slot.
- An alert's result table in the mail shows at most the first 100 rows.

### History and Download for audit

**History** (the fourth button next to *My worklist*, or `?view=history`) lists, newest first:

- **Agent runs:** when, who started it (`details.requested_by`), the tables processed and why, how many columns were new, updated or removed, and the issues opened and closed. *Show what changed* lists each changed column with its definition, rule (transformation), table rule, issues and definition source **before and after**. Runs that changed nothing are hidden behind a link.
- **Steward decisions** (set or edited a definition, confirmed "still correct", removed from the glossary, overrode or stopped overriding the personal data level) and **issue status changes** (status, assignee, comment), each with the person who made it. A run's column changes include `sensitivity` and `sensitivity_source` before and after.

The column detail has the same story for one column under *History of this column*: what the agent changed on it, the issues it opened and closed, and the decisions and statuses, in time order.

Everything is read from [change data feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed) with [`table_changes()`](https://docs.databricks.com/aws/en/sql/language-manual/functions/table_changes):

| What | Read from | Who |
|---|---|---|
| Column changes of a run | `table_changes('<cat>.sttm_meta.sttm', …)`, `update_preimage` / `update_postimage` pairs, for the commit versions in (`details.sttm_version_before`, `sttm_version`]. Runs logged before this was recorded use the previous run's `sttm_version`, which is the same thing because only the agent writes `sttm`. | `agent_runs.details.requested_by` |
| Issues opened and closed | `issues` CDF (`is_open` false → true and back) | the agent |
| Steward decisions | `glossary` and `sensitivity_overrides` CDF | `updated_by` (`current_user()` when the row was written). A removed row keeps no `updated_by`, so the app takes the commit's `userName` from [`DESCRIBE HISTORY`](https://docs.databricks.com/aws/en/sql/language-manual/delta-describe-history). |
| Issue statuses | `issue_status` CDF | `updated_by` |

`table_changes` needs only `SELECT` on the table, so the app's service principal stays read-only: `app-deploy` adds `SELECT` on `sttm` to its grants, and it already read the other three tables. `DESCRIBE HISTORY` also worked as the service principal.

**Retention.** Change data is only readable while the table keeps its files and log. On Unity Catalog managed tables, [predictive optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) runs `VACUUM` automatically, which deletes change files older than `delta.deletedFileRetentionDuration` (default 7 days), and table history is kept for 30 days by default. The agent therefore sets `delta.deletedFileRetentionDuration` and `delta.logRetentionDuration` to `STTM_HISTORY_RETENTION_DAYS` (default 30) on `sttm`, `glossary`, `sensitivity_overrides`, `issues` and `issue_status`, and only on those (`ensure_meta`, so `setup`, `agent` and `app-deploy` apply it; safe to re-run). If the oldest versions are gone anyway, the history starts at the oldest readable version and the page says so.

**Download for audit** (masthead) builds an `.xlsx` in the app with [openpyxl](https://openpyxl.readthedocs.io/). It respects the filters on screen (table, layer, issue type, search, *depends on*, …):

| Sheet | Content |
|---|---|
| Cover | When (UTC), who generated it (the signed-in user), the source table, the filters, the current Delta version of `sttm`, `sttm_export`, `glossary`, `sensitivity_overrides`, `issue_status` and `issues`, the latest agent run and the `sttm` version it wrote, and counts: columns, tables, columns to review, open issues by status, definitions by source, columns by personal data level. |
| One per table | The rows of that table, with the same columns as `sttm_export` / the CSV, including `sensitivity`, `sensitivity_source` and `sensitivity_reason`. With **Contains personal data** on, the workbook is an inventory of the personal data columns. |
| Open issues | Open issues on those columns, with status, assignee, comment, who set it and when. |
| Used by | The consumers that read those tables or tables built from those columns: type, name, owner, status, the tables it reads (in scope and all), first and last read, link. The cover counts them. |
| Recent history | The last `STTM_AUDIT_HISTORY_DAYS` (default 30) days, one line per changed field: agent runs, column changes, issues opened and closed, steward decisions and statuses. With filters, only lines for the filtered columns (runs are always listed). |

Text that starts with `=`, `+`, `-` or `@` is stored as text, never as a formula.

Measured in this workspace (19 runs, 190 column changes, 8 decisions, 8 status changes):
- `/api/history` takes 2.8 s locally and 4.6 s in the deployed app on a warm warehouse. Its statements (the change feeds, `agent_runs`, versions; eight since personal data was added) run in parallel. Results are cached for 60 s like the rest; a decision or a status change clears the cache.
- The full workbook is 80 KB (12 sheets, 260 history lines); filtered to gold it is 20–25 KB. It builds in about 2.7 s.
- Verified in History, locally and in the deployed app: the `silver.products.category` cycle (LLM draft → human UC comment "Product category as shown in the web shop menu." → comment cleared, new LLM draft), the phase A decisions (`order_count` edited, `avg_order_value_eur` and `orders` set, `order_date` confirmed) and the phase B statuses. A status set and reset in the deployed app appeared with the signed-in user as `updated_by`, and the next job run (102 s, `requested_by=phase-c-test`) recorded `sttm_version_before`.
- The UI was tested with jsdom against both the local and the deployed app (History tab, run details, column timeline, filtered download).

Limits:
- **Not a permanent audit store.** The history covers the retention window (30 days by default). Keeping it for years needs an append-only audit table that the agent fills from the change feed on each run.
- A `full` agent run rewrites every row; History shows those rows as "only the lineage fingerprint changed" when nothing visible changed.
- Who *started* a run is recorded only for runs started from the app (`requested_by`). Runs from the command line show "not recorded".

## Downstream consumers (impact beyond gold)

The STTM says how each column is built. Phase E adds **who uses it after gold**: the dashboards, jobs, notebooks, Genie spaces, apps and ad hoc queries that read the medallion tables. A change upstream, for example to `silver.orders.is_cancelled`, now names the dashboards it reaches and who owns them.

**Where it comes from.** Unity Catalog records every read it can attribute in [`system.access.table_lineage`](https://docs.databricks.com/aws/en/admin/system-tables/lineage). A read has a source table and no target ("Read-only: the source type is not null, but target type is null"). Each agent run, before it builds the STTM, pulls the new read events of the bronze, silver and gold tables (`sttm/consumers.py`):

| Step | What it does |
|---|---|
| Window | `event_time > watermark − 2 hours` (watermark `consumers_watermark` in `agent_state`), and only this workspace's events (`workspace_id`). The lineage tables cover every workspace attached to the metastore ("Lineage is aggregated across all workspaces attached to a Unity Catalog metastore", [data lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage)); a dashboard in another workspace could not be looked up from here. |
| Who read it | From `entity_metadata` first, because Genie spaces, apps and alerts arrive with a NULL `entity_type` and only an id there: `dashboard_id` → Dashboard, `legacy_dashboard_id`, `genie_space_id`, `alert_id`, `app_id`, `sql_query_id`, `job_info.job_id` (a notebook run by a job counts as the job), `notebook_id`, `dlt_pipeline_info`. Then `entity_type` / `entity_id` (`DASHBOARD_V3`, `DBSQL_DASHBOARD`, `DBSQL_QUERY`, `JOB`, `NOTEBOOK`, `PIPELINE`). |
| Left out | Hops inside the medallion (target in a medallion schema; those are in `uc_lineage`) and every pipeline that writes the medallion in the same window: the producer's refreshes also show up as reads of gold. |
| Tables built from ours | A target outside the medallion (a CTAS or `INSERT … SELECT` from gold into another schema) becomes a **Table** consumer, owner from the Tables API. |
| Ad hoc queries | Events with no entity at all: "no Databricks entity was involved in the event", for example JDBC, the SQL editor or the SDK. |
| Names, owners, links | Looked up through the workspace APIs on every run for the known consumers: Lakeview (dashboard name, owner = the user with a direct *Can manage*, since dashboards have no *Is owner*), Jobs, Pipelines, Genie, Queries, Alerts v2, Apps and Tables. A consumer the API no longer finds keeps its last name with status `deleted`, and the app hides it. |

**Ad hoc queries: grouped, not left out.** All reads without an entity become one node per table, *Ad hoc queries* under *SQL warehouse*. They are a real dependency: someone queries `silver.customers` directly, and a change there affects them, so leaving them out would understate the impact. But they have no name, owner or link to show, so one node per table is enough. Who ran them (`created_by`) is **not stored**: the table says that a table is read ad hoc and when, not by whom.

**`sttm_meta.consumers`**, one row per table and consumer:

| Column | Content |
|---|---|
| `source_table` | `<layer>.<table>` that is read |
| `consumer_key`, `consumer_type`, `consumer_id` | `DASHBOARD:<id>`, `JOB:<id>`, `TABLE:<full name>`, … or `ADHOC` |
| `name`, `owner`, `url`, `status` | From the APIs; `status` is `active`, `deleted` or `unknown` (no access or no API) |
| `first_seen`, `last_seen` | First and latest read event |
| `resolved_at` | When a lookup last changed the name, owner, link or status |

The app's service principal gets `SELECT` on it (`app-deploy`), like the other tables it reads. The STTM rows and `sttm_export` are unchanged: consumers hang off tables, and the app joins them to columns.

**In the app.**
- **Map:** a fifth lane, *Used by*, with dashed edges from each table to the consumers that read it. Selecting a table or a column lights the consumers downstream of it; selecting a consumer selects the tables it reads.
- **What depends on…:** a *Used by* box above the results lists every consumer that reads the column's table or any table built from it, with type, link, owner and last read.
- **Column detail:** the same list for that column.
- **Download for audit:** a *Used by* sheet, and the count on the cover.

The app knows which tables are built from a column from `sttm_export.upstream_columns` (through sources and table rules), so the list follows the same impact rules as the rest of the app. The lineage is **table-level**: a dashboard that reads `gold.daily_sales` is listed for every column of it, even one it does not show. The column detail says so.

**Demo consumer.** `run.py demo-dashboard` creates the AI/BI dashboard **STTM demo - Sales** (`STTM_DEMO_DASHBOARD_NAME`) in `/Workspace/Users/<you>/sttm-agent/dashboards`:
- three counters and two charts over `gold.daily_sales` and `gold.customer_360`, with bare table names and the catalog and gold schema passed as dataset defaults, so it follows `STTM_CATALOG` and `STTM_PREFIX`;
- created once, then updated in place by the id in `sttm_state.json` (or found by name in that folder), and published with embedded credentials;
- then refreshed once by a one-time job run with a [dashboard task](https://docs.databricks.com/aws/en/jobs/tasks/dashboard) ("refresh results on a published Databricks dashboard") (`runs submit`, no job is kept), so its queries run and Unity Catalog records the reads without anyone opening it in a browser.

`run.py destroy` moves it to the trash.

Measured in this workspace (26 November 2026):
1. `run.py demo-dashboard` created and published the dashboard, and the refresh run succeeded. A re-run updated it in place (same id) and refreshed it again.
2. The refresh's reads were recorded as `entity_type = DASHBOARD_V3` with the dashboard id, on both gold tables. `entity_metadata` also carried the `job_info` of the one-time run, so the order above matters: the dashboard wins over the job. Warehouse reads of gold without any entity a minute earlier (the dataset checks while the dashboard was created, or test queries: lineage cannot tell them apart) became *Ad hoc queries*.
3. The next agent run (56 s, nothing else to do) merged 4 new edges and made 2 lookups. The pass after it read the same events again in the overlap and wrote nothing. The agent job (103 s, `requested_by=phase-e-test`) ran the same pass with nothing new.
4. Locally and in the deployed app (jsdom against both): the *Used by* lane shows **Dashboard: STTM demo - Sales** and *Ad hoc queries*, with 7 dashed read edges. **What depends on `silver.orders.is_cancelled`** lists the dashboard with its owner and link, and lights it on the map. The column detail of `gold.daily_sales.revenue_eur` lists it. The filtered workbook has the *Used by* sheet with 2 consumers.

Limits:
- **Lag.** The docs give no latency for the lineage system tables. Measured here: the dashboard's reads at 18:25:43 UTC appeared between 18:42 and 18:45, about **17–20 minutes**. Earlier that day the whole table was briefly more than an hour behind (newest event 76 minutes old), so allow for more. Until then a new consumer is missing, and the run after the lag picks it up.
- **Only what Unity Catalog captures.** "Both lineage tables represent a subset of all read/write events, as it is not always possible to capture lineage" ([lineage system tables](https://docs.databricks.com/aws/en/admin/system-tables/lineage)). Reads from outside Databricks that do not go through a warehouse, and tools that copy data out first, are not seen.
- **Retention.** The lineage system tables keep 395 days. `sttm_meta.consumers` keeps what it has seen, so an old consumer stays listed with its last read date.
- **Notebooks have no name lookup by id.** They are shown as *Notebook &lt;id&gt;* with a link; the owner is the user with a direct *Can manage*, if any.
- **A consumer of a consumer** (a dashboard over a table built from gold) is not followed: only direct readers of the medallion are listed.

## Feasibility on Free Edition (checked against the workspace)

| Capability | Result in this workspace | Docs |
|---|---|---|
| UC lineage system tables | `system.access.table_lineage` and `column_lineage` exist and are populated. Reads are recorded too: pipeline refreshes (`PIPELINE`), AI/BI dashboards (`DASHBOARD_V3`), Genie spaces and apps (NULL `entity_type`, id in `entity_metadata`), and plain warehouse queries (no entity); see [downstream consumers](#downstream-consumers-impact-beyond-gold). **Table-level** lineage for every pipeline hop appears about **15–20 min** after the update. **Column-level** events for this pipeline arrive **without source columns** (65/65 rows with a NULL source). A trivial probe pipeline (`SELECT id, name FROM STREAM t`) and a warehouse CTAS did get column edges. So the SQL parser is the primary column-lineage source, and UC lineage confirms it at table level (`lineage_origin = "sql, uc table-confirmed"`) and is merged per column when present. | [Data lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage), [Lineage system tables](https://docs.databricks.com/aws/en/admin/system-tables/lineage) |
| Lakeflow Declarative Pipelines | Serverless pipelines work: streaming tables, materialized views, AUTO CDC, expectations, multi-schema publishing. Limit: one active pipeline per type. | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc), [Incremental refresh](https://docs.databricks.com/aws/en/optimizations/incremental-refresh), [read_files](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) |
| Jobs / SQL warehouse | Jobs work (5 concurrent tasks max). One 2X-Small serverless SQL warehouse. | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) |
| LLM | Foundation Model API pay-per-token endpoints are ready: Llama 3.3 70B, Llama 4 Maverick, GPT-OSS 20B/120B, Qwen3-Next 80B, Qwen 3.5 122B, Gemma 3. `ai_query()` works from the warehouse. The agent calls the endpoint through the SDK. Qwen3-Next 80B judged conflicts correctly and consistently at about 2 s per call. Llama 3.3 70B and Llama 4 Maverick flip-flopped on the same check. Fallback without an LLM: `STTM_LLM_ENDPOINT=none` (rule-based templates and keyword checks). | [ai_query](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query), [Supported models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) |
| Comments and tags | `COMMENT ON COLUMN` and `ALTER TABLE … ALTER COLUMN … SET TAGS` work on pipeline streaming tables and MVs, and read back through `information_schema.columns` and `column_tags`. **A pipeline refresh restores comments declared in the pipeline SQL**; external comments on other columns and all tags survive. Limits: 50 tags per column, 1,000 column tags per table, 256-character keys and values. | [COMMENT ON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment), [Tags](https://docs.databricks.com/aws/en/database-objects/tags), [COLUMNS](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/columns), [COLUMN_TAGS](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/column_tags), [CREATE MATERIALIZED VIEW](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view) |
| Change data feed | Works on managed tables and is on for pipeline tables. | [Change data feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed) |
| Groups for stewards | The workspace SCIM API creates only workspace-local groups. Unity Catalog grants need account groups, so `sttm-stewards` was created through the account SCIM endpoint (`/api/2.0/account/scim/v2/Groups`), which works from the workspace on Free Edition. | [Manage groups](https://docs.databricks.com/aws/en/admin/users-groups/groups) |
| Removing a column comment | On a pipeline materialized view, `COMMENT ON COLUMN … IS NULL` succeeds but leaves the comment in place. `IS ''` clears it. | [COMMENT ON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment) |

### What a paid workspace adds

- **Compute and scale.** Classic compute, bigger and multiple warehouses, more than one active pipeline per type, and no daily quota shutdown. Free Edition stops compute for the day when the quota is exceeded.
- **Models.** Provisioned-throughput and custom model serving, and the full model catalogue. Free Edition excludes some models, including the hosted Claude models listed in the docs.
- **Account-level features.** Account console and APIs, SSO/SCIM, and multiple workspaces and metastores. These matter if lineage and metadata must span workspaces.
- **Governance.** Governed tags with tag policies (fixed values, `ASSIGN` permission), ABAC column masks by tag, and Databricks data classification. They would replace or back the free-form `sttm_sensitivity` tag; see [write-back](#write-back).
- **Not tested here: column lineage.** Whether paid tiers capture pipeline column lineage with sources (instead of target-only events) was not tested. The docs describe column lineage for pipelines on DBR 13.3+ without a tier caveat.

## Limitations and next steps

- **LLM drafts are drafts.** Each draft is verified by a second judge call and fixed when it disagrees, but a few still over-generalise, for example "includes all customers…". They are tagged `sttm_definition_source=llm`. Approving it in the viewer app makes it governed, and the next run rewrites only that column.
- **Personal data by name only.** The agent never looks at values, so a column with a misleading name (`notes` holding emails) is only caught if the LLM or a steward flags it. `regexp_replace` counts as a copy, because it may clean a value as well as mask it. Free-text columns and quasi-identifiers (country, dates, age) are `none` unless a steward says otherwise. Data classification on a paid workspace would cover the value side.
- **Parser coverage.** It covers the SQL constructs used here: CTEs, joins, `QUALIFY`, aggregate `FILTER`, AUTO CDC and `read_files`. Python pipelines and UDFs are not parsed; there, UC column lineage would be the only source.
- **Table comments.** These are not yet checked; for example, the `daily_sales` table comment still says "excluding cancelled and returned".
- **History is bounded by retention.** See [History and Download for audit](#history-and-download-for-audit).
- **Last visit is per browser.** The worklist's **New** marker compares with a timestamp in local storage, not with a server-side record of the user's visits.
- **Scheduling.** The agent runs locally or as the `sttm-agent` job (the app's Run agent button). It is not yet triggered automatically after each pipeline update; a job with a pipeline task followed by the agent task would do that.
