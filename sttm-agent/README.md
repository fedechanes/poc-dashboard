# STTM agent on a synthetic medallion (Databricks Free Edition)

A small, reproducible proof of concept. It covers four things:

- **Medallion.** A synthetic online-retail medallion (bronze → silver → gold), built as one serverless Lakeflow Declarative Pipeline.
- **STTM.** A source-to-target mapping with one row per target column: source columns, transformation, type, business definition, owner, layer and lineage path back to bronze. It is kept as a Delta table and exported to [`output/sttm.csv`](output/sttm.csv) and [`output/sttm.md`](output/sttm.md).
- **Agent.** It reads the deployed pipeline SQL plus Unity Catalog (UC) metadata and lineage. It drafts missing definitions with a Foundation Model API LLM and writes them back as **column comments and tags**. It also flags gaps and conflicts.
- **Incremental updates.** Both the data and the metadata are refreshed incrementally. The [answer](#incremental-updates-the-answer) comes with evidence from a demo run.

It lives next to the dashboard-embedding POC in this repo but shares nothing with it except `pyproject.toml`. It has its own folder, its own schemas (`<catalog>.sttm_*`) and its own pipeline.

## Run it

Requirements: Python 3.12+, [uv](https://docs.astral.sh/uv/), and a Databricks CLI profile. The workspace needs Unity
Catalog, serverless, and a writable catalog (default `workspace`).

```bash
cp sttm-agent/.env.example sttm-agent/.env        # set DATABRICKS_CONFIG_PROFILE=<your profile>
uv run python sttm-agent/run.py setup             # schemas, volume, batch 1, pipeline, first agent run (~3-5 min)
uv run python sttm-agent/run.py demo              # incremental demo (see below)
uv run python sttm-agent/run.py agent             # incremental agent run; --full to reprocess, --dry-run to preview
uv run python sttm-agent/run.py destroy           # delete the pipeline and drop the sttm_* schemas
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
sttm_meta     sttm, table_fingerprints, uc_lineage, agent_state, agent_runs, pipeline_event_log
```

| Path | Role |
|---|---|
| [`pipeline/*.sql`](pipeline) | The pipeline source (bronze/silver/gold). `${bronze}` etc. are pipeline configuration values. |
| [`glossary.toml`](glossary.toml) | Curated definitions and owners: the governed input that wins over drafts. |
| [`sttm/datagen.py`](sttm/datagen.py) | Deterministic synthetic batches: duplicates, mixed case, string types, status-change events. |
| [`sttm/medallion.py`](sttm/medallion.py) | Schemas, volume, file landing, pipeline deploy/run, and event-log metrics. |
| [`sttm/sqlparse.py`](sttm/sqlparse.py) | Lakeflow SQL → per-column expression and sources (sqlglot), plus table rules (joins, filters, dedup, grain, AUTO CDC). |
| [`sttm/agent.py`](sttm/agent.py) | The agent: change detection, definitions, checks, MERGE, write-back, export. |
| [`run.py`](run.py) | The CLI above, including the demo. |

## The STTM table

`<cat>.sttm_meta.sttm` has one row per target column. Change data feed is enabled on it.

| Column | Content |
|---|---|
| `target_table`, `target_column`, `layer`, `data_type` | From UC `information_schema.columns`. |
| `source_columns` | Parsed from the pipeline SQL with sqlglot, through CTEs, aliases and `SELECT *`, unioned with UC column lineage when UC has it. Bronze columns point at `landing/<source>.<field>`. |
| `transformation`, `mapping_kind` | The column expression, with intermediate CTE steps (`… WHERE net_amount := …`). Kind is `direct`, `derived`, `constant` or `technical`. |
| `table_rule` | Rules that change every column of the table: joins, `WHERE` filters, `QUALIFY` dedup, `GROUP BY` grain, AUTO CDC keys and sequencing. |
| `business_definition`, `definition_source` | See [definitions](#how-definitions-are-chosen). |
| `owner` | From `glossary.toml` (`[owners]`, by layer or by table). |
| `lineage_path`, `bronze_sources` | Recursive walk back to bronze and the landed files, for example `gold.customer_360.lifetime_value_eur <- silver.orders.{is_cancelled, net_amount_eur} <- bronze.orders_raw.{currency, discount_amt, qty, status, unit_price} <- landing/orders.{…}`. |
| `lineage_origin` | `sql`, `sql, uc table-confirmed` (UC table lineage confirms every source table), or `sql+uc column` (UC column lineage agrees). |
| `issues` | `GAP` (no source column), `CONFLICT` (definition contradicts the SQL), `REVIEW` (an upstream rule changed under a curated definition), `FIX IN PIPELINE SQL`, `LINEAGE` (UC and SQL disagree), `INFO` (technical columns). |
| `proposed_definition` | The LLM's correction when it finds a conflict. |
| `rule_hash`, `lineage_hash`, `row_hash` | Change detection; see [incremental updates](#incremental-updates-the-answer). |
| `is_active`, `first_seen_run`, `last_changed_run`, `updated_at` | Soft delete and audit. |

### How definitions are chosen

Highest priority first:

1. **`glossary.toml`**: curated and governed. The agent writes it back as the column comment.
2. **A comment declared in the pipeline SQL** (column list of `CREATE MATERIALIZED VIEW`). The agent does **not** overwrite it: a pipeline refresh re-applies declared comments, which I tested. A conflict there is flagged `FIX IN PIPELINE SQL`.
3. **A human UC comment**, meaning any comment the agent did not write. The agent recognises its own comments by the `sttm_comment_hash` tag. Human comments are kept, never overwritten; conflicts are flagged with a proposed fix.
4. **The previous draft**, reused when the column's `lineage_hash` has not changed. No LLM call.
5. **A new LLM draft** (default `databricks-qwen3-next-80b-a3b-instruct`). One call per table, JSON in and out. With `STTM_LLM_ENDPOINT=none` it falls back to rule-based templates.

Definitions from sources 1–3 are **checked** against the transformation by the same LLM, acting as a judge with the upstream expressions resolved. The check runs only when the definition or anything upstream changed.

### Write-back

The agent writes only what differs from UC:

- **Comments:** `COMMENT ON COLUMN` for glossary, LLM and rule definitions.
- **Tags:** `ALTER TABLE … ALTER COLUMN … SET TAGS` for `sttm_layer`, `sttm_owner`, `sttm_definition_source`, `sttm_rule_hash` and `sttm_comment_hash`.

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

The run log is also in `sttm_meta.agent_runs`: one row per run, with counts and the reasons each table was processed.

### Gaps and conflicts it finds (current [`output/sttm.md`](output/sttm.md))

- **`gold.customer_360.avg_order_value_eur`: CONFLICT.** A stale human comment ("Total revenue … including cancelled orders", seeded by `setup`) contradicts `ROUND(lifetime / order_count)` over non-cancelled orders. The agent proposes a correction and keeps the human comment.
- **`gold.daily_sales.gross_margin_eur`: CONFLICT + FIX IN PIPELINE SQL.** The comment declared in the pipeline ("Revenue before discounts") contradicts `SUM(net_amount_eur - quantity * cost_price_eur)`.
- **`gold.daily_sales.report_currency`: GAP.** No source column (constant `'EUR'`).
- **Bronze `_source_file`, `_ingested_at`, `_rescued_data`: INFO.** Technical columns with no source field.

## Feasibility on Free Edition (checked against the workspace)

| Capability | Result in this workspace | Docs |
|---|---|---|
| UC lineage system tables | `system.access.table_lineage` and `column_lineage` exist and are populated. **Table-level** lineage for every pipeline hop appears about **15–20 min** after the update. **Column-level** events for this pipeline arrive **without source columns** (65/65 rows with a NULL source). A trivial probe pipeline (`SELECT id, name FROM STREAM t`) and a warehouse CTAS did get column edges. So the SQL parser is the primary column-lineage source, and UC lineage confirms it at table level (`lineage_origin = "sql, uc table-confirmed"`) and is merged per column when present. | [Data lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage), [Lineage system tables](https://docs.databricks.com/aws/en/admin/system-tables/lineage) |
| Lakeflow Declarative Pipelines | Serverless pipelines work: streaming tables, materialized views, AUTO CDC, expectations, multi-schema publishing. Limit: one active pipeline per type. | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc), [Incremental refresh](https://docs.databricks.com/aws/en/optimizations/incremental-refresh), [read_files](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) |
| Jobs / SQL warehouse | Jobs work (5 concurrent tasks max). One 2X-Small serverless SQL warehouse. | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) |
| LLM | Foundation Model API pay-per-token endpoints are ready: Llama 3.3 70B, Llama 4 Maverick, GPT-OSS 20B/120B, Qwen3-Next 80B, Qwen 3.5 122B, Gemma 3. `ai_query()` works from the warehouse. The agent calls the endpoint through the SDK. Qwen3-Next 80B judged conflicts correctly and consistently at about 2 s per call. Llama 3.3 70B and Llama 4 Maverick flip-flopped on the same check. Fallback without an LLM: `STTM_LLM_ENDPOINT=none` (rule-based templates and keyword checks). | [ai_query](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query), [Supported models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) |
| Comments and tags | `COMMENT ON COLUMN` and `ALTER TABLE … ALTER COLUMN … SET TAGS` work on pipeline streaming tables and MVs, and read back through `information_schema.columns` and `column_tags`. **A pipeline refresh restores comments declared in the pipeline SQL**; external comments on other columns and all tags survive. Limits: 50 tags per column, 1,000 column tags per table, 256-character keys and values. | [COMMENT ON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment), [Tags](https://docs.databricks.com/aws/en/database-objects/tags), [COLUMNS](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/columns), [COLUMN_TAGS](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/column_tags), [CREATE MATERIALIZED VIEW](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view) |
| Change data feed | Works on managed tables and is on for pipeline tables. | [Change data feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed) |

### What a paid workspace adds

- **Compute and scale.** Classic compute, bigger and multiple warehouses, more than one active pipeline per type, and no daily quota shutdown. Free Edition stops compute for the day when the quota is exceeded.
- **Models.** Provisioned-throughput and custom model serving, and the full model catalogue. Free Edition excludes some models, including the hosted Claude models listed in the docs.
- **Account-level features.** Account console and APIs, SSO/SCIM, and multiple workspaces and metastores. These matter if lineage and metadata must span workspaces.
- **Governance.** On paid tiers, governed tags with tag policies and ABAC work alongside the free-form tags used here.
- **Not tested here: column lineage.** Whether paid tiers capture pipeline column lineage with sources (instead of target-only events) was not tested. The docs describe column lineage for pipelines on DBR 13.3+ without a tier caveat.

## Limitations and next steps

- **LLM drafts are drafts.** Each draft is verified by a second judge call and fixed when it disagrees, but a few still over-generalise, for example "includes all customers…". They are tagged `sttm_definition_source=llm`. Promoting a definition into `glossary.toml` makes it governed, and the next run rewrites only that column.
- **Clearing a `REVIEW` flag.** The flag persists until the curated definition is touched (edit the glossary, comment, or pipeline SQL). There is no "acknowledge" action yet.
- **Parser coverage.** It covers the SQL constructs used here: CTEs, joins, `QUALIFY`, aggregate `FILTER`, AUTO CDC and `read_files`. Python pipelines and UDFs are not parsed; there, UC column lineage would be the only source.
- **Table comments.** These are not yet checked; for example, the `daily_sales` table comment still says "excluding cancelled and returned".
- **Scheduling.** The agent runs locally. It could run as a Databricks job task after the pipeline (a pipeline task followed by a Python task), with the same code.
