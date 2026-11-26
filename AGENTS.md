# AGENTS.md

Guidance for coding agents (and humans) working on this repo. The README explains *what* the POC is;
this file covers how to work on it without breaking things.

## What this is

A proof of concept: an external web app (FastAPI + two static HTML pages) that embeds a Databricks
AI/BI dashboard and passes parameters both ways.

- **Option 1** (`/`, `static/index.html`): basic iframe embedding. The viewer must be logged into
  Databricks. Parameters go app → dashboard as URL filter values (`f_<page>~<widget>=value`).
- **Option 2** (`/external`, `static/external.html`): embedding for external users. No Databricks
  login. The backend mints a scoped token per viewer with a service principal (`poc/embed_token.py`).
  Locked parameters travel inside the token (`__aibi_external_value` in SQL); interactive ones use
  `@databricks/aibi-client` `setFilters()` / `onFiltersChanged()`.

Diagrams of both flows: `docs/embed-flows.html`.

## Separate POC: `sttm-agent/`

`sttm-agent/` is an independent POC: an STTM agent over a synthetic Lakeflow medallion. It has its own
`README.md`, its own `.env` (`sttm-agent/.env`), its own state file (`sttm_state.json`, gitignored), and its own
schemas (`<catalog>.sttm_*`). The same rules apply: no workspace-specific values in code, and re-runs must be
safe. It shares only `pyproject.toml` with the dashboard POC. Run it with `uv run python sttm-agent/run.py <command>`.
Its viewer app (`sttm-agent/app/`, run with `uv run python sttm-agent/app/app.py`, port 8001) reads `sttm_export`,
`agent_runs`, `glossary`, `owners`, `issues` and `issue_status` as its service principal. Writes to `glossary`
(steward decisions) and `issue_status` (owner statuses) run as the signed-in user (forwarded user token); keep it that
way so Unity Catalog, not the app, decides who may write. Owner emails live only in the `owners` table (looked up with
the SDK at run time): never put them in committed files. The per-team SQL alerts are created by `run.py app-deploy`;
it must not change workspace-wide settings such as notification destinations.

## Layout

| Path | Role |
|---|---|
| `setup.py` | Creates/updates every Databricks object (warehouse pick, schema/tables, service principal, both dashboards, embedding allow-list). Idempotent. |
| `poc/common.py` | Config from env vars/`.env`, `run_sql()` helper, the shared query `TRIPS_SQL` / `EXTERNAL_SQL`, dashboard widget ids. |
| `poc/embed_token.py` | Option 2 token exchange: SP token → `tokeninfo` → scoped token. |
| `poc/api.py` | FastAPI app: serves pages, `/api/params`, `/api/history`, `/api/external/token`, `/api/external/event`. |
| `static/*.html` | Front ends. No build step; `external.html` loads `@databricks/aibi-client@1.2.1` from jsDelivr. |
| `docs/embed-flows.html` | Standalone diagrams page. |

## Setup and commands

Tooling: Python 3.12+, [uv](https://docs.astral.sh/uv/), Databricks CLI (for login only).

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com --profile poc
cp .env.example .env                       # set DATABRICKS_CONFIG_PROFILE=poc
uv run python setup.py                     # create/update Databricks objects, writes poc_state.json
uv run uvicorn poc.api:app --port 8000     # http://localhost:8000/external  or  /
```

There is no test suite. To check a change, smoke-test the API with the server running:

```bash
curl -s -X POST localhost:8000/api/params -H 'content-type: application/json' -d '{"pickup_zip":10003,"min_fare":15}'
curl -s -X POST localhost:8000/api/external/token -H 'content-type: application/json' -d '{"user_id":"bob"}'
curl -s -X POST localhost:8000/api/external/event -H 'content-type: application/json' -d '{"user_id":"bob","min_fare":15}'
curl -s localhost:8000/api/history
```

The embedded dashboards themselves can only be verified in a browser.

## Rules

- **Never commit `.env` or `poc_state.json`.** `.env` holds the service principal secret;
  `poc_state.json` holds workspace-specific ids. Both are gitignored. Keep `.env.example` in sync
  when adding a setting.
- **Nothing workspace-specific in code.** No hosts, workspace ids, warehouse ids, dashboard ids or
  profile names. New settings go through env vars read in `poc/common.py`, with a default when one
  makes sense.
- **`setup.py` must stay idempotent and safe on shared workspaces.** Update existing objects using
  the ids in `poc_state.json`; merge the embedding allow-list, never replace it.
- **Changing the dashboards means editing `setup.py`** (`dashboard_json()`), then re-running it.
  Edits made in the Databricks UI are overwritten on the next run.
- **Keep the app and dashboard queries identical.** The app re-runs `TRIPS_SQL` to return the same
  numbers the dashboard shows; `EXTERNAL_SQL` is derived from it.
- **Use parameterised SQL** (`run_sql(..., params)`), never string formatting with user input.
- **The secret stays server-side.** Only the scoped token from `mint_viewer_token()` may reach the
  browser.

## Databricks gotchas

- Filter ids are `<page name>~<widget name>` (`poc~fare_filter`, `poc~zip_filter`). They're used by
  option 1 URLs and by `setFilters()` in option 2. Renaming `PAGE_NAME` or the widget names in
  `poc/common.py` changes them.
- Embedding only works from domains in the workspace's approved list (`POC_EMBED_DOMAINS`, default
  `localhost,127.0.0.1`). Open the app as `http://localhost:8000`, not by IP. On a remote machine,
  tunnel: `ssh -N -L 127.0.0.1:8000:localhost:8000 <host>`.
- Dashboards are published with embedded credentials, so queries run as the publisher. The service
  principal only needs CAN RUN on the option 2 dashboard, no table grants.
- `external_value` + `external_viewer_id` must stay under 1 KB. `external_viewer_id` goes to audit
  logs: no PII.
- Scoped tokens last about an hour; `external.html` renews them through `getNewToken`.
- `setup.py` needs workspace admin rights (service principal, embedding settings) and Unity Catalog
  with the `samples` catalog. Use `POC_CATALOG` if there is no `workspace` catalog.
- Users in option 2 are simulated (`USERS` in `poc/api.py`).
