<img src="assets/icon.svg" width="64" height="64" alt="">

# Databricks dashboard embedding POC

A small external web app (FastAPI + plain HTML) that embeds a Databricks AI/BI dashboard and passes
parameters in both directions: app → dashboard, dashboard → app, and both → Databricks (Delta tables).

Two embedding options:

| | Option 1: basic embedding (`/`) | Option 2: external users (`/external`) |
|---|---|---|
| Viewer needs a Databricks account | yes, logged in, same browser, 3rd-party cookies allowed | **no** |
| Auth | viewer's Databricks session | your backend mints a 1-hour token per viewer with a service principal |
| App → dashboard | filter values in the iframe URL (`f_<page>~<widget>=…`) | `setFilters()` from `@databricks/aibi-client`, plus a **locked** value in the token (`__aibi_external_value`) |
| Dashboard → app | not possible (iframe can't send events); API re-runs the dashboard query | `onFiltersChanged()` → API |

> **Also in this repo:** [`sttm-agent/`](sttm-agent/README.md), a separate POC with an STTM (source-to-target
> mapping) agent on a synthetic bronze/silver/gold medallion. It has its own schemas, pipeline and README.

## Requirements

- Python 3.12+ and [uv](https://docs.astral.sh/uv/)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (only for logging in)
- A Databricks workspace with Unity Catalog, a SQL warehouse, and the `samples` catalog
- Rights to create a schema, a dashboard and a service principal, and to change the dashboard
  embedding settings (workspace admin), or ask an admin to do steps 3 and 5 of `setup.py`

## Run it against any workspace

```bash
git clone https://github.com/fedechanes/poc-dashboard.git && cd poc-dashboard

# 1. log in to the workspace (creates a profile in ~/.databrickscfg)
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile poc

# 2. configure
cp .env.example .env          # set DATABRICKS_CONFIG_PROFILE=poc (others are optional)

# 3. create everything in Databricks (safe to re-run)
uv run python setup.py

# 4. start the app
uv run uvicorn poc.api:app --port 8000
```

Open `http://localhost:8000/external` (option 2) or `http://localhost:8000/` (option 1).
Use `localhost`: only domains in `POC_EMBED_DOMAINS` may embed the dashboards. If the app runs on a
remote machine, tunnel the port (`ssh -N -L 127.0.0.1:8000:localhost:8000 <host>`) or add your real
domain to `POC_EMBED_DOMAINS` and re-run `setup.py`.

### What `setup.py` creates

| Step | Databricks object |
|---|---|
| 1 | picks a SQL warehouse (`DATABRICKS_WAREHOUSE_ID` or auto-detect, serverless first) |
| 2 | schema `POC_CATALOG.POC_SCHEMA` with tables `app_params` and `app_results` |
| 3 | service principal `poc-dashboard-embed` + 90-day OAuth secret, saved to `.env` |
| 4 | dashboards "POC - Embedded parameters" and "POC - External embed", published with embedded credentials; the SP gets CAN RUN on the second |
| 5 | adds `POC_EMBED_DOMAINS` to the workspace's approved embedding domains (merged, never replaced) |

It writes the ids to `poc_state.json` (gitignored). The app refuses to start if that file belongs to a
different workspace than the one you're logged into; re-run `setup.py` after switching workspaces.

## How it works

```
 Browser                         FastAPI (poc/api.py)                       Databricks
 user logs into YOUR app ─────▶ /api/external/token
                                 SP client_credentials ──────────────────▶ /oidc/v1/token
                                 tokeninfo(viewer id, external_value=ZIP) ▶ /lakeview/.../published/tokeninfo
                                 scoped token ◀────────────────────────── /oidc/v1/token
 DatabricksDashboard(token) ───────────────────────────────────────────▶ "POC - External embed"
   setFilters()       app → dashboard   (min fare)                         WHERE pickup_zip = __aibi_external_value
   onFiltersChanged() dashboard → app ─▶ /api/external/event ─▶ INSERT app_params / run query / INSERT app_results
```

- **Locked parameter** (pickup ZIP): chosen by the server per user and signed into the token as
  `external_value`. The viewer can't change it. Use this for tenant/customer/region scoping.
- **Interactive parameter** (min fare): a normal dashboard filter, synced both ways with the host page.
- Users are simulated (`USERS` in `poc/api.py`). Replace that with your real authentication.
- The app's own SQL (history tables) runs with your login from step 1; in production use a service account.

| File | Purpose |
|---|---|
| `setup.py` | creates/updates all Databricks objects |
| `poc/common.py` | config (env vars), SQL helper, the shared dashboard query |
| `poc/embed_token.py` | option 2 token exchange |
| `poc/api.py` | API + serves the pages |
| `static/index.html`, `static/external.html` | option 1 and option 2 front ends |
| `docs/embed-flows.html` | diagrams: token/user flow and parameter flows (open in a browser) |

## Cleaning up a workspace

Delete the two dashboards, the `poc-dashboard-embed` service principal, the schema
(`DROP SCHEMA <catalog>.poc_dashboard CASCADE`), and the domains you added under
Settings → Security → Embed dashboards.
