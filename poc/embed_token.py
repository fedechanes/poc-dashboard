"""Option 2 - embedding for external users: mint a per-viewer, dashboard-scoped token.

Flow (server side only, the SP secret never reaches the browser):
  1. client_credentials (scope=all-apis) with the service principal  -> broad SP token
  2. GET /api/2.0/lakeview/dashboards/{id}/published/tokeninfo       -> authorization_details + claims
       external_viewer_id : opaque per-user id for audit logs (no PII)
       external_value     : value exposed to dataset SQL as __aibi_external_value (<= 1 KB with the id)
  3. client_credentials again with those details                     -> narrow token, safe for the browser
"""

from __future__ import annotations

import json
import os

import httpx


def mint_viewer_token(host: str, workspace_id: int | str, dashboard_id: str,
                      external_viewer_id: str, external_value: str) -> dict:
    client_id = os.environ["DATABRICKS_SP_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_SP_CLIENT_SECRET"]
    token_url = f"{host}/oidc/v1/token"
    ws = {"o": str(workspace_id)}

    with httpx.Client(timeout=30) as http:
        # 1) broad service-principal token
        r = http.post(token_url, params=ws, auth=(client_id, client_secret),
                      data={"grant_type": "client_credentials", "scope": "all-apis"})
        r.raise_for_status()
        sp_token = r.json()["access_token"]

        # 2) token info for this viewer + dashboard
        r = http.get(f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published/tokeninfo",
                     params={**ws, "external_viewer_id": external_viewer_id, "external_value": external_value},
                     headers={"Authorization": f"Bearer {sp_token}"})
        r.raise_for_status()
        info = r.json()

        # 3) scoped token: every tokeninfo field is passed through, authorization_details as JSON
        form = {k: v for k, v in info.items() if k != "authorization_details"}
        form["grant_type"] = "client_credentials"
        form["authorization_details"] = json.dumps(info["authorization_details"])
        r = http.post(token_url, params=ws, auth=(client_id, client_secret), data=form)
        r.raise_for_status()
        tok = r.json()

    return {"token": tok["access_token"], "expires_in": tok.get("expires_in")}
