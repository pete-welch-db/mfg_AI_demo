"""Call Databricks model serving endpoint for delivery-risk what-if. Uses REST API."""
from __future__ import annotations

from app.config import DATABRICKS_HOST, DATABRICKS_TOKEN, SERVING_ENDPOINT_NAME


def get_auth_headers():
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        return None
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}


def invoke_delivery_risk(inputs: dict) -> float | None:
    """
    Invoke the delivery_risk model endpoint. inputs should include feature keys expected by the model.
    Returns probability of late delivery (0-1) or None on error.
    """
    if not SERVING_ENDPOINT_NAME:
        return None
    headers = get_auth_headers()
    if not headers:
        return None
    url = f"{DATABRICKS_HOST.rstrip('/')}/serving-endpoints/{SERVING_ENDPOINT_NAME}/invocations"
    try:
        import requests
        resp = requests.post(url, json={"inputs": [inputs]}, headers=headers, timeout=30)
        if resp.status_code != 200:
            return None
        out = resp.json()
        predictions = out.get("predictions", out.get("predictions", []))
        if predictions and isinstance(predictions[0], (list, dict)):
            p = predictions[0]
            return float(p.get("probability", p.get(1, 0))) if isinstance(p, dict) else float(p[1] if len(p) > 1 else p[0])
        return float(predictions[0]) if predictions else None
    except Exception:
        return None
