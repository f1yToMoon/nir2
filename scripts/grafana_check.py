"""Manual check of the CAPL dashboard (REVIEWER §4, stage 2): run every panel query through
Grafana's datasource proxy, as the panel does, and record whether it returns series.

    python3 scripts/grafana_check.py <out.json> [git_sha]      # Grafana on localhost:3000
"""
from __future__ import annotations

import json
import sys
import time

import requests

GRAFANA = "http://localhost:3000"
AUTH = ("admin", "admin")


def main(out: str, git_sha: str) -> None:
    found = requests.get(f"{GRAFANA}/api/search", params={"query": "CAPL"}, auth=AUTH, timeout=10).json()
    uid = next(x["uid"] for x in found if x["type"] == "dash-db")
    dash = requests.get(f"{GRAFANA}/api/dashboards/uid/{uid}", auth=AUTH, timeout=10).json()["dashboard"]
    sources = requests.get(f"{GRAFANA}/api/datasources", auth=AUTH, timeout=10).json()
    ds = next(x for x in sources if x["type"] == "prometheus")
    doc = {"dashboard": dash["title"], "git_sha": git_sha,
           "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "panels": []}
    for panel in dash.get("panels", []):
        for target in panel.get("targets", []):
            expr = target.get("expr")
            if not expr:
                continue
            r = requests.get(f"{GRAFANA}/api/datasources/proxy/uid/{ds['uid']}/api/v1/query",
                             params={"query": expr}, auth=AUTH, timeout=10).json()
            series = r.get("data", {}).get("result", [])
            doc["panels"].append({"title": panel.get("title"), "expr": expr, "status": r.get("status"),
                                  "series": len(series), "sample": series[:2]})
    with open(out, "w") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
    for p in doc["panels"]:
        print(p["series"], "|", p["title"])


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "unknown")
