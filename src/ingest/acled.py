"""
ingest/acled.py — pull ACLED events for Ukraine into the bronze layer.

ACLED requires a free myACLED account. Programmatic access uses OAuth:
POST credentials to the token endpoint, then send the bearer token on reads.
Tokens last 24h. Attribution is mandatory — cite ACLED in your README.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from storage import write_bronze  # noqa: E402


def get_token() -> str:
    config.require("ACLED_EMAIL", "ACLED_PASSWORD")
    resp = requests.post(
        config.SOURCES["acled"]["oauth_url"],
        data={
            "username": config.ACLED_EMAIL,
            "password": config.ACLED_PASSWORD,
            "grant_type": "password",
            "client_id": "acled",
            "scope": "authenticated",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_events(token: str) -> pd.DataFrame:
    cfg = config.SOURCES["acled"]
    countries = "|".join(cfg["countries"])   # OR filter across countries
    headers = {"Authorization": f"Bearer {token}"}
    rows: list[dict] = []
    page = 1
    while True:
        params = {
            "country": countries,
            "country_where": "=",
            "event_date": cfg["start_date"],
            "event_date_where": ">=",
            "limit": cfg["page_limit"],
            "page": page,
        }
        r = requests.get(cfg["base_url"], headers=headers, params=params, timeout=60)
        r.raise_for_status()
        batch = r.json().get("data", [])
        if not batch:
            break
        rows.extend(batch)
        print(f"[acled] page {page}: {len(batch)} rows (total {len(rows)})")
        if len(batch) < cfg["page_limit"]:
            break
        page += 1
    return pd.DataFrame(rows)


def run() -> None:
    token = get_token()
    df = fetch_events(token)
    write_bronze(df, table="acled_events", source="acled")


if __name__ == "__main__":
    run()
