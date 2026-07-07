"""
ingest/firms.py — pull NASA FIRMS active-fire detections over Ukraine.

Free MAP_KEY required (register at firms.modaps.eosdis.nasa.gov/api/map_key).
Area endpoint returns CSV:
  {base}/{MAP_KEY}/{SOURCE}/{west,south,east,north}/{day_range}[/{date}]
Limit: 5000 transactions / 10 min. VIIRS thermal signatures are a *proxy*
for events (fires can be agricultural/wildfire) — document that honestly.
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from storage import write_bronze  # noqa: E402


def fetch_sensor(source: str) -> pd.DataFrame:
    cfg = config.SOURCES["firms"]
    west, south, east, north = config.SOURCES["region"]["bbox"]
    area = f"{west},{south},{east},{north}"
    url = f"{cfg['base_url']}/{config.FIRMS_MAP_KEY}/{source}/{area}/{cfg['day_range']}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df["sensor_source"] = source
    print(f"[firms] {source}: {len(df)} detections")
    return df


def run() -> None:
    config.require("FIRMS_MAP_KEY")
    frames = [fetch_sensor(s) for s in config.SOURCES["firms"]["sources"]]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    write_bronze(df, table="firms_detections", source="firms")


if __name__ == "__main__":
    run()
