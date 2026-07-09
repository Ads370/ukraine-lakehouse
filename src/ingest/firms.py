"""
ingest/firms.py: pull NASA FIRMS active-fire detections over Ukraine.
Free MAP_KEY required (firms.modaps.eosdis.nasa.gov/api/map_key).
Area endpoint:
  {base}/{MAP_KEY}/{SOURCE}/{west,south,east,north}/{day_range}[/{start_date}]
    returns detections for [start_date .. start_date + day_range - 1]

Two access modes by dataset age:
  * NRT sources (e.g. VIIRS_SNPP_NRT): only the last ~2 months.
  * SP  sources (e.g. VIIRS_SNPP_SP): Standard Processing archive, older data.
For a 2025 backfill, the SP sources is being used. DAY_RANGE maxes at 5 per call,
so a month is tiled into 5-day chunks.
VIIRS thermal signatures are a proxy for events (fires can be agricultural
or wildfire). This is not a strike detector.
"""
from __future__ import annotations

import io
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from storage import write_bronze  # noqa: E402


def _date_chunks(start_iso: str, total_days: int, chunk: int) -> list[tuple[date, int]]:
    """Split a date span into (start_date, span) pieces of at most `chunk` days."""
    start = date.fromisoformat(start_iso)
    out: list[tuple[date, int]] = []
    d = 0
    while d < total_days:
        out.append((start + timedelta(days=d), min(chunk, total_days - d)))
        d += chunk
    return out


def fetch_sensor_range(source: str, start_iso: str, total_days: int, chunk: int) -> pd.DataFrame:
    cfg = config.SOURCES["firms"]
    west, south, east, north = config.SOURCES["region"]["bbox"]
    area = f"{west},{south},{east},{north}"
    frames = []
    for chunk_start, span in _date_chunks(start_iso, total_days, chunk):
        url = f"{cfg['base_url']}/{config.FIRMS_MAP_KEY}/{source}/{area}/{span}/{chunk_start.isoformat()}"
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        text = r.text
        # FIRMS returns plain-text error/notice instead of CSV when there's no data
        if "latitude" not in text.lower():
            print(f"[firms] {source} {chunk_start}: no data ({text.strip()[:80]!r})")
            continue
        df = pd.read_csv(io.StringIO(text))
        df["sensor_source"] = source
        frames.append(df)
        print(f"[firms] {source} {chunk_start} +{span}d: {len(df)} detections")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run() -> None:
    config.require("FIRMS_MAP_KEY")
    cfg = config.SOURCES["firms"]
    frames = [
        fetch_sensor_range(s, cfg["backfill_start"], cfg["backfill_days"], cfg["chunk_days"])
        for s in cfg["sources"]
    ]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"[firms] total detections pulled: {len(df)}")
    write_bronze(df, table="firms_detections", source="firms")


if __name__ == "__main__":
    run()
