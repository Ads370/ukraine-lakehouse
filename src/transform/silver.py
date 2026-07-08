"""
transform/silver.py — build the silver layer from bronze.

Reads bronze (never mutates it), cleans and deduplicates, stamps every event
and detection with a shared oblast key via point-in-polygon, and writes two
conformed Delta tables: silver/events and silver/detections.

    python src/pipeline.py silver
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from storage import read_delta, write_silver  # noqa: E402
from transform.spatial import Boundaries  # noqa: E402

BOUNDARY_PATH = str(
    Path(__file__).resolve().parent.parent.parent
    / config.SOURCES["boundaries"]["adm1_path"]
)


def _dedup_latest(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Keep the most recently ingested row per key (bronze is append-only)."""
    if "_ingested_at" in df.columns:
        df = df.sort_values("_ingested_at")
    return df.drop_duplicates(subset=keys, keep="last")


def build_events(bnd: Boundaries) -> pd.DataFrame:
    raw = read_delta("bronze", "acled_events")
    print(f"[silver:events] read {len(raw)} bronze rows")
    df = _dedup_latest(raw, ["event_id_cnty"])
    print(f"[silver:events] {len(df)} rows after dedup")

    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    keep = [
        "event_id_cnty", "event_date", "event_type", "sub_event_type",
        "actor1", "actor2", "admin1", "location",
        "latitude", "longitude", "fatalities", "notes",
    ]
    df = df[[c for c in keep if c in df.columns]]
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["fatalities"] = pd.to_numeric(df.get("fatalities"), errors="coerce").fillna(0).astype(int)

    df = bnd.assign(df, "longitude", "latitude")
    return df


def build_detections(bnd: Boundaries) -> pd.DataFrame:
    raw = read_delta("bronze", "firms_detections")
    print(f"[silver:detections] read {len(raw)} bronze rows")
    keys = ["latitude", "longitude", "acq_date", "acq_time", "sensor_source"]
    keys = [k for k in keys if k in raw.columns]
    df = _dedup_latest(raw, keys)
    print(f"[silver:detections] {len(df)} rows after dedup")

    # assemble a UTC timestamp from acq_date + acq_time (int like 1327 -> 13:27)
    t = pd.to_numeric(df["acq_time"], errors="coerce").fillna(0).astype(int)
    hhmm = t.astype(str).str.zfill(4)
    date_str = df["acq_date"].astype(str).str[:10]
    df["acq_datetime"] = pd.to_datetime(
        date_str + hhmm, format="%Y-%m-%d%H%M", errors="coerce", utc=True
    )

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    for col in ["bright_ti4", "frp"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = [
        "latitude", "longitude", "acq_datetime", "acq_date",
        "bright_ti4", "frp", "confidence", "daynight",
        "satellite", "sensor_source",
    ]
    df = df[[c for c in keep if c in df.columns]]
    df = bnd.assign(df, "longitude", "latitude")
    return df


def run() -> None:
    bnd = Boundaries(BOUNDARY_PATH)
    events = build_events(bnd)
    write_silver(events, "events")
    detections = build_detections(bnd)
    write_silver(detections, "detections")

    # quick coverage report: how many points landed outside all polygons?
    for name, df in [("events", events), ("detections", detections)]:
        n_null = df["oblast_name"].isna().sum()
        print(f"[silver:{name}] unmatched (no oblast): {n_null} / {len(df)}")


if __name__ == "__main__":
    run()
