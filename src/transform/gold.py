"""
transform/gold.py — build the gold (analytics-ready) layer from silver.

Two marts:
  * events_by_oblast_week      — events + fatalities per oblast per ISO week,
                                 full 2022-2025 span. Feeds the intensity heatmap.
  * thermal_event_corroboration — per oblast per day within the FIRMS overlap
                                 window, reported events beside independent
                                 thermal detections, FULL OUTER JOINed so you
                                 can measure how often the two coincide.

The corroboration window is derived from the FIRMS backfill config so the two
stay in sync automatically if you widen the window later.

    python src/pipeline.py gold
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from storage import read_delta, write_gold  # noqa: E402


def _overlap_window() -> tuple[str, str]:
    """Intersection window = the FIRMS backfill span (detections only exist there)."""
    f = config.SOURCES["firms"]
    start = date.fromisoformat(f["backfill_start"])
    end = start + timedelta(days=f["backfill_days"] - 1)
    return start.isoformat(), end.isoformat()


def build_events_by_oblast_week() -> pd.DataFrame:
    ev = read_delta("silver", "events")
    ev = ev[ev["oblast_name"].notna()].copy()
    ev["event_date"] = pd.to_datetime(ev["event_date"], errors="coerce")
    ev = ev.dropna(subset=["event_date"])

    iso = ev["event_date"].dt.isocalendar()
    ev["iso_year"] = iso["year"].astype(int)
    ev["iso_week"] = iso["week"].astype(int)
    # Monday of that ISO week, for clean time-axis plotting
    ev["week_start"] = (ev["event_date"] - pd.to_timedelta(ev["event_date"].dt.weekday, unit="D")).dt.normalize()

    g = (
        ev.groupby(["oblast_name", "oblast_pcode", "iso_year", "iso_week", "week_start"])
        .agg(n_events=("event_id_cnty", "count"), fatalities=("fatalities", "sum"))
        .reset_index()
        .sort_values(["oblast_name", "week_start"])
    )
    print(f"[gold:events_by_oblast_week] {len(g)} oblast-week rows")
    return g


def build_corroboration() -> pd.DataFrame:
    start, end = _overlap_window()
    print(f"[gold:corroboration] overlap window {start} .. {end}")

    # events per oblast-day within the window
    ev = read_delta("silver", "events")
    ev = ev[ev["oblast_name"].notna()].copy()
    ev["event_date"] = pd.to_datetime(ev["event_date"], errors="coerce")
    ev = ev[(ev["event_date"] >= start) & (ev["event_date"] <= end)]
    ev["day"] = ev["event_date"].dt.strftime("%Y-%m-%d")
    ev_daily = (
        ev.groupby(["oblast_name", "day"])
        .agg(n_events=("event_id_cnty", "count"), fatalities=("fatalities", "sum"))
        .reset_index()
    )

    # detections per oblast-day within the window
    det = read_delta("silver", "detections")
    det = det[det["oblast_name"].notna()].copy()
    det["acq_datetime"] = pd.to_datetime(det["acq_datetime"], errors="coerce", utc=True)
    det["day"] = det["acq_datetime"].dt.strftime("%Y-%m-%d")
    det_daily = (
        det.groupby(["oblast_name", "day"])
        .agg(
            n_detections=("latitude", "count"),
            total_frp=("frp", "sum"),
        )
        .reset_index()
    )

    # FULL OUTER JOIN on (oblast, day): keep days with events, detections, or both
    m = ev_daily.merge(det_daily, on=["oblast_name", "day"], how="outer")
    for col in ["n_events", "fatalities", "n_detections", "total_frp"]:
        m[col] = m[col].fillna(0)
    m["n_events"] = m["n_events"].astype(int)
    m["n_detections"] = m["n_detections"].astype(int)
    m["fatalities"] = m["fatalities"].astype(int)
    m["has_events"] = m["n_events"] > 0
    m["has_detections"] = m["n_detections"] > 0
    m["corroborated"] = m["has_events"] & m["has_detections"]
    m = m.sort_values(["oblast_name", "day"])

    # headline metric: of oblast-days with reported events, how many also show fire?
    with_events = m[m["has_events"]]
    rate = with_events["corroborated"].mean() if len(with_events) else float("nan")
    print(f"[gold:corroboration] {len(m)} oblast-day rows")
    print(f"[gold:corroboration] event-days: {len(with_events)}, "
          f"also with thermal: {int(with_events['corroborated'].sum())} "
          f"({rate:.1%} corroboration rate)")
    return m


def run() -> None:
    write_gold(build_events_by_oblast_week(), "events_by_oblast_week")
    write_gold(build_corroboration(), "thermal_event_corroboration")


if __name__ == "__main__":
    run()
