"""
scripts/export_for_tableau.py — write the gold marts to CSV for Tableau.

Tableau Public can't connect live to Delta tables, so we export the
presentation-ready gold layer to flat files. Run after `pipeline.py gold`.

    python scripts/export_for_tableau.py

Outputs to dashboards/exports/ :
    events_by_oblast_week.csv
    thermal_event_corroboration.csv

Geography note: Tableau's State/Province map for Ukraine has no separate
polygon for the special-status cities (Kyiv City, Sevastopol), so they
collide with their surrounding region on a filled map. We merge them into
that region here and RE-AGGREGATE from the underlying counts (rates are
recomputed, never averaged), so the choropleth is clean and correct.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from storage import read_delta  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "dashboards" / "exports"

# fold Kyiv City into its surrounding oblast (Tableau has no separate
# State/Province polygon for the capital, so they collide on a filled map).
# Sevastopol / Crimea place correctly in Tableau, so leave them untouched.
NAME_MAP = {
    "Kyiv": "Kyiv Oblast",
}


def _normalise_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["oblast_name"] = df["oblast_name"].replace(NAME_MAP)
    return df


def export_events_by_oblast_week() -> pd.DataFrame:
    df = _normalise_names(read_delta("gold", "events_by_oblast_week"))
    g = (
        df.groupby(["oblast_name", "iso_year", "iso_week", "week_start"], as_index=False)
        .agg(
            oblast_pcode=("oblast_pcode", "first"),
            n_events=("n_events", "sum"),
            fatalities=("fatalities", "sum"),
        )
        .sort_values(["oblast_name", "week_start"])
    )
    return g


def export_corroboration() -> pd.DataFrame:
    df = _normalise_names(read_delta("gold", "thermal_event_corroboration"))
    g = (
        df.groupby(["oblast_name", "day"], as_index=False)
        .agg(
            n_events=("n_events", "sum"),
            fatalities=("fatalities", "sum"),
            n_detections=("n_detections", "sum"),
            total_frp=("total_frp", "sum"),
        )
    )
    # recompute flags from the merged counts (correct after folding cities in)
    g["has_events"] = g["n_events"] > 0
    g["has_detections"] = g["n_detections"] > 0
    g["corroborated"] = g["has_events"] & g["has_detections"]
    return g.sort_values(["oblast_name", "day"])


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, builder in [
        ("events_by_oblast_week", export_events_by_oblast_week),
        ("thermal_event_corroboration", export_corroboration),
    ]:
        df = builder()
        path = OUT / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"wrote {len(df):>6} rows, {len(df.columns)} cols -> {path}")
    print(f"\nDone. Point Tableau at: {OUT}")


if __name__ == "__main__":
    main()
