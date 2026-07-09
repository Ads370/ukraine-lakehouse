"""
scripts/smoke_test.py: prove the lakehouse skeleton works end to end
WITHOUT any API credentials.
Writes synthetic ACLED-shaped rows to bronze twice, then reads them back
with DuckDB and shows Delta time-travel history. If this runs clean, the
storage layer is good and its possible to move on to real ingestion.

    python scripts/smoke_test.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from storage import query, table_history, write_bronze  # noqa: E402

TABLE = "acled_events_smoketest"


def fake_batch(start_id: int, n: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id_cnty": [f"UKR{start_id + i}" for i in range(n)],
            "event_date": ["2024-06-01", "2024-06-02", "2024-06-03"][:n],
            "event_type": ["Battles", "Explosions/Remote violence", "Battles"][:n],
            "admin1": ["Donetsk", "Kharkiv", "Zaporizhia"][:n],
            "latitude": [48.01, 49.99, 47.84][:n],
            "longitude": [37.80, 36.23, 35.14][:n],
            "fatalities": [3, 0, 5][:n],
        }
    )


def main() -> None:
    print(">>> writing two synthetic batches to bronze...")
    write_bronze(fake_batch(1, 3), table=TABLE, source="smoketest")
    write_bronze(fake_batch(4, 3), table=TABLE, source="smoketest")

    print("\n>>> DuckDB read-back — events by type:")
    print(
        query(
            "SELECT event_type, count(*) AS n, sum(fatalities) AS deaths "
            "FROM t GROUP BY event_type ORDER BY n DESC",
            "bronze",
            TABLE,
        ).to_string(index=False)
    )

    print("\n>>> total rows landed (expect 6):")
    print(query("SELECT count(*) AS rows FROM t", "bronze", TABLE).to_string(index=False))

    print("\n>>> Delta time-travel history (expect 2 commits):")
    hist = table_history("bronze", TABLE)[["version", "operation"]]
    print(hist.to_string(index=False))

    print("\nOK — storage layer verified. Fill in .env and run the real ingestors.")


if __name__ == "__main__":
    main()
