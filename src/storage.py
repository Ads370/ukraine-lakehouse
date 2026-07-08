"""
storage.py — thin wrappers over Delta Lake (delta-rs) and DuckDB.

Bronze is append-only and immutable: every run adds rows tagged with an
ingest timestamp, partitioned by ingest_date. We never mutate what landed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from deltalake import DeltaTable, write_deltalake

LAKEHOUSE_ROOT = Path(__file__).resolve().parent.parent / "lakehouse"


def _table_path(layer: str, table: str) -> str:
    path = LAKEHOUSE_ROOT / layer / table
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def write_bronze(df: pd.DataFrame, table: str, source: str) -> str:
    """
    Append a batch to a bronze Delta table.

    Adds ingest metadata and partitions by ingest_date so reprocessing and
    time-travel both work. Returns the table path.
    """
    if df.empty:
        print(f"[bronze:{table}] nothing to write (empty frame)")
        return _table_path("bronze", table)

    now = datetime.now(timezone.utc)
    df = df.copy()
    df["_source"] = source
    df["_ingested_at"] = now.isoformat()
    df["ingest_date"] = now.strftime("%Y-%m-%d")

    path = _table_path("bronze", table)
    write_deltalake(
        path,
        df,
        mode="append",
        partition_by=["ingest_date"],
    )
    print(f"[bronze:{table}] appended {len(df)} rows -> {path}")
    return path


def write_silver(df: pd.DataFrame, table: str) -> str:
    """
    Write a silver (or gold) table by overwrite.

    Silver is *derived* from bronze, so we rebuild it wholesale each run
    rather than appending. Delta keeps the previous version for time travel.
    """
    now = datetime.now(timezone.utc)
    df = df.copy()
    df["_built_at"] = now.isoformat()
    path = _table_path("silver", table)
    write_deltalake(path, df, mode="overwrite", schema_mode="overwrite")
    print(f"[silver:{table}] wrote {len(df)} rows -> {path}")
    return path


def read_delta(layer: str, table: str) -> pd.DataFrame:
    """Read a whole Delta table into pandas (fine at local scale)."""
    path = _table_path(layer, table)
    return DeltaTable(path).to_pandas()


def query(sql: str, layer: str, table: str) -> pd.DataFrame:
    """
    Run DuckDB SQL against a Delta table. The table is exposed as the view
    name `t`. Example: query("SELECT count(*) FROM t", "bronze", "acled_events")
    """
    df = read_delta(layer, table)  # noqa: F841 — referenced by DuckDB below
    con = duckdb.connect()
    con.register("t", df)
    return con.execute(sql).fetch_df()


def table_history(layer: str, table: str) -> pd.DataFrame:
    """Delta time-travel: show the commit history for a table."""
    dt = DeltaTable(_table_path(layer, table))
    return pd.DataFrame(dt.history())
