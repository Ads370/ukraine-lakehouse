# Ukraine Conflict Data Lakehouse

A local, medallion-architecture lakehouse ingesting conflict-event and thermal data on the Russia–Ukraine war. Batch ingestion → immutable bronze → conformed silver → analytics-ready gold, queried with DuckDB and Delta Lake. No Spark, no JVM.

> **Status:** skeleton / weekend 1. Bronze ingestion for ACLED and FIRMS is wired; silver and gold are next.

## Stack

- **Storage / tables:** Delta Lake via `deltalake` (delta-rs) — ACID, schema enforcement, time travel.
- **Query / transform:** DuckDB + pandas.
- **Orchestration:** plain Python entrypoint + cron for now (Prefect/Dagster later).

## Setup

1. **Python env** (3.11 or 3.12 recommended):
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
   (Or `conda create -n ukr python=3.12 && conda activate ukr && pip install -r requirements.txt`.)

2. **Prove the skeleton — no credentials needed:**
   ```bash
   python scripts/smoke_test.py
   ```
   Writes synthetic batches to bronze, reads them back with DuckDB, and shows Delta time-travel history. If this runs clean, storage is good.

3. **Get data access (do these yourself — they're account registrations):**
   - ACLED: register at <https://acleddata.com/user/register>
   - FIRMS: request a MAP_KEY at <https://firms.modaps.eosdis.nasa.gov/api/map_key/>

4. **Add secrets:** `cp .env.example .env` and fill in your values.

5. **Run real ingestion:**
   ```bash
   python src/pipeline.py            # all sources
   python src/pipeline.py acled      # just one
   ```

## Layout

```
config/sources.yaml   # region bbox, endpoints, date ranges
src/storage.py        # Delta write + DuckDB read helpers
src/config.py         # yaml + .env loader
src/ingest/acled.py   # OAuth + paginated read -> bronze
src/ingest/firms.py   # area CSV per sensor -> bronze
src/pipeline.py       # entrypoint the scheduler calls
scripts/smoke_test.py # credential-free storage verification
lakehouse/{bronze,silver,gold}/
```

## Data & attribution

- **ACLED** data are used under the myACLED terms of use; **attribution is mandatory**. Do not redistribute raw ACLED data (it is gitignored here).
- **NASA FIRMS** thermal detections are a *proxy* for events — fires can be agricultural or wildfire. This is not a strike detector; the corroboration layer (coming in gold) measures alignment, not causation.

## Roadmap

- [x] Bronze ingestion (ACLED, FIRMS)
- [ ] Silver: conform schemas, point-in-polygon join to oblast via HDX boundaries
- [ ] Gold: `events_by_oblast_week`, `thermal_event_corroboration`
- [ ] Dashboard
