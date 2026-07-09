# Ukraine Conflict Data Lakehouse

A local, medallion-architecture data lakehouse that ingests conflict-event and satellite thermal data on the Russia–Ukraine war (ukrainian territory), joins them on a shared geographic key, and measures **how well independent satellite fire detections corroborate reported conflict events**. Built end to end with open-source tools: no cloud, no Spark, no JVM.

**Headline finding:** across June 2025, **80% of oblast-days with a reported conflict event also showed an independent satellite fire detection in the same oblast**, but the relationship is weak at the individual level (R² = 0.29), and in agrarian oblasts (notably Kherson) the thermal signal is dominated by agricultural burning rather than combat. Corroboration is strong evidence of *spatial-temporal association*, not causation.

**Interactive dashboard:** [View on Tableau Public](https://public.tableau.com/app/profile/YOUR_PROFILE/viz/ukraine-dashboard)

---

## Why this project

Most portfolio "data pipeline" projects load a CSV into pandas and call it a lakehouse. This one models a realistic ingest → transform → store → serve workflow on genuinely messy, real-world data and, more importantly, it asks a question that the engineering exists to answer: *can open satellite data independently corroborate reported conflict activity, and where does that break down?*

The project deliberately foregrounds its own limitations. Naming where the thermal proxy fails (agricultural fires, urban combat that doesn't ignite, a 12-month data lag) is treated as part of the analysis, not a footnote.

---

## Architecture

A three-layer medallion pipeline. Each layer reads the one before it and never mutates it, so the whole structure is reproducible from raw.

```
   ACLED API            NASA FIRMS API           HDX geoBoundaries
 (conflict events)   (VIIRS thermal detections)   (ADM1 polygons)
        |                      |                        |
        +----------+-----------+------------+-----------+
                   v                        v
              BRONZE  (raw, immutable, append-only, partitioned by ingest_date)
                   |
                   v
              SILVER  (deduplicated, type-conformed, point-in-polygon joined to oblast)
                   |
                   v
              GOLD    (analytics marts: events-by-week, thermal-event corroboration)
                   |
                   v
        CSV export  ->  Tableau Public dashboard
```

The join between the two data sources happens **in the pipeline (gold layer)**, not in the BI tool. Tableau only ever visualizes pre-joined, presentation-ready tables, a deliberate separation of concerns.

---

## Data sources

| Source | What it provides | Access | Coverage in this build |
|---|---|---|---|
| **ACLED** (Armed Conflict Location & Event Data) | Geolocated political-violence events: date, coordinates, event type, actors, fatalities | Free myACLED account; OAuth token auth; **attribution required** | 195,764 events, 2022-02-24 → 2025-07-08 |
| **NASA FIRMS** | VIIRS active-fire / thermal-anomaly detections | Free MAP_KEY; area API | 13,881 detections, June 2025 (Standard Processing archive) |
| **HDX geoBoundaries** | Ukraine ADM1 (oblast) boundary polygons | Free download | 27 oblast polygons |

### Important data limitations

- **ACLED Research-tier lag.** The freely available Research tier serves event data on a **rolling ~12-month delay**. Data here ends mid-2025. This is why the project is a **retrospective analysis, not live monitoring**, the event feed and a real-time thermal feed can never temporally overlap on the free tier. The FIRMS window (June 2025) was chosen specifically to sit *inside* ACLED coverage.
- **FIRMS is a proxy, not a strike detector.** Thermal detections capture *any* fire (agricultural burning, wildfires, industrial heat) not only conflict. June is peak agricultural-burning season in Ukraine, which materially inflates detections in farming oblasts (see Kherson).
- **Bounding-box spillover.** FIRMS is queried by a rectangle around Ukraine, so ~24% of raw detections fall outside the national borders (Russia, Belarus, Black Sea). These are correctly assigned a null oblast in silver and excluded from analysis.

---

## Pipeline layers

### Bronze — raw landing
Immutable, append-only, partitioned by `ingest_date`. Source data stored verbatim (ACLED JSON, FIRMS CSV). This is the audit trail: silver and gold can always be rebuilt from bronze without re-hitting the APIs. Delta Lake provides ACID guarantees and time travel.

### Silver — cleaned and spatially joined
The core engineering step. Deduplicates (ACLED on `event_id_cnty`; FIRMS on a composite key), conforms types (real dates, a UTC timestamp assembled from FIRMS `acq_date` + `acq_time`), and assigns every event and every detection to an oblast via **point-in-polygon** against the ADM1 boundaries (shapely `STRtree`, vectorized).

Result: two clean tables sharing a single `oblast_name` / `oblast_pcode` key.
- **Events:** 0 / 195,764 unmatched: every event fell inside an oblast.
- **Detections:** 10,548 in-country; the remainder correctly null (bounding-box spillover).

### Gold — analytics marts
- **`events_by_oblast_week`** — events and fatalities per oblast per ISO week across the full 2022–2025 span (3,373 rows). Feeds the conflict-intensity timeline.
- **`thermal_event_corroboration`** — per oblast per day within the FIRMS window, reported events beside independent thermal detections, FULL OUTER JOINed so event-only, detection-only, and both-present days are all preserved (586 oblast-day rows). Computes the corroboration flag and rate.

---

## Key results

- **80.3% corroboration rate** — of 386 oblast-days with a reported event in June 2025, 310 also showed a fire detection in the same oblast.
- **Strong on the front line:** Donetsk, Zaporizhia, and Dnipropetrovsk corroborate at ~100% — dense combat reliably coincides with thermal signatures.
- **Weak / misleading in agrarian oblasts:** Kherson shows far more detections than events (agricultural fires along the Dnipro), so its high rate reflects near-daily farming fires as much as conflict.
- **Weak individual-level correlation:** events explain only ~29% of the variance in detections (R² = 0.29, log-log). Fires and fighting track loosely, not tightly — which is the honest, expected result given the proxy's noise.

**Interpretation.** These findings support *spatial-temporal association* between reported conflict and satellite-detected fires in high-intensity regions, while demonstrating that thermal data alone is an unreliable conflict indicator in agrarian areas. This is not evidence that any specific fire corresponds to any specific event.

---

## Tech stack

Deliberately right-sized for single-machine, local scale — the tools were chosen to fit the data, not to pad a résumé.

- **Storage / tables:** [Delta Lake](https://delta.io) via `deltalake` (delta-rs) — ACID, schema enforcement, time travel, **no JVM**.
- **Query / transform:** [DuckDB](https://duckdb.org) + pandas.
- **Spatial join:** [shapely](https://shapely.readthedocs.io) `STRtree` — point-in-polygon without GDAL.
- **Visualization:** Tableau Public.
- **Orchestration:** plain Python entrypoint (cron-ready). Prefect/Dagster noted as future work.

Spark and Airflow were considered and **rejected** — at this data volume they add operational overhead (JVM, a scheduler to maintain) for no benefit. DuckDB + delta-rs deliver Delta tables and SQL with none of that friction.

---

## Running the pipeline

```bash
# 1. Environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Verify the storage layer (no credentials needed)
python scripts/smoke_test.py

# 3. Add credentials (see .env.example)
cp .env.example .env    # fill in ACLED login + FIRMS MAP_KEY

# 4. Run the pipeline
python src/pipeline.py acled     # ingest events  -> bronze
python src/pipeline.py firms     # ingest thermal -> bronze
python src/pipeline.py silver    # clean + spatial join -> silver
python src/pipeline.py gold      # build marts -> gold

# 5. Export for Tableau
python scripts/export_for_tableau.py
```

Data sources require free registration (ACLED: <https://acleddata.com/user/register>; FIRMS MAP_KEY: <https://firms.modaps.eosdis.nasa.gov/api/map_key/>). Raw ACLED data is **not** redistributed in this repo, per their terms.

---

## Repository structure

```
ukraine-conflict-lakehouse/
├── config/
│   ├── sources.yaml            # endpoints, bounding box, backfill window
│   └── boundaries/             # HDX ADM1 geojson (committed)
├── src/
│   ├── storage.py              # Delta write + DuckDB read helpers
│   ├── config.py               # yaml + .env loader
│   ├── ingest/                 # acled.py, firms.py
│   ├── transform/              # spatial.py, silver.py, gold.py
│   └── pipeline.py             # entrypoint
├── scripts/                    # smoke_test.py, export_for_tableau.py, peek.py
├── dashboards/exports/         # gold marts as CSV (Tableau input)
└── lakehouse/{bronze,silver,gold}/   # Delta tables (gitignored)
```

---

## Limitations & honest caveats

- **Association, not causation.** An oblast is large; a shelling event in one town and a field fire 80 km away both register as "corroborated" on a given day.
- **Retrospective, not live** — a consequence of the ACLED Research-tier 12-month lag.
- **Agricultural contamination** of the thermal signal, seasonally worst in June.
- **Fatality figures are ACLED estimates** and are the least reliable field.
- Corroboration is measured at oblast-day grain; finer spatial or temporal matching would tighten (or weaken) the association and is left as future work.

---

## Future work

- **Statistical rigor:** confidence intervals on the correlation, spatial-lag testing, temporal cross-correlation (do fires lag shelling?).
- **Control for agricultural fires** via a land-use layer or seasonal baseline, to isolate conflict-driven detections.
- **Automated ingestion:** a Prefect/Dagster + cron scheduler to keep bronze fresh at each source's edge (freshness, not live corroboration — see the ACLED lag).
- **`src/` module extraction and unit tests** for the transform logic.

---

## Attribution

- Armed Conflict data: **ACLED** — [acleddata.com](https://acleddata.com)
- Thermal detections: **NASA FIRMS** / VIIRS
- Boundaries: **HDX geoBoundaries**

Built by Adamo Bovolenta as a portfolio project in geospatial data engineering and conflict analytics.
