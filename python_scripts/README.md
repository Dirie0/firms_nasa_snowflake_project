# FIRMS fire data pipeline (`python_scripts`)

Daily NASA FIRMS **VIIRS near-real-time (NRT)** fire detections from three satellites
(Suomi NPP, NOAA-20, NOAA-21 — defined in `config.SATELLITES`), cleaned, split into
tables and uploaded to S3 ready to load into Snowflake.

## Setup

```bash
poetry install
cp .env.example .env     # then fill in the values below
```

`.env`:

| Variable | Used by | Purpose |
|----------|---------|---------|
| `BUCKET_NAME` | all | Target S3 bucket |
| `BUCKET_REGION` | all | Bucket region |
| `ACCESS_KEY` / `SECRET_KEY` | all | AWS credentials |
| `MAP_KEY` | `extract.py` | NASA FIRMS API key |

## One-off: build the reference data

Run this **once** at setup, and again only when country boundaries change or a satellite
is added to `config.SATELLITES`. It takes no arguments.

```bash
poetry run python prepare_countries.py
```

Downloads the Natural Earth countries shapefile and writes three files to
`s3://<bucket>/reference/`:

- `countries.csv` — country polygons (WKT), read by the transform's spatial join
- `dim_country.csv` — country attributes (incl. `UNK` for ocean / unmatched fires), loaded once to Snowflake
- `dim_satellite.csv` — satellite attributes from `config.SATELLITES`, loaded once to Snowflake

The daily pipeline only *reads* `countries.csv`; it never regenerates these.

## Daily run order

Each step takes `--date YYYY-MM-DD`. Run them in order for a given date `D`:

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `extract.py --date D [--satellite N\|N20\|N21]` | Downloads one raw csv per satellite to `raw/date=D/satellite_<code>.csv` in S3 |
| 2 | `transform_data_script.py --date D [--raw-dir DIR]` | Combines the per-satellite files, cleans, validates, filters to NRT, dedupes, adds country |
| 3 | `split_data_into_tables.py --date D` | Splits the clean data into the fact and rejected tables |
| 4 | `upload_data_to_s3_bucket.py --date D [--gzip]` | Uploads the tables to `tables/date=D/` in S3 |

```bash
poetry run python extract.py --date 2026-10-16
poetry run python transform_data_script.py --date 2026-10-16
poetry run python split_data_into_tables.py --date 2026-10-16
poetry run python upload_data_to_s3_bucket.py --date 2026-10-16 --gzip
```

Flags:
- `extract.py --satellite N20` — extract a single satellite instead of all (one task per satellite lets Airflow run them in parallel).
- `transform_data_script.py --raw-dir ./local_raw` — read the raw `satellite_<code>.csv` files from a local folder instead of S3.
- `upload_data_to_s3_bucket.py --gzip` — compress each csv first; Snowflake `COPY INTO` reads `.csv.gz` directly.

All settings — columns, satellites, S3 paths, validation rules, output tables — live in `config.py`.

## What the transform does

Reads one raw csv per configured satellite and concatenates them. Every satellite must be
present (a missing file fails the run); a file with zero rows is fine. Then, in order:

1. **check** — type each column and give every row its *first* problem: missing value, not a
   number, out of range, value not allowed, bad date/time, bad version format.
2. **split** — rows with a problem go to `rejected.csv` with their `reject_reason`.
3. **filter** — valid rows that are not `NRT` latency are dropped (filtered, not an error).
4. **dedupe** — drop exact repeats. Grain = (detected_at, latitude, longitude, satellite),
   so the same fire seen by two satellites stays as two independent observations.
5. **enrich** — point-in-polygon join tags each fire with its `country_iso3` (`UNK` if it
   falls in no country polygon — ocean, gas flares, coastline gaps).
6. **rename** — standard column names, units and values (`l/n/h` → low/nominal/high,
   `D/N` → boolean `is_daytime`, `scan*track` → `pixel_area_km2`).

A sanity check fails the run if the row buckets (clean + rejected + filtered + duplicate)
don't sum back to the input count.

**Local outputs** (`data/date=D/transformed/`): `clean.csv`, `rejected.csv`, `run_summary.json`.

## Tables

Step 3 writes to `data/date=D/tables/`:

- `fact_fire_detection` — one row per detection; joins to `dim_country` on `country_iso3`
  and `dim_satellite` on `satellite`
- `rejected_detections` — failed rows with `reject_reason`
- `run_summary` (`run_summary.csv`) — one row of row counts for the run

`dim_country` and `dim_satellite` are static reference data from `prepare_countries.py`,
not regenerated daily.

There is no date dimension on purpose: the fact table carries a full UTC timestamp, so
Snowflake's date functions (`DATE_TRUNC`, `DAYNAME`, …) cover what a calendar table would.

## Files

| File | Role |
|------|------|
| `config.py` | All pipeline settings (API, satellites, S3 layout, validation rules, output columns) |
| `utils.py` | Shared helpers: logging, `--date` arg parsing, local folders, S3 get/put, csv reading |
| `prepare_countries.py` | One-off reference-data builder (see above) |
| `extract.py` | Step 1 — download raw FIRMS data to S3 |
| `transform_data_script.py` | Step 2 — clean and standardise |
| `split_data_into_tables.py` | Step 3 — split into daily tables |
| `upload_data_to_s3_bucket.py` | Step 4 — upload tables to S3 |


poetry run python extract.py --date 2026-10-17
poetry run python transform_data_script.py --date 2026-10-16
poetry run python split_data_into_tables.py --date 2026-10-16
poetry run python upload_data_to_s3_bucket.py --date 2026-10-16 