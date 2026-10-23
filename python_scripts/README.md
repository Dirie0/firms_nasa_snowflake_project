# FIRMS fire data pipeline (`python_scripts`)

The ETL code behind the pipeline. Pulls daily NASA FIRMS **VIIRS NOAA-20 near-real-time
(NRT)** fire detections, cleans and validates them, splits them into load-ready tables,
and writes everything to S3 for Snowflake to `COPY INTO`.

The satellite set is defined in `config.SATELLITES` (currently just `N20` / NOAA-20); add
more there and the scripts loop over all of them.

## Setup

```bash
poetry install
```

Create a `.env` in this folder (loaded by `python-dotenv`):

| Variable | Used by | Purpose |
|----------|---------|---------|
| `BUCKET_NAME` | all | Target S3 bucket |
| `MAP_KEY` | `extract.py` | NASA FIRMS API key |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION` | all | Standard boto3 AWS credentials and region |

Under Airflow the AWS values come from the `aws_default` connection, injected into each
task's environment by the DAG. The scripts don't read a separate bucket-region variable;
boto3 takes its region from `AWS_DEFAULT_REGION`.

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

Each step takes `--date YYYY-MM-DD`, reads and writes S3, and takes its settings from
`config.py`. Run them in order for a given date `D`:

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `extract.py --date D [--satellite N20]` | Downloads one raw csv per configured satellite to `raw/date=D/satellite_<code>.csv` in S3 |
| 2 | `transform_data_script.py --date D [--raw-dir DIR]` | Combines the per-satellite files, cleans, validates, filters to NRT, dedupes, adds country; writes `transformed/date=D/` |
| 3 | `split_data_into_tables.py --date D` | Splits the transformed output into the load-ready table csvs in `tables/date=D/` |

```bash
poetry run python extract.py --date 2026-10-16
poetry run python transform_data_script.py --date 2026-10-16
poetry run python split_data_into_tables.py --date 2026-10-16
```

Flags:
- `extract.py --satellite N20` — extract a single satellite instead of all configured ones.
- `transform_data_script.py --raw-dir ./local_raw` — read the raw `satellite_<code>.csv`
  files from a local folder instead of S3.

All settings — API URL, satellites, S3 paths, validation rules, output columns — live in `config.py`.

## What the transform does

Reads one raw csv per configured satellite and concatenates them. If the combined input is
empty it writes empty outputs and a zero-count summary. Otherwise, in order:

1. **check** — type each column and give every row its *first* problem: missing value, not a
   number, out of range, value not allowed (confidence, daynight, satellite, instrument),
   bad date/time, or bad version format.
2. **split** — rows with a problem go to `rejected.csv` with their `reject_reason`.
3. **filter** — valid rows that are not `NRT` latency are dropped (filtered, not an error).
4. **dedupe** — drop exact repeats. The `detection_id` is an md5 of
   (detected_at, latitude, longitude, satellite), so the same fire seen by two satellites
   stays as two independent observations.
5. **enrich** — point-in-polygon join tags each fire with its `country_iso3` (`UNK` if it
   falls in no country polygon — ocean, gas flares, coastline gaps).
6. **rename** — standard column names, units and values (`l/n/h` → low/nominal/high,
   `D/N` → boolean `is_daytime`, `scan*track` → `pixel_area_km2`).

A sanity check fails the run if the row buckets (clean + rejected + filtered + duplicate)
don't sum back to the input count.

**Outputs** (`transformed/date=D/` in S3): `clean.csv`, `rejected.csv`, `run_summary.json`.

## Tables

Step 3 reads `transformed/date=D/` and writes to `tables/date=D/` in S3:

- `fact_fire_detection.csv` — one row per clean detection; joins to `dim_country` on
  `country_iso3` and `dim_satellite` on `satellite`
- `rejected_detections.csv` — failed rows with `reject_reason` (always written, even empty)
- `run_summary.csv` — one row of row counts for the run

`dim_country` and `dim_satellite` are static reference data from `prepare_countries.py`,
not regenerated daily.

There is no date dimension on purpose: the fact table carries a full UTC timestamp, so
Snowflake's date functions (`DATE_TRUNC`, `DAYNAME`, …) cover what a calendar table would.

## Files

| File | Role |
|------|------|
| `config.py` | All pipeline settings (API, satellites, S3 layout, validation rules, output columns) |
| `utils.py` | Shared helpers: logging, `--date` arg parsing, env reading, S3 get/put, csv reading |
| `prepare_countries.py` | One-off reference-data builder (see above) |
| `extract.py` | Step 1 — download raw FIRMS data to S3 |
| `transform_data_script.py` | Step 2 — clean, validate and standardise |
| `split_data_into_tables.py` | Step 3 — split into the daily table csvs |
