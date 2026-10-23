# ❄️ FIRMS Fire-Data Pipeline — End-to-End ETL with Airflow, Snowflake & Terraform

A modular data pipeline that pulls daily NASA FIRMS **VIIRS NOAA-20 near-real-time (NRT)** fire detections, validates and enriches them in Python, stages them in S3, and loads them into Snowflake. Orchestrated by **Airflow**, provisioned by **Terraform**, and run locally with **Docker**.

## 📸 Overview

Every day the pipeline:

- 🛰️ **Extracts** raw fire detections from the NASA FIRMS API → S3 `raw/`
- 🧼 **Transforms** them: validate, filter to NRT, de-duplicate, tag each fire with its country → S3 `transformed/`
- ✂️ **Splits** the result into fact, rejected and run-summary tables → S3 `tables/`
- 🏔️ **Loads** them into Snowflake with `COPY INTO` (idempotent, per run date)
- ✅ **Quality-checks** what landed against the run summary
- ⚙️ All orchestrated by an Airflow DAG, backfilled one day at a time

Every step hands off through **S3**, keyed by the run date `D` (`{{ ds }}`), which is also the `BATCH_ID` in Snowflake — so any day can be re-run safely.

## 🧰 Tech Stack

| Tool | Purpose |
|------|---------|
| Python (pandas, geopandas) | ETL scripts |
| Airflow 3.3.2 | DAG orchestration |
| Docker Compose | Local Airflow stack (CeleryExecutor) |
| Snowflake | Data warehouse |
| Terraform | Infrastructure as code (Snowflake + AWS IAM) |
| S3 | Intermediate staging |
| Poetry | Python dependency management |

## 🚀 Architecture

```
NASA FIRMS API
     │
     ▼
 extract ─► transform ─► split ──► S3 (raw/ → transformed/ → tables/)
                                     │
                                     ▼  COPY INTO (per BATCH_ID)
                        Snowflake  FIRMS_DB.FIRMS  (5 tables, FIRMS_WH)
                                     │
                                     ▼
                              quality_check
```

All orchestrated by the `firms_snowflake_pipeline` Airflow DAG.

## 🗂️ Folder Structure

```
Diries_Snowflake_Project/
├── airflow/
│   ├── dags/
│   │   ├── pipeline_orchestration.py   ◀── Main DAG (firms_snowflake_pipeline)
│   │   └── sql/                         ◀── load + quality-check SQL
│   ├── Dockerfile                       ◀── Airflow 3 + Snowflake provider + Poetry
│   ├── docker-compose.yaml              ◀── Airflow + Postgres + Redis
│   └── Makefile                         ◀── CLI shortcuts
├── python_scripts/                      ◀── the ETL code
│   ├── extract.py / transform_data_script.py / split_data_into_tables.py
│   ├── prepare_countries.py             ◀── one-off reference-data builder
│   └── config.py / utils.py
├── terraform_snowflake/                 ◀── IaC: db, schema, warehouse, tables, stage, integration
├── .env                                 ◀── BUCKET_NAME, MAP_KEY (do NOT commit)
└── README.md
```

## ⚙️ How It Works

### DAG: `firms_snowflake_pipeline`

Runs `@daily` with `catchup=True`, one run at a time, so missed days backfill in order.

| Task | Description |
|------|-------------|
| `extract` | Download raw NOAA-20 detections from the FIRMS API |
| `transform` | Validate, filter to NRT, de-duplicate, add country |
| `split` | Write the fact, rejected and run-summary CSVs |
| `load_fact` / `load_rejected` / `load_run_summary` | `COPY INTO` each Snowflake table |
| `quality_check` | Fail the run if loaded data doesn't match the run summary |

```
extract ➝ transform ➝ split ➝ [load_fact, load_rejected, load_run_summary] ➝ quality_check
```

Airflow handles retries (3×, exponential backoff), logging and task chaining.

### 🛰️ The FIRMS API

`extract` pulls from the NASA FIRMS **Area API**, which returns CSV fire detections for a bounding box and day range:

```
/api/area/csv/[MAP_KEY]/[SOURCE]/[AREA]/[DAY_RANGE]/[DATE]
```

- **`MAP_KEY`** — your free API key (`MAP_KEY` in `.env`). Request one at <https://firms.modaps.eosdis.nasa.gov/api/area/>.
- **`SOURCE`** — the satellite feed; this pipeline uses `VIIRS_NOAA20_NRT` (near-real-time).
- **`AREA`** — `west,south,east,north` or `world` (`-180,-90,180,90`).
- **`DAY_RANGE`** — 1–5 days per request; the pipeline queries one day at a time.
- **`DATE`** — `YYYY-MM-DD`; returns `DATE … DATE + DAY_RANGE-1`. Omit for the most recent data.

Key usage is capped, so check remaining transactions with `/api/data_availability/` or the map-key status endpoint. NRT data is made available within ~60 minutes of satellite overpass.

### 📦 Poetry

Poetry manages the ETL scripts' dependencies in an isolated virtualenv — separate from Airflow's own Python, so pandas/geopandas can't clash with it.

- `pyproject.toml` declares dependencies; `poetry.lock` pins exact versions for reproducible installs.
- `poetry install` creates the venv and installs the locked versions.
- `poetry run python <script>.py` runs a script inside that venv.

The Airflow image installs the same lockfile and invokes the scripts with `BashOperator`, so a script runs identically by hand and in the DAG.

## 🏁 Setup

### ✅ 1. Prerequisites

Docker + Compose, Terraform, Poetry, an AWS account with an S3 bucket, a Snowflake account, and a NASA FIRMS API key (`MAP_KEY`).

### 🔑 2. Snowflake key pair

Terraform and Airflow authenticate with a key pair, not a password:

```bash
cd ~/.ssh
openssl genrsa 2048 | openssl pkcs8 topk8 -inform PEM -out snowflake_tf_snow_key.p8 -nocrypt
openssl rsa -in snowflake_tf_snow_key.p8 -pubout -out snowflake_tf_snow_key.pub
chmod 600 snowflake_tf_snow_key.p8
```

Attach the public key to the `TERRAFORM_SVC` user and grant integration rights (as `ACCOUNTADMIN`):

```sql
ALTER USER TERRAFORM_SVC SET RSA_PUBLIC_KEY = '<public key, no BEGIN/END lines>';
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN;
```

### 🧾 3. Environment files

Root `.env` (passed to the scripts and Airflow):

```
BUCKET_NAME=<bucket>
MAP_KEY=<nasa firms api key>
```

`airflow/.env` (read by Docker Compose):


AWS keys live in the `aws_default` Airflow connection (step 7), not in `.env`.

### 🛠️ 4. Provision infrastructure with Terraform

```
example terraform.tfvars
organization_name = ""
account_name      = ""
private_key_path  = ""
aws_region        = ""
aws_account_id    = ""
bucket_name       = ""
```

```bash
cd terraform_snowflake
# create terraform.tfvars (account, private_key_path, aws_account_id, bucket_name, ...)
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

Creates the database, schema, warehouse, 5 tables, S3 storage integration, stage, file format, and the IAM role Snowflake assumes.

The config is split into four modules under `terraform_snowflake/modules/`:

| Module | What it creates |
|--------|-----------------|
| `foundation` | The database, schema and warehouse everything else hangs off |
| `tables` | The 5 Snowflake tables (fact, rejected, run summary, 2 dimensions) |
| `s3_integration` | The storage integration, external stage and CSV file format Snowflake uses to read S3 |
| `aws_iam` | The AWS IAM role Snowflake assumes — trusts Snowflake's user, grants read-only on the bucket's `tables/` and `reference/` prefixes |

`s3_integration` and `aws_iam` form a handshake: the integration points at the role ARN, and the role trusts the IAM user Snowflake generates in return.

### 🌍 5. Build the reference data (one-off)

```bash
cd python_scripts
poetry install
poetry run python prepare_countries.py
```

Writes `countries.csv`, `dim_country.csv` and `dim_satellite.csv` to `s3://<bucket>/reference/`. The two dimension tables are static, so load them once:

```sql
COPY INTO FIRMS_DB.FIRMS.DIM_COUNTRY   FROM @FIRMS_DB.FIRMS.FIRMS_S3_STAGE/reference/ FILES=('dim_country.csv')   FILE_FORMAT=(FORMAT_NAME='FIRMS_DB.FIRMS.FIRMS_CSV');
COPY INTO FIRMS_DB.FIRMS.DIM_SATELLITE FROM @FIRMS_DB.FIRMS.FIRMS_S3_STAGE/reference/ FILES=('dim_satellite.csv') FILE_FORMAT=(FORMAT_NAME='FIRMS_DB.FIRMS.FIRMS_CSV');
```

### 🐳 6. Start Airflow

Edit the private-key mount path in `docker-compose.yaml` to match your machine, then:

```bash
cd airflow
make init   # first time only: migrate the DB, create the admin user
make up     # build and start the stack
```

UI at <http://localhost:8080> (login `airflow` / `airflow`).

### 🌐 7. Configure Airflow connections

In **Admin → Connections**, create and **Test** both:

- **`aws_default`** (Amazon Web Services) — the pipeline's IAM keys, Extra `{"region_name": "<region>"}`
- **`snowflake_default`** (Snowflake) — Login `TERRAFORM_SVC`, no password, Schema `FIRMS`, Extra:

```json
{
  "account": "<org>-<account>",
  "warehouse": "FIRMS_WH",
  "database": "FIRMS_DB",
  "role": "SYSADMIN",
  "private_key_file": "/opt/airflow/secrets/rsa_key.p8"
}
```

`private_key_file` is the in-container path (the right side of the mount).

## 🧪 Running the Pipeline

Unpause `firms_snowflake_pipeline` in the Airflow UI — it backfills from the start date, one day at a time. Watch the tasks go green: **extract ➝ transform ➝ split ➝ load ➝ quality_check**. Then check the data in Snowflake:

```sql
SELECT * FROM FIRMS_DB.FIRMS.RUN_SUMMARY ORDER BY BATCH_ID;
```

Run a single step by hand:

```bash
cd python_scripts
poetry run python extract.py --date 2026-10-16
```

Common Airflow ops live in `airflow/Makefile`: `make up`, `make down`, `make restart` (after editing `.env`), `make logs`, `make ps`.

## 🧠 Design Notes

- **ETL, not ELT** — validation, de-duplication and the country join happen in Python; Snowflake only ever sees clean tables.
- **S3 is the handoff** between steps (not XCom), keyed by date, so every run is deterministic, re-runnable
- **Idempotent loads** — each load is delete-then-`COPY INTO` per `BATCH_ID` in a single transaction.
- **Bad rows are data** — invalid rows go to `REJECTED_DETECTIONS` with a reason; the run continues.
- **Isolated Python environment**- the scripts run in their own Poetry virtualenv and Airflow calls them with BashOperator instead of PythonOperator. pandas and geopandas never share a Python environment with Airflow, so their dependencies can’t clash, and each script runs the same way by hand as in the DAG.

## 📈 Future Enhancements

- 🔔 Alerting on failures and reject-rate spikes
- 📊 Dashboard over the fact and dimension tables (fires per day, top countries, intensity by confidence) plus a pipeline-health view built from RUN_SUMMARY
- 🗃️ Remote Terraform backend (S3 + locking)
- 🧊 Staging tables + `MERGE` for late or overlapping batches
- 🧹 S3 lifecycle rules on `raw/` and `transformed/`
- 🔐 Dedicated Snowflake role for Airflow, limited to FIRMS_WH and the FIRMS schema, created in Terraform
- 🧪 CI: run full coverage pytests, terraform fmt -check and terraform validate on every push

## 👨‍💻 Author

Dirie — Data Engineer.

## 📜 Licence

MIT Licence — free to use, modify, and learn 