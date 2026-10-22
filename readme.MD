# FIRMS Fire-Data Pipeline

An end-to-end ETL pipeline that pulls daily NASA FIRMS **VIIRS NOAA-20 near-real-time (NRT)** fire detections, validates and enriches them with Python, stages them in S3, and loads them into Snowflake. **Airflow** orchestrates the run, **Terraform** provisions the Snowflake and AWS infrastructure, and **Docker** runs everything locally.

```
                    ┌──────────────────────── Airflow DAG: firms_snowflake_pipeline ────────────────────────┐
                    │                                                                                       │
 NASA FIRMS API ──► extract ──► transform ──► split ──► load_fact ──────┐                                   │
                    │   │          │            │       load_rejected ───┼──► quality_check                  │
                    │   ▼          ▼            ▼       load_run_summary ┘                                   │
                    └───┼──────────┼────────────┼───────────▲──────────────────────────────────────────────┘
                        ▼          ▼            ▼           │ COPY INTO
                    ┌────────────── S3 bucket ───────────┐  │ (FIRMS_S3_STAGE via FIRMS_S3_INT)
                    │ raw/ → transformed/ → tables/      │──┘
                    │ reference/ (countries + dimensions)│            Snowflake: FIRMS_DB.FIRMS
                    └────────────────────────────────────┘            5 tables, warehouse FIRMS_WH
```

Data moves between steps **through S3, not XCom**. The run date `D` (Airflow's `{{ ds }}`) links every step. It is the S3 folder name and the `BATCH_ID` column in Snowflake.

---

## Contents

1. [Repository layout](#repository-layout)
2. [How it works](#how-it-works)
3. [Setup, first time](#setup-first-time)
4. [Running and operating](#running-and-operating)
5. [Verifying a run](#verifying-a-run)
6. [Reference: Python scripts](#reference-python-scripts)
7. [Reference: Airflow](#reference-airflow)
8. [Reference: Terraform and Snowflake](#reference-terraform-and-snowflake)
9. [Design logic and trade-offs](#design-logic-and-trade-offs)
10. [Troubleshooting](#troubleshooting)
11. [Security notes and known limitations](#security-notes-and-known-limitations)

---

## Repository layout

```
.
├── .env                         # Python + Airflow container settings (do NOT commit)
├── .gitignore
├── README.md                    # this file
├── airflow/
│   ├── Dockerfile               # Airflow 3 + Snowflake provider + Poetry
│   ├── docker-compose.yaml      # local CeleryExecutor stack
│   ├── .env                     # compose variables, e.g. FERNET_KEY (do NOT commit)
│   ├── config/                  # mounted at /opt/airflow/config
│   ├── plugins/
│   ├── logs/
│   └── dags/
│       ├── firms_snowflake_pipeline.py
│       └── sql/
│           ├── load_table.sql       # DELETE + COPY INTO template, rendered once per table
│           └── quality_checks.sql   # post-load assertions
├── python_scripts/              # Poetry project: the ETL code
│   ├── config.py                # all settings: columns, validation rules, S3 layout
│   ├── utils.py                 # logging, --date parsing, S3 helpers
│   ├── prepare_countries.py     # one-off reference-data builder
│   ├── extract.py               # step 1
│   ├── transform_data_script.py # step 2
│   ├── split_data_into_tables.py# step 3
│   ├── pyproject.toml / poetry.lock
│   └── .env.example
└── terraform_snowflake/         # infrastructure as code
    ├── main.tf  variables.tf  tables.tf  providers.tf  versions.tf  outputs.tf
    ├── terraform.tfvars         # your values (do NOT commit)
    ├── schema/                  # one YAML column list per table
    └── modules/
        ├── foundation/          # database, schema, warehouse
        ├── tables/              # tables built from local.tables
        ├── s3_integration/      # storage integration, file format, stage
        └── aws_iam/             # IAM role Snowflake assumes
```

---

## How it works

### The daily DAG

`firms_snowflake_pipeline` runs `@daily` with `catchup=True`, `start_date` 2026-10-15 and `max_active_runs=1`, so missed days are backfilled one at a time, in order.

| Task | Operator | What it does |
|------|----------|--------------|
| `extract` | Bash | Downloads raw NOAA-20 detections to `raw/date=D/satellite_N20.csv` |
| `transform` | Bash | Validates, filters to NRT, de-duplicates, adds country; writes `transformed/date=D/` |
| `split` | Bash | Writes the three load-ready CSVs to `tables/date=D/` |
| `load_fact` | SQL | Loads `FACT_FIRE_DETECTION` |
| `load_rejected` | SQL | Loads `REJECTED_DETECTIONS` |
| `load_run_summary` | SQL | Loads `RUN_SUMMARY` |
| `quality_check` | SQL check | Fails the run if loaded data doesn't match expectations |

```
extract >> transform >> split >> [load_fact, load_rejected, load_run_summary] >> quality_check
```

Defaults: 3 retries, 2-minute delay with exponential backoff, 30-minute timeout per task.

### S3 layout

| Prefix | Written by | Contents |
|--------|------------|----------|
| `raw/date=D/` | `extract` | One raw CSV per satellite |
| `transformed/date=D/` | `transform` | `clean.csv`, `rejected.csv`, `run_summary.json` |
| `tables/date=D/` | `split` | `fact_fire_detection.csv`, `rejected_detections.csv`, `run_summary.csv` |
| `reference/` | `prepare_countries.py` | `countries.csv`, `dim_country.csv`, `dim_satellite.csv` |

### Loading and idempotency

`load_table.sql` is one template rendered per table. In a single transaction it deletes that `BATCH_ID`'s rows, then runs `COPY INTO` from `tables/date=D/<table>.csv` with `FORCE = TRUE` and `ON_ERROR = ABORT_STATEMENT`. Re-running a date replaces its data and never duplicates it. If the copy fails, the delete rolls back.

All three table files are **always written, even when empty** (header only), so every load finds its file. The file format has `SKIP_HEADER = 1`, so a header-only file loads zero rows without error. A day with no rejects shows `rows_rejected = 0` in `RUN_SUMMARY` and no rows in `REJECTED_DETECTIONS`.

### Quality checks

`quality_checks.sql` returns four booleans for the run date; any false or null fails the task:

- fact row count equals `RUN_SUMMARY.ROWS_CLEAN`
- rejected row count equals `RUN_SUMMARY.ROWS_REJECTED`
- no duplicate `DETECTION_ID`
- no nulls in key columns, and all coordinates within valid ranges

### How credentials flow

| Connection | Used by | How |
|------------|---------|-----|
| `aws_default` | The three Python steps | The DAG renders the connection's key, secret and region into environment variables for each script, and boto3 picks them up |
| `snowflake_default` | The SQL tasks | Key-pair authentication through the Snowflake provider |

The Python scripts run in their own Poetry virtualenv with no Airflow installed, so they can't read Airflow connections themselves. The DAG's `script_task()` helper bridges this: it renders `{{ conn.aws_default.login }}` and friends into the task's environment at run time, so no AWS keys exist in code or in `.env`.

---

## Setup, first time

### 1. Prerequisites

Docker with Compose, Terraform, Poetry, an AWS account with an S3 bucket, a Snowflake account, and a NASA FIRMS API key (`MAP_KEY`).

### 2. Snowflake key pair

Terraform and Airflow authenticate to Snowflake with a key pair, not a password.

```bash
cd ~/.ssh
openssl genrsa 2048 | openssl pkcs8 topk8 -inform PEM -out snowflake_tf_snow_key.p8 -nocrypt
openssl rsa -in snowflake_tf_snow_key.p8 -pubout -out snowflake_tf_snow_key.pub
chmod 600 snowflake_tf_snow_key.p8
```

Attach the public key (the `.pub` contents without the `BEGIN`/`END` lines) to the `TERRAFORM_SVC` user, and run the one-off integration grant as `ACCOUNTADMIN`:

```sql
ALTER USER TERRAFORM_SVC SET RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN;
```

### 3. Environment files

**Root `.env`** (read by the scripts and passed to the Airflow containers):

```
BUCKET_NAME=<bucket>
BUCKET_REGION=<region>
MAP_KEY=<nasa firms api key>
```

AWS keys are **not** stored here. They live in the `aws_default` Airflow connection (step 7).

**`airflow/.env`** (read by Docker Compose):

```
FERNET_KEY=<generated key>
```

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 4. Build the infrastructure

```bash
cd terraform_snowflake
# create terraform.tfvars (see Terraform variables below)
terraform init
terraform plan  -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

### 5. Build the reference data (one-off)

```bash
cd python_scripts
poetry install
poetry run python prepare_countries.py
```

Writes `countries.csv`, `dim_country.csv` and `dim_satellite.csv` to `s3://<bucket>/reference/`. Re-run only if country boundaries or the satellite list change. The two dimension tables are static and not loaded by the DAG, so load them once:

```sql
COPY INTO FIRMS_DB.FIRMS.DIM_COUNTRY
FROM @FIRMS_DB.FIRMS.FIRMS_S3_STAGE/reference/
FILES = ('dim_country.csv')
FILE_FORMAT = (FORMAT_NAME = 'FIRMS_DB.FIRMS.FIRMS_CSV');

COPY INTO FIRMS_DB.FIRMS.DIM_SATELLITE
FROM @FIRMS_DB.FIRMS.FIRMS_S3_STAGE/reference/
FILES = ('dim_satellite.csv')
FILE_FORMAT = (FORMAT_NAME = 'FIRMS_DB.FIRMS.FIRMS_CSV');
```

### 6. Start Airflow

`docker-compose.yaml` mounts the private key into every Airflow container, read-only:

```yaml
- /Users/<you>/.ssh/snowflake_tf_snow_key.p8:/opt/airflow/secrets/rsa_key.p8:ro
```

The left side is the path on your machine, so edit it to match yours.

```bash
cd airflow
docker compose up airflow-init        # first time only: migrates the DB, creates the admin user
docker compose up -d --build
```

The UI is at <http://localhost:8080>. The default login is `airflow` / `airflow`.

### 7. Create the Airflow connections

In **Admin → Connections**, create both and click **Test** on each.

**`aws_default`**

| Field | Value |
|-------|-------|
| Connection Type | Amazon Web Services |
| AWS Access Key ID / Secret | the pipeline's IAM user keys |
| Extra | `{"region_name": "<region>"}` |

The region must be in Extra, because the DAG reads it from there.

**`snowflake_default`**

| Field | Value |
|-------|-------|
| Connection Type | Snowflake |
| Login | `TERRAFORM_SVC` |
| Password | empty (key-pair auth) |
| Schema | `FIRMS` |
| Extra | see below |

```json
{
  "account": "<org>-<account>",
  "warehouse": "FIRMS_WH",
  "database": "FIRMS_DB",
  "role": "SYSADMIN",
  "private_key_file": "/opt/airflow/secrets/rsa_key.p8"
}
```

`private_key_file` is the **container** path, the right side of the mount. The UI shows secrets as `***`, which is only redaction.

### 8. Run it

DAGs start paused. Unpause `firms_snowflake_pipeline` and it backfills from the start date, one day at a time.

---

## Running and operating

| Task | How |
|------|-----|
| Re-run one day | Clear that run's tasks in the UI. Start from `extract` or `split` if the S3 files need regenerating. Clearing only a load task reuses the existing files |
| Backfill a range | **Trigger → Backfill** in the UI |
| Run a script by hand | `cd python_scripts && poetry run python <script>.py --date YYYY-MM-DD` (needs `BUCKET_NAME`, `BUCKET_REGION`, `MAP_KEY` and standard `AWS_*` variables set locally) |
| Change Python code | Edit `python_scripts/`. The folder is mounted, so no rebuild is needed |
| Change dependencies | Update `pyproject.toml` / `poetry.lock`, then `docker compose build` |
| After editing `.env` | `docker compose up -d --force-recreate` (a plain restart doesn't reload it) |

Local run order for date `D`:

```bash
poetry run python extract.py --date D
poetry run python transform_data_script.py --date D
poetry run python split_data_into_tables.py --date D
```

---

## Verifying a run

1. **Airflow:** the run's graph shows all seven tasks green.
2. **S3:** `tables/date=D/` contains the three CSVs, including `rejected_detections.csv` even on a zero-reject day.
3. **Snowflake counts:**
   ```sql
   SELECT 'FACT' t, COUNT(*) FROM FIRMS_DB.FIRMS.FACT_FIRE_DETECTION
   UNION ALL SELECT 'REJECTED', COUNT(*) FROM FIRMS_DB.FIRMS.REJECTED_DETECTIONS
   UNION ALL SELECT 'SUMMARY', COUNT(*) FROM FIRMS_DB.FIRMS.RUN_SUMMARY;
   ```
4. **Run summary:** `SELECT * FROM FIRMS_DB.FIRMS.RUN_SUMMARY ORDER BY BATCH_ID;`
5. **Stage access:** `LIST @FIRMS_DB.FIRMS.FIRMS_S3_STAGE/tables/;`
6. **Terraform:** `terraform state list`, then `terraform plan` should end with "No changes."

---

## Reference: Python scripts

All scripts take `--date YYYY-MM-DD`, read and write S3, and take their settings from `config.py`.

### Steps

| Script | Role |
|--------|------|
| `prepare_countries.py` | One-off. Downloads the Natural Earth countries shapefile and writes the three `reference/` files |
| `extract.py` | Downloads one raw CSV per configured satellite from the FIRMS API (`VIIRS_NOAA20_NRT`, world, 1 day) |
| `transform_data_script.py` | Cleans, validates, filters, de-duplicates and enriches (below) |
| `split_data_into_tables.py` | Splits the transformed output into the fact, rejected and run-summary files |

### Transform stages

1. **Check:** type each column and record each row's first problem.
2. **Split:** rows with a problem go to `rejected.csv` with a `reject_reason`.
3. **Filter:** valid rows that aren't `NRT` latency are dropped. They are counted as "filtered", not errors.
4. **Dedupe:** exact repeats removed. Grain is (detected time, latitude, longitude, satellite), so two satellites seeing the same fire stay as two observations.
5. **Enrich:** point-in-polygon join adds `country_iso3`, or `UNK` for ocean and unmatched points.
6. **Rename:** standard names and units (`l/n/h` → low/nominal/high, `D/N` → boolean `is_daytime`, `scan × track` → `pixel_area_km2`).

A sanity check fails the run if clean + rejected + filtered + duplicate rows don't add up to the input count. If a day has no fires, the transform writes empty files and a zero-count summary.

### Validation rules (`config.py`)

| Column | Rule |
|--------|------|
| `latitude` / `longitude` | −90 to 90 / −180 to 180 |
| `bright_ti4` / `bright_ti5` | 150–400 / 150–450 |
| `scan`, `track` | 0.1–1.5 |
| `frp` | ≥ 0 |
| `confidence` | one of `l`, `n`, `h` |
| `daynight` | `D` or `N` |
| `version` | matches `<number>.<number><latency letters>`; only `NRT` kept |
| any column | missing values (`""`, `nan`, `null`, `none`, `-`) are rejected |

### Configuration

Required environment variables: `BUCKET_NAME`, `BUCKET_REGION`, `MAP_KEY` (extract only), plus standard AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`). Under Airflow the AWS ones come from the `aws_default` connection. Locally, put them in `python_scripts/.env`.

Satellites are defined in `config.SATELLITES`. Adding one means re-running `prepare_countries.py` to refresh `dim_satellite.csv`.

---

## Reference: Airflow

### Docker stack

CeleryExecutor with PostgreSQL (metadata) and Redis (broker): `airflow-apiserver`, `scheduler`, `dag-processor`, `worker`, `triggerer`, plus a one-shot `airflow-init`.

The custom image (`airflow/Dockerfile`) extends `apache/airflow:3.3.2`, installs `apache-airflow-providers-snowflake` and Poetry, and pre-installs the `python_scripts` dependencies into a Poetry virtualenv, so each task starts quickly.

Mounted into the containers:

| Host | Container |
|------|-----------|
| `airflow/dags`, `logs`, `config`, `plugins` | `/opt/airflow/...` |
| `python_scripts/` | `/opt/airflow/python_scripts` |
| Snowflake private key | `/opt/airflow/secrets/rsa_key.p8` (read-only) |

### The DAG file

`script_task()` builds each Python step. It runs `cd /opt/airflow/python_scripts && poetry run python <script> --date {{ ds }}` and injects the AWS credentials from `aws_default` into the task's environment. `append_env=True` keeps `PATH` so `poetry` is found.

`load_task()` builds each Snowflake load from the same SQL template with a different `table` parameter.

---

## Reference: Terraform and Snowflake

### What gets created

| Module | Resources |
|--------|-----------|
| `foundation` | `FIRMS_DB` database, `FIRMS` schema, `FIRMS_WH` warehouse (`STANDARD`, `XSMALL`, auto-suspend 60 s, auto-resume, created suspended) |
| `tables` | `FACT_FIRE_DETECTION`, `REJECTED_DETECTIONS`, `RUN_SUMMARY`, `DIM_COUNTRY`, `DIM_SATELLITE` |
| `s3_integration` | `FIRMS_S3_INT` storage integration, `FIRMS_S3_STAGE` stage (bucket root), `FIRMS_CSV` file format |
| `aws_iam` | Role `snowflake-firms-s3-role` with a read-only policy on `tables/*` and `reference/*` |

### Tables

| Table | Loaded | Description |
|-------|--------|-------------|
| `FACT_FIRE_DETECTION` | Daily | One row per cleaned detection |
| `REJECTED_DETECTIONS` | Daily | Rows that failed validation, with `REJECT_REASON` |
| `RUN_SUMMARY` | Daily | One row of counts per run |
| `DIM_COUNTRY` | Once | Keyed on `COUNTRY_ISO3`, includes `UNK` |
| `DIM_SATELLITE` | Once | Keyed on `SATELLITE` |

Column definitions live in `schema/*.yaml`. To add a table, create its YAML file and add an entry to `local.tables` in `tables.tf`. The tables module turns that list into one `snowflake_table` per entry, keyed by name, so adding or removing a table doesn't touch the others.

### File format and stage

`FIRMS_CSV`: CSV, `,` delimiter, `"` optional enclosure, `skip_header = 1`, empty strings as `NULL`. It is a standalone format, so every `COPY INTO` names it. The stage points at the bucket root, so load paths include the folder (`tables/date=D/`).

### IAM and the circular dependency

Snowflake reads S3 by assuming the IAM role. The integration needs the role ARN, and the role's trust policy needs the integration's outputs. `s3_integration` builds the ARN from names (`arn:aws:iam::<account>:role/<role_name>`) rather than referencing the role, which breaks the cycle. Snowflake has no access to `raw/` or `transformed/` and cannot write.

### Terraform variables

| Variable | Default | Description |
|----------|---------|-------------|
| `organization_name`, `account_name` | none | Snowflake account identifier parts |
| `private_key_path` | none | Path to the `.p8` key for `TERRAFORM_SVC` |
| `aws_account_id` | none | 12-digit AWS account ID |
| `bucket_name` | none | S3 bucket |
| `database_name` | `FIRMS_DB` | |
| `schema_name` | `FIRMS` | |
| `warehouse_name` | `FIRMS_WH` | |
| `warehouse_size` | `XSMALL` | |
| `aws_region` | `eu-west-2` | |
| `iam_role_name` | `snowflake-firms-s3-role` | |

---

## Design logic and trade-offs

### Guiding principles

1. **Every step re-runnable for any date.** A failure at any stage should be fixable by clearing tasks, with no manual cleanup.
2. **Keep the raw data.** Whatever the transform does, `raw/` stays in S3, so a bad rule can be fixed and the data reprocessed.
3. **Bad rows are data, not exceptions.** Rows that fail validation are kept and explained, not silently dropped or allowed to crash the run.
4. **Least privilege and no stored passwords.** Key-pair auth, a read-only IAM role scoped to two prefixes, and credentials injected at run time.
5. **Simple first.** One schema, one warehouse, one satellite, daily batches. Add complexity only when a real need appears.

### Summary

| Decision | Chosen | Main alternative | Biggest trade-off |
|----------|--------|------------------|-------------------|
| Where to transform | Python, before Snowflake (ETL) | Load raw, transform in Snowflake (ELT/dbt) | Logic lives outside the warehouse |
| Step handoff | S3 keyed by date | XCom or local disk | Extra S3 reads and writes per step |
| Running the scripts | `BashOperator` + Poetry virtualenv | `PythonOperator`/hooks, `DockerOperator` | Credentials must be bridged into the subprocess |
| Load strategy | Delete-then-copy per `BATCH_ID` | `MERGE` on a key | Rewrites a whole day on each rerun |
| Empty days | Always write header-only files | Skip the load when a file is missing | A file exists even when it has nothing in it |
| Quality gate | Post-load assertions | Pre-load validation or staging tables | A bad batch is in the tables until fixed |
| Invalid rows | Reject table, run continues | Fail the run | A spike in rejects doesn't stop anything |
| Table management | Terraform + YAML | Migration tool (schemachange, dbt) | Column changes can force a table rebuild |
| S3 access | Storage integration + IAM role | Keys in the stage | More setup, and the circular dependency |
| Scheduling | Daily batch, `catchup=True`, one run at a time | Streaming or hourly | Data is up to a day behind |

### Transform in Python, not in Snowflake

**Choice.** Validation, filtering, de-duplication and the country join happen in `transform_data_script.py`. Snowflake receives clean tables.

**Why.** The point-in-polygon join and the row-by-row "first problem" reason are easier to write and test in pandas. The load SQL stays trivial (`COPY INTO`), and Snowflake never sees invalid rows.

**Cost.** The business logic isn't visible in the warehouse, and analysts can't change a rule with SQL. Because rejects and raw data are kept, nothing is lost, but reprocessing means re-running Python.

**Revisit when** transformations multiply or analysts need to own them. The natural move is to load `clean.csv` into a staging table and model from there with dbt.

### S3 as the handoff between steps

**Choice.** Each step reads and writes `raw/`, `transformed/` or `tables/` under `date=D/`. Nothing passes through XCom.

**Why.** With CeleryExecutor, consecutive tasks may run on different workers, so local files can't be shared. XCom is meant for small values, not datasets. S3 gives every step a durable, inspectable input and output, and the date makes reruns deterministic.

**Cost.** Extra reads and writes, and S3 holds three copies of each day's data. At this volume that costs very little. Add a lifecycle rule on `raw/` and `transformed/` if storage grows.

### Scripts run as subprocesses in their own Poetry environment

**Choice.** `BashOperator` runs `poetry run python <script>`. The dependencies are installed in a separate virtualenv.

**Why.** pandas, geospatial libraries and Airflow's own pinned dependencies can conflict. Keeping them apart avoids that, and the same scripts run unchanged on a laptop with no Airflow.

**Cost.** Airflow connections aren't visible to the scripts, so `script_task()` renders them into environment variables. Output reaches Airflow only as logs, with no structured return values, and each task pays a short startup cost.

**Alternatives.** `PythonOperator` or `@task` with `S3Hook` removes the credential bridge but forces the dependencies into the Airflow image. `DockerOperator` or `KubernetesPodOperator` gives stronger isolation at the cost of more infrastructure.

### Idempotent loads: delete, then copy

**Choice.** Each load runs in a transaction: `DELETE ... WHERE BATCH_ID = D`, then `COPY INTO ... FORCE = TRUE`.

**Why.** Any run can be repeated safely. If the copy fails, the delete rolls back. `FORCE = TRUE` is needed because Snowflake otherwise remembers loaded files and skips a reload, so the delete is what prevents duplicates.

**Cost.** Each rerun rewrites that day's rows. That is fine at daily-batch scale, but it creates micro-partition churn on very large tables. There is also no protection against the same detection appearing in two different batches.

**Revisit when** batches overlap or arrive late. Switch to `MERGE` on `DETECTION_ID`, or load into a staging table and merge.

### Rejects are kept, the run continues

**Choice.** Invalid rows go to `REJECTED_DETECTIONS` with a `reject_reason`. The run still succeeds. The transform fails only if the row buckets don't add up to the input count.

**Why.** One bad value shouldn't block a day's fire data, and a reject table makes problems easy to audit.

**Cost.** A broken upstream feed could reject most of a day and the pipeline would still go green. Nothing currently alerts on the reject rate. The `RUN_SUMMARY` row (`rows_rejected` against `rows_in`) holds the numbers, so adding a threshold check to `quality_checks.sql` would cover this.

### Always write all three table files

**Choice.** `split_data_into_tables.py` writes `rejected_detections.csv` even when it has no rows, as a header-only file.

**Why.** It keeps the load tasks identical for every day. An earlier version deleted the file when empty, so `COPY INTO ... FILES = (...)` failed with "file not found" on any zero-reject day, and retries couldn't fix it.

**Cost.** Empty files in S3, and a day with no rejects leaves no row in `REJECTED_DETECTIONS`. `RUN_SUMMARY` is the record that the day was processed.

### Quality checks run after the load

**Choice.** `quality_check` runs once all three loads finish and compares loaded rows to the summary, checks for duplicate IDs, and checks keys and coordinates.

**Why.** It tests what actually landed in Snowflake, including load problems that checking the files alone would miss.

**Cost.** By the time it fails, the bad batch is already in the tables. Rerunning the date replaces it, but anything querying in between could see the bad data.

**Revisit when** consumers depend on the tables continuously. Load into staging tables, check there, then swap or insert.

### Terraform manages tables from YAML

**Choice.** `schema/*.yaml` feeds a single `snowflake_table` resource via `for_each`, keyed by table name.

**Why.** Adding a table is one YAML file and one list entry. Keying by name means adding or removing a table never disturbs the others.

**Cost.** Terraform is weaker at managing data-bearing objects. Changing a column type can force the table to be destroyed and recreated, which drops its data. `snowflake_table` is also still a preview resource. Add `prevent_destroy` on the tables, or move table DDL to a migration tool and keep Terraform for infrastructure.

### Storage integration and IAM role for S3

**Choice.** The stage uses a storage integration, which assumes an IAM role limited to `tables/*` and `reference/*`.

**Why.** No AWS keys are stored in Snowflake, access is read-only, and Snowflake can't see `raw/` or `transformed/`.

**Cost.** More moving parts. The integration needs the role ARN while the role's trust policy needs the integration's generated values, so the ARN is built from names to break the cycle. The trust policy should also use the integration's external ID.

### Authentication

**Choice.** Key-pair authentication for Snowflake, and an `aws_default` connection holding the pipeline's AWS credentials.

**Why.** No Snowflake password exists anywhere, and AWS keys are held by Airflow instead of sitting in files or code.

**Cost.** The private key must be mounted into the containers, and key rotation is manual. Airflow currently connects as `TERRAFORM_SVC` with `SYSADMIN`, which is broader than loading needs. In production, use a dedicated low-privilege user, an IAM role, and a secrets backend.

### Scheduling

**Choice.** `@daily`, `catchup=True`, `max_active_runs=1`, 3 retries with exponential backoff.

**Why.** Daily partitions match how the data is organised. One run at a time makes backfills ordered and keeps load transactions from overlapping. Retries absorb transient failures such as an API timeout or a Snowflake hiccup.

**Cost.** Data is up to a day behind, and a backfill of many days is serial and slow. Retries can't fix a deterministic problem (a missing file, a bad row), so such a failure only appears after the retry delays have passed.

### Data model choices

- **Grain is (detected time, latitude, longitude, satellite).** The same fire seen by two satellites is two observations. This preserves each sensor's view but means a naive count can double-count a fire.
- **Country is assigned at transform time** by a point-in-polygon join against fixed boundaries. Points in no polygon (ocean, gas flares, coastline gaps) become `UNK`. If boundaries change, past data needs reprocessing.
- **NRT only.** Valid rows with another latency class are filtered out and counted. Near-real-time detections are provisional and may differ from later reprocessed data.
- **No date dimension.** The fact table carries a full UTC timestamp, and Snowflake's `DATE_TRUNC` and `DAYNAME` cover what a calendar table would.

### Infrastructure sizing

One schema and one `XSMALL` warehouse keep things simple. The 60-second auto-suspend limits idle cost, though Snowflake bills a minimum of 60 seconds each time a warehouse resumes. Loads and analytical queries share a warehouse, which is fine now. Split them if queries start slowing loads. The local stack uses CeleryExecutor, Postgres and Redis, which is heavier than a laptop needs, but it matches a production topology.

### Out of scope, and the likely next steps

| Gap | Likely next step |
|-----|------------------|
| No alerting on failure or reject rate | Airflow failure callbacks or email, plus a reject-rate check |
| Local Terraform state | Remote backend (S3 with locking) |
| Admin-level Airflow user | Dedicated Airflow user and role |
| Post-load quality gate | Staging tables, or Snowflake data metric functions |
| Late or overlapping batches | `MERGE` on `DETECTION_ID` |
| Analytics layer | dbt models over the fact and dimension tables |
| S3 growth | Lifecycle rules on `raw/` and `transformed/` |

---

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Snowflake connection test fails with a key error | Wrong `private_key_file` (must be the in-container path), missing mount, or public key not attached to the user |
| AWS errors such as `InvalidAccessKeyId` or `NoCredentialsError` | `aws_default` missing, keys wrong, or `region_name` missing from its Extra. Check Rendered Templates on the task |
| `AccessDenied` on S3 | The IAM user lacks permission on the bucket |
| `load_*` fails with "file not found" | The file is missing from `tables/date=D/`. Re-run `split` for that date |
| `load_*` retries then fails | Retries can't fix a missing file or a bad row. Read the end of the task log |
| `quality_check` fails | Run its sub-queries by hand to find the false assertion. Compare `RUN_SUMMARY` to table counts for that `BATCH_ID` |
| `poetry: command not found` in a task | `append_env=True` was dropped, so `PATH` was replaced |
| `LIST @FIRMS_S3_STAGE/tables/` access error | IAM role ARN or trust policy doesn't match the integration. Run `DESC INTEGRATION FIRMS_S3_INT` |
| Plan shows an in-place change on a table you didn't touch | Often provider-normalised attributes. Read the diff before applying |

---

## Security notes and known limitations

- **Never commit** `.env`, `airflow/.env`, `*.p8` keys, `terraform.tfvars` or `terraform.tfstate*`.
- The compose file is **local development only**: default admin password, example DAGs enabled, hard-coded key path.
- Airflow connects to Snowflake as `TERRAFORM_SVC` with `SYSADMIN`. A dedicated Airflow user and role limited to `FIRMS_WH` and the `FIRMS` schema would be safer.
- **ExternalId:** the trust policy should include an `sts:ExternalId` condition using the integration's external ID. Verify it does.
- Terraform state is local. A remote backend (for example S3 with locking) is recommended before sharing the repo. Consider `prevent_destroy` on the tables.
- No resource monitor caps warehouse spend; auto-suspend at 60 seconds limits idle cost.
