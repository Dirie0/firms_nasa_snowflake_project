# ❄️ Snowflake ETL Pipeline - Build Challenge

**Your Mission:** Build a complete end-to-end ETL pipeline that processes CSV data and loads it into Snowflake using modern data engineering tools.

## 🎯 Project Objectives

By the end of this challenge, you will have built a production-ready data pipeline that:

- Transforms raw CSV data using Python
- Orchestrates tasks with Apache Airflow
- Manages infrastructure with Terraform
- Loads data into Snowflake data warehouse
- Runs everything in a containerized Docker environment

## 🧩 What You Need to Build

### Core Components

- **Data Transformation Scripts** — Clean and process raw CSV files
- **Airflow DAG** — Orchestrate the entire ETL workflow
- **Terraform Configuration** — Provision Snowflake infrastructure
- **Docker Environment** — Containerize Airflow and dependencies
- **Data Loading Process** — Move processed data into Snowflake

### Expected Architecture

```text
Raw CSV → Python Transform → S3 Storage → Snowflake Tables
                  ↑
        Orchestrated by Airflow DAG
```

## 📋 Prerequisites & Setup

### Required Accounts

- AWS Account (for S3 storage)
- Snowflake Account (free trial available)

### Tools to Install

- Docker Desktop
- Terraform
- Git

### Initial Project Structure

Create this folder structure to organize your work:

```text
your-snowflake-project/
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── plugins/
├── assets/datasets/
│   ├── Original_file/
│   ├── Transformed_full/
│   └── Transformed_tables/
├── python_scripts/
├── terraform/
├── docker-compose.yaml
├── .env
└── Makefile
```

## 🎯 Challenge Tasks

### Task 1: Environment Configuration

**Goal:** Set up your development environment and credentials.

**Hints:**

- Create an `.env` file for storing sensitive credentials
- You'll need AWS access keys and Snowflake connection details
- Research environment variable best practices
- Think about what services need which credentials

**Questions to Research:**

- How do you securely store credentials in containerized applications?
- What AWS permissions does your pipeline need for S3 operations?
- What's the proper format for Snowflake account identifiers?

# env file contains: aws credentials, api key
# local dev will use env, will move this to secrets manager later on
# will need permission to write to s3 bucket for raw data
# will need permission to read from s3 bucket to read raw data and transformed data
# will need permission to upload transformed data in s3 to snowflake think about snowflake connection details setup


### Task 2: Docker Environment Setup

**Goal:** Create a Docker Compose configuration that runs Airflow.

**Hints:**

- Airflow needs a database backend (PostgreSQL recommended)
- You'll need a webserver, scheduler, and worker services
- Research the official Airflow Docker Compose setup
- Consider using Redis for task queuing

**Questions to Research:**

- What are the minimum Airflow services needed for a functioning setup?
- How do you mount local directories into Docker containers?
- What environment variables does Airflow require?

# airflow scheduler- scheduler monitors all tasks and dags, once dependcies are complete task instances are triggered
# airflow-dag-processor- dag processor parses dag files
# airflow-api-server- api server is avaiable at localhost:8080 in my case
# airflow worker - worker executes at tasks given by the scheduler
# airflow triggerer- triggerer runs an event loop for deferrable tasks
# aiflow-init = initialization service
# postgres- the database
# redis broker that forwards messages from scheduler to worker 

# i mount local directories by editing this section in docker compose yaml file:
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins

# env variables for configurig airflow and applications
executor-celeryexecutor
metadata database - postgressql
messagebroker- redis
encryption-fer_netkey
jew_authentication- default secret set

env variables set in docker-compose.yaml
further config in config/airflow.cfg



### Task 3: Python Data Processing Scripts

**Goal:** Write scripts to transform and split your CSV data.

**Create these scripts:**

- `transform_data_script.py` — Clean and standardize raw CSV data
- `split_data_into_tables.py` — Split data into logical tables (orders, products, customers)
- `upload_data_to_s3_bucket.py` — Upload processed files to S3

**Hints:**

- Use pandas for data manipulation
- Think about data quality: handle missing values, standardize formats
- Consider using boto3 for AWS S3 operations
- Make your scripts reusable and configurable

**Questions to Research:**

- How do you handle different CSV formats and encodings?
- What's the best way to structure data for analytical queries?
- How do you efficiently upload large files to S3?

### Task 4: Terraform Infrastructure

**Goal:** Create Infrastructure as Code for your Snowflake resources.

**Resources to Provision:**

- Snowflake warehouse
- Database and schema
- Data tables
- External stage (for S3 integration)
- Storage integration

**Hints:**

- Research the Snowflake Terraform provider
- Start with basic resources, then add integrations
- Use variables for configurable values
- Consider using modules for reusability

**Questions to Research:**

- How do you authenticate Terraform with Snowflake?
- What's the difference between a stage and a storage integration?
- How do you structure Terraform files for maintainability?

### Task 5: Airflow DAG Creation

**Goal:** Build an Airflow DAG that orchestrates your entire pipeline.

**DAG Tasks Should Include:**

- Data transformation task
- Data splitting task
- S3 upload task
- Snowflake loading task

**Hints:**

- Use PythonOperator for running your scripts
- Research SnowflakeOperator for database operations
- Set up proper task dependencies
- Add error handling and retries

**Questions to Research:**

- How do you pass data between Airflow tasks?
- What's the best way to handle task failures?
- How do you configure Airflow connections for external services?

### Task 6: Airflow Connections Setup

**Goal:** Configure Airflow to connect to Snowflake and AWS.

**Connections Needed:**

- Snowflake connection
- AWS connection

**Hints:**

- Use the Airflow web UI **Admin > Connections**
- Research the required connection parameters for each service
- Test your connections before running the DAG

**Questions to Research:**

- What information does each connection type require?
- How do you troubleshoot connection issues?
- What's stored in the "Extra" field for connections?

### Task 7: SQL Loading Scripts

**Goal:** Write SQL scripts to load data from S3 into Snowflake tables.

**Hints:**

- Use Snowflake's `COPY` command
- Reference your external stage
- Handle different file formats and structures
- Consider using file format objects

**Questions to Research:**

- How does Snowflake's `COPY` command work with external stages?
- What file formats does Snowflake support?
- How do you handle schema evolution?

### Task 8: Testing & Validation

**Goal:** Verify your pipeline works end-to-end.

**Test Scenarios:**

- Pipeline runs without errors
- Data is correctly transformed
- Files upload to S3 successfully
- Data loads into Snowflake tables
- Data quality is maintained

**Questions to Research:**

- How do you monitor Airflow DAG execution?
- What are good data validation practices?
- How do you troubleshoot pipeline failures?

## 🔍 Research Areas

As you work through this challenge, you'll need to research:

### Docker & Containerization

- Docker Compose service definitions
- Volume mounting strategies
- Environment variable management
- Container networking

### Apache Airflow

- DAG structure and syntax
- Task operators and their uses
- Connection management
- Scheduling and monitoring

### Snowflake

- Data warehouse concepts
- External stages and integrations
- SQL best practices
- Security and access control

### Terraform

- Provider configuration
- Resource dependencies
- State management
- Variable usage

### Data Engineering Best Practices

- ETL vs ELT patterns
- Data quality validation
- Error handling strategies
- Performance optimization

## 🎯 Success Criteria

Your pipeline is successful when:

- [ ] Raw CSV files are automatically processed
- [ ] Transformed data is uploaded to S3
- [ ] Data is loaded into Snowflake tables
- [ ] The entire process is orchestrated by Airflow
- [ ] Infrastructure is managed through Terraform
- [ ] Pipeline can be easily deployed using Docker

## 💡 Getting Stuck?

When you encounter challenges:

- **Read the documentation** — Each tool has comprehensive docs
- **Check error logs** — Airflow, Docker, and Terraform provide detailed logs
- **Start simple** — Build incrementally, test each component
- **Use the community** — Stack Overflow, Reddit, tool-specific forums

## 🏆 Bonus Challenges

Once you have the basic pipeline working:

- [ ] Add data validation checks
- [ ] Implement incremental loading
- [ ] Create monitoring and alerting
- [ ] Add data lineage tracking
- [ ] Build a simple dashboard

## 📚 Recommended Resources

- Apache Airflow Documentation
- Snowflake Documentation
- Terraform Snowflake Provider
- Docker Compose Documentation
- AWS S3 Developer Guide

> **Remember:** The goal is to learn by doing. Take your time, experiment, and don't be afraid to break things — that's how you learn!


# personal notes

# dependent on the project 

# raw csv -> python script for transformation -> write to s3 storage -> snowflake tables read s3
# airflow orchestrates every step
# terraform to provision snowflake infra and aws infra
# docker to containerize airflo and depenencies 



# firms nasa api 
# api call to each of the four sensors for that day , load response as csv to s3 
# nasa_firms_raw/date/sensor/*.csv
# read from the s3 bucket
#  nasa_firms_raw/date/sensor/*.csv
# transformation logic 
# validate schema column names and data types 
# data validation row level (null check)
# combine the four 




# env file contains: aws credentials, api key
# local dev will use env, will move this to secrets manager later on
# will need permission to write to s3 bucket for raw data
# will need permission to read from s3 bucket to read raw data and transformed data
# will need permission to upload transformed data in s3 to snowflake think about snowflake connection details setup


# create docker compose environemnt that runs airflow 
# database backend with postgressql
# needs webserver. scheduler and worker service 
# redis for task queing 




# roles which permissions to give for the project



