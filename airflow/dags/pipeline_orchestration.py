from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

PROJECT_DIR = "/opt/airflow/python_scripts" 
RUN = f"cd {PROJECT_DIR} && poetry run python"

AWS_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_WAREHOUSE = "FIRMS_WH"
SQL_PARAMS = {
    "schema": "FIRMS_DB.FIRMS",
    "stage": "FIRMS_S3_STAGE",
    "file_format": "FIRMS_CSV",
}

AWS_ENV = {
    "AWS_ACCESS_KEY_ID": f"{{{{ conn.{AWS_CONN_ID}.login }}}}",
    "AWS_SECRET_ACCESS_KEY": f"{{{{ conn.{AWS_CONN_ID}.password }}}}",
    "AWS_DEFAULT_REGION": f"{{{{ conn.{AWS_CONN_ID}.extra_dejson.region_name }}}}",
}

default_args = {
    "owner": "dirie",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


def script_task(task_id, script):
    return BashOperator(
        task_id=task_id,
        bash_command=f"{RUN} {script} --date {{{{ ds }}}}",
        env=AWS_ENV,
        append_env=True,  
    )


def load_task(task_id, table):
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=SNOWFLAKE_CONN_ID,
        hook_params={"warehouse": SNOWFLAKE_WAREHOUSE},
        sql="load_table.sql",
        split_statements=True,
        autocommit=True,
        params={**SQL_PARAMS, "table": table},
    )


with DAG(
    dag_id="firms_snowflake_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 10, 15),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/dags/sql"],
    tags=["firms", "snowflake"],
) as dag:

    
    extract = script_task("extract", "extract.py")
    transform = script_task("transform", "transform_data_script.py")
    split = script_task("split", "split_data_into_tables.py") 

   
    load_fact = load_task("load_fact", "FACT_FIRE_DETECTION")
    load_rejected = load_task("load_rejected", "REJECTED_DETECTIONS")
    load_run_summary = load_task("load_run_summary", "RUN_SUMMARY")

    quality_check = SQLCheckOperator(
        task_id="quality_check",
        conn_id=SNOWFLAKE_CONN_ID,
        hook_params={"warehouse": SNOWFLAKE_WAREHOUSE},
        sql="quality_checks.sql",
        params=SQL_PARAMS,
    )

    extract >> transform >> split
    split >> [load_fact, load_rejected, load_run_summary] >> quality_check