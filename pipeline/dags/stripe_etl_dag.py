"""
stripe_etl_dag.py
Airflow DAG: extract -> transform -> quality check -> load
Pulls Stripe TEST MODE charges daily, validates, and loads into DuckDB.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from extract import extract_charges
from transform import transform_charges
from quality_checks import run_quality_checks
from load import load_charges

default_args = {
    "owner": "anu",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _extract(**context):
    path = extract_charges(limit=100)
    context["ti"].xcom_push(key="raw_path", value=path)


def _transform(**context):
    raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="extract")
    df = transform_charges(raw_path)
    df.to_parquet("data/staging_charges.parquet")


def _validate(**context):
    import pandas as pd
    df = pd.read_parquet("data/staging_charges.parquet")
    run_quality_checks(df)


def _load(**context):
    import pandas as pd
    df = pd.read_parquet("data/staging_charges.parquet")
    load_charges(df)


with DAG(
    dag_id="stripe_charges_etl",
    description="Extract, validate, and load Stripe test-mode charges into DuckDB",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stripe", "etl", "data-quality"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=_extract)
    transform_task = PythonOperator(task_id="transform", python_callable=_transform)
    validate_task = PythonOperator(task_id="validate", python_callable=_validate)
    load_task = PythonOperator(task_id="load", python_callable=_load)

    extract_task >> transform_task >> validate_task >> load_task
