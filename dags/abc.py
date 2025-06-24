from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with models.DAG(
    dag_id="gcs_to_bq_demo_dag",
    schedule_interval=None,  # Manual trigger
    default_args=default_args,
    catchup=False,
    tags=["demo", "gcs", "bigquery"],
) as dag:

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket="us-central1-airflow-demo-de-53ac71a6-bucket",
        source_objects=["data/sample.csv"], 
        destination_project_dataset_table="airflow-demo-project-463501.demo_dataset.sample_data",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )