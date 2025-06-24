from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import pandas as pd
from faker import Faker
import random
from google.cloud import storage
import io
import logging

# Configuration
PROJECT_ID = "airflow-demo-project"
BUCKET_NAME = "us-central1-airflow-demo-de-53ac71a6-bucket"
GCS_PATH = "data/sample3.csv"
BIGQUERY_DATASET = "nonexistent_dataset"
BIGQUERY_TABLE = "sample_data2"
TRANSFORMED_TABLE = "transformed_data"
LARGE_ORDERS_TABLE = "large_data"
FAILURE_LOG_PATH = "logs/load_failure.txt"

# Schema definition
schema_fields = [
    {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "order_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "product", "type": "STRING", "mode": "NULLABLE"},
]

# Function to generate sales data and upload to GCS
def generate_and_upload_sales_data(bucket_name, gcs_path, num_orders=500):
    fake = Faker()
    data = {
        "order_id": [i for i in range(1, num_orders + 1)],
        "customer_name": [fake.name() for _ in range(num_orders)],
        "order_amount": [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_orders)],
        "order_date": [fake.date_between(start_date="-30d", end_date="today") for _ in range(num_orders)],
        "product": [fake.word() for _ in range(num_orders)],
    }

    df = pd.DataFrame(data)

    # Convert to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(csv_data, content_type="text/csv")
    print(f"Data uploaded to gs://{bucket_name}/{gcs_path}")

# Function to handle failure of load_to_bigquery
def handle_bq_failure():
    message = f"[ERROR] BigQuery load failed at {datetime.utcnow().isoformat()} UTC\n"
    print(message)

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(FAILURE_LOG_PATH)
    if blob.exists():
        existing = blob.download_as_text()
        message = existing + message
    blob.upload_from_string(message)
    print(f"Failure log updated at gs://{BUCKET_NAME}/{FAILURE_LOG_PATH}")

# Default arguments
default_args = {
    "start_date": datetime(2025, 1, 24),
    "catchup": False,
    "depends_on_past": True,
    "retries": 1,
}

# DAG definition
with DAG(
    "sales_orders_to_bigquery_with_transformation",
    default_args=default_args,
    schedule_interval="@hourly",  # Every hour
    description="End-to-end sales pipeline with failure handling",
    tags=["bigquery", "gcs", "airflow", "demo"]
) as dag:

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    generate_sales_data = PythonOperator(
        task_id="generate_sales_data",
        python_callable=generate_and_upload_sales_data,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "gcs_path": GCS_PATH,
            "num_orders": 500,
        },
    )

    load_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/{GCS_PATH}"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": BIGQUERY_TABLE,
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_APPEND",
                "skipLeadingRows": 1,
                "schema": {"fields": schema_fields},
            }
        },
        location="US",
        project_id=PROJECT_ID,
    )

    transform_bq_data = BigQueryInsertJobOperator(
        task_id="transform_bigquery_data",
        configuration={
            "query": {
                "query": f"""
                    SELECT 
                        order_id,
                        customer_name,
                        order_amount,
                        CASE
                            WHEN order_amount < 100 THEN 'Small'
                            WHEN order_amount BETWEEN 100 AND 500 THEN 'Medium'
                            ELSE 'Large'
                        END AS order_category,
                        order_date,
                        product,
                        CURRENT_TIMESTAMP() AS load_timestamp
                    FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": TRANSFORMED_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
        project_id=PROJECT_ID,
    )

    large_order_data = BigQueryInsertJobOperator(
        task_id="large_order_data",
        configuration={
            "query": {
                "query": f"""
                    SELECT 
                        order_id,
                        customer_name,
                        order_amount,
                        order_date,
                        product,
                        CURRENT_TIMESTAMP() AS load_timestamp
                    FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
                    WHERE order_amount >= 500
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": LARGE_ORDERS_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        location="US",
        project_id=PROJECT_ID,
    )

    handle_bq_load_failure = PythonOperator(
        task_id="handle_bq_load_failure",
        python_callable=handle_bq_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Dependencies
    start_task >> generate_sales_data >> load_to_bigquery
    load_to_bigquery >> [transform_bq_data, handle_bq_load_failure]
    transform_bq_data >> large_order_data >> end_task
    handle_bq_load_failure >> end_task
