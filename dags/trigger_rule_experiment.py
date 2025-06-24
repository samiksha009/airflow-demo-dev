from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import pandas as pd
from faker import Faker
import random
from google.cloud import storage
import io

# Configuration
PROJECT_ID = "airflow-demo-project-463501"
BUCKET_NAME = "us-central1-airflow-demo-de-53ac71a6-bucket"
MULTI_GCS_PATHS = [
    "data/trigger_test_sample_data1.csv",
    "data/trigger_test_sample_data2.csv",
]
BIGQUERY_DATASET = "demo_dataset"
BIGQUERY_TABLES = {
    "data/trigger_test_sample_data1.csv": "trigger_test_data1",
    "data/trigger_test_sample_data2.csv": "trigger_test_data2",
}

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


# Default arguments
default_args = {
    "start_date": datetime(2025, 1, 24),
    "catchup": False,  
    "depends_on_past": True,  
    "retries": 1,  
}

# DAG definition
with DAG(
    "trigger_rule_experiment_dag",
    default_args=default_args,
    schedule_interval="0 0 1 * *", 
) as dag:

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    # Task 1: Generate sales data and upload to GCS
    generate_tasks = {}

    for i, gcs_path in enumerate(MULTI_GCS_PATHS, start=1):
        task = PythonOperator(
            task_id=f"generate_sales_data_table{i}",
            python_callable=generate_and_upload_sales_data,
            op_kwargs={
                "bucket_name": BUCKET_NAME,
                "gcs_path": gcs_path,
                "num_orders": 10,
            },
        )
        generate_tasks[f"generate_sales_data_table{i}"] = task


    # Task 2: Load data from GCS to BigQuery
    load_tasks = {}

    for i, (gcs_path, table) in enumerate(BIGQUERY_TABLES.items(), start=1):
        task = BigQueryInsertJobOperator(
            task_id=f"load_to_bigquery_table{i}",
            configuration={
                "load": {
                    "sourceUris": [f"gs://{BUCKET_NAME}/{gcs_path}"],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": table,
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
        load_tasks[f"load_to_bigquery_table{i}"] = task


    # Task 3: Transform data in BigQuery
    trigger_rule_tests = {
        "all_success": TriggerRule.ALL_SUCCESS,
        "all_failed": TriggerRule.ALL_FAILED,
        "one_success": TriggerRule.ONE_SUCCESS,
        "all_done": TriggerRule.ALL_DONE,
    }
    
    join_test_tasks = {}

    for name, rule in trigger_rule_tests.items():
        join_task = BigQueryInsertJobOperator(
            task_id=f"join_with_{name}",
            trigger_rule=rule,
            configuration={
                "query": {
                    "query": f"""
                        SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.trigger_test_data1`
                        UNION ALL
                        SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.trigger_test_data2`
                    """,
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"joined_trigger_test_data_{name}",
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            location="US",
            project_id=PROJECT_ID,
        )
        join_test_tasks[f"join_with_{name}"] = join_task


    # Task dependencies

    for i in range(1, len(generate_tasks) + 1):
        start_task >> generate_tasks[f"generate_sales_data_table{i}"]
    
    for i in range(1, len(load_tasks) + 1):
        generate_tasks[f"generate_sales_data_table{i}"] >> load_tasks[f"load_to_bigquery_table{i}"]
    
    for name in join_test_tasks:
        load_tasks[f"load_to_bigquery_table{i}"] >> join_test_tasks[name]

    list(join_test_tasks.values()) >> end_task