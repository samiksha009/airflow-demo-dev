from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
from faker import Faker
import random
from google.cloud import storage
import io

# Configuration
PROJECT_ID = "airflow-demo-project-463501"
BUCKET_NAME = "us-central1-airflow-demo-de-53ac71a6-bucket"
# MULTI_GCS_PATHS = [
#     "data/multitable_sample_data1.csv",
#     "data/multitable_sample_data2.csv",
# ]
BIGQUERY_DATASET = "demo_dataset"
BIGQUERY_TABLES = {
    "data/multitable_sample_data1.csv": "multitable_sample_data1",
    "data/multitable_sample_data2.csv": "multitable_sample_data2",
}
JOINED_TABLE = "joined_multitable_sample_data"


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
    "joining_multiple_input_tables_optimized",
    default_args=default_args,
    schedule_interval="0 0 1 * *", 
) as dag:

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    # Task 1: Generate sales data and upload to GCS
    # Task 2: Load data from GCS to BigQuery

    with TaskGroup("gneerate_and_load_data", tooltip = "Generate data and load to BigQuery") as gen_load_group:
        for i, (gcs_path, table) in enumerate(BIGQUERY_TABLES.items(), start=1):
            generate_task = PythonOperator(
                task_id=f"generate_sales_data_table{i}",
                python_callable=generate_and_upload_sales_data,
                op_kwargs={
                    "bucket_name": BUCKET_NAME,
                    "gcs_path": gcs_path,
                    "num_orders": 10,
                },
            )

            load_task = BigQueryInsertJobOperator(
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

            generate_task >> load_task
            
    # Task 3: join loaded tables in BigQuery
    join_loaded_tables = BigQueryInsertJobOperator(
        task_id="join_loaded_tables",
        configuration={
            "query": {
                "query": f"""
                    SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.multitable_sample_data1`
                    UNION ALL
                    SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.multitable_sample_data2`
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": JOINED_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",  # Overwrites table on each run
            }
        },
        location="US",
        project_id=PROJECT_ID,
    )


    # Dag dependencies

    start_task >> gen_load_group >> join_loaded_tables >> end_task

'''
    BIGQUERY_TABLES = {
        "tbl1":{
            "GCS":"data/multitable_sample_data1.csv",
            "BQ":"multitable_sample_data1",
            },
        "tbl2":{
            "GCS":"data/multitable_sample_data2.csv",
            "BQ":"multitable_sample_data2"
            },
    }

    for table_name,table_item in BIGQUERY_TABLES:
        # do operator gcs
        
        # do operator for bq
        print(table_name)
        print(table_item["GCS"]) # data/multitable_sample_data1.csv


todo:
- multi_input_tables.py code optimization
- trigger rule testing (case error/operator failures)

'''







'''
read mySql data to gcs via on-premise airflow? row by row or partitions?
MySQLToGCSOperator()

splitting mysql db reading using limit in sql??

how mysql db works? cursor :sscursor??

'''