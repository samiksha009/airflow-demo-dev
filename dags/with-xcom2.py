from airflow import DAG
from utils.gcs_helpers import generate_and_upload_sales_data
from utils.airflow_variables import BUCKET_NAME, PROJECT_ID, BIGQUERY_DATASET, JOINED_TABLE, BIGQUERY_TABLES, schema_fields
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Default arguments
default_args = {
    "start_date": datetime(2025, 1, 24),
    "catchup": False,  
    "depends_on_past": True,  
    "retries": 1,  
}

# DAG definition
with DAG(
    "with_xcom2",
    default_args=default_args,
    schedule_interval="0 0 1 * *", 
) as dag:

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    # Task 1: Generate sales data and upload to GCS
    # Task 2: Load data from GCS to BigQuery

    with TaskGroup(group_id="generate_and_load_data", tooltip = "Generate data and load to BigQuery") as gen_load_group:
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
                        "sourceUris": [f"{{{{ ti.xcom_pull(task_ids='generate_and_load_data.generate_sales_data_table{i}', key='gcs_url') }}}}"],
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
                gcp_conn_id = "gcp_connection",
            )

            generate_task >> load_task
            
    # Task 3: join loaded tables in BigQuery
    join_loaded_tables = BigQueryInsertJobOperator(
        task_id="join_loaded_tables",
        configuration={
            "query": {
                "query": "{% include 'sql/join_tables.sql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": JOINED_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",  
            }
        },
        location="US",
        project_id=PROJECT_ID,
        gcp_conn_id = "gcp_connection",
        params={
            "project_id": PROJECT_ID,
            "dataset": BIGQUERY_DATASET,
        },
    )


    # Dag dependencies
    start_task >> gen_load_group >> join_loaded_tables >> end_task