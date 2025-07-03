from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from utils.gcs_helpers import unzip_file, clean_and_convert_xlsx
from config.python_variables import UNZIP_DIR, XLSX_FILENAME, GCS_DEST_PATH, GCS_BUCKET, GCS_URI, BQ_TABLE
import os
import pandas as pd

# Default arguments
default_args = {
    "start_date": datetime(2025, 1, 24),
    "catchup": False,  
    "depends_on_past": True,  
    "retries": 1,  
}

# DAG definition
with DAG(
    "xlsx_to_gcs_To_bq",
    default_args=default_args,
    schedule_interval="0 0 1 * *", 
) as dag:

    unzip_task = PythonOperator(
        task_id="unzip_file",
        python_callable=unzip_file
    )

    clean_convert_task = PythonOperator(
        task_id="clean_and_convert_to_csv",
        python_callable=clean_and_convert_xlsx
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_xlsx_to_gcs",
        src=os.path.join(UNZIP_DIR, XLSX_FILENAME),
        dst=GCS_DEST_PATH,
        bucket=GCS_BUCKET
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_xlsx_to_bq",
        configuration={
            "load": {
                "sourceUris": [GCS_URI],
                "destinationTable": {
                    "projectId": BQ_TABLE.split(".")[0],
                    "datasetId": BQ_TABLE.split(".")[1],
                    "tableId": BQ_TABLE.split(".")[2],
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    unzip_task >> clean_convert_task >> upload_to_gcs >> load_to_bq



what if the order of columns in xlsx is changed? can be automated reading columns names as schema fields

