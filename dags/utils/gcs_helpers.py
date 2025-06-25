import pandas as pd
import random
import io
from faker import Faker
from google.cloud import storage
from airflow.models import Variable
import json

# Airflow variables
BUCKET_NAME = Variable.get("BUCKET_NAME")
PROJECT_ID = Variable.get("PROJECT_ID")
BIGQUERY_DATASET = Variable.get("BIGQUERY_DATASET")
JOINED_TABLE = Variable.get("JOINED_TABLE")
BIGQUERY_TABLES = json.loads(Variable.get("BIGQUERY_TABLES"))
schema_fields = json.loads(Variable.get("SCHEMA_FIELDS"))


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