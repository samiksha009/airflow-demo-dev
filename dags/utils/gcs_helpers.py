import pandas as pd
import random
import io
import os
from faker import Faker
from google.cloud import storage
import zipfile
from config.python_variables import ZIP_PATH, UNZIP_DIR, XLSX_FILENAME, CLEANED_CSV

def generate_and_upload_sales_data(bucket_name, gcs_path, num_orders=500, **kwargs):
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

    full_gcs_url = f"gs://{bucket_name}/{gcs_path}"
    kwargs['ti'].xcom_push(key='gcs_url', value=full_gcs_url)

    print(f"Data uploaded to {full_gcs_url}")


def unzip_file():
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(UNZIP_DIR)

    print(f"Unzipped files to {UNZIP_DIR}")

def clean_and_convert_xlsx():
    xlsx_path = os.path.join(UNZIP_DIR, XLSX_FILENAME)
    df = pd.read_excel(xlsx_path, header=1, engine='openpyxl')  # Skip first row, use second as header

    # Optional: Rename columns for consistency
    df.columns = ['no', 'str1', 'str2', 'int1', 'int2', 'num1', 'num2', 'num3', 'num4', 'dt1', 'dt2', 'dt3']

    # Clean num1: remove commas
    df['num1'] = df['num1'].astype(str).str.replace(",", "").astype(float)

    # Convert num4: "(2,323.07)" to -2323.07
    df['num4'] = df['num4'].astype(str) \
        .str.replace(",", "") \
        .str.replace("(", "-") \
        .str.replace(")", "") \
        .astype(float)

    # Convert dates
    for col in ['dt1', 'dt2', 'dt3']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # Save to CSV
    output_csv = os.path.join(UNZIP_DIR, CLEANED_CSV)
    df.to_csv(output_csv, index=False)
    print(f"CSV saved to {output_csv}")