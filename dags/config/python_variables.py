# --------- CONFIG ---------
ZIP_PATH = "Users\samiksha.jangid\Downloads\Rakuten\airflow-demo-dev\assets\sample.zip"
UNZIP_DIR = "Users\samiksha.jangid\Downloads\Rakuten\airflow-demo-dev\assets\sample1.xlsx"
XLSX_FILENAME = "box_data.xlsx" 
CLEANED_CSV = "cleaned_data.csv"
GCS_BUCKET = "us-central1-airflow-demo-de-53ac71a6-bucket"
GCS_DEST_PATH = "data/data.xlsx"
BQ_TABLE = "airflow-demo-project-463501.demo_dataset.output_table"
GCS_URI = f"gs://{GCS_BUCKET}/{GCS_DEST_PATH}"
# --------------------------