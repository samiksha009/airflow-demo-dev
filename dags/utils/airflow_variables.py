from airflow.models import Variable
import json

# Airflow variables
BUCKET_NAME = Variable.get("BUCKET_NAME")
PROJECT_ID = Variable.get("PROJECT_ID")
BIGQUERY_DATASET = Variable.get("BIGQUERY_DATASET")
JOINED_TABLE = Variable.get("JOINED_TABLE")
BIGQUERY_TABLES = json.loads(Variable.get("BIGQUERY_TABLES"))
schema_fields = json.loads(Variable.get("SCHEMA_FIELDS"))