import pandas as pd
import re
import os
import traceback
from datetime import datetime
from utils.load_json import load_schema_config
from utils.validate_columns import validate_columns

def clean_and_convert_xlsx_to_csv(input_xlsx_path, output_csv_path, raw_gcs_upload_func, error_gcs_upload_func):

    error_logs = []
    raw_file_uploaded = False
    file_name = os.path.basename(input_xlsx_path)

    # load schema config dynamically
    config = load_schema_config('schema_config.json')
    # file_config = validate_columns(df, config, file_name, error_logs)
    column_types = file_config.get('column_types', {})

    try:
        # load excel
        df = pd.read_excel(input_xlsx_path, header=1, engine='openpyxl')

        # process columns
        for col in df.columns:
            col_type = column_types.get(col, 'string')  # default to string

            try:
                if col_type == 'date':
                    df = clean_date_column(df, col, error_logs, file_name)
                
                else:
                    continue 
                    # add other column type processings, if needed

            except Exception as e:
                error_logs.append({
                    "column": col,
                    "file": file_name,
                    "error": f"Vectorized column processing failed: {str(e)}"
                })
            

        # save cleaned file
        df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        print(f"Cleaned CSV saved to {output_csv_path}")

        # upload error logs, if any
        if error_logs:
            raw_gcs_upload_func(input_xlsx_path, f"raw_files/{file_name}")
            raw_file_uploaded = True

            error_log_path = f"{output_csv_path.replace('.csv', '')}_error_log.csv"
            pd.DataFrame(error_logs).to_csv(error_log_path, index=False, encoding='utf-8-sig')
            error_gcs_upload_func(error_log_path, f"error_logs/{os.path.basename(error_log_path)}")
            print(f"Issues found. Raw + error log uploaded.")

    except Exception as e:
        print("Fatal error:")
        traceback.print_exc()
        if not raw_file_uploaded:
            raw_gcs_upload_func(input_xlsx_path, f"raw_files/{file_name}")  