import pandas as pd
import numpy as np
import re

def clean_and_convert_xlsx_to_csv(input_xlsx_path, output_csv_path):

    # skip first row, use second as header
    df = pd.read_excel(input_xlsx_path, header=1, engine= 'openpyxl')

    # rename columns to column1, column2, column3 ....
    # df.columns = [f'columns{i+1}' for i in range(len(df.columns))]
    df.columns = ['no', 'str1', 'str2', 'int1', 'int2', 'num1', 'num2', 'num3', 'num4', 'num5', 'dt1', 'dt2', 'dt3']

    # define numeric columns to clean
    numeric_cols = ['num1', 'num2', 'num3', 'num4', 'num5']

    for col in numeric_cols:

        # remove commas
        df[col] = df[col].astype(str).str.replace(',', '', regex=False)

        # convert bracketed negatives like (123.02) to -123.02
        df[col] = df[col].apply(lambda x: -float(x.strip("()")) if re.fullmatch(r"\(\d+\.?\d*\)", x.strip()) else x)

        # convert to float, invalid one becomes NaN (also handles num5 division issue)
        df[col] = pd.to_numeric(df[col], errors = 'coerce')

        '''
        df['num4'] = df['num4'].astype(str) \
        .str.replace(",", "") \
        .str.replace("(", "-") \
        .str.replace(")", "") \
        .astype(float)
        '''

    # clean date columns
    date_cols = ['dt1', 'dt2', 'dt3']

    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors = 'coerce')

    
    # export to csv
    df.to_csv(output_csv_path, index= False, encoding = 'utf-8')
    print(f"CSV saved to {output_csv_path}")