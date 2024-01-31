import os
from io import StringIO

import boto3
import duckdb
import pandas as pd
from dotenv import load_dotenv
from validator import validator_df

load_dotenv()
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
s3_bucket = os.getenv('s3_bucket')
s3_directory = os.getenv('s3_directory')

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_directory)
files = [obj['Key'] for obj in response.get('Contents', [])]

dfs = []

for file in files:

    response = s3.get_object(Bucket=s3_bucket, Key=file)
    df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))

    errors = validator_df(df)

    if not errors:
        print(f'File {file} is valid. Concatenating...')
        dfs.append(df)
    else:
        print(f'Validation errors in file {file}:')
        for error in errors:
            print(error)

if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    print('Concatenated DataFrame created.')
    final_df.to_parquet('data/final_df.parquet')
    duckdb.connect(database='db/sales_from_s3.db')
    conn = duckdb.connect('db/sales_from_s3.db')
    query = "CREATE TABLE sales AS SELECT * FROM read_parquet('data/final_df.parquet',filename=True)"
    conn.execute(query)
else:
    print('No valid files found to concatenate.')
