import pandas as pd
import boto3
from io import StringIO
from configparser import ConfigParser
from validator import validator_df
import duckdb

config = ConfigParser()
config.read('connections/aws_config.ini')

aws_access_key_id = config.get('Credentials', 'aws_access_key_id')
aws_secret_access_key = config.get('Credentials', 'aws_secret_access_key')
s3_bucket = config.get('S3', 's3_bucket')
s3_directory = config.get('S3', 's3_directory')

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_directory)
files = [obj['Key'] for obj in response.get('Contents', [])]

dfs = []

for file in files:
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=file)
        df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))

        errors = validator_df(df)
        
        if not errors:
            print(f"File {file} is valid. Concatenating...")
            dfs.append(df)
        else:
            print(f"Validation errors in file {file}:")
            for error in errors:
                print(error)
    
    except Exception as e:
        print(f"Error reading CSV file {file}: {e}")

if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    print("Concatenated DataFrame created.")
else:
    print("No valid files found to concatenate.")
    df.to_csv('data/final_df.csv')

duckdb.connect(database='db/sales_from_s3.db')
conn = duckdb.connect('db/sales_from_s3.db')
query = "CREATE TABLE sales AS SELECT * FROM read_csv('data/final_df.csv',filename=True)"
conn.execute(query)