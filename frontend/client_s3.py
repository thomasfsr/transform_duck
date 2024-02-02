import os
import duckdb
from dotenv import load_dotenv

load_dotenv()
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
s3_bucket = os.getenv('s3_bucket')
table = 'sales_retail'

conn = duckdb.connect(database=':memory:')
conn.execute(f"INSTALL httpfs;")
conn.execute(f"LOAD 'httpfs';")
conn.execute(f"SET s3_region = 'us-east-2';")
conn.execute(f"SET s3_access_key_id = '{aws_access_key_id}';")
conn.execute(f"SET s3_secret_access_key = '{aws_secret_access_key}';")

parquet_file = f's3://{s3_bucket}/{table}.parquet'

def query_s3(query:str):
    result = conn.execute(query)
    return result.fetch_df()

def close_conn():
    conn.close()