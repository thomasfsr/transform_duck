from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy.orm import sessionmaker
#import psycopg2

load_dotenv()

postgres_conn_str = os.getenv('db_external_url')
postgres_engine = create_engine(postgres_conn_str)

file = 'db/df_sales_retail.parquet'
df = pd.read_parquet(file)

df.to_sql('sales', postgres_engine, if_exists='replace', index=False)

postgres_engine.dispose()
