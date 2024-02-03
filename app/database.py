import os

import duckdb
import psycopg
from dotenv import load_dotenv

load_dotenv()

postgres_conn_str = os.getenv('db_external_url')

conn_duck = duckdb.connect('db/sales.db')
df = conn_duck.execute('SELECT * FROM sales_retail').fetch_df()
conn_duck.close()

conn = psycopg.connect(postgres_conn_str, sslmode='require')
print('Before to_sql')

df.to_sql(
    con=conn,
    name='sales_retail',
    if_exists='replace',
    index=False,
    method='multi',
)
print('After to_sql')
conn.close()
