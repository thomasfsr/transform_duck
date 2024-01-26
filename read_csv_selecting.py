import duckdb
import pandas as pd

conn = duckdb.connect('db_csvs.db')
reader_csv = '''CREATE TABLE sales_retail AS SELECT * FROM read_csv_auto([
                                            'data/arquivos_csv/daily_sales_retail_0.csv',
                                            'data/arquivos_csv/daily_sales_retail_1.csv',
                                            'data/arquivos_csv/daily_sales_retail_2.csv',
                                            'data/arquivos_csv/daily_sales_retail_3.csv',
                                            'data/arquivos_csv/daily_sales_retail_4.csv',
                                            ], union_by_name=True, filename = True)'''
try: 
    conn.execute(reader_csv)
except:
    pass
try:
    conn.execute("COPY sales_retail TO 'sales_retail.parquet' (FORMAT PARQUET)")
except:
    pass
conn.close()
df= pd.read_parquet('sales_retail.parquet')
print(df.info())
print(df.describe())

df2 = pd.read_parquet('database/df_transactions.parquet')
print(df2.info())
print(df2.describe())