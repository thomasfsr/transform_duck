import os
import duckdb
import pandas as pd
from pandera import infer_schema

def infer(df:pd.DataFrame, directory_path:str= 'infered_schema'):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    schema = infer_schema(df)
    file_path = os.path.join(directory_path, "schema_inferred.py")
    with open(file_path, "w", encoding="utf-8") as arquivo:
        arquivo.write(schema.to_script())

if __name__ == "__main__":
    con = duckdb.connect()
    df = con.execute("select * from read_csv('/mnt/c/users/w0504970/workspace/project001/transform_duck/data/arquivos_csv/daily_sales_retail_0.csv', AUTO_DETECT=TRUE)")
    df = df.fetch_df()
    infer(df)