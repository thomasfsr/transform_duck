from app.db_transform import DuckdbTransform
from app.db_transform import export_to_s3

dbname = 'sales'
path = 'data/arquivos_csv'
tablename = 'sales_retail'

duckdb_creator = DuckdbTransform(
    db_name=dbname, input_path=path, tbl_name=tablename
)
duckdb_creator.start()
parquet_file = f'db/{tablename}.parquet'
export_to_s3(parquet_file)
export_to_s3('db/sales.db')
