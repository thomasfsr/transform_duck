from app.db_transform import DuckdbTransform, export_to_s3

dbname = 'sales'
path = '/mnt/c/users/Home/workspace/project001/transform_duck/data/arquivos_csv'
tablename = 'sales_retail'

duckdb_creator = DuckdbTransform(
    db_name=dbname, input_path=path, tbl_name=tablename
)
duckdb_creator.start()
parquet_file = f'db/{tablename}.parquet'
export_to_s3(parquet_file)
export_to_s3('db/sales.db')
