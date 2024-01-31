from app.db_transform import DuckdbTransform

dbname = 'sales'
path = 'data/arquivos_csv'
tablename = 'sales_retail'

duckdb_creator = DuckdbTransform(
    db_name=dbname, input_path=path, tbl_name=tablename
)
duckdb_creator.start()
