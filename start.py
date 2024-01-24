from app.db_tool import DuckdbTransform

dbname = 'transactions'
path = 'data/arquivos_csv'
tablename = 'transactions'
# Instantiate the class
duckdb_creator = DuckdbTransform(db_name = dbname, input_path= path, tbl_name = tablename)

# Call the start method to begin the process
duckdb_creator.start()