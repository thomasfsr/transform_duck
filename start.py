from app.db_tools import DuckdbClass

dbname = 'transactions'
path = 'data/arquivos_csv'
tablename = 'transactions'
# Instantiate the class
duckdb_creator = DuckdbClass(
    db_name=dbname, input_path=path, tbl_name=tablename
)

# Call the start method to begin the process
duckdb_creator.start()
