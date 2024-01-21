from app.db_tool import DuckdbTransform

dbname = 'transaction'
path = 'data/arquivos_csv'
# Instantiate the class
duckdb_creator = DuckdbTransform(name_db= dbname, input_path= path)

# Call the start method to begin the process
duckdb_creator.start()