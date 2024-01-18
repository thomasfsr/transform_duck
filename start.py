from app.db_creator import DuckdbCreation

dbname = 'transaction'
path = 'data/arquivos_csv'
# Instantiate the class
duckdb_creator = DuckdbCreation(name_db= dbname, path= path)

# Call the start method to begin the process
duckdb_creator.start()