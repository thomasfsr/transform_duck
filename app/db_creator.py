import duckdb

class DuckdbCreation:
    def __init__(self, name_db:str='database', path:str='data/arquivos_csv'):
        self.name_db = name_db
        self.path = path
        self.parquet_file = f"df_{self.name_db}"
    
    def start(self):
        conn = self.create_db_conn()
        self.merge_csv_files(conn)
        self.save_as_parquet(conn)
        self.close_connection(conn) 
          
    def create_db_conn(self):
        conn = duckdb.connect(database=f'{self.name_db}.db')
        return conn
        
    def merge_csv_files(self, conn):
        conn.execute(f"CREATE TABLE transactions AS SELECT * FROM read_csv_auto('{self.path}/*.csv', filename=true)")

    def save_as_parquet(self, conn):
        #conn.execute(f"COPY transactions TO "{self.parquet_file}.parquet" ")
        conn.execute(f"COPY transactions TO '{self.parquet_file}.parquet' (FORMAT PARQUET)")
    
    def close_connection(self, conn):
        conn.close()


