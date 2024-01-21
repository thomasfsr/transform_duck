import duckdb
import os

class DuckdbTransform:
    def __init__(self, name_db:str='database', name_tbl: str = 'transactions', input_path:str='data/arquivos_csv'):
        self.name_db = name_db
        self.input_path = input_path
        self.parquet_file = f"df_{self.name_db}"
        self.output_dir = 'database'
        self.name_tbl = name_tbl

    def start(self):
        self.create_output_directory()
        conn = self.connect_to_db()
        self.merge_csv_files(conn)
        self.validation(conn)
        self.save_as_parquet(conn)
        self.close_connection(conn)

    def create_output_directory(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
          
    def connect_to_db(self):
        conn = duckdb.connect(database=f'{self.output_dir}/{self.name_db}.db')
        return conn
        
    def merge_csv_files(self, conn):
        conn.execute(f"CREATE TABLE {self.name_tbl} AS SELECT * FROM read_csv_auto('{self.input_path}/*.csv', filename=true)")

    def save_as_parquet(self, conn):        
        conn.execute(f"COPY cleansed_{self.name_tbl} TO '{self.output_dir}/{self.parquet_file}.parquet' (FORMAT PARQUET)")
    
    def close_connection(self, conn):
        conn.close()
    
    def validation(self, conn):
        validation_query = f'''CREATE TABLE cleansed_{self.name_tbl} AS SELECT * FROM {self.name_tbl} WHERE price > 0 '''
        conn.execute(validation_query)

