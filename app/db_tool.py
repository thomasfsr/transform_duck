import duckdb
import os

class DuckdbTransform:
    def __init__(self, db_name:str='database', tbl_name: str = 'table', input_path:str='data/arquivos_csv'):
        self.db_name = db_name
        self.input_path = input_path
        self.parquet_file = f"df_{self.db_name}"
        self.output_dir = 'database'
        self.tbl_name = tbl_name

    def start(self):
        self.create_output_directory()
        conn = self.connect_to_db()
        self.merge_csv_files(conn)
        self.cleaner(conn)
        self.save_as_parquet(conn)
        self.close_connection(conn)

    def create_output_directory(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
          
    def connect_to_db(self):
        conn = duckdb.connect(database=f'{self.output_dir}/{self.db_name}.db')
        return conn
        
    def merge_csv_files(self, conn):
        conn.execute(f"CREATE TABLE {self.tbl_name} AS SELECT * FROM read_csv_auto('{self.input_path}/*.csv', filename=true)")

    def save_as_parquet(self, conn):        
        conn.execute(f"COPY cleansed_{self.tbl_name} TO '{self.output_dir}/{self.parquet_file}.parquet' (FORMAT PARQUET)")
    
    def close_connection(self, conn):
        conn.close()
    
    def cleaner(self, conn):
        cleaner_query = f'''CREATE TABLE cleansed_{self.tbl_name} AS SELECT * FROM {self.tbl_name} WHERE price > 0 '''
        conn.execute(cleaner_query)

