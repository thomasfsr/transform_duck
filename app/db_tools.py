import os
import duckdb

class DuckdbClass:
    def __init__(self, db_name='database', tbl_name='table', input_path='data/arquivos_csv', output_dir:str = 'database'):
        self.db_name = db_name
        self.input_path = input_path
        self.parquet_file = f"df_{self.db_name}"
        self.output_dir = output_dir
        self.tbl_name = tbl_name

    def start(self):
        self.create_output_directory_def()
        conn = self.connect_to_db_def()
        try:
            self.merge_csv_files_def(conn)
            self.cleaner_def(conn)
            self.save_as_parquet_def(conn)
        finally:
            self.close_connection_def(conn)

    def create_output_directory_def(self):
        self.create_output_directory(self.output_dir)

    def connect_to_db_def(self):
        return self.connect_to_db(self.output_dir, self.db_name)

    def merge_csv_files_def(self, conn):
        self.merge_csv_files(conn, self.tbl_name, self.input_path)

    def save_as_parquet_def(self, conn):
        self.save_as_parquet(conn, self.parquet_file, self.output_dir, self.tbl_name)

    def close_connection_def(self, conn):
        self.close_connection(conn)

    def cleaner_def(self, conn):
        self.cleaner(conn, self.tbl_name)

    @staticmethod
    def create_output_directory(output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    @staticmethod
    def connect_to_db(output_dir, db_name):
        return duckdb.connect(database=f'{output_dir}/{db_name}.db')

    @staticmethod
    def merge_csv_files(conn, tbl_name, input_path):
        query = f"CREATE TABLE {tbl_name} AS SELECT * FROM read_csv_auto('{input_path}/*.csv', filename=true)"
        conn.execute(query)

    @staticmethod
    def save_as_parquet(conn, parquet_file, output_dir, tbl_name):
        query = f"COPY {parquet_file} TO '{output_dir}/{parquet_file}.parquet' (FORMAT PARQUET)"
        conn.execute(query)

    @staticmethod
    def close_connection(conn):
        conn.close()

    @staticmethod
    def cleaner(conn, tbl_name):
        timestamp_query = f'ALTER TABLE {tbl_name} ALTER COLUMN transaction_time SET DATA TYPE TIMESTAMP'
        dash_query = f'CREATE TABLE df_{tbl_name} AS SELECT transaction_time, product_name, price, store  FROM {tbl_name} WHERE price > 0'
        conn.execute(timestamp_query)
        conn.execute(dash_query)
