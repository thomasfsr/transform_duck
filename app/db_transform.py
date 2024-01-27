import os
import duckdb
from app.validator import validator

class DuckdbTransform:
    def __init__(
        self,
        db_name='sales',
        tbl_name='sales_retail',
        input_path='data/arquivos_csv',
        output_dir: str = 'db',
    ):
        self.db_name = db_name
        self.input_path = input_path
        self.output_dir = output_dir
        self.tbl_name = tbl_name
        self.parquet_file = f'df_{self.tbl_name}'

    def start(self):
        self.create_output_directory()
        conn = self.connect_to_db()
        try:
            passed_list = self.validation_call()
            self.merge_csv_files(conn, passed_list)
            self.save_as_parquet(conn)
        finally:
            self.close_connection(conn)

    def create_output_directory(self):
        self.create_output_directory_static(self.output_dir)

    def connect_to_db(self):
        return self.connect_to_db_static(self.output_dir, self.db_name)
    
    def validation_call(self):
        return self.validation_call_static(input_path=self.input_path)

    def merge_csv_files(self, conn, passed_list):
        self.merge_csv_files_static(conn, self.tbl_name, passed_list=passed_list)

    def save_as_parquet(self, conn):
        self.save_as_parquet_static(
            conn, self.parquet_file, self.output_dir, tbl_name= self.tbl_name
        )

    def close_connection(self, conn):
        self.close_connection_static(conn)
    
    @staticmethod
    def create_output_directory_static(output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    @staticmethod
    def connect_to_db_static(output_dir, db_name):
        return duckdb.connect(database=f'{output_dir}/{db_name}.db')
    
    @staticmethod
    def validation_call_static(input_path):
        passed_list = validator(input_path)
        return passed_list

    @staticmethod
    def merge_csv_files_static(conn, tbl_name, passed_list):
        query = f'''CREATE TABLE {tbl_name} AS SELECT transaction_time, product_name, price, store FROM read_csv_auto({passed_list},
                                                        filename=True, union_by_name=True)'''
        conn.execute(query)

    @staticmethod
    def save_as_parquet_static(conn, parquet_file, output_dir, tbl_name):
        query = f"COPY {tbl_name} TO '{output_dir}/{parquet_file}.parquet' (FORMAT PARQUET)"
        conn.execute(query)

    @staticmethod
    def close_connection_static(conn):
        conn.close()
