import os
import shutil
import unittest
from app.db_transform import DuckdbTransform

class TestDuckdbTransform(unittest.TestCase):
    def setUp(self):
        self.duckdb_creator = DuckdbTransform(
            'database_test', 'tbl_test', 'tests/data_test', 'tests/output_test'
        )

    def tearDown(self):
        if os.path.exists(self.duckdb_creator.output_dir):
            shutil.rmtree(self.duckdb_creator.output_dir)  

    def test_create_output_directory(self):
        self.duckdb_creator.create_output_directory()
        assert os.path.exists(self.duckdb_creator.output_dir)
    
    def test_connect_to_db(self):
        self.duckdb_creator.create_output_directory()
        conn = self.duckdb_creator.connect_to_db()
        assert conn is not None