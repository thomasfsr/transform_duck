from app.db_tools import DuckdbClass
import os

def test_create_output_directory():
    duckdb_creator = DuckdbClass()
    duckdb_creator.create_output_directory()
    assert os.path.exists(duckdb_creator.output_dir)

def test_connect_to_db():
    duckdb_creator = DuckdbClass()
    conn = duckdb_creator.connect_to_db()
    assert conn is not None