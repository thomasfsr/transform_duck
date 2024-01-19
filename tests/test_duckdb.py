from app.db_creator import DuckdbCreation
import os

def test_create_output_directory():
    duckdb_creator = DuckdbCreation()
    duckdb_creator.create_output_directory()
    assert os.path.exists(duckdb_creator.output_dir)

def test_connect_to_db():
    duckdb_creator = DuckdbCreation()
    conn = duckdb_creator.connect_to_db()
    assert conn is not None