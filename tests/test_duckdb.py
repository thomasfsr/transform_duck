from app.db_tool import DuckdbTransform
import os

def test_create_output_directory():
    duckdb_creator = DuckdbTransform()
    duckdb_creator.create_output_directory()
    assert os.path.exists(duckdb_creator.output_dir)

def test_connect_to_db():
    duckdb_creator = DuckdbTransform()
    conn = duckdb_creator.connect_to_db()
    assert conn is not None