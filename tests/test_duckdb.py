import os

from app.db_transform import DuckdbTransform

def test_create_output_directory():
    duckdb_creator = DuckdbTransform(
        'database_test', 'tbl_test', 'tests/data_test', 'tests/output_test'
    )
    duckdb_creator.create_output_directory()
    assert os.path.exists(duckdb_creator.output_dir)

def test_connect_to_db():
    duckdb_creator = DuckdbTransform(
        'database_test', 'tbl_test', 'tests/data_test', 'tests/output_test'
    )
    conn = duckdb_creator.connect_to_db()
    assert conn is not None
