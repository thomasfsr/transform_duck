import os
import shutil
import pytest
from app.db_tools import DuckdbClass

@pytest.fixture
def duckdb_creator(request):
    # Instantiate DuckdbClass
    duckdb_creator = DuckdbClass()

    # Create output directory
    duckdb_creator.create_output_directory('output_teste')

    # Define a finalizer to clean up the output directory after the test
    def fin():
        shutil.rmtree('output_teste')

    # Register the finalizer
    request.addfinalizer(fin)

    return duckdb_creator

def test_create_output_directory(duckdb_creator):
    assert os.path.exists(duckdb_creator.output_dir)

def test_connect_to_db(duckdb_creator):
    conn = duckdb_creator.connect_to_db('output_teste', 'db_name_teste')
    assert conn is not None
