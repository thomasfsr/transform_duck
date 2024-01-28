from typing import List
import pandas as pd
import os
import pandera as pa
from app.contract_pandera import SalesRetailSchema

sales_retail_schema = SalesRetailSchema()

def validate_csv(file_path: str) -> List[str]:
    df = pd.read_csv(file_path)
    errors = []

    # Validate each row against the schema
    for row_num, row in df.iterrows():
        try:
            sales_retail_schema(row.to_dict())
        except pa.errors.SchemaError as e:
            for column, error in e.errors.items():
                errors.append(f"CSV: {file_path}, Row: {row_num + 1}, Column: {column}, {error}")
    
    return errors

def validator(folder: str) -> List[str]:
    passed_files = []  
    
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder, filename)
            errors = validate_csv(file_path)
            
            if errors:
                print(f"Validation errors in file: {file_path}")
                for error in errors:
                    print(error)
            else:
                print(f"File {file_path} is valid.")
                passed_files.append(file_path)
    
    print(f"Arquivos aceitos foram: {passed_files}")
    return passed_files
