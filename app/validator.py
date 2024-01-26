from pydantic import ValidationError
from typing import List
import pandas as pd
import os

from app.contract import SalesRetail

def validate_csv(file_path: str) -> List[ValidationError]:
    df = pd.read_csv(file_path)
    rows = df.to_dict(orient='records')
    errors = []
    
    for row_num, row in enumerate(rows, start=1):
        try:
            SalesRetail(**row)
        except ValidationError as e:
            for error in e.errors():
                errors.append(f"CSV: {file_path}, Row: {row_num}, {error}")
    
    return errors

def validator(folder: str) -> List[str]:
    passed_files = [] 
    
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder, filename)
            errors = validate_csv(file_path)
            
            if errors:
                print(f"Validation errors in file: {file_path}")
                #for error in errors:
                    #print(error)
            else:
                print(f"File {file_path} is valid.")
                passed_files.append(file_path)
    
    return passed_files
