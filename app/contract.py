import os
from datetime import datetime
import pandas as pd

from pydantic import BaseModel, PositiveFloat, field_validator

from pandera import infer_schema

class SalesRetail(BaseModel):
    product_name: str
    transaction_time: datetime
    price: PositiveFloat
    store: int

    @field_validator('price')
    def validate_price(cls, v):
        if v <= 0:
            raise ValueError('O preço deve ser positivo')
        return v

def infer(df:pd.DataFrame, directory_path:str= 'schema'):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    schema = infer_schema(df)
    file_path = os.path.join(directory_path, "schema_inferred.py")
    with open(file_path, "w", encoding="utf-8") as arquivo:
        arquivo.write(schema.to_script())

if __name__ == "__main__":
    df = pd.read_csv('/mnt/c/users/Home/workspace/project001/transform_duck/data/arquivos_csv/daily_sales_retail_0.csv')
    infer(df)