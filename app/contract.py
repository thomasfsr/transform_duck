from pydantic import BaseModel, PositiveFloat, validator
from datetime import datetime

class SalesRetail(BaseModel):
    product_name: str
    transaction_time: datetime
    price: PositiveFloat
    store: int

    @validator("price")
    def validate_price(cls, v):
        if v <= 0:
            raise ValueError("O preço deve ser positivo")
        return v
    
