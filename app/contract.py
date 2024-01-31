from datetime import datetime

from pydantic import BaseModel, PositiveFloat, field_validator


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
