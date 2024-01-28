from datetime import datetime
import pandera as pa

class SalesRetailSchema(pa.SchemaModel):
    product_name: pa.Column[str]
    transaction_time: pa.Column[datetime]
    price: pa.Column[pa.Float, pa.Check(lambda x: x > 0, element_wise=True, error="Price must be positive")]
    store: pa.Column[int]

sales_retail_schema = SalesRetailSchema()

def validate_sales_retail(record):
    try:
        sales_retail_schema(record)
    except pa.errors.SchemaError as e:
        raise ValueError(str(e))