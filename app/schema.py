from pandas import Timestamp
import pandera as pa

class SchemaCRM(pa.SchemaModel):
    transaction_id = pa.Column(
        dtype="object",
        nullable=False,
        required=True,
    )
    transaction_time = pa.Column(
        dtype="datetime64[ns]",
        nullable=False,
        required=True,
    )
    ean = pa.Column(
        dtype="object",
        nullable=False,
        required=True,
    )
    product_name = pa.Column(
        dtype="object",
        nullable=False,
        required=True,
    )
    price = pa.Column(
        dtype="float64",
        checks=[
            pa.Check.greater_than(0),
        ],
        nullable=False,
        required=True,
    )
    store = pa.Column(
        dtype="int64",
        nullable=False,
        required=True,
    )
    pos_number = pa.Column(
        dtype="int64",
        nullable=False,
        required=True,
    )
    pos_system = pa.Column(
        dtype="object",
        nullable=False,
        required=True,
    )
    pos_version = pa.Column(
        dtype="float64",
        nullable=False,
        required=True,
    )
    pos_last_maintenance = pa.Column(
        dtype="datetime64[ns]",
        nullable=False,
        required=True,
    )
    operator = pa.Column(
        dtype="int64",
        nullable=False,
        required=True,
    )

    class Config:
        index = pa.Index(
            dtype="int64",
            nullable=False,
        )
