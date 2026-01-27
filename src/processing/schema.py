from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               TimestampType, DoubleType)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), nullable=False),
        StructField("product_id", IntegerType(), nullable=False),
        StructField("product_category", StringType(), nullable=False),
        StructField("transaction_datetime", TimestampType(), nullable=False),
        StructField("purchase_amount", DoubleType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("transaction_status", StringType(), nullable=False),
    ]
)
