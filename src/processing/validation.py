from typing import Dict, Tuple

from pyspark.sql import DataFrame, functions as F

ALLOWED_STATUSES = {"Success", "Failed", "Pending"}


def detect_nulls(df: DataFrame) -> Dict[str, int]:
    """Return a dictionary with null counts per column."""
    exprs = [F.sum(F.col(column).isNull().cast("int")).alias(column) for column in df.columns]
    row = df.agg(*exprs).collect()[0]
    return {column: row[column] for column in df.columns}


def validate_types(df: DataFrame, expected_types: Dict[str, str]) -> Dict[str, bool]:
    """Check whether DataFrame column data types match the expected specification."""
    actual_types = dict(df.dtypes)
    return {column: actual_types.get(column) == dtype for column, dtype in expected_types.items()}


def filter_invalid_records(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Filter out invalid records based on domain rules and return clean plus rejected records."""
    invalid_status_condition = ~F.col("transaction_status").isin(list(ALLOWED_STATUSES))
    invalid_purchase_condition = (F.col("purchase_amount") <= 0) | F.col("purchase_amount").isNull()

    invalid_rows = df.filter(invalid_status_condition | invalid_purchase_condition)
    clean_rows = df.filter(~(invalid_status_condition | invalid_purchase_condition))
    return clean_rows, invalid_rows
