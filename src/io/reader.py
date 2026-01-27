from pathlib import Path
from typing import Dict
from pyspark.sql import DataFrame, SparkSession

from src.processing.schema import TRANSACTION_SCHEMA


def read_distributed_dataset(
    spark: SparkSession,
    config: Dict[str, Dict[str, str]],
    source_format: str = "csv",
) -> DataFrame:
    """Load a dataset in a distributed fashion using the configured schema and partitions."""
    paths = config.get("paths", {})
    partitions = config.get("spark", {}).get("input_partitions", 8)

    if source_format == "csv":
        path = paths.get("csv_input")
        reader = (
            spark.read.option("header", "true")
            .option("mode", "PERMISSIVE")
            .schema(TRANSACTION_SCHEMA)
        )
    elif source_format == "parquet":
        path = paths.get("parquet_input")
        reader = spark.read.schema(TRANSACTION_SCHEMA)
    else:
        raise ValueError(f"Unsupported source format: {source_format}")

    if not path:
        raise FileNotFoundError(f"Input path missing for source format '{source_format}'")

    resolved_path = Path(path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Input data path not found: {resolved_path}")

    df = reader.format(source_format).load(str(resolved_path))
    return df.repartition(partitions)
