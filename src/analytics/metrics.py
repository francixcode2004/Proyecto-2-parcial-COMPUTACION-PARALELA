from typing import Dict

from pyspark.sql import DataFrame, Window, functions as F
def total_sales_by_region(df: DataFrame) -> DataFrame:
    return df.groupBy("region").agg(F.sum("purchase_amount").alias("total_sales"))


def top_products(df: DataFrame, limit: int = 10) -> DataFrame:
    return (
        df.groupBy("product_id", "product_category")
        .agg(
            F.count("*").alias("units_sold"),
            F.sum("purchase_amount").alias("revenue"),
        )
        .orderBy(F.col("units_sold").desc(), F.col("revenue").desc())
        .limit(limit)
    )


def failed_ratio_by_region(df: DataFrame) -> DataFrame:
    failed = df.filter(F.col("transaction_status") == "Failed")
    total = df.groupBy("region").agg(F.count("*").alias("total_transactions"))
    failed_by_region = failed.groupBy("region").agg(F.count("*").alias("failed_transactions"))
    return (
        total.join(failed_by_region, on="region", how="left")
        .fillna({"failed_transactions": 0})
        .withColumn(
            "failed_ratio",
            F.round(F.col("failed_transactions") / F.col("total_transactions"), 4),
        )
    )


def failed_transactions_by_interval(df: DataFrame, minutes: int) -> DataFrame:
    interval_window = Window.partitionBy("region").orderBy("bucket_start")
    aggregated = (
        df.filter(F.col("transaction_status") == "Failed")
        .groupBy("region", F.window("transaction_datetime", f"{minutes} minutes").alias("time_window"))
        .agg(F.count("*").alias("failed_count"))
        .withColumn("bucket_start", F.col("time_window").getField("start"))
        .drop("time_window")
    )
    return (
        aggregated
        .withColumn("lagged_failed_count", F.lag("failed_count").over(interval_window))
        .orderBy("region", "bucket_start")
    )


def detect_spikes_simple(df: DataFrame, threshold: float) -> DataFrame:
    return df.filter(F.col("failed_ratio") >= threshold)


def detect_spikes_stddev(df: DataFrame, multiplier: float) -> DataFrame:
    if not df.head(1):
        return df.limit(0)

    stats = df.agg(F.avg("failed_ratio").alias("mean"), F.stddev("failed_ratio").alias("std")).collect()[0]
    mean_value = stats["mean"] if stats["mean"] is not None else 0.0
    std_value = stats["std"] if stats["std"] is not None else 0.0
    upper_bound = mean_value + multiplier * std_value if std_value else mean_value
    return df.filter(F.col("failed_ratio") >= upper_bound)


def detect_spikes_percentile(df: DataFrame, percentile: int) -> DataFrame:
    if not df.head(1):
        return df.limit(0)

    quantiles = df.approxQuantile("failed_ratio", [percentile / 100.0], 0.01)
    if not quantiles:
        return df.limit(0)

    percentile_value = quantiles[0]
    return df.filter(F.col("failed_ratio") >= percentile_value)


def total_purchase_reduce(df: DataFrame) -> float:
    """Totaliza compras usando APIs DataFrame para compatibilidad con PySpark + Python 3.14."""
    result = df.select(F.sum("purchase_amount").alias("total_amount")).collect()
    return result[0]["total_amount"] if result else 0.0


def status_percentage(df: DataFrame) -> Dict[str, float]:
    """Distribución por estado con operaciones enteramente DataFrame."""
    total_count = df.count()
    if total_count == 0:
        return {}

    status_rows = (
        df.groupBy("transaction_status")
        .agg((F.count("*") / F.lit(total_count)).alias("share"))
        .collect()
    )
    return {row.transaction_status: row.share for row in status_rows}
