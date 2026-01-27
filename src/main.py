import argparse
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.analytics.metrics import (
    detect_spikes_percentile,
    detect_spikes_simple,
    detect_spikes_stddev,
    failed_transactions_by_interval,
    failed_ratio_by_region,
    status_percentage,
    top_products,
    total_purchase_reduce,
    total_sales_by_region,
)
from src.io.reader import read_distributed_dataset
from src.processing.schema import TRANSACTION_SCHEMA
from src.processing.validation import detect_nulls, filter_invalid_records, validate_types
from src.utils.config_loader import load_config
from src.utils.spark_session import build_spark_session, ensure_hadoop_environment


def report_spark_ui(spark: SparkSession) -> None:
    """Informar la URL del Spark UI para facilitar la verificación manual."""
    ui_url = spark.sparkContext.uiWebUrl  # type: ignore[attr-defined]
    if ui_url:
        print(f"🔗 Spark UI disponible en: {ui_url}")
    else:
        print("⚠️ Spark UI no está expuesto en esta ejecución (modo local sin puerto UI).")


def run_basic_verification(df: DataFrame) -> None:
    """Comparar resultados de un subconjunto reducido contra cálculos locales."""
    print("🧪 Verificación funcional básica con un subconjunto reducido de datos...")
    sample_df = df.limit(1000)
    sample_count = sample_df.count()

    if sample_count == 0:
        print("No hay registros para verificar.")
        return

    distributed_total = sample_df.agg(F.sum("purchase_amount").alias("total")).collect()[0]["total"]
    local_rows = sample_df.select("purchase_amount", "transaction_status").collect()
    manual_total = sum(row.purchase_amount for row in local_rows)
    manual_failed_ratio = sum(1 for row in local_rows if row.transaction_status == "Failed") / sample_count

    print(f"Total distribuido (Spark): {distributed_total}")
    print(f"Total manual (colección local): {manual_total}")
    print(f"Ratio de fallos manual para {sample_count} registros: {manual_failed_ratio:.4f}")


def configure_spark(spark: SparkSession, config: Dict) -> None:
    spark_cfg = config.get("spark", {})
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        spark_cfg.get("shuffle_partitions", 200)
    )


def persist_outputs(df: DataFrame, invalid_df: DataFrame, config: Dict) -> None:
    paths = config.get("paths", {})

    curated_path = paths.get("curated_output")
    rejected_path = paths.get("rejected_output")

    if not curated_path or not rejected_path:
        raise ValueError("Las rutas de salida deben estar definidas en el archivo de configuración")

    print("💾 Persistiendo datasets depurados e inválidos en almacenamiento distribuido...")

    df.write.mode("overwrite").parquet(curated_path)
    invalid_df.write.mode("overwrite").parquet(rejected_path)

    print("✅ Escritura distribuida completada correctamente.")


def run_quality_checks(df: DataFrame) -> None:
    print("🔎 Iniciando validaciones de calidad de datos...")

    expected = {
        field.name: field.dataType.simpleString()
        for field in TRANSACTION_SCHEMA.fields
    }

    null_report = detect_nulls(df)
    type_match = validate_types(df, expected)

    print("📌 Validación de valores nulos por columna:")
    print(null_report)

    print("📌 Validación de tipos de datos según el esquema definido:")
    print(type_match)


def run_analytics(df: DataFrame, config: Dict) -> None:
    print("⚙️ Ejecutando transformaciones distribuidas (filter, groupBy, agg, reduce)")
    print("▶️ Ejecutando acciones (count, collect, show, write)")

    df.cache()

    print("💰 Total de ventas por región:")
    sales_by_region = total_sales_by_region(df)
    sales_by_region.show(truncate=False)

    print("🏆 Top 10 productos más vendidos:")
    best_products = top_products(df)
    best_products.show(truncate=False)

    print("📉 Porcentaje de transacciones fallidas por región:")
    failed_ratio = failed_ratio_by_region(df)
    failed_ratio.show(truncate=False)

    print("🧮 Cálculo del monto total de compras usando agregación distribuida:")
    total_amount = total_purchase_reduce(df)
    print("Total de compras:", total_amount)

    print("📊 Distribución porcentual de estados de transacción:")
    status_share = status_percentage(df)
    print(status_share)

    interval_minutes = config.get("window", {}).get("failed_tx_interval_minutes", 60)
    print(f"⏱️ Conteo de transacciones fallidas por intervalos de {interval_minutes} minutos:")

    failed_by_interval = failed_transactions_by_interval(df, interval_minutes)
    failed_by_interval.show(truncate=False)

    spike_threshold = config.get("detection", {}).get("spike_threshold", 0.9)
    std_multiplier = config.get("detection", {}).get("std_multiplier", 2.0)
    percentile_value = config.get("detection", {}).get("percentile", 95)

    print("🚨 Detección de picos mediante umbral simple:")
    simple_spikes = detect_spikes_simple(failed_ratio, spike_threshold)
    simple_spikes.show(truncate=False)

    print("📊 Detección de picos usando media + desviación estándar:")
    std_spikes = detect_spikes_stddev(failed_ratio, std_multiplier)
    std_spikes.show(truncate=False)

    print(f"📈 Detección de picos usando el percentil {percentile_value}:")
    percentile_spikes = detect_spikes_percentile(failed_ratio, percentile_value)
    percentile_spikes.show(truncate=False)

    analytics_path = config.get("paths", {}).get("analytics_output")
    if analytics_path:
        print("💾 Guardando resultados analíticos en almacenamiento distribuido...")
        sales_by_region.write.mode("overwrite").parquet(f"{analytics_path}/sales_by_region")
        best_products.write.mode("overwrite").parquet(f"{analytics_path}/top_products")
        failed_ratio.write.mode("overwrite").parquet(f"{analytics_path}/failed_ratio")
        failed_by_interval.write.mode("overwrite").parquet(f"{analytics_path}/failed_by_interval")

    df.unpersist()
    print("✅ Análisis distribuido finalizado correctamente.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Análisis distribuido de transacciones usando Apache Spark"
    )
    parser.add_argument(
        "--config",
        default="config/app_config.yaml",
        help="Ruta al archivo de configuración"
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="parquet",
        help="Formato del dataset de entrada"
    )

    args = parser.parse_args()

    config = load_config(args.config)

    ensure_hadoop_environment(config)

    spark = build_spark_session(config)
    configure_spark(spark, config)
    report_spark_ui(spark)

    print("📥 Iniciando lectura distribuida del dataset...")
    df = read_distributed_dataset(spark, config, args.format).cache()

    print("📐 Esquema explícito aplicado al dataset:")
    df.printSchema()

    total_records = df.count()
    print("📊 Total de registros ingeridos:", total_records)

    run_quality_checks(df)

    print("🧹 Filtrando registros inválidos...")
    clean_df, invalid_df = filter_invalid_records(df)

    print("✔️ Registros válidos:", clean_df.count())
    print("❌ Registros inválidos:", invalid_df.count())

    persist_outputs(clean_df, invalid_df, config)

    run_analytics(clean_df, config)
    run_basic_verification(clean_df)

    print("🎯 Proceso completo: lectura distribuida, validación, análisis y detección de picos ejecutados correctamente.\n")
    input("Presione Enter para salir...")
    spark.stop()


if __name__ == "__main__":
    main()


