import os
import platform
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession


def build_spark_session(settings: Dict[str, Dict[str, str]]) -> SparkSession:
    """Create and configure a SparkSession from configuration settings."""
    spark_cfg = settings.get("spark", {})
    builder = (
        SparkSession.builder.appName(spark_cfg.get("app_name", "SparkApp"))
        .master(spark_cfg.get("master", "local[*]"))
        .config("spark.sql.shuffle.partitions", spark_cfg.get("shuffle_partitions", 200))
        .config("spark.executor.memory", spark_cfg.get("executor_memory", "2g"))
        .config("spark.executor.cores", spark_cfg.get("executor_cores", 2))
    )
    temp_dir = spark_cfg.get("local_temp_dir")
    if temp_dir:
        resolved_temp = Path(temp_dir).expanduser()
        resolved_temp.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", str(resolved_temp))
    return builder.getOrCreate()


def ensure_hadoop_environment(settings: Dict[str, Dict[str, str]]) -> None:
    """Make sure Windows runs with a valid Hadoop home to avoid winutils errors."""
    if platform.system().lower() != "windows":
        return

    env_cfg = settings.get("environment", {})
    configured_home = env_cfg.get("hadoop_home")
    hadoop_home = configured_home or os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if not hadoop_home:
        raise EnvironmentError(
            "HADOOP_HOME is required on Windows. Set environment.hadoop_home in the config or define the variable."
        )

    hadoop_path = Path(hadoop_home)
    if not hadoop_path.exists():
        raise FileNotFoundError(f"Hadoop home directory not found: {hadoop_path}")

    os.environ["HADOOP_HOME"] = str(hadoop_path)
    os.environ["hadoop.home.dir"] = str(hadoop_path)

    bin_path = hadoop_path / "bin"
    if bin_path.exists():
        current_path = os.environ.get("PATH", "")
        path_entries = current_path.split(";") if current_path else []
        if str(bin_path) not in path_entries:
            os.environ["PATH"] = f"{bin_path};{current_path}" if current_path else str(bin_path)
