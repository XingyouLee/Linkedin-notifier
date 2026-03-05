import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """Build a SparkSession for in-container local execution."""
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.showConsoleProgress", "false")
    )
    return builder.getOrCreate()
