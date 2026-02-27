"""
SparkSession factory — used by both notebooks and the Streamlit app.
Import with:  from src.spark_session import get_spark
"""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "CS570-MovieLens") -> SparkSession:
    """Return (or reuse) a local SparkSession."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")   # keep it light locally
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")            # suppress INFO noise
    return spark
