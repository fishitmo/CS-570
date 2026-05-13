"""
SparkSession factory — used by both notebooks and the Streamlit app.
Import with:  from src.spark_session import get_spark
"""
import os
from pyspark.sql import SparkSession

# Windows requires winutils.exe to write files. Set HADOOP_HOME automatically
# if a local hadoop/bin/winutils.exe exists and the env var is not already set.
_WINUTILS_CANDIDATES = [
    r"C:\hadoop",
    r"C:\winutils",
    os.path.join(os.path.dirname(__file__), "..", "hadoop"),
]
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    for _candidate in _WINUTILS_CANDIDATES:
        _candidate = os.path.normpath(_candidate)
        if os.path.isfile(os.path.join(_candidate, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = _candidate
            break


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
