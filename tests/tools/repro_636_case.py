"""Confirm issue #636: F.col('ID') when column is 'id' (case-insensitive)."""
import sys
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro_636").getOrCreate()
df = spark.createDataFrame([(1, "a", 100), (2, "b", 200)], ["id", "name", "value"])
try:
    result = df.select(F.col("ID")).collect()
    print("OK", len(result))
    sys.exit(0)
except Exception as e:
    print("FAIL", type(e).__name__, str(e)[:150])
    sys.exit(1)
