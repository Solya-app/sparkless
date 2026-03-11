"""Confirm issue #634: createDataFrame with struct column (dict by field name)."""
import sys
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro_634").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("person", StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)]), True),
])
try:
    df = spark.createDataFrame([(1, {"name": "Alice", "age": 30}), (2, {"name": "Bob", "age": 25})], schema=schema)
    rows = df.collect()
    print("OK", rows[0]["person"]["name"])
    sys.exit(0)
except Exception as e:
    print("FAIL", type(e).__name__, str(e)[:200])
    sys.exit(1)
