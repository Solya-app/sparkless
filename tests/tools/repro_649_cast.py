"""Confirm issue #649: cast invalid string to int -> Robin errors, PySpark returns null."""
import sys
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType
import sparkless.sql.functions as F

spark = SparkSession.builder.appName("repro_649").getOrCreate()
schema = StructType([StructField("text", StringType(), True)])
df = spark.createDataFrame([{"text": "x"}, {"text": "123"}], schema=schema)
try:
    result = df.select(F.col("text").cast("int").alias("num"))
    rows = result.collect()
    print("OK", rows[0]["num"], rows[1]["num"])
    sys.exit(0)
except Exception as e:
    print("FAIL", type(e).__name__, str(e)[:200])
    sys.exit(1)
