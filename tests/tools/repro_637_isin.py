"""Confirm issue #637: col.isin([]) -> 0 rows in PySpark."""
import sys
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro_637").getOrCreate()
df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
try:
    result = df.filter(F.col("id").isin([])).collect()
    n = len(result)
    if n == 0:
        print("OK 0 rows")
        sys.exit(0)
    else:
        print("FAIL expected 0 rows got", n)
        sys.exit(1)
except Exception as e:
    print("FAIL", type(e).__name__, str(e)[:200])
    sys.exit(1)
