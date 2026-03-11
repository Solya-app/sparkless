"""Check aggregation result column names: PySpark uses avg(Value), Robin may use different."""
import sys
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("repro_agg").getOrCreate()
df = spark.createDataFrame([("a", 10.0), ("a", 20.0), ("b", 30.0)], ["key", "Value"])
grouped = df.groupBy("key").agg(F.avg("Value"))
try:
    rows = grouped.collect()
    r0 = rows[0]
    print("Row keys:", list(r0.asDict().keys()) if hasattr(r0, "asDict") else list(r0.keys()) if hasattr(r0, "keys") else str(r0))
    # PySpark uses "avg(Value)" as column name
    if "avg(Value)" in (r0.asDict() if hasattr(r0, "asDict") else dict(r0)):
        print("OK has avg(Value)")
        sys.exit(0)
    print("FAIL missing avg(Value), keys:", list(r0.asDict().keys()) if hasattr(r0, "asDict") else r0)
    sys.exit(1)
except Exception as e:
    print("ERROR", type(e).__name__, str(e)[:200])
    sys.exit(2)
