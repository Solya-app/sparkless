"""Confirm issue #646: filter(string_expr) -> predicate must be Boolean, got String."""
import sys
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro_646").getOrCreate()
df = spark.createDataFrame(
    [{"name": "Alice", "dept": "IT", "salary": 50000}, {"name": "Bob", "dept": "HR", "salary": 60000}, {"name": "Charlie", "dept": "IT", "salary": 70000}]
)
try:
    result = df.filter("dept = 'IT'")
    rows = result.collect()
    print("OK", len(rows))
    sys.exit(0)
except Exception as e:
    print("FAIL", type(e).__name__, str(e)[:200])
    sys.exit(1)
