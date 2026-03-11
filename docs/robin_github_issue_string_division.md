# [PySpark parity] Division between string columns: div operation not supported for dtypes str and str

**Upstream issue:** [#683](https://github.com/eddiethedean/robin-sparkless/issues/683)

## Summary

When dividing two string-typed columns (e.g. `F.col("a") / F.col("b")` where both are strings), PySpark can attempt numeric coercion and fail with a clear error or return null. The robin-sparkless crate fails at collect with: **div operation not supported for dtypes `str` and `str`**. For parity, either both should reject with a similar semantic (invalid operation) or both should coerce and compute; the error message/behavior should align.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"a": "10", "b": "2"}, {"a": "20", "b": "4"}],
    ["a", "b"],
)
result = df.withColumn("c", F.col("a") / F.col("b"))
result.collect()
```

**Observed:** `ValueError: ... collect failed: div operation not supported for dtypes \`str\` and \`str\`.` (or similar).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"a": "10", "b": "2"}, {"a": "20", "b": "4"}],
    ["a", "b"],
)
# PySpark: division of string columns may cast to numeric and divide, or raise
result = df.withColumn("c", F.col("a") / F.col("b"))
rows = result.collect()
# Typical: 10/2=5.0, 20/4=5.0 or null if cast fails
```

**Expected:** Either both engines reject string/string division with a clear error, or both coerce and compute; behavior and error message should match PySpark.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- Failing test: `test_string_arithmetic.py::TestStringArithmetic::test_string_arithmetic_with_string_column`.
