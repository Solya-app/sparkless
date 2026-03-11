# [PySpark parity] casewhen / bitwise not: dtype Unknown(Any) not supported in 'not' operation

**Upstream issue:** [#682](https://github.com/eddiethedean/robin-sparkless/issues/682)

## Summary

When using a bitwise-not-like expression inside when/otherwise (e.g. `F.when(~F.col("flag"), value)` or similar), the robin-sparkless crate fails at collect with: **dtype Unknown(Any) not supported in 'not' operation**. PySpark supports logical/bitwise not on Boolean columns in such expressions.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"flag": True, "x": 1}, {"flag": False, "x": 2}],
    ["flag", "x"],
)
result = df.withColumn("c", F.when(~F.col("flag"), F.col("x")).otherwise(F.lit(0)))
result.collect()
```

**Observed:** `ValueError: ... collect failed: dtype Unknown(Any) not supported in 'not' operation`.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"flag": True, "x": 1}, {"flag": False, "x": 2}],
    ["flag", "x"],
)
result = df.withColumn("c", F.when(~F.col("flag"), F.col("x")).otherwise(F.lit(0)))
rows = result.collect()
# flag=True -> ~True is False -> otherwise 0; flag=False -> ~False is True -> then x
assert rows[0]["c"] == 0 and rows[1]["c"] == 2
```

**Expected:** Logical not on Boolean column is supported in when/otherwise.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- Failing test: `test_issue_288_casewhen_operators.py::TestIssue288CaseWhenOperators::test_casewhen_bitwise_not`.
