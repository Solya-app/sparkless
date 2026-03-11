# [PySpark parity] when/otherwise: invalid series dtype expected Boolean, got i32/i64/f64/str

**Upstream issue:** [#680](https://github.com/eddiethedean/robin-sparkless/issues/680)

## Summary

When using `F.when(cond).otherwise(value)` or chained `.when(cond, value)`, the robin-sparkless crate can fail at collect with: **invalid series dtype: expected `Boolean`, got `i32`** (or `i64`, `f64`, `str`) for a series with name `literal`. In PySpark, when/otherwise expressions that evaluate to numeric or string in branches still produce a valid result column; the condition must be Boolean but branch values can be any type. The crate appears to require Boolean in a context where PySpark allows other types.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}],
    ["a", "b"],
)
result = df.withColumn("c", F.when(F.col("a") > 2, F.lit(100)).otherwise(F.lit(0)))
result.collect()
```

**Observed:** `ValueError: ... collect failed: invalid series dtype: expected \`Boolean\`, got \`i32\` for series with name \`literal\`.` (or similar for i64/f64/str).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"a": 1, "b": 10}, {"a": 2, "b": 20}, {"a": 3, "b": 30}],
    ["a", "b"],
)
result = df.withColumn("c", F.when(F.col("a") > 2, F.lit(100)).otherwise(F.lit(0)))
rows = result.collect()
assert rows[0]["c"] == 0 and rows[1]["c"] == 0 and rows[2]["c"] == 100
```

**Expected:** when/otherwise evaluates; condition is Boolean, then/else values can be numeric or other types.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless parity matrix: [docs/robin_parity_matrix.md](robin_parity_matrix.md).
- Failing tests: e.g. `test_issue_288_casewhen_operators.py::test_casewhen_nested_expressions`, `test_casewhen_multiple_when_conditions`.
