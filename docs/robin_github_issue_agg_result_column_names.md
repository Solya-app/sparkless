# [PySpark parity] Aggregation result column names (e.g. avg(Value) vs Value)

**Upstream issue:** [#672](https://github.com/eddiethedean/robin-sparkless/issues/672)

## Summary

When using **groupBy().agg(F.avg("Value"))** (or mean/sum/min/max), PySpark names the resulting column **`avg(Value)`** in the schema and in collected rows, so `row["avg(Value)"]` works. The robin-sparkless crate appears to use a different output name (e.g. **`Value`**), so code that expects `row["avg(Value)"]` gets **KeyError: "Key 'avg(Value)' not found in row"**. Available columns are reported as `[Name, Value]` instead of `[Name, avg(Value)]`. Same applies to other aggregates: `sum(Value)`, `min(Value)`, `max(Value)`.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([("Alice", 5.0), ("Alice", 6.0), ("Bob", 5.0)], ["Name", "Value"])
result = df.groupBy("Name").agg(F.avg("Value"))
rows = result.collect()
# PySpark: rows[0]["avg(Value)"] is 5.5 for Alice
row = rows[0]
assert "avg(Value)" in row, "Expected column name 'avg(Value)' in row keys: %s" % (list(row.asDict().keys()) if hasattr(row, "asDict") else row)
print(row["avg(Value)"])
```

**Observed:** `KeyError: "Key 'avg(Value)' not found in row"`; row keys are e.g. `['Name', 'Value']` instead of `['Name', 'avg(Value)']`. In some runs, `select failed: not found: Column 'avg(Value)' not found. Available columns: [Name, Value].`

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame([("Alice", 5.0), ("Alice", 6.0), ("Bob", 5.0)], ["Name", "Value"])
result = df.groupBy("Name").agg(F.avg("Value"))
rows = result.collect()
assert "avg(Value)" in rows[0].asDict()
assert rows[0]["avg(Value)"] == 5.5  # Alice
print(rows[0]["avg(Value)"])  # 5.5
```

**Expected:** Aggregation result column is named `avg(Value)` (and similarly `sum(Value)`, `min(Value)`, `max(Value)` for other aggs), so downstream code can access by that name.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless parity matrix: [docs/robin_parity_matrix.md](robin_parity_matrix.md).
- Failing tests: test_issue_337_grouped_data_mean.py, test_issue_437_mean_string_column.py, parity/dataframe/test_grouped_data_mean_parity.py.
