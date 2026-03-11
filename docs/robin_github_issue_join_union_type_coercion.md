# [PySpark parity] Join/union type coercion: String vs Int64 / assert False

**Upstream issue:** [#681](https://github.com/eddiethedean/robin-sparkless/issues/681) (see also #613, #551)

## Summary

When joining or combining DataFrames with columns that have different but compatible types (e.g. string vs long in unionByName or join), PySpark may coerce types and produce a result. The robin-sparkless crate can produce different row counts, wrong types, or fail so that tests that compare to PySpark (e.g. `assert result == expected`) see **assert False** or type/schema mismatch. Example: inner join with one column string and one long; PySpark can coerce; Robin may not or may return different results.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
# Two small DataFrames; join or union with type difference
df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df2 = spark.createDataFrame([(1, 100), (2, 200)], ["id", "value"])
joined = df1.join(df2, on="id", how="inner")
rows = joined.collect()
# PySpark: 2 rows, id/int, name/str, value/int
# Robin: may differ in schema (e.g. id as string vs long) or row content
assert len(rows) == 2
assert rows[0]["value"] == 100
```

**Observed:** AssertionError (e.g. schema field count mismatch, or key not found, or wrong value). Or `collect failed: type String is incompatible with expected type Int64`.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df2 = spark.createDataFrame([(1, 100), (2, 200)], ["id", "value"])
joined = df1.join(df2, on="id", how="inner")
rows = joined.collect()
assert len(rows) == 2
assert rows[0]["value"] == 100
assert rows[0]["id"] == 1
```

**Expected:** Join succeeds; types are consistent; row count and values match.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- [docs/robin_parity_matrix.md](robin_parity_matrix.md) — union/join type coercion (#613, #551).
- Failing tests: `test_pyspark_parity_int64_string_inner`, `test_pyspark_parity_double_precision_string`, `test_column_methods_exist`, `test_first_ignorenulls_type_preservation`.
