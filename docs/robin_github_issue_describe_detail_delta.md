# [PySpark parity] DESCRIBE DETAIL (Delta Lake) not supported or different semantics

**Upstream issue:** [#678](https://github.com/eddiethedean/robin-sparkless/issues/678)

## Summary

In PySpark with Delta Lake, the SQL command `DESCRIBE DETAIL table_name` returns one row of table metadata (format, id, name, location, numFiles, sizeInBytes, minReaderVersion, minWriterVersion, partitionColumns, etc.). When using the robin-sparkless crate (e.g. via Sparkless v4 with Robin backend), this command may be unsupported, may fail at parse/execute time, or may return a different schema/behavior. Tests that run `spark.sql("DESCRIBE DETAIL delta_table")` and assert on the result (e.g. `row["format"] == "delta"`, `row["numFiles"] >= 0`) are skipped for Robin or fail.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin backend) and Delta support:

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()

# Create a Delta table (if crate supports saveAsTable with delta format)
data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
df = spark.createDataFrame(data)
spark.sql("DROP TABLE IF EXISTS test_detail_basic")
df.write.format("delta").saveAsTable("test_detail_basic")

# Execute DESCRIBE DETAIL (Delta Lake specific)
result = spark.sql("DESCRIBE DETAIL test_detail_basic")
rows = result.collect()

# Expected: one row with columns format, name, numFiles, sizeInBytes, minReaderVersion, minWriterVersion, etc.
assert len(rows) == 1
assert rows[0]["format"] == "delta"
assert "test_detail_basic" in rows[0]["name"]
assert rows[0]["numFiles"] >= 0
assert rows[0]["sizeInBytes"] >= 0
```

**Observed (Robin):** Unsupported SQL statement, parse error, or execution error when running `DESCRIBE DETAIL`; or missing/incorrect result schema.

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession

# With Delta Lake enabled (e.g. delta-spark package)
spark = SparkSession.builder.appName("repro") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
df = spark.createDataFrame(data)
spark.sql("DROP TABLE IF EXISTS test_detail_basic")
df.write.format("delta").saveAsTable("test_detail_basic")

result = spark.sql("DESCRIBE DETAIL test_detail_basic")
rows = result.collect()

assert len(rows) == 1
row = rows[0]
assert row["format"] == "delta"
assert "test_detail_basic" in row["name"]
assert row["numFiles"] >= 0
assert row["sizeInBytes"] >= 0
assert "partitionColumns" in row.asDict()
assert "minReaderVersion" in row.asDict()
assert "minWriterVersion" in row.asDict()
```

**Expected:** Single row with Delta table metadata; schema includes at least `format`, `name`, `numFiles`, `sizeInBytes`, `partitionColumns`, `minReaderVersion`, `minWriterVersion`, and optionally `id`, `description`, `location`, `createdAt`, `lastModified`, `properties`.

## Environment

- Sparkless v4 (Robin backend), robin-sparkless crate 0.15.0.
- PySpark 3.x with delta-spark for expected behavior.

## References

- Sparkless parity matrix: `docs/robin_parity_matrix.md` (Describe detail / Delta).
- Delta Lake docs: [DESCRIBE DETAIL](https://docs.delta.io/latest/delta-utility.html#describe-detail).
