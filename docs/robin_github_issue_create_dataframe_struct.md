# [PySpark parity] create_dataframe_from_rows: struct value must be object (by field name)

**Upstream issue:** [#634](https://github.com/eddiethedean/robin-sparkless/issues/634)

## Summary

When creating a DataFrame from rows that include **struct-typed columns** (nested dicts keyed by field name), the robin-sparkless crate fails with: **struct value must be object (by field name) or array**. PySpark accepts rows where a struct column is a Python dict with keys matching the struct fields (e.g. `{"name": "Alice", "age": 30}` for `StructType([StructField("name", StringType()), StructField("age", IntegerType())])`).

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField(
        "person",
        StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]),
        True,
    ),
])

# Rows: struct column as dict by field name (PySpark standard)
df = spark.createDataFrame(
    [(1, {"name": "Alice", "age": 30}), (2, {"name": "Bob", "age": 25})],
    schema=schema,
)
df.collect()
```

**Observed:** `ValueError: Robin execute_plan failed: create_dataframe_from_rows: struct value must be object (by field name) or array` (or similar).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField(
        "person",
        StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]),
        True,
    ),
])

df = spark.createDataFrame(
    [(1, {"name": "Alice", "age": 30}), (2, {"name": "Bob", "age": 25})],
    schema=schema,
)
rows = df.collect()
assert rows[0]["person"]["name"] == "Alice"
assert rows[1]["person"]["age"] == 25
```

**Expected:** createDataFrame succeeds; struct columns are built from dicts keyed by field name.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless parity matrix: [docs/robin_parity_matrix.md](robin_parity_matrix.md).
- Failing tests: e.g. `test_issue_358_getfield.py::TestIssue358GetField::test_getfield_struct_field_by_name`, `test_withfield.py::TestWithField::test_withfield_add_new_field`.
