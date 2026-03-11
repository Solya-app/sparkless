# [PySpark parity] collect: type conversion semantics (str→int, str→datetime, f64→i32, string→boolean)

**Upstream issue:** [#649](https://github.com/eddiethedean/robin-sparkless/issues/649)

## Summary

When **collecting** a DataFrame after expressions that cast or compare types, the robin-sparkless crate can fail with conversion errors that PySpark handles differently (e.g. returns null, or raises with a different message). Observed: **conversion from `str` to `i32` failed**, **conversion from `str` to `datetime[μs]` failed**, **casting from f64 to i32 not supported**, **casting from string to boolean failed for value ''**. PySpark may coerce, return null for invalid values, or document different casting rules.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only). Example: invalid string to int.

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType
import sparkless.sql.functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
schema = StructType([StructField("text", StringType(), True)])
df = spark.createDataFrame([{"text": "x"}, {"text": "123"}], schema=schema)
# Cast invalid string to int: PySpark often returns null for "x"; Robin may error
result = df.select(F.col("text").cast("int").alias("num"))
result.collect()
```

**Observed (example):** `ValueError: Robin execute_plan failed: collect failed: conversion from \`str\` to \`i32\` failed in column 'text' for value 'x'.`

Other cases:
- **str → datetime:** empty or non-date string in a timestamp column.
- **f64 → i32:** `casting from f64 to i32 not supported` (PySpark may truncate or allow).
- **string → boolean:** `casting from string to boolean failed for value ''` (PySpark often treats '' as null or false).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
schema = StructType([StructField("text", StringType(), True)])
df = spark.createDataFrame([{"text": "x"}, {"text": "123"}], schema=schema)
result = df.select(F.col("text").cast("int").alias("num"))
rows = result.collect()
# PySpark typically returns null for non-numeric string, 123 for "123"
assert rows[0]["num"] is None
assert rows[1]["num"] == 123
```

**Expected:** Invalid cast values either yield null or a documented error; behavior should match PySpark semantics where possible.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless parity matrix: [docs/robin_parity_matrix.md](robin_parity_matrix.md).
- Failing tests: e.g. `test_column_astype.py::TestColumnAstype::test_astype_invalid_string_to_int`, `test_astype_empty_string_handling`, `test_astype_double_to_int`, `test_astype_string_to_boolean`; `test_issue_432_cast_datetime_date_noop.py`.
