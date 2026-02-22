**Still reproduces** (confirmed 2026-02-22 with Sparkless + robin-sparkless).

**Minimal reproduction (Robin/Sparkless):**

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType
import sparkless.sql.functions as F

spark = SparkSession.builder.appName("repro").getOrCreate()
schema = StructType([StructField("text", StringType(), True)])
df = spark.createDataFrame([{"text": "x"}, {"text": "123"}], schema=schema)
result = df.select(F.col("text").cast("int").alias("num"))
result.collect()  # fails here
```

**Observed:** `ValueError: collect failed: conversion from \`str\` to \`i32\` failed in column 'text' for value "x"`

**PySpark (expected):** Same code runs; invalid string `"x"` casts to `null`, `"123"` to `123`. PySpark returns null for non-numeric strings rather than failing the whole collect.
