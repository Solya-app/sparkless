**Still reproduces** (confirmed 2026-02-22 with Sparkless + robin-sparkless).

**Minimal reproduction (Robin/Sparkless):**

```python
from sparkless import SparkSession
from sparkless.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("repro").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]), True),
])
df = spark.createDataFrame(
    [(1, {"name": "Alice", "age": 30}), (2, {"name": "Bob", "age": 25})],
    schema=schema,
)
df.collect()  # fails here
```

**Observed:** `ValueError: create_dataframe_from_rows failed: struct value must be object (by field name) or array (by position). PySpark accepts dict or tuple/list for struct columns.`

**PySpark (expected):** Same code runs; struct column is built from dicts keyed by field name; `rows[0]["person"]["name"] == "Alice"`.
