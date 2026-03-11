**Still reproduces** (confirmed 2026-02-22 with Sparkless + robin-sparkless).

**Minimal reproduction (Robin/Sparkless):**

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [{"name": "Alice", "dept": "IT", "salary": 50000}, {"name": "Bob", "dept": "HR", "salary": 60000}, {"name": "Charlie", "dept": "IT", "salary": 70000}]
)
result = df.filter("dept = 'IT'")
result.collect()  # fails here
```

**Observed:** `ValueError: collect failed: filter predicate must be of type \`Boolean\`, got \`String\``

**PySpark (expected):** Same code runs and returns 2 rows (Alice, Charlie). String filter expressions are evaluated as Boolean.

**Resolved plan until failure:** `FILTER "dept = 'IT'" FROM DF ["dept", "name", "salary"]` — the string expression is not being evaluated to a Boolean predicate.
