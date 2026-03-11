# [PySpark parity] Filter predicate must be of type Boolean, got String

**Upstream issue:** [#646](https://github.com/eddiethedean/robin-sparkless/issues/646)

## Summary

When calling `DataFrame.filter()` with a **string expression** (e.g. `df.filter("dept = 'IT'")`), the robin-sparkless crate fails at collect with: **filter predicate must be of type `Boolean`, got `String`**. In PySpark, string filter expressions are valid and are evaluated as SQL expressions yielding a Boolean column.

## Robin-sparkless reproduction

Run with **Sparkless v4** (Robin-only):

```python
from sparkless import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [
        {"name": "Alice", "dept": "IT", "salary": 50000},
        {"name": "Bob", "dept": "HR", "salary": 60000},
        {"name": "Charlie", "dept": "IT", "salary": 70000},
    ]
)

# PySpark: filter with string expression works
result = df.filter("dept = 'IT'")
result.collect()
```

**Observed:** `ValueError: Robin execute_plan failed: collect failed: filter predicate must be of type \`Boolean\`, got \`String\`.` (or similar).

## PySpark reproduction (expected behavior)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("repro").getOrCreate()
df = spark.createDataFrame(
    [
        {"name": "Alice", "dept": "IT", "salary": 50000},
        {"name": "Bob", "dept": "HR", "salary": 60000},
        {"name": "Charlie", "dept": "IT", "salary": 70000},
    ]
)

result = df.filter("dept = 'IT'")
rows = result.collect()
assert len(rows) == 2
assert rows[0]["name"] == "Alice" and rows[1]["name"] == "Charlie"
```

**Expected:** Filter succeeds; string expressions are evaluated as Boolean predicates.

## Environment

- Sparkless v4 (Robin-only), robin-sparkless crate 0.15.0 via PyO3 extension.
- PySpark 3.x.

## References

- Sparkless parity matrix: [docs/robin_parity_matrix.md](robin_parity_matrix.md).
- Failing tests: e.g. `test_issue_203_filter_with_string.py::TestIssue203FilterWithString::test_filter_with_string_equals`.
