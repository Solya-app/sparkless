#!/usr/bin/env python3
"""
Parse test run output, classify crate vs wrapper failures, group by canonical
crate error, and create GitHub issues in eddiethedean/robin-sparkless.

Run from repo root:
  python scripts/create_robin_parity_issues.py [--dry-run] [--run-file tests/.robin_parity_run.txt]

Requires: gh CLI authenticated with access to eddiethedean/robin-sparkless.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"
BODY_FILE = Path("tests/.robin_issue_body.txt")

# Crate-originated: message contains one of these prefixes (from src/*.rs format!("... failed: {e}"))
CRATE_PREFIXES = (
    "select failed:",
    "collect failed:",
    "count failed:",
    "with_column failed:",
    "filter failed:",
    "create_dataframe_from_rows failed:",
    "agg failed:",
    "join failed:",
    "union failed:",
    "union_by_name failed:",
    "group_by failed:",
    "order_by failed:",
    "order_by_exprs failed:",
    "drop failed:",
    "limit failed:",
    "distinct failed:",
    "schema failed:",
    "avg failed:",
    "sum failed:",
    "min failed:",
    "max failed:",
    "cast failed:",
    "array failed:",
    "create_map failed:",
    "to_date failed:",
    "SQL failed:",
    "table(",
    "read_parquet failed:",
    "read_csv failed:",
    "Robin SQL failed:",
    "collect_as_json_rows failed:",
    "Robin read_",
    "Robin write_",
    "pivot sum failed:",
    "pivot avg failed:",
    "pivot min failed:",
    "pivot max failed:",
    "pivot count failed:",
)

# Wrapper-originated: do not report to robin-sparkless
WRAPPER_PATTERNS = (
    "Expression columns",
    "not yet supported in select/filter",
    "select expects Column or str",
    "cannot convert to Column",
    "RobinColumn is not callable",
    "ColumnOperation object cannot be converted",
    "groupBy with expression columns is not yet supported",
    "is not implemented for the Robin backend",
    "join on expression",
    "not supported",
    "get_item key must be int",  # wrapper enforces; if crate has different msg, crate wins
)


def parse_failed_lines(run_file: Path) -> list[tuple[str, str]]:
    """Extract (test_id, error_message) from FAILED lines."""
    text = run_file.read_text(encoding="utf-8", errors="replace")
    out = []
    # FAILED tests/path.py::test_name - ValueError: message
    pattern = re.compile(r"^FAILED\s+(.+?)\s+-\s+(?:[\w.]+:\s+)(.+)$", re.MULTILINE)
    for m in pattern.finditer(text):
        test_id, msg = m.group(1).strip(), m.group(2).strip()
        out.append((test_id, msg))
    return out


def is_wrapper(message: str) -> bool:
    for p in WRAPPER_PATTERNS:
        if p in message:
            return True
    return False


def is_crate(message: str) -> bool:
    if is_wrapper(message):
        return False
    for prefix in CRATE_PREFIXES:
        if message.startswith(prefix) or (f" {prefix}" in message and "failed:" in message):
            return True
    # Also crate: result row key naming (KeyError "Key 'X' not found in row")
    if "not found in row" in message and "Key " in message:
        return True
    # collect failed / select failed etc. might be phrased with ValueError in front
    if "failed:" in message:
        for prefix in CRATE_PREFIXES:
            if prefix in message:
                return True
    return False


def canonical_key(message: str) -> str:
    """Normalize to a key for grouping: one issue per distinct root cause."""
    msg = message.split("\n")[0].strip()
    # Consolidate to root-cause groups (plan: one issue per canonical cause)
    if "duplicate" in msg and "column with name 'count'" in msg:
        return "count failed: duplicate column name 'count'"
    if "select failed:" in msg and "not found: Column" in msg:
        return "select failed: not found Column (alias/naming)"
    if "not found: Column" in msg and "with_column failed" not in msg:
        return "select failed: not found Column (alias/naming)"
    if "with_column failed:" in msg and "not found: Column" in msg:
        return "with_column failed: not found Column (alias e.g. to_timestamp)"
    if "field not found" in msg and ("collect failed" in msg or "E1" in msg or "struct" in msg.lower()):
        return "collect failed: field not found (struct/nested)"
    if "collect failed:" in msg and ("cannot compare string" in msg or "string" in msg and "numeric" in msg):
        return "collect failed: string vs numeric comparison"
    if "collect failed:" in msg and "dtype" in msg and "not supported" in msg:
        return "collect failed: dtype not supported (e.g. when/otherwise)"
    if "create_dataframe_from_rows failed" in msg or "struct value must be" in msg or "struct value must be object" in msg:
        return "create_dataframe_from_rows: struct/array/map type"
    if "array column" in msg or "map column" in msg and "expects JSON" in msg:
        return "create_dataframe_from_rows: struct/array/map type"
    if "json_value" in msg and "unsupported" in msg:
        return "create_dataframe_from_rows / json: unsupported type"
    if "not found in row" in msg and "Key " in msg:
        return "Key not found in row (case/schema)"
    if "union failed:" in msg or "union: column order" in msg:
        return "union: column order/names must match"
    if "SQL failed:" in msg or "SQL:" in msg:
        # Group all SQL limitation messages
        if "only SELECT" in msg:
            return "SQL failed: only SELECT / DDL supported"
        if "only INNER, LEFT" in msg or "JOIN" in msg:
            return "SQL failed: join type limitation"
        if "subquery" in msg:
            return "SQL failed: subquery not supported"
        if "parse error" in msg:
            return "SQL failed: parse error"
        return "SQL failed: other"
    if "duplicate: projections" in msg or "duplicate output name" in msg:
        return "duplicate: projections duplicate output name"
    if "arithmetic on string and numeric" in msg:
        return "arithmetic on string and numeric not allowed"
    if "boolean parse:" in msg or "int parse:" in msg:
        return "parse: boolean/int strict parsing"
    if "conversion from" in msg and "failed" in msg:
        return "conversion/cast failed in column"
    if "DESCRIBE DETAIL" in msg or "not a Delta table" in msg:
        return "DESCRIBE DETAIL: not Delta table"
    if "test_schema" in msg:
        return "schema/table not found (test_schema)"
    # Fallback: strip first "X failed: " and use short rest
    for prefix in CRATE_PREFIXES:
        if prefix in msg:
            idx = msg.find(prefix)
            rest = msg[idx + len(prefix) :].strip()
            if "." in rest:
                rest = rest.split(".")[0].strip() + "."
            return (rest[:80] + "..") if len(rest) > 80 else rest
    return msg[:80]


def classify_and_group(run_file: Path) -> tuple[dict[str, list[tuple[str, str]]], dict[str, list[tuple[str, str]]]]:
    """Return (crate_groups, wrapper_groups). Each group: canonical_key -> [(test_id, message), ...]."""
    failed = parse_failed_lines(run_file)
    crate_groups: dict[str, list[tuple[str, str]]] = {}
    wrapper_groups: dict[str, list[tuple[str, str]]] = {}
    for test_id, msg in failed:
        key = canonical_key(msg) if ("failed:" in msg or "KeyError:" in msg) else msg[:80]
        if is_crate(msg):
            crate_groups.setdefault(key, []).append((test_id, msg))
        else:
            # Group wrapper by short tag
            tag = msg[:80] + ("..." if len(msg) > 80 else "")
            wrapper_groups.setdefault(tag, []).append((test_id, msg))
    return crate_groups, wrapper_groups


def get_issue_content(canonical_key: str, example_test: str, example_msg: str) -> tuple[str, str]:
    """Return (title, body) for the crate issue."""
    # Map known canonical keys to titles and body snippets
    key_lower = canonical_key.lower()
    if "duplicate" in key_lower and "count" in key_lower:
        title = "[Parity] count() with multiple count exprs causes duplicate column name"
        body = _body_count_duplicate(example_msg)
    elif "not found: column" in key_lower and ("map_col" in key_lower or "alias" in key_lower or "not found" in key_lower):
        title = "[Parity] select() with aliased column reports 'Column not found'"
        body = _body_select_column_not_found(example_msg)
    elif "cannot compare string with numeric" in key_lower or "string" in key_lower and "numeric" in key_lower:
        title = "[Parity] filter/compare string column with numeric literal errors"
        body = _body_string_numeric_compare(example_msg)
    elif "field not found" in key_lower:
        title = "[Parity] collect() / struct nested field not found (e.g. E1)"
        body = _body_field_not_found(example_msg)
    elif "create_dataframe_from_rows failed" in key_lower or "struct value" in key_lower or "array column" in key_lower:
        title = "[Parity] create_dataframe_from_rows: struct/array/map type handling"
        body = _body_create_dataframe_from_rows(example_msg)
    elif "key" in key_lower and "not found in row" in key_lower:
        title = "[Parity] Row key / schema case sensitivity (Key not found in row)"
        body = _body_key_not_found_in_row(example_msg)
    elif "with_column failed" in key_lower and "not found" in key_lower:
        title = "[Parity] with_column() fails when column name from alias not found"
        body = _body_with_column_not_found(example_msg)
    elif "dtype" in key_lower and "not supported" in key_lower:
        title = "[Parity] collect() / expression dtype not supported (e.g. when/otherwise)"
        body = _body_dtype_not_supported(example_msg)
    else:
        title = f"[Parity] Crate error: {canonical_key[:60]}"
        body = _body_generic(canonical_key, example_test, example_msg)
    return title, body


def _pyspark_snippet(message: str) -> str:
    try:
        from scripts.robin_issue_pyspark_snippets import get_pyspark_snippet
        return get_pyspark_snippet(message)
    except Exception:
        return """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df.show()"""


def _body_count_duplicate(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a", 10), (1, "b", 20), (2, "c", 30)], ["g", "x", "v"])
# Multiple count() in agg; PySpark allows with distinct aliases
df.groupBy("g").agg(F.count("*").alias("cnt1"), F.count("x").alias("cnt2")).show()"""
    return f"""## Summary

When using multiple count expressions in a groupBy agg (e.g. `count(*)` and `count(column)`), the crate fails with duplicate column name 'count'. PySpark allows this by using aliases.

## robin-sparkless (current behavior)

Using Sparkless v4 with Robin backend, run a groupBy with multiple count exprs (e.g. after a join). The error is:

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_select_column_not_found(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 10.0)], ["id", "value"])
# Aliased agg column is available in select
df.groupBy("id").agg(F.avg("value").alias("avg_value")).select("id", "avg_value").show()"""
    return f"""## Summary

Selecting an aliased column (e.g. from create_map or agg alias) fails with "Column 'X' not found". PySpark makes agg aliases and expression aliases available in subsequent select.

## robin-sparkless (current behavior)

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_string_numeric_compare(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("123",), ("456",)], ["s"])
# PySpark coerces string column to numeric when comparing to numeric literal
df.filter(F.col("s") == 123).collect()"""
    return f"""## Summary

Comparing a string column to a numeric literal in filter causes "cannot compare string with numeric type". PySpark coerces and evaluates.

## robin-sparkless (current behavior)

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_field_not_found(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("nested", StructType([StructField("a", IntegerType()), StructField("b", StringType())])),
])
df = spark.createDataFrame([("x", (1, "y"))], schema)
# Access nested field
df.select("id", "nested.a").collect()"""
    return f"""## Summary

When collecting rows that include struct columns, accessing a nested field fails with "field not found" (e.g. E1). PySpark exposes struct fields by name.

## robin-sparkless (current behavior)

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_create_dataframe_from_rows(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("nested", StructType([StructField("a", IntegerType()), StructField("b", StringType())])),
])
df = spark.createDataFrame([("x", {"a": 1, "b": "y"})], schema)
df.collect()"""
    return f"""## Summary

create_dataframe_from_rows fails for struct/array/map column values in formats PySpark accepts (e.g. dict for struct, list for array).

## robin-sparkless (current behavior)

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_key_not_found_in_row(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a")], ["id", "name"])
# PySpark row keys typically match schema; case-insensitive in some configs
df.select(F.col("id").alias("ID"), "name").collect()"""
    return f"""## Summary

After select/agg, row keys may not match expected names (e.g. case difference), causing KeyError when accessing by key. PySpark preserves or normalizes column names in row keys.

## robin-sparkless (current behavior)

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_with_column_not_found(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("2024-01-15 12:00:00",)], ["s"])
df.withColumn("ts", F.to_timestamp(F.col("s"))).show()"""
    return f"""## Summary

with_column fails when the new column expression uses an alias (e.g. to_timestamp) that the crate does not expose as the column name. Error: column not found (e.g. to_timestamp_*).

## robin-sparkless (current behavior)

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_dtype_not_supported(example_msg: str) -> str:
    pyspark = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
df.select(F.when(F.col("a") > 1, F.col("b")).otherwise(0).alias("x")).collect()"""
    return f"""## Summary

Collect fails when the result has a dtype the crate does not support in 'not' or other operations (e.g. Unknown(Any) from when/otherwise). PySpark evaluates and returns.

## robin-sparkless (current behavior)

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def _body_generic(canonical_key: str, example_test: str, example_msg: str) -> str:
    pyspark = _pyspark_snippet(example_msg)
    return f"""## Summary

Crate-originated error observed when running Sparkless v4 with Robin backend. See repro and PySpark expected behavior below.

## robin-sparkless (current behavior)

Example test: `{example_test}`

```
{example_msg}
```

## PySpark (expected behavior)

```python
{pyspark}
```

## Environment

Sparkless v4, robin-sparkless 0.17.0, Python 3.x.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Create robin-sparkless parity issues from test run")
    parser.add_argument("--dry-run", action="store_true", help="Print only; do not create issues")
    parser.add_argument(
        "--run-file",
        type=Path,
        default=Path("tests/.robin_parity_run.txt"),
        help="Path to pytest run output",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    run_file = root / args.run_file if not args.run_file.is_absolute() else args.run_file

    if not run_file.exists():
        print(f"Run file not found: {run_file}", file=sys.stderr)
        return 1

    crate_groups, wrapper_groups = classify_and_group(run_file)

    # Report
    print("Crate-originated groups (will create issues):")
    for key, pairs in sorted(crate_groups.items(), key=lambda x: -len(x[1])):
        example_test, example_msg = pairs[0]
        print(f"  [{len(pairs)}] {key[:70]}...")
        print(f"       e.g. {example_test}")

    print("\nWrapper-originated (not reported):")
    for tag, pairs in sorted(wrapper_groups.items(), key=lambda x: -len(x[1]))[:15]:
        print(f"  [{len(pairs)}] {tag[:70]}...")

    created = []
    body_file = root / BODY_FILE
    body_file.parent.mkdir(parents=True, exist_ok=True)

    for canonical_key, pairs in crate_groups.items():
        example_test, example_msg = pairs[0]
        title, body = get_issue_content(canonical_key, example_test, example_msg)
        if args.dry_run:
            print(f"\n--- Would create: {title} ---")
            print(body[:400] + "..." if len(body) > 400 else body)
            continue
        body_file.write_text(body, encoding="utf-8")
        cmd = ["gh", "issue", "create", "-R", REPO, "--title", title, "--body-file", str(body_file)]
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=str(root))
            url = (result.stdout or "").strip()
            if url:
                created.append(url)
                print(f"Created: {url}")
        except subprocess.CalledProcessError as e:
            print(f"Failed: {e.stderr or str(e)}", file=sys.stderr)
        finally:
            if body_file.exists():
                body_file.unlink(missing_ok=True)

    if not args.dry_run and created:
        print(f"\nCreated {len(created)} issue(s) in {REPO}.")
        for u in created:
            print(f"  {u}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
