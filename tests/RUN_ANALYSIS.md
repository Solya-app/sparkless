# Test Run Analysis

**Command:** `python -m pytest tests/ -n 10 -q --tb=no`  
**Results file:** `run_results_20260222_173157.txt` (or latest `run_results_*.txt`)

## Summary

| Metric    | Count |
|----------|--------|
| **Passed**  | 770  |
| **Failed**  | 1,784 |
| **Skipped** | 21   |
| **XPassed** | 1    |
| **Total**   | 2,576 |
| **Duration**| ~55s  |
| **Exit code** | 1 (failures) |

---

## Failure categories (by error pattern)

Failures are grouped by the primary error type. Many tests are known Robin-backend limitations and are listed in `tests/robin_skip_list.json` when running with `SPARKLESS_TEST_BACKEND=robin`.

### 1. **Robin backend not implemented / skip-list items**
- `NotImplementedError: join on expression ...` — join on Column expression not supported.
- `NotImplementedError: crossJoin() is not implemented`
- `NotImplementedError: rollup() / cube() is not implemented`
- `NotImplementedError: explode / array_distinct / row_number ... is not implemented`
- **Action:** Run with `SPARKLESS_TEST_BACKEND=robin` so these are skipped via `robin_skip_list.json`.

### 2. **Select / column conversion (upstream or Sparkless wiring)**
- `ValueError: select expects Column or str` — select() receiving something Rust doesn’t accept (e.g. expression or wrong type).
- `ValueError: cannot convert to Column` — expression/array passed where a Column is expected.
- **Action:** Upstream #645, #644; or Sparkless select/agg normalization (unwrap RobinColumn, reject SortOrder in select).

### 3. **PyColumn missing methods (crate or wrapper)**
- `'builtins.PyColumn' object has no attribute 'format'` — Column.format() for printf-style formatting.
- `'builtins.PyColumn' object has no attribute 'contains'` — Column.contains(substring).
- `'builtins.PyColumn' object has no attribute 'isNotNull'` — alias / naming (e.g. is_not_null vs isNotNull).
- `'builtins.PyColumn' object has no attribute 'when' / 'otherwise'` — WhenThen builder on Column.
- `'builtins.PyColumn' object has no attribute 'mode'` — DataFrameWriter.mode(); wrong object in context.
- **Action:** Add missing methods on Rust PyColumn or Python RobinColumn; fix test/fixture using Column where Writer is expected (mode).

### 4. **Aggregation / result schema**
- `KeyError: "Key 'avg(Value)' not found in row"` — Robin uses different agg result names than PySpark.
- `TypeError: count_distinct() takes 1 positional arguments but 2 were given` — F.countDistinct(col, alias) or similar.
- **Action:** Upstream #672 (agg result column names); Sparkless F.count_distinct wrapper if needed.

### 5. **Case sensitivity**
- `is_case_sensitive()` returns False when test expects True.
- `Failed: DID NOT RAISE` for case-sensitive tests — Robin doesn’t enforce case-sensitive resolution.
- **Action:** Config/crate behavior; or keep tests skipped when running with Robin.

### 6. **createDataFrame / data ingestion**
- `ValueError: createDataFrame requires schema when data is list of tuples`
- `TypeError: 'str' object cannot be converted to 'PyDict'` — pandas/tuple row handling for Robin.
- **Action:** Sparkless createDataFrame path for Robin (tuple/pandas → crate-friendly format).

### 7. **SQL / DDL / Delta**
- `AssertionError: assert 'test_db' in ['default', 'global_temp']` — CREATE DATABASE not reflected.
- `ValueError: SQL failed: ... statement type not supported, got Drop { object_type: Database ...`
- `AttributeError: 'builtins.PyColumn' object has no attribute 'mode'` — DDL tests using wrong API.
- **Action:** Robin SQL/DDL support; or skip DDL tests when backend is Robin.

### 8. **Missing F / functions**
- `module 'sparkless.sql.functions' has no attribute 'array_contains' / 'array_position' / 'size' / 'element_at' / 'array_join' / 'ltrim' / 'rtrim'`
- **Action:** Expose from crate or add stubs and add tests to Robin skip list.

### 9. **Other**
- `KeyError: "Key 'name' not found in row"` — schema/alias mismatch.
- `AttributeError: 'NoneType' object has no attribute 'fields'` — unionByName / schema.
- `AssertionError: DataFrames are not equivalent` — string/trim/split/regexp semantics or schema.

---

## Recommendations

1. **Run with Robin skip list for a green CI:**  
   `SPARKLESS_TEST_BACKEND=robin python -m pytest tests/ -n 10 -q --tb=no`  
   This skips tests in `robin_skip_list.json` so the suite can exit 0.

2. **Reduce failures without skip list:**  
   - Add PyColumn `format` and `contains` (and `isNotNull` alias if needed) in Rust or Python.  
   - Normalize select/agg inputs (unwrap RobinColumn, reject SortOrder in select).  
   - Fix F.count_distinct signature and agg result column name handling where feasible.

3. **Merge new failures into skip list:**  
   `python tests/tools/merge_robin_failures_into_skip_list.py tests/run_results_<timestamp>.txt -o tests/robin_skip_list.json`

4. **Re-run and confirm:**  
   After changes, run again with `-n 10` and, for Robin, with `SPARKLESS_TEST_BACKEND=robin` to confirm 0 failed or expected skips.
