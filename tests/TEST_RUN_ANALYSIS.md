# Test Run Analysis (8 workers, no archive)

## Latest run

**Run:** `pytest tests -n 8 -v --tb=short --ignore=tests/archive`  
**Result:** **1526 failed**, **1028 passed**, 21 skipped, 1 xpassed in **47.02s**  
**Workers:** 8  
**Total collected:** 2576 items  

**vs previous run:** **-29 failed**, **+29 passed** (was 1555 failed / 999 passed). Biggest shift: Rust boundary failures (`cannot convert to Column`) now surface as a clearer Python-side error (**Expression columns … not yet supported**).

Dominant failure modes: **Expression columns not yet supported** (499), **select expects Column or str** (214), **udf not implemented** (176), **RobinColumn is not callable** (162), **join on expression not supported** (100). Notable new bucket: **count failed: duplicate 'count'** (16).

---

## Summary

| Outcome | Count (latest) |
|--------|--------|
| Passed  | 1028  |
| Failed  | 1526 |
| Skipped | 21   |
| XPassed | 1    |

About **61%** of non-archive tests still fail. Remaining failures: Robin backend not implemented (udf, explode, expr, struct, join-on-expression, etc.), ColumnOperation / list not converted to Robin column, select/filter receiving wrong types, and SQL/catalog limits.

---

## Top failure categories (by error type, latest run)

| Count | Error / category |
|------|-------------------|
| 499 | `ValueError: Expression columns ... not yet supported in select/filter` — ColumnOperation/expression currently rejected in Robin select/filter paths |
| 214 | `ValueError: select expects Column or str` — non-Column/non-str in select (still leaking through some paths) |
| 176 | `NotImplementedError: udf is not implemented for the Robin backend` |
| 162 | `TypeError: 'RobinColumn' object is not callable` — F.* used as callable (e.g. selectExpr, replace) |
| 100 | `NotImplementedError: join on expression (e.g. df1.a == df2.b) is not supported` |
| 94  | `NotImplementedError: explode is not implemented` |
| 74  | `NotImplementedError: rlike is not implemented` |
| 66  | `AssertionError: DataFrames are not equivalent` |
| 64  | `NotImplementedError: struct is not implemented` |
| 62  | `NotImplementedError: with_field is not implemented` |
| 54  | `NotImplementedError: row_number is not implemented` |
| 52  | `NotImplementedError: posexplode is not implemented` |
| 52  | `NotImplementedError: expr is not implemented` |
| 46  | `ValueError: SQL failed: only SELECT, CREATE SCHEMA/DATABASE, and DROP TABLE/VIEW/SCHEMA are supported` |
| 42  | `TypeError: 'ColumnOperation' object cannot be converted to 'PyColumn'` |
| 28  | `NotImplementedError: array_distinct is not implemented` |
| 26  | `NotImplementedError: approx_count_distinct is not implemented` |
| 24  | `NotImplementedError: input_file_name is not implemented` |
| 20  | `ValueError: select failed: not found: Column 'E1-Extract' not found` (struct/expr naming) |
| 20  | `ValueError: groupBy with expression columns is not yet supported` (new Python-side guard) |
| 20  | `ValueError: get_item key must be int (array index) or str (map key)` |
| 18  | `ValueError: collect failed: field not found: E1` |
| 18  | `TypeError: log() takes 1 positional arguments but 2 were given` |
| 16  | `KeyError: "Key 'NaMe' not found in row"` (case sensitivity) |
| 16  | `ValueError: count failed: duplicate: column with name 'count' has more than one occurrence` |
| 14  | `ValueError: select failed: not found: Column 'map_col' not found` (create_map alias) |
| 14  | `NotImplementedError: stddev is not implemented` |
| 14  | `NotImplementedError: over() is not implemented` |
| 14  | `AssertionError: assert (None == 0.0)` (fillna/agg) |

Additional: SQL (UPDATE/DELETE, JOIN types), join type leftsemi, isin/list values, datetime/type mismatches, case sensitivity, table/view not found, union column order.

---

## Root cause groups

1. **Column / PyColumn conversion (Sparkless ↔ Robin)**  
   - “cannot convert to Column” and “select expects Column or str” often mean Sparkless is passing a wrapper or expression object where Robin expects a `Column`/str.  
   - “RobinColumn object is not callable” and “ColumnOperation cannot be converted to PyColumn” point to F.* returning or using Robin types in a way the rest of Sparkless doesn’t expect.

2. **Robin backend not implemented**  
   - UDF, explode, posexplode, struct, expr, row_number, percent_rank, dense_rank, cume_dist, rollup, cube, crossJoin, over(), isnan, to_timestamp, sin/cos/tan, keys(), etc.  
   - These are either missing in the Robin crate or not wired in Sparkless.

3. **Missing or misnamed F.* in Sparkless (Robin path)**  
   - `rlike`, `with_field`, `approx_count_distinct`, `input_file_name`, `stddev`, `array_contains`, `posexplode_outer`, `date_trunc`, and many string/array/other functions.  
   - Tests assume PySpark-style `F.xxx`; Sparkless either doesn’t expose them or exposes under a different name for Robin.

4. **SQL / catalog / execution limits**  
   - Only SELECT, CREATE SCHEMA/DATABASE, DROP TABLE/VIEW/SCHEMA supported; no UPDATE/DELETE, limited JOIN support, no DESCRIBE DETAIL for non-Delta tables.  
   - Table/view/schema registration and “table not found” show catalog/scope differences.

5. **Semantic / result parity**  
   - Aggregation column names (e.g. `avg(Value)` vs Robin’s output name).  
   - Row keys, schema field count, datetime/date representation (string vs Python datetime/date).  
   - Case sensitivity and filter behavior (“DID NOT RAISE”) where PySpark would raise.

6. **Filter/string expressions**  
   - `filter("string expression")` and string SQL expressions not supported or converted differently (e.g. “filter predicate must be Boolean” or wrong type).

---

## Recommendations

1. **Column conversion layer**  
   - Audit all call sites that pass “column-like” objects into Robin (select, filter, groupBy, join, etc.) and ensure Sparkless converts to Robin `Column` or accepted str consistently.  
   - Fix handling of list-of-columns and expression objects so they are converted or rejected with a clear error.

2. **F.* coverage and type consistency**  
   - Add or fix Robin-backed implementations for high-impact missing functions (rlike, with_field, approx_count_distinct, stddev, array_contains, posexplode_outer, date_trunc, etc.).  
   - Ensure F.* return types are consistent (e.g. not returning RobinColumn where a callable or a different wrapper is expected).

3. **Aggregation / groupBy naming**  
   - Align Robin aggregation output names with PySpark (e.g. `avg(column)` → same key in row/schema) so parity tests and KeyError on `avg(Value)` go away.

4. **SQL and catalog**  
   - Document and/or extend supported SQL and catalog operations; add clear errors for unsupported DML (UPDATE/DELETE) and DESCRIBE DETAIL when not Delta.  
   - Fix or document table/view registration and scope (e.g. test_schema.test_table not found).

5. **Test strategy**  
   - Reintroduce a **Robin skip list** (e.g. `robin_skip_list.json`) for known gaps so CI can be green while tracking parity.  
   - Add a small “Robin smoke” suite that runs only tests that currently pass, and grow it as gaps are fixed.

6. **Prioritize by impact**  
   - Fix “cannot convert to Column” and “select expects Column or str” first (451 failures).  
   - Then “RobinColumn is not callable” and missing F.* (rlike, with_field, etc.).  
   - Then UDF, explode, struct, expr, and join-on-expression for broader parity.

---

## Run / analysis notes

- **Command used:** `python -m pytest tests -n 8 -v --tb=short --ignore=tests/archive`
- **Artifact:** Full output was written to the agent tools path; this doc summarizes failure reasons extracted from it.
- **Agg aliases:** If `KeyError 'avg(Value)'` / `'count'` persist after code changes, ensure the Robin extension is rebuilt and loaded (e.g. `maturin develop` in the project root) so Python uses the updated F.avg/F.count alias logic.
- **Priority fixes by impact:** (1) Column conversion — cannot convert to Column + select expects Column or str (~717); (2) RobinColumn callable / ColumnOperation → PyColumn (~204); (3) agg row keys (avg/count) once extension is current; (4) known-not-implemented (udf, explode, expr, join-on-expr, etc.) via skip list or stubs.

*Generated from test run output; failure counts are approximate from aggregated error messages.*
