# Test Run Analysis (8 workers, no archive)

**Run:** `pytest tests -n 8 -v --tb=no --ignore=tests/archive`  
**Result:** 1559 failed, 995 passed, 21 skipped, 1 xpassed in **55.65s**  
**Workers:** 8  
**Total collected:** 2576 items  

---

## Summary

| Outcome | Count |
|--------|--------|
| Passed  | 995  |
| Failed  | 1559 |
| Skipped | 21   |
| XPassed | 1    |

About **61%** of non-archive tests fail. Failures are almost entirely due to **Robin backend** limitations and **Sparkless↔Robin** integration (Column conversion, missing F.*, SQL/API parity).

---

## Top failure categories (by error type)

| Count | Error / category |
|------|-------------------|
| 305 | `ValueError: cannot convert to Column` — Python/Sparkless Column not converted to RobinColumn where Robin expects it |
| 146 | `ValueError: select expects Column or str` — select() given something other than Column or string (e.g. list of columns, expression object) |
| 88  | `NotImplementedError: udf is not implemented for the Robin backend` |
| 81  | `TypeError: 'RobinColumn' object is not callable` — F.* returning RobinColumn used as callable (e.g. F.xxx used as F.xxx()) |
| 47  | `NotImplementedError: explode is not implemented` |
| 37  | `AttributeError: 'RobinFunctions' object has no attribute 'rlike'` |
| 34  | `AssertionError: DataFrames are not equivalent` — parity / result mismatch |
| 32  | `NotImplementedError: struct is not implemented` |
| 31  | `AttributeError: 'RobinFunctions' object has no attribute 'with_field'` |
| 28  | `KeyError: "Key 'avg(Value)' not found in row"` — aggregation alias/name mismatch (Robin uses different naming) |
| 27  | `NotImplementedError: row_number is not implemented` |
| 26  | `NotImplementedError: posexplode is not implemented` |
| 26  | `NotImplementedError: expr is not implemented` |
| 23  | `ValueError: SQL failed: only SELECT, CREATE SCHEMA/DATABASE, and DROP TABLE/VIEW/SCHEMA are supported` |
| 21  | `TypeError: 'ColumnOperation' object cannot be converted to 'PyColumn'` |
| 19  | `NotImplementedError: join on expression ... not supported` (join on df1.a == df2.b) |
| 14  | `NotImplementedError: array_distinct is not implemented` |
| 13  | `AttributeError: module 'sparkless.sql.functions' has no attribute 'approx_count_distinct'` |
| 12  | `AttributeError: ... has no attribute 'input_file_name'` |

Additional notable categories: missing F.* (`stddev`, `array_contains`, `posexplode_outer`, `date_trunc`, string/array functions), SQL (UPDATE/DELETE, JOIN syntax, DESCRIBE DETAIL), `KeyError` for aggregation/row keys, and assertion/type mismatches (datetime/date, schema, None vs value).

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

*Generated from test run output; failure counts are approximate from aggregated error messages.*
