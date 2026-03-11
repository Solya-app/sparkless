# Test Run Summary & Failure Analysis

**Run:** `python -m pytest tests/ -n 10 -v --ignore=tests/archive`  
**Results file:** `test_results.txt`

## Totals

| Result | Count |
|--------|--------|
| **Passed** | 684 |
| **Failed** | 1,870 |
| **Skipped** | 21 |
| **XPassed** | 1 |
| **Total** | 2,576 |

---

## Failure Categories (by error type)

### 1. **Robin backend API gaps / missing methods**
- `'RobinFunctions' object has no attribute 'get_item'` – `F.get_item()` not exposed in sparkless_robin.
- `'RobinFunctions' object has no attribute 'over'` – Window `.over()` not implemented.
- `'RobinGroupedData' object has no attribute 'count'` / `'avg'` – GroupedData aggregation API differs.
- `'builtins.PyColumn' object has no attribute '__add__'`, `__mul__`, `contains`, `format`, `__invert__` – Column operator/method shims missing for RobinColumn.

**Affected areas:** unit (issues 225–231, 270, 355), parity (aggregations, groupby, filter), unit/window_arithmetic.

### 2. **NotImplementedError (known Robin parity)**
- `row_number` / `percent_rank` / `ntile` / `lag` / `lead` “not implemented for the Robin backend” – documented in `docs/robin_parity_matrix.md`, `tests/robin_skip_list.json`.
- `join on expression (e.g. df1.a == df2.b) is not supported` – only join on column names.
- `crossJoin() is not implemented`.

**Affected:** unit/test_window_arithmetic.py, parity/dataframe/test_join.py, integration/test_case_sensitivity.py.

### 3. **Type / conversion errors**
- `ValueError: cannot convert to Column` – Python objects (e.g. from unionByName or select) not converted to Robin Column.
- `ValueError: select expects Column or str` – agg/groupby/select receiving wrong type (e.g. PySortOrder or non-column).
- `ValueError: Invalid column type: <class 'builtins.PySortOrder'>. Must be str or Column` – sort order used where column expected.
- `TypeError: 'RobinColumn' object is not callable` – test or code treating F.col / Column as callable.
- `ValueError: create_dataframe_from_rows failed: ...` – e.g. struct/map row format (dict vs string), or “createDataFrame requires schema when data is list of tuples”.

**Affected:** issue tests (260, 270, 286, 288, 355), parity aggregations, groupby, first_method, join, DDL/DML.

### 4. **Robin engine / upstream behavior**
- `ValueError: collect failed: cannot compare string with numeric type` – Robin vs PySpark coercion (issue 225).
- `ValueError: isin requires a list of int or str values` / `'is_in' cannot check for List(Int64) values in String data` – isin() type handling.
- `ValueError: create_dataframe_from_rows failed: map column 'map' expects JSON object (dict), got String(...)` – Python dict string repr passed instead of dict.

**Affected:** test_issues_225_231 (isin, string/numeric coercion, getItem/map).

### 5. **Session / config**
- `RuntimeError: Cannot perform sum aggregate function: No active SparkSession found` – session not set in some code path.
- `'_RobinRuntimeConfig' object has no attribute 'is_case_sensitive'` – case sensitivity config not implemented for Robin.
- Tests expecting exceptions in case-sensitive mode: “Failed: DID NOT RAISE” – Robin may not enforce case-sensitive mode.

**Affected:** test_issue_355 (unionByName), integration/test_case_sensitivity.py.

### 6. **Aggregation / alias naming**
- `KeyError: "Key 'avg(Value)' not found in row"` – Robin returns different agg column names than tests expect (e.g. no `avg(Value)` alias).
- `TypeError: max()/min() takes 1 positional arguments but 2 were given` – F.max/F.min called with two args (column + alias?) not supported.

**Affected:** parity/dataframe/test_aggregations.py, test_grouped_data_mean_parity.py.

### 7. **Parquet / table / DDL**
- Parquet format table append tests – storage/visibility of appended data across sessions.
- DDL tests: create_database, drop_database, create_table_*, drop_table, set_current_database, table_in_specific_database – catalog/schema support in Robin.

**Affected:** parity/sql/test_ddl.py, parity/dataframe/test_parquet_format_table_append.py, test_dml (insert_into).

### 8. **Misc**
- `AttributeError: 'NoneType' object has no attribute 'fields'` – schema/DataFrame missing in unionByName case.
- `KeyError: "Key 'name' not found in row"` – row key casing or structure.
- Pandas DataFrame support: “createDataFrame requires schema when data is list of tuples” / “'str' object cannot be converted to 'PyDict'” – createDataFrame(pandas_df) path.

---

## Recommended next steps

1. **Add to Robin skip list**  
   Mark known “not implemented” (row_number, percent_rank, join on expr, crossJoin, etc.) in `tests/robin_skip_list.json` so CI stays green and gaps are explicit.

2. **Implement missing Python shims**  
   - Expose `get_item` (and possibly `over`) in `_robin_functions` / sparkless_robin.  
   - Add Column-like operators on RobinColumn where possible (e.g. `__add__`, `__mul__`, `contains`) by delegating to F.* or Robin.

3. **Fix Column/agg/sort types**  
   - Ensure select/agg/groupby accept only Column or str; convert PySortOrder to sort spec instead of passing as column.  
   - Fix “cannot convert to Column” in unionByName and similar by normalizing to Robin Column in the Python layer.

4. **Session and config**  
   - Ensure an active SparkSession where aggregations are run (e.g. unionByName diamond test).  
   - Add `is_case_sensitive` (or equivalent) to Robin runtime config and enforce in tests or document as unsupported.

5. **Aggregation naming**  
   - Align Robin agg result column names with PySpark (e.g. `avg(column)` alias) or adapt tests to Robin’s naming.

6. **Upstream Robin**  
   - For coercion (string/numeric), isin types, and map/struct row format, consider opening or referencing issues in robin-sparkless and document in `docs/robin_parity_matrix.md`.

---

*Generated from test run with output in `test_results.txt`.*
