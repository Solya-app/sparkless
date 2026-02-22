# Test Failure Analysis (Robin Backend)

**Run:** `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12`  
**Results file:** `sparkless/tests/run_results_n9_20260222_183727.txt`

## Summary

| Phase     | Passed | Failed | Errors | Skipped |
|-----------|--------|--------|--------|---------|
| Unit      | 876    | 1678    | 0    | 21       |
| Parity    | 0     | 0    | 0     | 0       |

**Total failures/errors:** 1678 unit + 0 parity = **1678** (excluding passed/skipped).

---

## 1. Tests removed


The following obsolete tests were deleted or moved so they no longer run.


### 1.1 Deleted files


| File | Reason |
|------|--------|
| `tests/unit/backend/test_robin_unsupported_raises.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/unit/backend/test_robin_optional.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/unit/backend/test_robin_materializer.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/test_backend_capability_model.py` | Obsolete backend/BackendFactory/Polars. |
| `tests/test_issue_160_*.py` (13 files) | Polars/BackendFactory/cache; backend removed. |

### 1.2 Moved to archive


- `tests/unit/dataframe/test_inferschema_parity.py → tests/archive/unit/dataframe/test_inferschema_parity.py` (ignored by pytest `--ignore=tests/archive`).


---

## 2. Robin-sparkless (upstream crate)


Failures that indicate the **robin-sparkless** crate does not match PySpark semantics. Fix or track upstream.


| Error pattern | Example tests | Action |
|---------------|----------------|--------|
| cannot convert to Column | test_date_column_vs_string_column_filter, test_select_dropped_column_raises_proper_error, test_date_column_vs_string_column_all_operators[>-expected_names0], test_date_column_vs_string_column_all_operators[>=-expected_names1] (+273) | Crate: fix semantics or track upstream. |
| select expects Column or str | test_join_then_aggregate_with_join_keys, test_cast_alias_select_multiple_aggregations_parity, test_select_dropped_column_with_f_col, test_count_distinct_minus_one (+126) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'St... | test_struct_field_with_alias_parity, test_struct_field_with_alias_multiple_fields_parity, test_struct_field_with_alias_and_other_columns_parity, test_column_subscript_with_null_field (+45) | Crate: fix semantics or track upstream. |
| collect failed: casting from Utf8View to Boolean not supported | test_filter_with_string_equals, test_filter_with_string_and_condition, test_filter_values_in_string_literal, test_filter_values_in_numeric_literal (+39) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: struct value must be object (by field name... | test_getfield_struct_field_by_name, test_withfield_nested_struct_arithmetic, test_withfield_with_column_expression, test_withfield_with_computed_expression (+31) | Crate: fix semantics or track upstream. |
| collect failed: arithmetic on string and numeric not allowed, try an explicit... | test_power_string_coercion, test_reverse_operations_with_string_columns, test_numeric_literal_divided_by_string, test_string_addition_with_numeric (+25) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `i64` for serie... | test_casewhen_division, test_casewhen_modulo, test_casewhen_bitwise_or, test_casewhen_with_literal (+19) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: array element ... | test_getfield_nested_array_access, test_getfield_chained_access, test_posexplode_nested_arrays, test_array_type_elementtype_with_struct_type (+6) | Crate: fix semantics or track upstream. |
| assert False | test_pyspark_parity_int64_string_inner, test_pyspark_parity_double_precision_string, test_first_ignorenulls_type_preservation, test_column_methods_exist (+6) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `i32` for serie... | test_casewhen_multiple_when_conditions, test_when_comparison_with_none_exact_issue, test_when_comparison_with_none_and_show, test_casewhen_cast_to_long_issue_243 (+5) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'm'... | test_map_column_subscript_key_not_found, test_map_column_subscript_then_filter, test_map_column_subscript_null_key_returns_null, test_map_column_subscript_coalesce_default (+4) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `f64` for serie... | test_casewhen_with_floats, test_power_with_conditional, test_casewhen_cast_to_int, test_reverse_operations_with_when_otherwise | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `str` for serie... | test_between_in_when_otherwise_expression, test_between_string_column_in_when_otherwise, test_casewhen_cast_with_multiple_when, test_expressions_with_case_variations | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported string f... | test_array_contains_join_empty_arrays, test_posexplode_alias_two_names_empty_array, test_posexplode_empty_array, test_posexplode_alias_empty_array | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported boolean ... | test_explode_with_booleans, test_array_distinct_boolean_arrays, test_array_type_elementtype_with_all_primitive_types | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'me... | test_drop_duplicates_with_dict_column_mock_only, test_describe_detail_complex_schema, test_array_type_elementtype_in_complex_schema | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 's' ... | test_cast_string_to_timestamp_still_works, test_cast_date_only_string_to_timestamp | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `i32` failed in column 'text' for va... | test_astype_invalid_string_to_int, test_astype_empty_string_handling | Crate: fix semantics or track upstream. |
| table(test_schema.test_table) failed: Table or view 'test_schema.test_table' ... | test_parquet_format_append_detached_df_visible_to_active_session, test_parquet_format_append_detached_df_visible_to_multiple_sessions | Crate: fix semantics or track upstream. |
| collect failed: dtype Unknown(Any) not supported in 'not' operation | test_casewhen_bitwise_not | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Le... | test_column_subscript_deeply_nested_struct | Crate: fix semantics or track upstream. |
| assert ('At least one column' in 'cannot convert to Column' or 'must be speci... | test_window_orderby_list_empty_list_error | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ou... | test_column_subscript_with_nested_struct | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 'Dat... | test_exact_scenario_from_issue_432 | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported int for ... | test_drop_duplicates_with_nulls_in_array | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ma... | test_map_column_subscript_with_column_key_exact_issue_441 | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'da... | test_map_column_subscript_in_select | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported long for... | test_array_distinct_with_nulls_in_array | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i32) | test_between_string_column_in_select_expression | Crate: fix semantics or track upstream. |
| collect failed: casting from string to boolean failed for value '' | test_astype_string_to_boolean | Crate: fix semantics or track upstream. |
| collect failed: div operation not supported for dtypes `str` and `str` | test_string_arithmetic_with_string_column | Crate: fix semantics or track upstream. |
| collect failed: 'is_in' cannot check for List(Int64) values in String data | test_isin_with_mixed_types | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i64) | test_numeric_eq_string | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'ma... | test_getItem_with_map_key | Crate: fix semantics or track upstream. |

---

## 3. Fix Sparkless (Python / Robin integration layer)


Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.


### 3.1 Schema / NoneType


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'NoneType' object has no attribute 'fields' | test_select_all_case_variations, test_unionByName_all_case_variations, test_case_insensitive_unionByName | Implement or fix in Sparkless (see current analysis). |

---

## 4. Other


Failures not categorized as robin_sparkless or fix_sparkless: **1013**.


Sample (first 15):


- `tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_storage_manager_detached_write_visible_to_session` — 'NoneType' object has no attribute 'create_schema'

- `tests/parity/sql/test_show_describe.py::TestSQLShowDescribeParity::test_describe_column` — SQL failed: SQL parse error: sql parser error: Expected: ...

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_json_tuple` — module 'sparkless.sql.functions' has no attribute 'json_t...

- `tests/test_issue_153_to_timestamp_returns_none.py::TestIssue153ToTimestampReturnsNone::test_to_timestamp_with_clean_string` — to_timestamp is not implemented for the Robin backend. Us...

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_substring_index` — module 'sparkless.sql.functions' has no attribute 'substr...

- `tests/test_issue_156_select_dropped_column.py::test_select_dropped_column_minimal_repro` — select failed: not found: Column 'col2' not found. Availa...

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_repeat` — module 'sparkless.sql.functions' has no attribute 'repeat'

- `tests/test_column_availability.py::TestColumnAvailability::test_materialized_columns_are_available` — _get_available_columns

- `tests/test_issue_280_join_groupby_ambiguity.py::TestJoinThenGroupByNoAmbiguity::test_join_with_nulls_then_groupby` — "Key 'count' not found in row"

- `tests/test_column_availability.py::TestColumnAvailability::test_columns_available_after_collect` — _get_available_columns

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_reverse` — module 'sparkless.sql.functions' has no attribute 'reverse'

- `tests/parity/functions/test_datetime.py::TestDatetimeFunctionsParity::test_year` — DataFrames are not equivalent:

- `tests/test_column_availability.py::TestColumnAvailability::test_columns_available_after_show` — _get_available_columns

- `tests/parity/functions/test_datetime.py::TestDatetimeFunctionsParity::test_month` — DataFrames are not equivalent:

- `tests/test_column_availability.py::TestColumnAvailability::test_dataframe_is_marked_materialized` — _materialized


… and 998 more.


---

## 5. Suggested order of work


1. **Delete** obsolete tests (section 1) — done.

2. **Sparkless fixes** that unblock many tests: PyColumn (astype, desc/asc, isin, getItem, substr, reverse operators), RobinDataFrameReader.option(), RobinSparkSession.stop(), Functions (first, rank), fillna(subset=...), withField/createDataFrame tuple→dict conversion.

3. **Robin-sparkless** (upstream): create_dataframe_from_rows (map, array, struct), select/Column conversion, comparison/union/join coercion, temp view lookup.

4. **Re-run** with the command below and iterate.


---

## 6. How to re-run and regenerate


Run the full suite and save output:


```bash

SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12 2>&1 | tee tests/results_robin_$(date +%Y%m%d_%H%M%S).txt

```


Then parse the new results file and regenerate this report:


```bash

python tests/tools/parse_robin_results.py tests/results_robin_<timestamp>.txt -o tests/robin_results_parsed.json

python tests/tools/generate_failure_report.py -i tests/robin_results_parsed.json -o tests/TEST_FAILURE_ANALYSIS.md

```
