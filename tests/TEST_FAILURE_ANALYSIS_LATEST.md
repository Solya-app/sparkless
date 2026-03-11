# Test Failure Analysis (Robin Backend)

**Run:** `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12`  
**Results file:** `sparkless/tests/run_results_20260222_180810.txt`

## Summary

| Phase     | Passed | Failed | Errors | Skipped |
|-----------|--------|--------|--------|---------|
| Unit      | 809    | 1745    | 0    | 21       |
| Parity    | 0     | 0    | 0     | 0       |

**Total failures/errors:** 1745 unit + 0 parity = **1745** (excluding passed/skipped).

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
| cannot convert to Column | test_string_concat_with_empty_strings, test_string_concat_with_none_values, test_nested_string_concat, test_string_concat_vs_numeric_addition (+260) | Crate: fix semantics or track upstream. |
| select expects Column or str | test_max_modulo, test_reverse_operations, test_chained_arithmetic, test_multiple_aggregate_arithmetic (+124) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'St... | test_struct_field_with_alias_parity, test_struct_field_with_alias_multiple_fields_parity, test_struct_field_with_alias_and_other_columns_parity, test_struct_field_with_alias_schema_verification (+45) | Crate: fix semantics or track upstream. |
| collect failed: casting from Utf8View to Boolean not supported | test_filter_with_string_equals, test_filter_with_string_and_condition, test_filter_like_with_show, test_filter_like_prefix_pattern (+39) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: struct value must be object (by field name... | test_getfield_struct_field_by_name, test_withfield_combined_with_filter, test_withfield_combined_with_select, test_withfield_replace_existing_field (+31) | Crate: fix semantics or track upstream. |
| collect failed: arithmetic on string and numeric not allowed, try an explicit... | test_power_string_coercion, test_reverse_operations_with_string_columns, test_string_arithmetic_with_very_large_numbers, test_string_arithmetic_in_select (+25) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `i64` for serie... | test_casewhen_nested_expressions, test_casewhen_division_by_zero, test_casewhen_modulo_by_zero, test_casewhen_with_zero_and_negative (+19) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: array element ... | test_getfield_nested_array_access, test_getfield_chained_access, test_posexplode_nested_arrays, test_array_type_elementtype_nested_arrays (+6) | Crate: fix semantics or track upstream. |
| assert False | test_pyspark_parity_int64_string_inner, test_pyspark_parity_double_precision_string, test_first_ignorenulls_type_preservation, test_column_methods_exist (+6) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `i32` for serie... | test_casewhen_multiple_when_conditions, test_when_comparison_with_none_exact_issue, test_when_comparison_with_none_and_show, test_casewhen_cast_to_long_issue_243 (+5) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'm'... | test_map_column_subscript_key_not_found, test_map_column_subscript_then_filter, test_map_column_subscript_null_key_returns_null, test_map_column_subscript_coalesce_default (+4) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `f64` for serie... | test_casewhen_with_floats, test_power_with_conditional, test_casewhen_cast_to_int, test_reverse_operations_with_when_otherwise | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `str` for serie... | test_between_in_when_otherwise_expression, test_between_string_column_in_when_otherwise, test_casewhen_cast_with_multiple_when, test_expressions_with_case_variations | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported string f... | test_array_contains_join_empty_arrays, test_posexplode_alias_two_names_empty_array, test_posexplode_empty_array, test_posexplode_alias_empty_array | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported boolean ... | test_explode_with_booleans, test_array_distinct_boolean_arrays, test_array_type_elementtype_with_all_primitive_types | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'me... | test_drop_duplicates_with_dict_column_mock_only, test_describe_detail_complex_schema, test_array_type_elementtype_in_complex_schema | Crate: fix semantics or track upstream. |
| table(test_schema.test_table) failed: Table or view 'test_schema.test_table' ... | test_parquet_format_append_detached_df_visible_to_active_session, test_parquet_format_append_detached_df_visible_to_multiple_sessions | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 's' ... | test_cast_date_only_string_to_timestamp, test_cast_string_to_timestamp_still_works | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `i32` failed in column 'text' for va... | test_astype_empty_string_handling, test_astype_invalid_string_to_int | Crate: fix semantics or track upstream. |
| collect failed: dtype Unknown(Any) not supported in 'not' operation | test_casewhen_bitwise_not | Crate: fix semantics or track upstream. |
| assert ('At least one column' in 'cannot convert to Column' or 'must be speci... | test_window_orderby_list_empty_list_error | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ou... | test_column_subscript_with_nested_struct | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Le... | test_column_subscript_deeply_nested_struct | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported long for... | test_array_distinct_with_nulls_in_array | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ma... | test_map_column_subscript_with_column_key_exact_issue_441 | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'da... | test_map_column_subscript_in_select | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported int for ... | test_drop_duplicates_with_nulls_in_array | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 'Dat... | test_exact_scenario_from_issue_432 | Crate: fix semantics or track upstream. |
| collect failed: casting from string to boolean failed for value '' | test_astype_string_to_boolean | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i32) | test_between_string_column_in_select_expression | Crate: fix semantics or track upstream. |
| collect failed: div operation not supported for dtypes `str` and `str` | test_string_arithmetic_with_string_column | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i64) | test_numeric_eq_string | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'ma... | test_getItem_with_map_key | Crate: fix semantics or track upstream. |
| collect failed: 'is_in' cannot check for List(Int64) values in String data | test_isin_with_mixed_types | Crate: fix semantics or track upstream. |

---

## 3. Fix Sparkless (Python / Robin integration layer)


Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.


### 3.1 PyColumn (missing methods)


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'builtins.PyColumn' object has no attribute 'drop' | test_na_drop_with_subset_exact_issue_scenario, test_na_drop_no_subset_drops_any_null, test_na_drop_subset_as_string (+19) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'flatMap' | test_rdd_flatmap_words, test_rdd_flatmap_empty_iterable, test_rdd_flatmap_then_map (+9) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'getField' | test_getfield_array_index, test_getfield_equivalent_to_getitem, test_getfield_negative_index | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'over' | test_first_value, test_first_over_partition_order_desc | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'keys' | test_tuple_data_preserves_order | Implement or fix in Sparkless (see current analysis). |

### 3.2 Session / lifecycle


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| Cannot perform mean aggregate function: No active SparkSession foun... | test_cast_alias_select_complex_nested_operations, test_cast_alias_select_with_join, test_cast_alias_select_with_union (+15) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform sum aggregate function: No active SparkSession found... | test_window_function_comparison_with_sum, test_window_orderby_list_with_rows_between, test_window_function_comparison_with_rowsBetween (+3) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform count aggregate function: No active SparkSession fou... | test_functions_are_static_methods, test_function_signatures_match_pyspark, test_window_function_comparison_with_count (+1) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform max aggregate function: No active SparkSession found... | test_window_function_comparison_with_max, test_grouped_data_mean_with_multiple_aggregations, test_create_map_in_groupby_agg | Implement or fix in Sparkless (see current analysis). |
| Cannot perform avg aggregate function: No active SparkSession found... | test_window_function_comparison_with_avg, test_window_orderby_list_with_aggregation_functions | Implement or fix in Sparkless (see current analysis). |
| Cannot perform countDistinct aggregate function: No active SparkSes... | test_window_orderby_list_with_count_distinct, test_window_function_comparison_with_countDistinct | Implement or fix in Sparkless (see current analysis). |
| Cannot perform min aggregate function: No active SparkSession found... | test_window_function_comparison_with_min | Implement or fix in Sparkless (see current analysis). |
| Cannot perform stddev aggregate function: No active SparkSession fo... | test_window_orderby_list_with_stddev_variance | Implement or fix in Sparkless (see current analysis). |

### 3.3 Schema / NoneType


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'NoneType' object has no attribute 'fields' | test_unionByName_all_case_variations, test_select_all_case_variations, test_case_insensitive_unionByName | Implement or fix in Sparkless (see current analysis). |

### 3.4 Error message parity


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| createDataFrame requires schema when data is list of tuples | test_createDataFrame_from_pandas, test_createDataFrame_from_pandas_with_nulls | Implement or fix in Sparkless (see current analysis). |

---

## 4. Other


Failures not categorized as robin_sparkless or fix_sparkless: **1016**.


Sample (first 15):


- `tests/test_groupby_rollup_cube_with_list.py::test_rollup_with_tuple` — rollup() is not implemented for the Robin backend. See do...

- `tests/parity/functions/test_array.py::TestArrayFunctionsParity::test_array_contains` — module 'sparkless.sql.functions' has no attribute 'array_...

- `tests/parity/dataframe/test_join.py::TestJoinParity::test_semi_join` — join on expression (e.g. df1.a == df2.b) is not supported...

- `tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_date_types` — 'RobinColumn' object is not callable

- `tests/parity/sql/test_advanced.py::TestSQLAdvancedParity::test_sql_with_in_clause` — SQL failed: 'is_in' cannot check for List(Int64) values i...

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_string_concat` — DataFrames are not equivalent:

- `tests/parity/dataframe/test_join.py::TestJoinParity::test_anti_join` — join on expression (e.g. df1.a == df2.b) is not supported...

- `tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_datetime_types` — 'RobinColumn' object is not callable

- `tests/parity/functions/test_array.py::TestArrayFunctionsParity::test_array_position` — module 'sparkless.sql.functions' has no attribute 'array_...

- `tests/test_groupby_rollup_cube_with_list.py::test_rollup_backward_compatibility` — rollup() is not implemented for the Robin backend. See do...

- `tests/parity/sql/test_ddl.py::TestSQLDDLParity::test_create_database` — assert 'test_db' in ['default', 'global_temp']

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_string_split` — DataFrames are not equivalent:

- `tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_column_vs_literal` — 'RobinColumn' object is not callable

- `tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_integer_literal` — 'RobinColumn' object is not callable

- `tests/parity/functions/test_array.py::TestArrayFunctionsParity::test_size` — module 'sparkless.sql.functions' has no attribute 'size'


… and 1001 more.


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
