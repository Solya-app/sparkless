# Test Failure Analysis (Robin Backend)

**Run:** `SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12`  
**Results file:** `sparkless/test_results.txt`

## Summary

| Phase     | Passed | Failed | Errors | Skipped |
|-----------|--------|--------|--------|---------|
| Unit      | 684    | 1870    | 0    | 21       |
| Parity    | 0     | 0    | 0     | 0       |

**Total failures/errors:** 1870 unit + 0 parity = **1870** (excluding passed/skipped).

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
| select expects Column or str | test_arithmetic_with_floats, test_arithmetic_with_negative_numbers, test_arithmetic_with_zero, test_division_by_zero_handling (+105) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'St... | test_struct_field_with_alias_parity, test_struct_field_with_alias_multiple_fields_parity, test_struct_field_with_alias_and_other_columns_parity, test_struct_field_with_alias (+45) | Crate: fix semantics or track upstream. |
| collect failed: filter predicate must be of type `Boolean`, got `String` | test_filter_with_string_equals, test_filter_with_string_and_condition, test_filter_is_null_and_string_equality, test_filter_in_multiple_literals (+39) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: struct value must be object (by field name... | test_getfield_struct_field_by_name, test_withfield_with_conditional_expression, test_withfield_with_cast_operation, test_withfield_replace_with_different_type (+31) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: array element ... | test_posexplode_nested_arrays, test_getfield_nested_array_access, test_getfield_chained_access, test_array_type_elementtype_nested_arrays (+6) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'm'... | test_map_column_subscript_key_not_found, test_map_column_subscript_then_filter, test_map_column_subscript_null_key_returns_null, test_map_column_subscript_coalesce_default (+4) | Crate: fix semantics or track upstream. |
| assert False | test_pyspark_parity_int64_string_inner, test_pyspark_parity_double_precision_string, test_column_methods_exist, test_first_ignorenulls_type_preservation (+3) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported string f... | test_posexplode_alias_two_names_empty_array, test_posexplode_empty_array, test_array_contains_join_empty_arrays, test_posexplode_alias_empty_array | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported boolean ... | test_explode_with_booleans, test_array_distinct_boolean_arrays, test_array_type_elementtype_with_all_primitive_types | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'me... | test_drop_duplicates_with_dict_column_mock_only, test_describe_detail_complex_schema, test_array_type_elementtype_in_complex_schema | Crate: fix semantics or track upstream. |
| table(test_schema.test_table) failed: Table or view 'test_schema.test_table' ... | test_parquet_format_append_detached_df_visible_to_active_session, test_parquet_format_append_detached_df_visible_to_multiple_sessions | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `i32` failed in column 'text' for va... | test_astype_empty_string_handling, test_astype_invalid_string_to_int | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 's' ... | test_cast_string_to_timestamp_still_works, test_cast_date_only_string_to_timestamp | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ou... | test_column_subscript_with_nested_struct | Crate: fix semantics or track upstream. |
| assert ('At least one column' in 'cannot convert to Column' or 'must be speci... | test_window_orderby_list_empty_list_error | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Le... | test_column_subscript_deeply_nested_struct | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i32) | test_between_string_column_in_select_expression | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported long for... | test_array_distinct_with_nulls_in_array | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported int for ... | test_drop_duplicates_with_nulls_in_array | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 'Dat... | test_exact_scenario_from_issue_432 | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ma... | test_map_column_subscript_with_column_key_exact_issue_441 | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'da... | test_map_column_subscript_in_select | Crate: fix semantics or track upstream. |
| collect failed: casting from f64 to i32 not supported | test_astype_double_to_int | Crate: fix semantics or track upstream. |
| collect failed: casting from string to boolean failed for value '' | test_astype_string_to_boolean | Crate: fix semantics or track upstream. |
| collect failed: 'is_in' cannot check for List(Int64) values in String data | test_isin_with_mixed_types | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i64) | test_numeric_eq_string | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'ma... | test_getItem_with_map_key | Crate: fix semantics or track upstream. |

---

## 3. Fix Sparkless (Python / Robin integration layer)


Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.


### 3.1 PyColumn (missing methods)


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'builtins.PyColumn' object has no attribute '__mul__' | test_struct_with_expressions, test_with_column, test_struct_with_math_operations (+70) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'mode' | test_sql_with_in_clause, test_create_table_from_dataframe, test_create_table_with_select (+65) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'otherwise' | test_casewhen_nested_expressions, test_casewhen_division_by_zero, test_casewhen_modulo_by_zero (+34) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__truediv__' | test_avg_divide, test_fillna_float_multiple_calculated_columns, test_fillna_float_after_filter (+21) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'format' | test_parquet_format_append_to_existing_table, test_parquet_format_append_to_new_table, test_parquet_format_multiple_append_operations (+20) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'drop' | test_na_drop_all_rows_have_null_how_any, test_na_drop_how_all_only_drops_fully_null_rows, test_na_drop_thresh_one_keeps_rows_with_at_least_one_non_null (+19) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'isNotNull' | test_eqnullsafe_with_date_types, test_eqnullsafe_with_datetime_types, test_eqnullsafe_with_column_vs_literal (+17) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__add__' | test_arithmetic_with_nulls, test_mixed_aggregate_functions, test_columns_available_after_collect (+17) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'flatMap' | test_rdd_flatmap_words, test_rdd_flatmap_empty_iterable, test_rdd_flatmap_then_map (+9) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__sub__' | test_count_star_arithmetic, test_power_negative_exponent_exact_issue, test_power_negative_exponent_multiple_values (+7) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'when' | test_casewhen_multiple_when_conditions, test_between_string_column_in_when_otherwise, test_casewhen_cast_with_multiple_when (+2) | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'getField' | test_getfield_array_index, test_getfield_equivalent_to_getitem, test_getfield_negative_index | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__mod__' | test_reverse_operations_modulo_by_zero, test_reverse_modulo, test_string_modulo_with_numeric | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'contains' | test_concat_filter_after, test_string_operations_in_filters | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute 'over' | test_first_value | Implement or fix in Sparkless (see current analysis). |
| 'builtins.PyColumn' object has no attribute '__invert__' | test_logical_operations_in_filters | Implement or fix in Sparkless (see current analysis). |

### 3.2 Session / lifecycle


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| Cannot perform mean aggregate function: No active SparkSession foun... | test_cast_alias_select_basic, test_cast_alias_select_multiple_aggregations, test_cast_alias_select_different_cast_types (+15) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform sum aggregate function: No active SparkSession found... | test_window_function_comparison_with_sum, test_window_orderby_list_with_rows_between, test_window_function_comparison_with_rowsBetween (+3) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform count aggregate function: No active SparkSession fou... | test_functions_are_static_methods, test_function_signatures_match_pyspark, test_cast_alias_select_all_aggregation_functions (+1) | Implement or fix in Sparkless (see current analysis). |
| Cannot perform max aggregate function: No active SparkSession found... | test_grouped_data_mean_with_multiple_aggregations, test_window_function_comparison_with_max, test_create_map_in_groupby_agg | Implement or fix in Sparkless (see current analysis). |
| Cannot perform avg aggregate function: No active SparkSession found... | test_window_function_comparison_with_avg, test_window_orderby_list_with_aggregation_functions | Implement or fix in Sparkless (see current analysis). |
| Cannot perform countDistinct aggregate function: No active SparkSes... | test_window_function_comparison_with_countDistinct, test_window_orderby_list_with_count_distinct | Implement or fix in Sparkless (see current analysis). |
| Cannot perform min aggregate function: No active SparkSession found... | test_window_function_comparison_with_min | Implement or fix in Sparkless (see current analysis). |
| Cannot perform stddev aggregate function: No active SparkSession fo... | test_window_orderby_list_with_stddev_variance | Implement or fix in Sparkless (see current analysis). |

### 3.3 Schema / NoneType


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'NoneType' object has no attribute 'fields' | test_select_all_case_variations, test_unionByName_all_case_variations, test_case_insensitive_unionByName | Implement or fix in Sparkless (see current analysis). |

### 3.4 Error message parity


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| createDataFrame requires schema when data is list of tuples | test_createDataFrame_from_pandas, test_createDataFrame_from_pandas_with_nulls | Implement or fix in Sparkless (see current analysis). |

### 3.5 Other (fix_sparkless)


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'str' object cannot be converted to 'PyDict' | test_createDataFrame_from_pandas_empty, test_createDataFrame_from_pandas_with_schema | Implement or fix in Sparkless (see current analysis). |

---

## 4. Other


Failures not categorized as robin_sparkless or fix_sparkless: **947**.


Sample (first 15):


- `tests/parity/functions/test_array.py::TestArrayFunctionsParity::test_array_contains` — module 'sparkless.sql.functions' has no attribute 'array_...

- `tests/test_issue_286_aggregate_function_arithmetic.py::TestIssue286AggregateFunctionArithmetic::test_max_modulo` — max() takes 1 positional arguments but 2 were given

- `tests/test_groupby_rollup_cube_with_list.py::test_rollup_with_tuple` — rollup() is not implemented for the Robin backend. See do...

- `tests/parity/dataframe/test_join.py::TestJoinParity::test_semi_join` — join on expression (e.g. df1.a == df2.b) is not supported...

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_string_concat` — DataFrames are not equivalent:

- `tests/parity/sql/test_ddl.py::TestSQLDDLParity::test_create_database` — assert 'test_db' in ['default', 'global_temp']

- `tests/test_issue_286_aggregate_function_arithmetic.py::TestIssue286AggregateFunctionArithmetic::test_reverse_operations` — count_distinct() takes 1 positional arguments but 2 were ...

- `tests/parity/dataframe/test_join.py::TestJoinParity::test_anti_join` — join on expression (e.g. df1.a == df2.b) is not supported...

- `tests/parity/functions/test_array.py::TestArrayFunctionsParity::test_array_position` — module 'sparkless.sql.functions' has no attribute 'array_...

- `tests/parity/functions/test_array.py::TestArrayFunctionsParity::test_size` — module 'sparkless.sql.functions' has no attribute 'size'

- `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_string_split` — DataFrames are not equivalent:

- `tests/test_issue_286_aggregate_function_arithmetic.py::TestIssue286AggregateFunctionArithmetic::test_chained_arithmetic` — count_distinct() takes 1 positional arguments but 2 were ...

- `tests/test_groupby_rollup_cube_with_list.py::test_rollup_backward_compatibility` — rollup() is not implemented for the Robin backend. See do...

- `tests/parity/sql/test_ddl.py::TestSQLDDLParity::test_create_database_if_not_exists` — assert 'test_db2' in ['default', 'global_temp']

- `tests/test_groupby_rollup_cube_with_list.py::test_cube_with_list` — cube() is not implemented for the Robin backend. See docs...


… and 932 more.


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
