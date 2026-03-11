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
| udf is not implemented for the Robin backend. Use tests/robin_skip_list.json ... | test_udf_two_arguments, test_udf_two_arguments_string_names, test_udf_three_arguments, test_udf_multiply_arguments (+84) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'St... | test_struct_field_with_alias_parity, test_struct_field_with_alias_multiple_fields_parity, test_struct_field_with_alias_and_other_columns_parity, test_column_subscript_with_null_field (+45) | Crate: fix semantics or track upstream. |
| explode is not implemented for the Robin backend. See docs/robin_parity_matri... | test_split_with_limit_parity, test_split_with_limit_1_parity, test_split_without_limit_parity, test_split_with_limit_minus_one_parity (+43) | Crate: fix semantics or track upstream. |
| collect failed: casting from Utf8View to Boolean not supported | test_filter_with_string_equals, test_filter_with_string_and_condition, test_filter_values_in_string_literal, test_filter_values_in_numeric_literal (+39) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: struct value must be object (by field name... | test_getfield_struct_field_by_name, test_withfield_nested_struct_arithmetic, test_withfield_with_column_expression, test_withfield_with_computed_expression (+31) | Crate: fix semantics or track upstream. |
| DataFrames are not equivalent: | test_year, test_month, test_date_add, test_date_sub (+30) | Crate: fix semantics or track upstream. |
| struct is not implemented for the Robin backend. See docs/robin_parity_matrix... | test_struct_basic, test_struct_with_col_function, test_struct_single_column, test_struct_with_nulls (+28) | Crate: fix semantics or track upstream. |
| join failed: not found: Column 'array_contains(<builtins.PyColumn object at 0... | test_array_contains_join_right, test_array_contains_join_outer, test_array_contains_join_with_select, test_array_contains_join_with_filter (+25) | Crate: fix semantics or track upstream. |
| collect failed: arithmetic on string and numeric not allowed, try an explicit... | test_power_string_coercion, test_reverse_operations_with_string_columns, test_numeric_literal_divided_by_string, test_string_addition_with_numeric (+25) | Crate: fix semantics or track upstream. |
| row_number is not implemented for the Robin backend. See docs/robin_parity_ma... | test_window_orderby_list_basic_parity, test_window_orderby_list_multiple_columns_parity, test_window_partitionby_list_parity, test_row_number (+23) | Crate: fix semantics or track upstream. |
| expr is not implemented for the Robin backend. See docs/robin_parity_matrix.m... | test_filter_in_or_string_literal_no_parse_exception, test_expr_like_standalone, test_filter_regexp_exact_issue_433, test_filter_rlike_same_as_regexp (+22) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `i64` for serie... | test_casewhen_division, test_casewhen_modulo, test_casewhen_bitwise_or, test_casewhen_with_literal (+19) | Crate: fix semantics or track upstream. |
| SQL failed: SQL: only SELECT, CREATE SCHEMA/DATABASE, and DROP TABLE/VIEW/SCH... | test_create_table_with_select, test_insert_into_table, test_insert_into_specific_columns, test_insert_multiple_values (+19) | Crate: fix semantics or track upstream. |
| posexplode is not implemented for the Robin backend. See docs/robin_parity_ma... | test_posexplode_alias_single_element, test_posexplode_alias_mixed_columns, test_posexplode_alias_string_array, test_posexplode_alias_column_object (+18) | Crate: fix semantics or track upstream. |
| join on expression (e.g. df1.a == df2.b) is not supported by the Robin backen... | test_join_different_column_names_exact_issue, test_join_different_column_names_reverse_order, test_join_different_column_names_left_no_match, test_join_different_column_names_inner (+15) | Crate: fix semantics or track upstream. |
| array_distinct is not implemented for the Robin backend. See docs/robin_parit... | test_array_distinct, test_array_distinct_null_array_returns_null, test_array_distinct_float_arrays_preserves_type, test_array_distinct_all_duplicates_int (+10) | Crate: fix semantics or track upstream. |
| SQL failed: SQL parse error: sql parser error: Expected: end of statement, fo... | test_describe_column, test_delta_create_or_replace_table_as_select, test_describe_detail_all_required_columns, test_describe_detail_table_properties (+9) | Crate: fix semantics or track upstream. |
| assert 0 == 1 | test_sql_with_having, test_date_less_than_datetime, test_datetime_greater_than_date, test_date_eq_datetime (+9) | Crate: fix semantics or track upstream. |
| SQL failed: SQL: only INNER, LEFT, RIGHT, FULL JOIN are supported. | test_sql_with_left_join, test_sql_with_left_join_and_aliases, test_robust_self_join_manager_column_and_row_count, test_robust_sql_where_table_prefixed (+8) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: array element ... | test_getfield_nested_array_access, test_getfield_chained_access, test_posexplode_nested_arrays, test_array_type_elementtype_with_struct_type (+6) | Crate: fix semantics or track upstream. |
| assert False | test_pyspark_parity_int64_string_inner, test_pyspark_parity_double_precision_string, test_first_ignorenulls_type_preservation, test_column_methods_exist (+6) | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `i32` for serie... | test_casewhen_multiple_when_conditions, test_when_comparison_with_none_exact_issue, test_when_comparison_with_none_and_show, test_casewhen_cast_to_long_issue_243 (+5) | Crate: fix semantics or track upstream. |
| assert None == 'A' | test_first_with_ignorenulls_true, test_first_ignorenulls_with_groupby, test_first_ignorenulls_pyspark_parity, test_first_ignorenulls_mixed_types (+5) | Crate: fix semantics or track upstream. |
| DID NOT RAISE <class 'Exception'> | test_tuple_data_error_message_matches_pyspark, test_case_sensitive_mode, test_case_sensitive_mode_exact_match_required, test_case_sensitive_withColumn_fails_with_wrong_case (+5) | Crate: fix semantics or track upstream. |
| assert None is not None | test_column_astype_method, test_astype_substring_date_pyspark_parity, test_astype_on_column_operation, test_astype_issue_239_example (+4) | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'm'... | test_map_column_subscript_key_not_found, test_map_column_subscript_then_filter, test_map_column_subscript_null_key_returns_null, test_map_column_subscript_coalesce_default (+4) | Crate: fix semantics or track upstream. |
| over() is not implemented for the Robin backend. See docs/robin_parity_matrix... | test_first_value, test_first_over_partition_order_desc, test_avg_over_partition_order_desc, test_avg_string_column (+3) | Crate: fix semantics or track upstream. |
| assert (None == 0.0) | test_fillna_float_fills_integer_column, test_fillna_float_subset_single_int_column, test_fillna_float_subset_tuple_int_columns, test_fillna_float_all_nulls_integer_column (+3) | Crate: fix semantics or track upstream. |
| unsupported join type: leftsemi | test_leftsemi_join_excludes_right_columns, test_leftsemi_join_multiple_keys, test_leftsemi_join_no_matches, test_leftsemi_join_all_match (+2) | Crate: fix semantics or track upstream. |
| percent_rank is not implemented for the Robin backend. See docs/robin_parity_... | test_percent_rank, test_window_function_rmul, test_window_function_multiply, test_window_function_chained_operations (+1) | Crate: fix semantics or track upstream. |
| isnan is not implemented for the Robin backend. See docs/robin_parity_matrix.... | test_isnan_on_string_column_filter_does_not_error_and_returns_empty, test_isnan_on_numeric_column_true_only_for_nan, test_isnan_literal_matches_python_math, test_isnan_on_string_and_numeric_columns_in_select (+1) | Crate: fix semantics or track upstream. |
| to_timestamp is not implemented for the Robin backend. Use skip list for test... | test_to_timestamp_with_clean_string, test_to_timestamp_with_validation_rule_not_null, test_to_timestamp_with_datetime_operations, test_to_timestamp_returns_actual_values | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `f64` for serie... | test_casewhen_with_floats, test_power_with_conditional, test_casewhen_cast_to_int, test_reverse_operations_with_when_otherwise | Crate: fix semantics or track upstream. |
| rollup() is not implemented for the Robin backend. See docs/robin_parity_matr... | test_rollup_with_list, test_rollup_with_tuple, test_rollup_backward_compatibility, test_rollup_cube_all_case_variations | Crate: fix semantics or track upstream. |
| collect failed: invalid series dtype: expected `Boolean`, got `str` for serie... | test_between_in_when_otherwise_expression, test_between_string_column_in_when_otherwise, test_casewhen_cast_with_multiple_when, test_expressions_with_case_variations | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported string f... | test_array_contains_join_empty_arrays, test_posexplode_alias_two_names_empty_array, test_posexplode_empty_array, test_posexplode_alias_empty_array | Crate: fix semantics or track upstream. |
| DID NOT RAISE <class 'sparkless.core.exceptions.analysis.ColumnNotFoundExcept... | test_na_drop_invalid_subset_column_raises, test_fillna_subset_nonexistent_column_raises_error, test_fillna_subset_multiple_nonexistent_columns_raises_error, test_na_fill_nonexistent_column | Crate: fix semantics or track upstream. |
| assert None == datetime.datetime(2023, 1, 1, 12, 0) | test_cast_datetime_to_timestamp_noop, test_cast_datetime_to_timestamp_with_nulls, test_cast_select_with_cast, test_cast_with_datatype_objects | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["id", "name"], Rig... | test_union_empty_dataframe_left, test_union_empty_dataframe_right, test_union_both_empty, test_union_then_select | Crate: fix semantics or track upstream. |
| assert 'None' == '2026-02-03' | test_to_date_cast_string_type_exact_issue, test_to_date_cast_with_nulls, test_to_date_cast_different_format, test_to_date_cast_integer_type_column | Crate: fix semantics or track upstream. |
| assert None is False | test_eqnullsafe_literal_semantics[x-None-False], test_eqnullsafe_in_select_expression, test_eqnullsafe_chained_with_other_operations | Crate: fix semantics or track upstream. |
| assert LongType(nullable=True) == IntegerType(nullable=True) | test_createDataFrame_with_explicit_schema, test_na_fill_schema_preservation, test_createDataFrame_from_pandas_with_schema | Crate: fix semantics or track upstream. |
| assert 'count' in ['dept', 'year', 'sales'] | test_groupBy_with_list, test_groupBy_with_tuple, test_groupBy_backward_compatibility | Crate: fix semantics or track upstream. |
| assert '25' == 25 | test_createDataFrame_list_rows_with_different_data_types, test_createDataFrame_single_list_row, test_createDataFrame_list_rows_with_none_values | Crate: fix semantics or track upstream. |
| cube() is not implemented for the Robin backend. See docs/robin_parity_matrix... | test_cube_with_list, test_cube_with_tuple, test_cube_backward_compatibility | Crate: fix semantics or track upstream. |
| SQL failed: SQL: statement type not supported, got Update(Update { update_tok... | test_update_table, test_update_multiple_columns, test_update_table_basic | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported boolean ... | test_explode_with_booleans, test_array_distinct_boolean_arrays, test_array_type_elementtype_with_all_primitive_types | Crate: fix semantics or track upstream. |
| select failed: not found: Column 'avg(Value)' not found. Available columns: [... | test_grouped_data_mean_with_select, test_grouped_data_mean_with_nested_select, test_mean_string_column_select_after | Crate: fix semantics or track upstream. |
| assert None == 4 | test_hour_minute_second_with_different_formats, test_hour_minute_second_in_select, test_hour_minute_second_with_null_values | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['a', 'b']' not found. Available columns:... | test_orderby_with_list_of_column_names, test_orderby_desc_with_list, test_orderby_list_empty_dataframe | Crate: fix semantics or track upstream. |
| assert ('1' == 1) | test_createDataFrame_from_rdd_single_row, test_createDataFrame_from_rdd_preserves_schema_order, test_union_createDataFrame_tuple_column_names | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'me... | test_drop_duplicates_with_dict_column_mock_only, test_describe_detail_complex_schema, test_array_type_elementtype_in_complex_schema | Crate: fix semantics or track upstream. |
| select failed: not found: Column 'col2' not found. Available columns: [col1].... | test_select_dropped_column_minimal_repro, test_minimal_reproduction | Crate: fix semantics or track upstream. |
| assert None == 'null-456' | test_format_string_with_null_parity, test_format_string_with_null_values | Crate: fix semantics or track upstream. |
| crossJoin() is not implemented for the Robin backend. See docs/robin_parity_m... | test_eqnullsafe_in_join_condition, test_cross_join | Crate: fix semantics or track upstream. |
| dense_rank is not implemented for the Robin backend. See docs/robin_parity_ma... | test_dense_rank, test_dense_rank_with_arithmetic | Crate: fix semantics or track upstream. |
| SQL failed: SQL: JOIN ON must use same column name on both sides (e.g. a.id =... | test_sql_with_inner_join, test_sql_with_inner_join_and_aliases | Crate: fix semantics or track upstream. |
| SQL failed: 'is_in' cannot check for List(Int64) values in String data | test_sql_with_in_clause, test_sql_in_clause_basic | Crate: fix semantics or track upstream. |
| SQL failed: SQL: statement type not supported, got Drop { object_type: Databa... | test_drop_database, test_table_in_specific_database | Crate: fix semantics or track upstream. |
| SQL failed: SQL: statement type not supported, got Delete(Delete { delete_tok... | test_delete_from_table, test_delete_all_rows | Crate: fix semantics or track upstream. |
| assert 7.999999999999998 == 8 | test_integer_power_column, test_column_power_column | Crate: fix semantics or track upstream. |
| assert 7.999999999999998 == 8.0 | test_power_in_select, test_power_with_nulls | Crate: fix semantics or track upstream. |
| assert None == 10 | test_orderby_with_null_values, test_first_ignorenulls_with_numeric_values | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x111286630>... | test_groupby_multiple_cols_one_aliased, test_groupby_alias_select_after | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x10f4dfb30>... | test_groupby_alias_count, test_groupby_list_syntax_with_alias | Crate: fix semantics or track upstream. |
| assert ('Alice' == 'Alice' | test_createDataFrame_from_rdd_with_schema_list, test_fillna_float_subset_calculated_column | Crate: fix semantics or track upstream. |
| assert None == datetime.date(2024, 1, 1) | test_cast_date_to_date_noop, test_cast_date_to_date_with_nulls | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 's' ... | test_cast_string_to_timestamp_still_works, test_cast_date_only_string_to_timestamp | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["id", "age", "name... | test_union_different_column_order_by_position, test_union_with_nulls | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `i32` failed in column 'text' for va... | test_astype_invalid_string_to_int, test_astype_empty_string_handling | Crate: fix semantics or track upstream. |
| assert ["['a', None,..., None, 'c']"] == ['a', None, 'b', None, 'c'] | test_array_type_elementtype_with_nullable_element, test_array_type_elementtype_array_with_nulls | Crate: fix semantics or track upstream. |
| DID NOT RAISE <class 'sparkless.core.exceptions.validation.IllegalArgumentExc... | test_tuple_data_mismatched_length, test_tuple_data_empty_schema | Crate: fix semantics or track upstream. |
| table(test_schema.test_table) failed: Table or view 'test_schema.test_table' ... | test_parquet_format_append_detached_df_visible_to_active_session, test_parquet_format_append_detached_df_visible_to_multiple_sessions | Crate: fix semantics or track upstream. |
| collect failed: dtype Unknown(Any) not supported in 'not' operation | test_casewhen_bitwise_not | Crate: fix semantics or track upstream. |
| Expected numeric type, got StringType | test_schema_inference_for_numeric_columns | Crate: fix semantics or track upstream. |
| Expected LongType, got StringType | test_schema_inference_for_integer_columns | Crate: fix semantics or track upstream. |
| assert 'StringType' == 'LongType' | test_schema_inference_mixed_types | Crate: fix semantics or track upstream. |
| assert 'test_catalog_db' in ['default', 'global_temp'] | test_create_database_catalog | Crate: fix semantics or track upstream. |
| assert 'list_tables_test' in [] | test_list_tables | Crate: fix semantics or track upstream. |
| assert 1 == 3 | test_arithmetic_with_nulls | Crate: fix semantics or track upstream. |
| assert {'age', 'name'} == {'age', 'city', 'name'} | test_merge_schema_append | Crate: fix semantics or track upstream. |
| out of range integral type conversion attempted | test_math_round | Crate: fix semantics or track upstream. |
| assert 'SparklessApp' == 'test_fixture' | test_session_creation_in_fixture | Crate: fix semantics or track upstream. |
| assert 25 == 23 | test_multiple_sessions_in_fixture | Crate: fix semantics or track upstream. |
| assert 27 == 26 | test_session_cleanup_after_test | Crate: fix semantics or track upstream. |
| sin is not implemented for the Robin backend. See docs/robin_parity_matrix.md... | test_math_sin | Crate: fix semantics or track upstream. |
| cos is not implemented for the Robin backend. See docs/robin_parity_matrix.md... | test_math_cos | Crate: fix semantics or track upstream. |
| assert 'count' in ['dept', 'sales', 'year'] | test_groupBy_with_df_columns | Crate: fix semantics or track upstream. |
| tan is not implemented for the Robin backend. See docs/robin_parity_matrix.md... | test_math_tan | Crate: fix semantics or track upstream. |
| cume_dist is not implemented for the Robin backend. See docs/robin_parity_mat... | test_cume_dist | Crate: fix semantics or track upstream. |
| assert 4 == 2 | test_sql_with_limit | Crate: fix semantics or track upstream. |
| SQL failed: SQL: only SELECT (no UNION/EXCEPT/INTERSECT) is supported. | test_sql_with_union | Crate: fix semantics or track upstream. |
| SQL failed: SQL: unsupported expression in WHERE: Subquery(Query { with: None... | test_sql_with_subquery | Crate: fix semantics or track upstream. |
| assert datetime.date(2024, 1, 1) in {None} | test_between_with_date_values | Crate: fix semantics or track upstream. |
| SQL failed: SQL: unsupported expression with alias: Case { case_token: TokenW... | test_sql_with_case_when | Crate: fix semantics or track upstream. |
| 2 is not implemented for the Robin backend. See docs/robin_parity_matrix.md a... | test_ntile | Crate: fix semantics or track upstream. |
| schema failed: casting from Utf8View to Boolean not supported | test_filter_with_string_expression | Crate: fix semantics or track upstream. |
| assert ["['A', 'B']"] == ['A', 'B'] | test_arraytype_positional_arguments | Crate: fix semantics or track upstream. |
| assert 'test_db' in ['default', 'global_temp'] | test_create_database | Crate: fix semantics or track upstream. |
| assert 'test_db2' in ['default', 'global_temp'] | test_create_database_if_not_exists | Crate: fix semantics or track upstream. |
| assert ["['a', 'b']"] == ['a', 'b'] | test_arraytype_positional_with_dataframe | Crate: fix semantics or track upstream. |
| Cast column not found in schema | test_mean_cast_string_issue_265 | Crate: fix semantics or track upstream. |
| assert 'test_schema' in ['default', 'global_temp'] | test_create_schema | Crate: fix semantics or track upstream. |
| assert None == datetime.date(2026, 1, 1) | test_row_kwargs_with_createDataFrame | Crate: fix semantics or track upstream. |
| select failed: not found: Column 'col' not found. Available columns: [col-wit... | test_withColumnRenamed_special_characters_in_names | Crate: fix semantics or track upstream. |
| assert 'NaMe' in ['name'] | test_multiple_matches_uses_requested_name | Crate: fix semantics or track upstream. |
| assert 'NaMe' in ['name', 'value', 'score'] | test_drop_after_select | Crate: fix semantics or track upstream. |
| assert 255.99999999999994 == 256.0 | test_float_power_nested_expression | Crate: fix semantics or track upstream. |
| math domain error: base must be positive for __rpow__ | test_power_zero_base | Crate: fix semantics or track upstream. |
| assert None == 'one-2-3.0-four-5-six' | test_format_string_many_columns | Crate: fix semantics or track upstream. |
| assert 'Bob' == 'Alice' | test_cast_alias_select_basic_parity | Crate: fix semantics or track upstream. |
| assert 63.99999999999997 == 64.0 | test_power_chained_operations | Crate: fix semantics or track upstream. |
| assert 15.999999999999998 == 16 | test_power_mixed_types | Crate: fix semantics or track upstream. |
| assert None == 'null-null-null' | test_format_string_all_null | Crate: fix semantics or track upstream. |
| assert 1.0 < 0.01 | test_power_fractional_exponent | Crate: fix semantics or track upstream. |
| assert 243.00000000000017 == 243 | test_power_large_numbers | Crate: fix semantics or track upstream. |
| assert (None == 'ff' or None == 'FF') | test_format_string_format_specifiers | Crate: fix semantics or track upstream. |
| assert None == '00042' | test_format_string_precision_formatting | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Le... | test_column_subscript_deeply_nested_struct | Crate: fix semantics or track upstream. |
| assert "['E1', 'E2']" == 'E1' | test_getfield_array_index | Crate: fix semantics or track upstream. |
| assert None == 5 | test_orderby_mixed_nulls_and_values | Crate: fix semantics or track upstream. |
| assert 1073741823.999999 == 1073741824 | test_power_very_large_exponent | Crate: fix semantics or track upstream. |
| assert (10 == 30) | test_getfield_negative_index | Crate: fix semantics or track upstream. |
| Expected hour=4, got None | test_hour_minute_second_from_string_timestamps | Crate: fix semantics or track upstream. |
| Expected hour=4 for UTC, got None | test_hour_minute_second_with_different_timezone_formats | Crate: fix semantics or track upstream. |
| assert 0 == 2 | test_hour_minute_second_with_filter | Crate: fix semantics or track upstream. |
| assert None == 4.5 | test_hour_minute_second_in_groupby_agg | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x111287630>... | test_groupby_all_cols_aliased | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x111285f30>... | test_groupby_alias_avg_max_min | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x111287430>... | test_groupby_alias_with_nulls | Crate: fix semantics or track upstream. |
| assert 'avg(Value)' in Row(Value=5.5) | test_grouped_data_mean_with_drop | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['a']' not found. Available columns: [a].... | test_orderby_with_single_column_list | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x111287230>... | test_groupby_alias_agg_alias_matches_issue_expected | Crate: fix semantics or track upstream. |
| assert ('At least one column' in 'cannot convert to Column' or 'must be speci... | test_window_orderby_list_empty_list_error | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["x"], Right: ["x:"] | test_f_dataframe_union_empty_df | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['dept', 'name', 'salary']' not found. Av... | test_orderby_with_df_columns | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['dept', 'name']' not found. Available co... | test_orderby_with_string_columns | Crate: fix semantics or track upstream. |
| assert 'avg(Value)' in ['Name', 'Value'] | test_grouped_data_mean_schema_verification | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['a', 'b', 'c']' not found. Available col... | test_orderby_with_three_columns | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['x']' not found. Available columns: [x].... | test_orderby_then_limit | Crate: fix semantics or track upstream. |
| order_by failed: casting from Utf8View to Boolean not supported | test_filter_then_orderby_list | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['a']' not found. Available columns: [a, ... | test_orderby_then_select | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column 'avg(Value) DESC' not found. Available col... | test_grouped_data_mean_with_desc_orderBy | Crate: fix semantics or track upstream. |
| order_by failed: not found: Column '['x']' not found. Available columns: [x, ... | test_orderby_with_explicit_list_variable | Crate: fix semantics or track upstream. |
| assert 0 == 3 | test_date_datetime_orderby | Crate: fix semantics or track upstream. |
| cast failed: unknown type name: decimal | test_avg_cast_decimal_drop | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ou... | test_column_subscript_with_nested_struct | Crate: fix semantics or track upstream. |
| collect failed: conversion from `str` to `datetime[μs]` failed in column 'Dat... | test_exact_scenario_from_issue_432 | Crate: fix semantics or track upstream. |
| assert None == datetime.date(2024, 1, 15) | test_cast_string_to_date_still_works | Crate: fix semantics or track upstream. |
| assert None == datetime.datetime(2024, 3, 15, 0, 0) | test_cast_date_to_timestamp_midnight | Crate: fix semantics or track upstream. |
| assert None == datetime.date(2024, 5, 10) | test_cast_datetime_to_date_truncates_time | Crate: fix semantics or track upstream. |
| assert None == 2.0 | test_mean_string_column_exact_issue_scenario | Crate: fix semantics or track upstream. |
| '>' not supported between instances of 'str' and 'int' | test_rdd_flatmap_empty_iterable | Crate: fix semantics or track upstream. |
| assert ['1111111111'... '3333333333'] == [10, 20, 30] | test_rdd_flatmap_one_element_per_row | Crate: fix semantics or track upstream. |
| assert None == datetime.date(2024, 2, 29) | test_cast_leap_day | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported int for ... | test_drop_duplicates_with_nulls_in_array | Crate: fix semantics or track upstream. |
| can only concatenate str (not "int") to str | test_rdd_flatmap_then_reduce | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'Ma... | test_map_column_subscript_with_column_key_exact_issue_441 | Crate: fix semantics or track upstream. |
| assert None == 0.0 | test_fillna_float_dict_int_column | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["a", "b"], Right: ... | test_union_chained_three_dataframes | Crate: fix semantics or track upstream. |
| unsupported join type: leftanti | test_leftanti_join_excludes_right_columns | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'da... | test_map_column_subscript_in_select | Crate: fix semantics or track upstream. |
| assert (0 == 0.5) | test_fillna_float_multiple_calculated_columns | Crate: fix semantics or track upstream. |
| assert (3 == 3 and None == 0.0) | test_fillna_float_after_filter | Crate: fix semantics or track upstream. |
| select failed: casting from Utf8View to Boolean not supported | test_filter_and_select_after | Crate: fix semantics or track upstream. |
| assert (1 == 1 and None == 0.0) | test_fillna_float_subset_string_single_column | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["id"], Right: ["ot... | test_union_single_column | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["c1", "c2", "c3", ... | test_union_many_columns_different_names | Crate: fix semantics or track upstream. |
| assert 'None' == '2026-01-15' | test_to_date_cast_string_literal | Crate: fix semantics or track upstream. |
| assert 'None' == '2026-01-01' | test_to_date_cast_in_select | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["id", "val"], Righ... | test_union_then_order_by | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["id", "score"], Ri... | test_union_then_filter | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: json_value_to_series: unsupported long for... | test_array_distinct_with_nulls_in_array | Crate: fix semantics or track upstream. |
| union failed: union: column order/names must match. Left: ["id"], Right: ["x"] | test_union_count | Crate: fix semantics or track upstream. |
| with_column failed: not found: Column 'a' not found. Available columns: [a:].... | test_alias_cast_withcolumn_empty_dataframe | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x11484c330>... | test_groupby_alias_exact_issue | Crate: fix semantics or track upstream. |
| group_by failed: not found: Column '<builtins.PyColumn object at 0x11484da30>... | test_groupby_alias_with_show | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i32) | test_between_string_column_in_select_expression | Crate: fix semantics or track upstream. |
| collect failed: casting from string to boolean failed for value '' | test_astype_string_to_boolean | Crate: fix semantics or track upstream. |
| assert '0' is None | test_na_fill_type_mismatch_silently_ignored | Crate: fix semantics or track upstream. |
| assert (1234, 'A', 'X') in {('1234', 'A', 'X'), ('4567', 'B', 'Y')} | test_join_int64_with_string | Crate: fix semantics or track upstream. |
| assert (100, 'A', 'X') in {('100', 'A', 'X'), ('200', 'B', 'Y')} | test_join_int32_with_string | Crate: fix semantics or track upstream. |
| assert (1.5, 'A', 'X') in {('1.5', 'A', 'X'), ('2.5', 'B', 'Y')} | test_join_float_with_string | Crate: fix semantics or track upstream. |
| assert (1234, 'A', 'X', 'M') in {('1234', 'A', 'X', 'M'), ('4567', 'B', 'Y', ... | test_join_multiple_keys_with_type_mismatch | Crate: fix semantics or track upstream. |
| assert '1234' == 1234 | test_join_type_coercion_parity_pyspark | Crate: fix semantics or track upstream. |
| assert 1234 in {'1234', '4567', '9999'} | test_join_left_outer_with_type_mismatch | Crate: fix semantics or track upstream. |
| assert ["['x', 'y']"] == ['x', 'y'] | test_drop_duplicates_string_arrays | Crate: fix semantics or track upstream. |
| assert (100 in {'100', '200'} or 100.0 in {'100', '200'}) | test_pyspark_parity_null_values_in_join_keys | Crate: fix semantics or track upstream. |
| collect failed: div operation not supported for dtypes `str` and `str` | test_string_arithmetic_with_string_column | Crate: fix semantics or track upstream. |
| schema failed: arithmetic on string and numeric not allowed, try an explicit ... | test_string_arithmetic_result_type | Crate: fix semantics or track upstream. |
| assert (1234 in {'1234', '4567'} or 1234.0 in {'1234', '4567'}) | test_pyspark_parity_invalid_numeric_strings | Crate: fix semantics or track upstream. |
| assert ((100, 'A') in {('100', 'A'), ('200', 'B')} or (100.0, 'A') in {('100'... | test_pyspark_parity_multiple_keys_complex | Crate: fix semantics or track upstream. |
| assert (0 in {'-456', '0', '123', '999'} or 0.0 in {'-456', '0', '123', '999'}) | test_pyspark_parity_mixed_numeric_strings | Crate: fix semantics or track upstream. |
| assert not True | test_pyspark_parity_schema_verification | Crate: fix semantics or track upstream. |
| assert '999' is None | test_fillna_subset_type_mismatch_string_column_int_fill | Crate: fix semantics or track upstream. |
| assert '' == 'Hel' | test_substr_zero_start | Crate: fix semantics or track upstream. |
| substr(0, 3) failed for row 0 (text='Hello'): expected 'Hel', got '' | test_substr_pyspark_parity_comprehensive | Crate: fix semantics or track upstream. |
| agg failed: duplicate: column with name 'count' has more than one occurrence | test_substr_in_groupBy | Crate: fix semantics or track upstream. |
| with_column failed: arithmetic on string and numeric not allowed, try an expl... | test_string_arithmetic_all_operations_comprehensive | Crate: fix semantics or track upstream. |
| Cast column not found | test_aggregate_function_cast_with_string_type | Crate: fix semantics or track upstream. |
| assert None is True | test_first_ignorenulls_with_boolean_values | Crate: fix semantics or track upstream. |
| assert None == 3.14 | test_first_ignorenulls_with_float_values | Crate: fix semantics or track upstream. |
| assert None == '' | test_first_ignorenulls_with_empty_strings | Crate: fix semantics or track upstream. |
| assert None == 'FIRST_NON_NULL' | test_first_ignorenulls_large_group | Crate: fix semantics or track upstream. |
| Expected Delta-related error, got: SQL failed: SQL parse error: sql parser er... | test_describe_detail_non_delta_table_raises | Crate: fix semantics or track upstream. |
| Expected table not found error, got: sql failed: sql parse error: sql parser ... | test_describe_detail_nonexistent_table | Crate: fix semantics or track upstream. |
| assert None == 0 | test_fillna_all_case_variations | Crate: fix semantics or track upstream. |
| assert 3 == 2 | test_dropna_all_case_variations | Crate: fix semantics or track upstream. |
| assert 'full_name' in {'Age': 25, 'Dept': 'IT', 'Name': 'Alice', 'Salary': 5000} | test_withColumnRenamed_all_case_variations | Crate: fix semantics or track upstream. |
| assert '<builtins.Py... 0x1128b7d30>' == 'Name' | test_attribute_access_all_case_variations | Crate: fix semantics or track upstream. |
| keys() is not implemented for the Robin backend. See docs/robin_parity_matrix... | test_tuple_data_preserves_order | Crate: fix semantics or track upstream. |
| collect failed: 'is_in' cannot check for List(Int64) values in String data | test_isin_with_mixed_types | Crate: fix semantics or track upstream. |
| collect failed: cannot compare string with numeric type (i64) | test_numeric_eq_string | Crate: fix semantics or track upstream. |
| create_dataframe_from_rows failed: create_dataframe_from_rows: map column 'ma... | test_getItem_with_map_key | Crate: fix semantics or track upstream. |
| 4 is not implemented for the Robin backend. See docs/robin_parity_matrix.md a... | test_ntile_with_arithmetic | Crate: fix semantics or track upstream. |

---

## 3. Fix Sparkless (Python / Robin integration layer)


Failures that should be fixed in **this repo** (Sparkless): missing APIs, wrong conversion, or session/reader implementation.


### 3.1 Schema / NoneType


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'NoneType' object has no attribute 'fields' | test_select_all_case_variations, test_unionByName_all_case_variations, test_case_insensitive_unionByName | Implement or fix in Sparkless (see current analysis). |

### 3.2 Other (fix_sparkless)


| Error pattern | Example tests | Fix |
|---------------|----------------|-----|
| 'RobinColumn' object is not callable | test_eqnullsafe_example_from_issue_260, test_eqnullsafe_coexists_with_standard_equality, test_intersect (+78) | Implement or fix in Sparkless (see current analysis). |
| 'RobinFunctions' object has no attribute 'rlike' | test_rlike_nested_lookahead, test_rlike_lookbehind_with_digits, test_rlike_combined_lookahead_lookbehind (+34) | Implement or fix in Sparkless (see current analysis). |
| "Key 'avg(Value)' not found in row" | test_grouped_data_mean_single_column, test_grouped_data_mean_with_null_values, test_grouped_data_mean_equals_avg (+25) | Implement or fix in Sparkless (see current analysis). |
| 'ColumnOperation' object cannot be converted to 'PyColumn' | test_cast_alias_select_basic, test_cast_alias_select_multiple_aggregations, test_cast_alias_select_different_cast_types (+18) | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'approx_count_dis... | test_approx_count_distinct_rsd_issue_266, test_approx_count_distinct_rsd_parity, test_approx_count_distinct_window_parity (+10) | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'input_file_name' | test_input_file_name_returns_string_column, test_input_file_name_exact_issue_scenario, test_input_file_name_select_only (+9) | Implement or fix in Sparkless (see current analysis). |
| log() takes 1 positional arguments but 2 were given | test_log_with_float_base_parity, test_log_with_different_bases_parity, test_log_with_float_base (+6) | Implement or fix in Sparkless (see current analysis). |
| "Key 'NaMe' not found in row" | test_join_different_case_select_third_case, test_join_different_case_select_left_column, test_different_join_types (+5) | Implement or fix in Sparkless (see current analysis). |
| "Key 'count' not found in row" | test_join_with_nulls_then_groupby, test_join_then_drop_other_columns, test_basic_left_join_then_groupby (+4) | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'stddev' | test_stddev_arithmetic, test_cast_with_stddev_variance, test_stddev_over_window_partition (+4) | Implement or fix in Sparkless (see current analysis). |
| isin requires a list of int or str values | test_negation_isin_string_column_int_list, test_negation_isin_show, test_isin_without_negation_string_column_int_list (+3) | Implement or fix in Sparkless (see current analysis). |
| 'RobinFunctions' object has no attribute 'with_field' | test_withfield_empty_struct, test_withfield_multiple_fields_in_sequence, test_withfield_with_array_field (+3) | Implement or fix in Sparkless (see current analysis). |
| _robin_functions_module.<locals>.<lambda>() takes 0 positional argu... | test_lag, test_lead, test_lag_over_partition_order_desc (+2) | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'array_contains' | test_array_contains, test_array_contains_join_basic_parity, test_array_contains_join_multiple_matches_parity (+2) | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'posexplode_outer' | test_posexplode_outer_null_handling, test_posexplode_outer_alias_returns_exploded_rows, test_posexplode_outer_alias_two_names (+1) | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'date_trunc' | test_date_trunc_preserves_nulls, test_date_trunc_month_on_date_column, test_date_trunc_timestamp_core_units (+1) | Implement or fix in Sparkless (see current analysis). |
| _get_available_columns | test_materialized_columns_are_available, test_columns_available_after_collect, test_columns_available_after_show | Implement or fix in Sparkless (see current analysis). |
| _robin_functions_module.<locals>._wrap1.<locals>._w() missing 1 req... | test_rank, test_window_functions_with_case_variations, test_rank_with_arithmetic | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'size' | test_size, test_array_type_elementtype_with_select_operation, test_array_type_elementtype_with_withcolumn_operation | Implement or fix in Sparkless (see current analysis). |
| argument of type 'NoneType' is not iterable | test_format_string_different_format_specifiers, test_format_string_numeric_edge_cases, test_format_string_mixed_types | Implement or fix in Sparkless (see current analysis). |
| 'RobinSparkSessionBuilder' object is not callable | test_builder_callable_returns_self, test_builder_callable_full_chain, test_builder_property_and_call_equivalent | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'agg' | test_pivot_multiple_aggregates, test_pivot_single_aggregate_with_alias, test_pivot_all_case_variations | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'json_tuple' | test_json_tuple, test_json_tuple_missing_fields_and_invalid_json | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'substring_index' | test_substring_index, test_substring_index_edge_cases | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'element_at' | test_element_at, test_split_in_filter_context | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'soundex' | test_soundex, test_soundex_null_and_empty | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'translate' | test_translate, test_translate_edge_cases | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'levenshtein' | test_levenshtein, test_levenshtein_nulls_and_empty_strings | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'crc32' | test_crc32, test_crc32_known_values_and_null | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'xxhash64' | test_xxhash64, test_xxhash64_known_values_and_null | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'get_json_object' | test_get_json_object, test_get_json_object_missing_path_and_invalid_json | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'explode_outer' | test_explode_outer_with_null_arrays, test_explode_outer_with_empty_arrays | Implement or fix in Sparkless (see current analysis). |
| "Key 'avg(Value1)' not found in row" | test_grouped_data_mean_multiple_columns, test_grouped_data_mean_multiple_columns | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'regexp_extract_all' | test_regexp_extract_all_multiple_matches_and_nulls, test_regexp_extract_all_basic_groups | Implement or fix in Sparkless (see current analysis). |
| 'RobinDataFrameWriter' object has no attribute 'partitionBy' | test_describe_detail_multiple_partitions, test_describe_detail_partition_columns | Implement or fix in Sparkless (see current analysis). |
| 'NoneType' object has no attribute 'create_schema' | test_storage_manager_detached_write_visible_to_session | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'repeat' | test_repeat | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'reverse' | test_reverse | Implement or fix in Sparkless (see current analysis). |
| _materialized | test_dataframe_is_marked_materialized | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'dayofmonth' | test_dayofmonth | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'dayofweek' | test_dayofweek | Implement or fix in Sparkless (see current analysis). |
| Database 'test_current_db2' does not exist | test_set_current_database | Implement or fix in Sparkless (see current analysis). |
| Database 'list_db' does not exist | test_list_tables_in_database | Implement or fix in Sparkless (see current analysis). |
| Robin save_as_table: saveAsTable append: new DataFrame missing colu... | test_merge_schema_bidirectional | Implement or fix in Sparkless (see current analysis). |
| 'RobinSparkSession' object does not support the context manager pro... | test_session_context_manager | Implement or fix in Sparkless (see current analysis). |
| 'RobinSparkSession' object has no attribute 'sparkContext' | test_sparkcontext_available_in_session | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'variance' | test_variance_arithmetic | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'last' | test_last_value | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'isnull' | test_isnull | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'isnotnull' | test_isnotnull | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'nvl' | test_nvl | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'nullif' | test_nullif | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'nanvl' | test_nanvl | Implement or fix in Sparkless (see current analysis). |
| Database 'test_current_db' does not exist | test_set_current_database | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'ltrim' | test_string_ltrim | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'rtrim' | test_string_rtrim | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'array_position' | test_array_position | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'array_join' | test_array_join | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'array_union' | test_array_union | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'ascii' | test_ascii | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'array_sort' | test_array_sort | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'hex' | test_hex | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'array_remove' | test_array_remove | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'base64' | test_base64 | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'initcap' | test_initcap | Implement or fix in Sparkless (see current analysis). |
| 'AggregateFunction' object cannot be converted to 'PyColumn' | test_cast_alias_select_backward_compatibility | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'stddev_samp' | test_stddev_samp_over_window | Implement or fix in Sparkless (see current analysis). |
| module 'sparkless.sql.functions' has no attribute 'stddev_pop' | test_stddev_pop_over_window | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'count_distinct' | test_pivot_count_distinct | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'collect_list' | test_pivot_collect_list | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'collect_set' | test_pivot_collect_set | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'first' | test_pivot_first | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'last' | test_pivot_last | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'stddev' | test_pivot_stddev | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'variance' | test_pivot_variance | Implement or fix in Sparkless (see current analysis). |
| 'RobinPivotedGroupedData' object has no attribute 'mean' | test_pivot_mean | Implement or fix in Sparkless (see current analysis). |
| 'StringType' object has no attribute 'element_type' | test_array_type_in_schema_with_elementtype | Implement or fix in Sparkless (see current analysis). |
| 'dict' object cannot be converted to 'PyColumn' | test_first_after_groupby_agg | Implement or fix in Sparkless (see current analysis). |
| "Key 'name' not found in row" | test_ambiguity_detection | Implement or fix in Sparkless (see current analysis). |

---

## 4. Other


Failures that did not match any robin_sparkless or fix_sparkless pattern (assertion/catalog/one-off): 
**0**.



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
