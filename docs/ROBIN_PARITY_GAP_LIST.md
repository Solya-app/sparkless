# Robin–PySpark parity gap list (from test run)

Generated from parsed Robin backend test results. Maps **robin_sparkless** failure themes to parity matrix rows, existing docs, and upstream issue numbers.

## Summary

| # | Short name | Error pattern (abbrev) | Count | Parity matrix | Has doc | Upstream issue | Action |
|---|------------|------------------------|-------|----------------|---------|----------------|--------|
| 1 | cannot convert to Column | cannot convert to Column | 264 | cannot convert to Column | — | #176 / #503 | Link to #176, #503 |
| 2 | select expects Column or str | select expects Column or str | 109 | select expects Column or str | — | #176, #503 | Link to #176, #503 |
| 3 | create_dataframe map (struct alias) | create_dataframe_from_rows: map column 'Struc... | 49 | create_dataframe (map/struct) | — | #627 | Link to #627 |
| 4 | filter predicate Boolean | collect failed: filter predicate must be of type `Boolean`, got `String` | 43 | — | **new** | — | Create issue |
| 5 | create_dataframe struct | struct value must be object (by field name) | 35 | create_dataframe_from_rows (struct) | **new** | — | Create issue |
| 6 | create_dataframe array | array element type / json_value_to_series | 10+4+3+1+1 | create_dataframe (array) | — | Crate | Doc optional |
| 7 | create_dataframe map (other) | map column 'm' / 'metadata' / 'MapVal' / 'data' / 'map' | 8+2+1+1+1+1 | create_dataframe (map) | — | #627 | Link #627 |
| 8 | assert False (join coercion) | assert False | 7 | collect failed: type String is incompatible / union coercion | — | Crate | Doc exists (join) |
| 9 | Table or view not found | table(...) failed: Table or view ... not found | 2 | Table or view not found | — | #629 | Link #629 |
| 10 | collect conversion (str→i32, datetime, cast) | conversion from str to i32/datetime; casting f64 to i32; string to boolean | 2+2+1+1+1 | Crate: comparison/cast | **new** | — | Create issue |
| 11 | isin type | 'is_in' cannot check for List(Int64) values in String data | 1 | isin / type | robin_github_issue_unsupported_isin | — | Open issue from doc |
| 12 | string vs numeric compare | cannot compare string with numeric type (i64/i32) | 1+1 | collect failed: cannot compare string with numeric | — | #628 | Link #628 |
| 13 | window orderBy empty list | cannot convert to Column / At least one column | 1 | — | — | — | Skip-list |
| 14 | Describe detail / nested struct | map column 'Outer' / 'Level' / etc. | 2 | Describe detail / struct | — | Crate | Doc optional |

## Docs and upstream issues (all linked)

All parity gaps above are reported upstream. Issue numbers are in [upstream.md](upstream.md) and linked from [robin_parity_matrix.md](robin_parity_matrix.md).

- [robin_github_issue_case_sensitivity.md](robin_github_issue_case_sensitivity.md) → **#492**, **#636**
- [robin_github_issue_right_outer_semi_anti_join.md](robin_github_issue_right_outer_semi_anti_join.md) → **#639**
- [robin_github_issue_isin_empty_list.md](robin_github_issue_isin_empty_list.md) → **#637**
- [robin_github_issue_unsupported_isin.md](robin_github_issue_unsupported_isin.md) → **#638**, **#650**
- [robin_github_issue_between_power_cast_plan.md](robin_github_issue_between_power_cast_plan.md) → **#640**
- [robin_github_issue_groupby_agg_plan.md](robin_github_issue_groupby_agg_plan.md) → **#641**
- [robin_github_issue_window_row_number_plan.md](robin_github_issue_window_row_number_plan.md) → **#642**
- [robin_github_issue_empty_df_parquet_append.md](robin_github_issue_empty_df_parquet_append.md) → **#643**
- [robin_github_issue_create_map_empty.md](robin_github_issue_create_map_empty.md) → Fixed in crate 0.11.5
- [robin_github_issue_filter_predicate_boolean.md](robin_github_issue_filter_predicate_boolean.md) → **#646**
- [robin_github_issue_create_dataframe_struct.md](robin_github_issue_create_dataframe_struct.md) → **#634**
- [robin_github_issue_collect_type_conversion.md](robin_github_issue_collect_type_conversion.md) → **#649**
