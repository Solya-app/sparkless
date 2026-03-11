# Robin-sparkless parity investigation report

**Date:** 2026-02-22  
**Test run:** `pytest tests -n 8 -v --tb=line --ignore=tests/archive`  
**Result:** 1526 failed, 1028 passed, 21 skipped, 1 xpassed  
**Artifact:** `tests/.robin_parity_run.txt`

## Scope

- **Target repo:** [eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless) (Rust crate).
- **Reported:** Only **crate-originated** errors (e.g. `select failed:`, `collect failed:`, `count failed:`, `with_column failed:`, `create_dataframe_from_rows failed:`, etc.).
- **Not reported:** Wrapper-originated errors (Expression columns not yet supported, select expects Column or str, RobinColumn not callable, NotImplementedError for Robin backend, join on expression not supported, etc.).

---

## 1. Crate-originated failure groups (issues created)

| Count | Canonical cause | Example test | GitHub issue |
|------:|-----------------|--------------|--------------|
| 52 | select failed: not found Column (alias/naming) | test_issue_156_select_dropped_column::test_select_dropped_column_minimal_repro | [#1008](https://github.com/eddiethedean/robin-sparkless/issues/1008) |
| 37 | with_column failed: not found Column (alias e.g. to_timestamp) | test_issue_135_datetime_filter::test_to_timestamp_with_multiple_operations_and_filter | [#1009](https://github.com/eddiethedean/robin-sparkless/issues/1009) |
| 23 | SQL failed: only SELECT / DDL supported | parity/sql/test_ddl::test_create_table_with_select | [#1010](https://github.com/eddiethedean/robin-sparkless/issues/1010) |
| 19 | collect failed: field not found (struct/nested) | parity/functions/test_struct_field_alias_parity::test_struct_field_with_alias_parity | [#1011](https://github.com/eddiethedean/robin-sparkless/issues/1011) |
| 18 | create_dataframe_from_rows: struct/array/map type | test_issue_293_explode_withcolumn::test_explode_with_booleans | [#1012](https://github.com/eddiethedean/robin-sparkless/issues/1012) |
| 14 | SQL failed: other | test_issues_376_382_robust::test_robust_self_join_manager_column_and_row_count | [#1013](https://github.com/eddiethedean/robin-sparkless/issues/1013) |
| 13 | union: column order/names must match | test_issue_413_union_createDataFrame::test_union_different_column_order_by_position | [#1014](https://github.com/eddiethedean/robin-sparkless/issues/1014) |
| 9 | count failed: duplicate column name 'count' | test_issue_280_join_groupby_ambiguity::test_basic_left_join_then_groupby | [#1015](https://github.com/eddiethedean/robin-sparkless/issues/1015) |
| 11 | Key not found in row (case/schema) | test_issue_297_join_different_case_select::test_multiple_ambiguous_columns | [#1016](https://github.com/eddiethedean/robin-sparkless/issues/1016) |
| 7 | SQL failed: join type limitation | test_issues_376_382_robust::test_robust_sql_where_table_prefixed | [#1017](https://github.com/eddiethedean/robin-sparkless/issues/1017) |
| 3 | collect failed: string vs numeric comparison | unit/dataframe/test_string_arithmetic::test_string_arithmetic_complex_expression | [#1018](https://github.com/eddiethedean/robin-sparkless/issues/1018) |
| 2 | SQL failed: parse error | parity/sql/test_show_describe::test_describe_column | [#1019](https://github.com/eddiethedean/robin-sparkless/issues/1019) |
| 2 | conversion/cast failed in column | unit/dataframe/test_column_astype::test_astype_invalid_string_to_int | [#1020](https://github.com/eddiethedean/robin-sparkless/issues/1020) |
| 2 | schema/table not found (test_schema) | parity/dataframe/test_parquet_format_table_append::test_parquet_format_append_detached_df_visible_to_active_session | [#1021](https://github.com/eddiethedean/robin-sparkless/issues/1021) |
| 1 | SQL failed: subquery not supported | parity/sql/test_advanced::test_sql_with_subquery | [#1022](https://github.com/eddiethedean/robin-sparkless/issues/1022) |
| 1 | collect failed: dtype not supported (when/otherwise) | test_issue_288_casewhen_operators::test_casewhen_bitwise_not | [#1023](https://github.com/eddiethedean/robin-sparkless/issues/1023) |
| 1 | duplicate: projections duplicate output name | test_issues_376_382_robust::test_robust_self_join_manager_column_and_row_count | [#1024](https://github.com/eddiethedean/robin-sparkless/issues/1024) |
| 1 | DESCRIBE DETAIL: not Delta table | (parity/integration) | [#1025](https://github.com/eddiethedean/robin-sparkless/issues/1025) |

**Total: 18 new issues created** (1008–1025).

---

## 2. Wrapper-originated (not reported to robin-sparkless)

| Count | Error / category |
|------:|------------------|
| 249 | Expression columns (e.g. from operations or when/otherwise) are not yet supported in select/filter |
| 107 | select expects Column or str |
| 88 | udf is not implemented for the Robin backend |
| 81 | 'RobinColumn' object is not callable |
| 50 | join on expression (e.g. df1.a == df2.b) is not supported |
| 47 | explode is not implemented for the Robin backend |
| 37 | rlike is not implemented for the Robin backend |
| 33 | DataFrames are not equivalent |
| 32 | struct is not implemented for the Robin backend |
| 31 | with_field is not implemented for the Robin backend |
| 27 | row_number is not implemented for the Robin backend |
| 26 | posexplode is not implemented for the Robin backend |
| 26 | expr is not implemented for the Robin backend |
| 21 | 'ColumnOperation' object cannot be converted to 'PyColumn' |
| 14 | array_distinct is not implemented for the Robin backend |
| … | (additional NotImplementedError / wrapper patterns) |

---

## 3. Created issue URLs

- https://github.com/eddiethedean/robin-sparkless/issues/1008
- https://github.com/eddiethedean/robin-sparkless/issues/1009
- https://github.com/eddiethedean/robin-sparkless/issues/1010
- https://github.com/eddiethedean/robin-sparkless/issues/1011
- https://github.com/eddiethedean/robin-sparkless/issues/1012
- https://github.com/eddiethedean/robin-sparkless/issues/1013
- https://github.com/eddiethedean/robin-sparkless/issues/1014
- https://github.com/eddiethedean/robin-sparkless/issues/1015
- https://github.com/eddiethedean/robin-sparkless/issues/1016
- https://github.com/eddiethedean/robin-sparkless/issues/1017
- https://github.com/eddiethedean/robin-sparkless/issues/1018
- https://github.com/eddiethedean/robin-sparkless/issues/1019
- https://github.com/eddiethedean/robin-sparkless/issues/1020
- https://github.com/eddiethedean/robin-sparkless/issues/1021
- https://github.com/eddiethedean/robin-sparkless/issues/1022
- https://github.com/eddiethedean/robin-sparkless/issues/1023
- https://github.com/eddiethedean/robin-sparkless/issues/1024
- https://github.com/eddiethedean/robin-sparkless/issues/1025

---

## 4. Implementation

- **Script:** `scripts/create_robin_parity_issues.py`
  - Reads test run output (`tests/.robin_parity_run.txt`),
  - Classifies failures as crate vs wrapper,
  - Groups crate failures by canonical root cause,
  - Drafts title and body (Summary, robin-sparkless current behavior, PySpark expected, Environment),
  - Writes body to `tests/.robin_issue_body.txt` and runs `gh issue create -R eddiethedean/robin-sparkless --title "..." --body-file ...`.
- **Repros:** Existing repros in `tests/tools/robin_issues_repro/` (e.g. issue1_struct_create_dataframe.py, issue3_filter_string_numeric.py); added `issue_count_duplicate_column.py` for count duplicate column name.
- **PySpark snippets:** `scripts/robin_issue_pyspark_snippets.py` used for “Expected behavior” in generic issue bodies.

---

## 5. References

- Plan: `.cursor/plans/robin-sparkless_parity_issues_5fd68168.plan.md`
- Test run analysis: `tests/TEST_RUN_ANALYSIS.md`
- Robin parity matrix: `docs/robin_parity_matrix.md`
- Issue bodies / repro patterns: `tests/tools/robin_sparkless_issues.md`, `docs/robin_github_issue_*.md`
