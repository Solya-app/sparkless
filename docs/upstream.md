# Upstream Coordination Notes

## Robin-sparkless crate version

Sparkless v4 depends on **robin-sparkless 0.15.0** (see `Cargo.toml`). **0.15.0** (and 0.12.2) fix Sparkless-reported parity issues, including:

- **[#492](https://github.com/eddiethedean/robin-sparkless/issues/492)** — Case-insensitive `orderBy` on mixed-case column names (PySpark parity).
- **[#176](https://github.com/eddiethedean/robin-sparkless/issues/176)** — `select()` with column expressions and `regexp_extract_all` for PySpark compatibility.
- **[#503](https://github.com/eddiethedean/robin-sparkless/issues/503)** — `select()` with column names: accept `payload.columns` (invalid plan error).
- **[#627](https://github.com/eddiethedean/robin-sparkless/issues/627)** — `create_dataframe_from_rows`: map&lt;string,string&gt; support.
- **[#628](https://github.com/eddiethedean/robin-sparkless/issues/628)** — collect: string vs numeric type comparison.
- **[#629](https://github.com/eddiethedean/robin-sparkless/issues/629)** — Table or view not found after `createOrReplaceTempView`.
- **[#633](https://github.com/eddiethedean/robin-sparkless/issues/633)** — create_dataframe_from_rows: unsupported type map&lt;string,string&gt;.
- **[#634](https://github.com/eddiethedean/robin-sparkless/issues/634)** — create_dataframe_from_rows: struct value must be object or array.
- **[#635](https://github.com/eddiethedean/robin-sparkless/issues/635)** — collect failed: cannot compare string with numeric type.
- **[#636](https://github.com/eddiethedean/robin-sparkless/issues/636)** — Case sensitivity and column-not-found (not found: ID).
- **[#637](https://github.com/eddiethedean/robin-sparkless/issues/637)** — isin with empty list semantics.
- **[#638](https://github.com/eddiethedean/robin-sparkless/issues/638)** — isin: cannot check List(Int64) in String data (mixed types).
- **[#639](https://github.com/eddiethedean/robin-sparkless/issues/639)** — Right, outer, semi, anti join wrong row count.
- **[#640](https://github.com/eddiethedean/robin-sparkless/issues/640)** — between, power, cast in logical plan.
- **[#641](https://github.com/eddiethedean/robin-sparkless/issues/641)** — groupBy + agg (sum, count) in logical plan.
- **[#642](https://github.com/eddiethedean/robin-sparkless/issues/642)** — row_number() over (partition by col) window.
- **[#643](https://github.com/eddiethedean/robin-sparkless/issues/643)** — Empty DataFrame + parquet table append / catalog.
- **[#644](https://github.com/eddiethedean/robin-sparkless/issues/644)** — cannot convert to Column.
- **[#645](https://github.com/eddiethedean/robin-sparkless/issues/645)** — select expects Column or str.
- **[#646](https://github.com/eddiethedean/robin-sparkless/issues/646)** — filter predicate must be Boolean, got String.
- **[#649](https://github.com/eddiethedean/robin-sparkless/issues/649)** — Cast/conversion semantics (str to datetime/i32, f64 to i32, string to boolean).
- **[#672](https://github.com/eddiethedean/robin-sparkless/issues/672)** — Aggregation result column names (e.g. avg(Value) vs Value); PySpark uses \`avg(Value)\`, Robin may use \`Value\`.
- **[#678](https://github.com/eddiethedean/robin-sparkless/issues/678)** — DESCRIBE DETAIL (Delta Lake): not supported or different semantics.
- **[#680](https://github.com/eddiethedean/robin-sparkless/issues/680)** — when/otherwise: invalid series dtype expected Boolean, got i32/i64/f64/str.
- **[#681](https://github.com/eddiethedean/robin-sparkless/issues/681)** — Join/union type coercion (String vs Int64); assert/schema mismatch.
- **[#682](https://github.com/eddiethedean/robin-sparkless/issues/682)** — casewhen / bitwise not: dtype Unknown(Any) not supported in 'not' operation.
- **[#683](https://github.com/eddiethedean/robin-sparkless/issues/683)** — Division between string columns: div operation not supported for dtypes str and str.

## Robin-Sparkless PySpark parity

- **Goal:** Sparkless v4 runs entirely on the `robin-sparkless` Rust engine while preserving **PySpark semantics**.
- **Policy:** For any observed behavior difference between Sparkless and PySpark:
  - First, **verify the mismatch against real PySpark** with a minimal, self-contained example.
  - If the minimal example shows that **robin-sparkless** itself disagrees with PySpark (independent of Sparkless glue code), treat this as an **upstream Robin parity gap**.
  - Open an issue in [eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless) that:
    - Clearly labels the problem as “PySpark parity” in the title.
    - Includes code samples for both robin-sparkless and PySpark demonstrating the difference.
    - Notes the exact crate/PySpark versions and links back to any Sparkless issue/PR.
  - In Sparkless, keep a short “Robin gaps” list and link each entry to the corresponding upstream issue so we can clean up skips/workarounds once the crate is fixed.

## Delta Schema Evolution
- **Issue:** Polars backend raises `ValueError: type String is incompatible with expected type Null`
  when appending with `mergeSchema=true`.
- **Local Mitigation:** `sparkless/backend/polars/schema_utils.py` now coerces write batches to the
  registered schema and fills new columns with correctly typed nulls.
- **Upstream Ask:** Confirm whether Polars intends to support concatenating `Null` and
  concrete dtypes automatically. If yes, track the fix; if no, document the requirement
  to normalise data before insertion.
- **Artifacts:** Regression test `tests/unit/delta/test_delta_schema_evolution.py` reproduces
  the scenario.

## Datetime Conversions
- **Issue:** `to_date`/`to_timestamp` now return Python `date`/`datetime` objects under
  Polars, breaking substring-based assertions in downstream code.
- **Local Mitigation:** New helpers in `sparkless.compat.datetime` offer column- and
  value-level normalisation.
- **Upstream Ask:** Clarify whether future Polars releases will expose string-returning
  variants or document recommended migration paths.

## Documentation Examples
- **Issue:** Documentation tests execute `python3 examples/...` which resolves to the global
  interpreter and imports the pip-installed `sparkless`. This diverges from the workspace
  version.
- **Proposed Action:** Decide whether to ship a CLI wrapper that sets `PYTHONPATH` before
  running examples or update tests to invoke `sys.executable` instead of `python3`.

