"""
Parse pytest Robin results file and categorize failures for the test failure report.

Usage:
  python tests/tools/parse_robin_results.py tests/results_robin_20260218_194747.txt
  python tests/tools/parse_robin_results.py results.txt -o tests/robin_results_parsed.json

Reads a results file from run_all_tests.sh (Robin backend), extracts summary and
FAILED/ERROR lines, categorizes each failure, and writes JSON for generate_failure_report.py.

Categories:
  - robin_sparkless: Robin engine/crate semantics (collect, create_dataframe_from_rows,
    SQL/join/union/order_by limits, "not implemented for the Robin backend", type/parity mismatches).
  - fix_sparkless: Sparkless Python layer (missing F.*/Column/Session/Reader APIs, Row key naming,
    catalog, context manager, create_schema, etc.).
  - other: Unmatched (assertion/catalog/one-off).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def _categorize(message: str, exception_type: str = "") -> str:
    """Categorize failure/error message as robin_sparkless, fix_sparkless, or other."""
    if not message:
        return "other"
    msg = message
    exc = exception_type or ""
    # robin_sparkless
    if re.search(r"collect failed:", msg):
        return "robin_sparkless"
    if re.search(r"create_dataframe_from_rows failed:", msg):
        return "robin_sparkless"
    if re.search(r"select expects Column or str|cannot convert to Column", msg):
        return "robin_sparkless"
    if re.search(r"Table or view .* not found", msg):
        return "robin_sparkless"
    if "AssertionError" in exc and (
        "tuple" in msg and "set" in msg
        or "incompatible with expected type" in msg
        or "==" in msg and "1234" in msg
    ):
        return "robin_sparkless"
    if "AssertionError" in exc and (
        "'1234'" in msg or "1234" in msg and "==" in msg
        or "in {" in msg and "assert " in msg
    ):
        return "robin_sparkless"
    if "cannot compare string with numeric" in msg or "type String is incompatible with expected type" in msg:
        return "robin_sparkless"
    if "is not implemented for the Robin backend" in msg:
        return "robin_sparkless"
    if re.search(r"SQL failed:|SQL parse error|only SELECT|only INNER|only LEFT|only RIGHT|only FULL", msg):
        return "robin_sparkless"
    if "union failed:" in msg or "group_by failed:" in msg or "order_by failed:" in msg:
        return "robin_sparkless"
    if "schema failed:" in msg or "casting from" in msg and "not supported" in msg:
        return "robin_sparkless"
    if "Expected numeric type" in msg or "Expected LongType" in msg or "out of range integral" in msg:
        return "robin_sparkless"
    if "math domain error" in msg:
        return "robin_sparkless"
    if "join on expression" in msg and "not supported" in msg:
        return "robin_sparkless"
    if "unsupported join type:" in msg:
        return "robin_sparkless"
    if "DataFrames are not equivalent" in msg:
        return "robin_sparkless"
    if "select failed: not found: Column" in msg or "join failed: not found:" in msg:
        return "robin_sparkless"
    if "assert False" in msg:
        return "robin_sparkless"
    # fix_sparkless
    if re.search(r"PyColumn.*has no attribute", msg):
        return "fix_sparkless"
    if re.search(r"RobinColumn.*is not callable", msg):
        return "fix_sparkless"
    if re.search(r"RobinFunctions.*has no attribute|'RobinFunctions' object has no attribute", msg):
        return "fix_sparkless"
    if re.search(r"module ['\"]sparkless\.sql\.functions['\"] has no attribute", msg):
        return "fix_sparkless"
    if re.search(r"RobinDataFrameReader.*option", msg):
        return "fix_sparkless"
    if re.search(r"RobinSparkSession.*stop", msg):
        return "fix_sparkless"
    if re.search(r"tuple.*cannot be converted to.*PyDict", msg):
        return "fix_sparkless"
    if re.search(r"NotImplementedError.*GroupedData|GroupedData\.agg\(\) not yet", msg):
        return "fix_sparkless"
    if re.search(r"RobinGroupedData.*pivot", msg):
        return "fix_sparkless"
    if re.search(r"module.*has no attribute ['\"]first['\"]", msg):
        return "fix_sparkless"
    if re.search(r"module.*has no attribute ['\"]rank['\"]", msg):
        return "fix_sparkless"
    if re.search(r"PyColumn.*is not callable", msg):
        return "fix_sparkless"
    if re.search(r"NoneType.*fields", msg):
        return "fix_sparkless"
    if "_has_active_session" in msg:
        return "fix_sparkless"
    if "No active SparkSession" in msg:
        return "fix_sparkless"
    if "Regex pattern did not match" in msg or "can not infer schema from empty" in msg or "createDataFrame requires schema" in msg:
        return "fix_sparkless"
    if re.search(r"PyColumn.*is not iterable", msg):
        return "fix_sparkless"
    if "RobinSparkSession" in msg and ("_storage" in msg or "stop" in msg):
        return "fix_sparkless"
    if re.search(r"unsupported operand type.*PyColumn", msg):
        return "fix_sparkless"
    if re.search(r"module.*has no attribute ['\"]udf['\"]", msg):
        return "fix_sparkless"
    if re.search(r"module.*has no attribute ['\"]row_number['\"]", msg) or re.search(r"module.*has no attribute ['\"]percent_rank['\"]", msg):
        return "fix_sparkless"
    if "'str' object cannot be converted to 'PyDict'" in msg or "createDataFrame_from_pandas" in msg:
        return "fix_sparkless"
    if re.search(r"Key ['\"].*['\"] not found in row", msg):
        return "fix_sparkless"
    if re.search(r"Database .* does not exist", msg):
        return "fix_sparkless"
    if "_get_available_columns" in msg or "_materialized" in msg:
        return "fix_sparkless"
    if "log() takes 1 positional" in msg or "takes 0 positional arguments but 2" in msg:
        return "fix_sparkless"
    if "context manager protocol" in msg and "RobinSparkSession" in msg:
        return "fix_sparkless"
    if "Robin save_as_table" in msg or "save_as_table" in msg and "missing columns" in msg:
        return "fix_sparkless"
    if "isin requires a list" in msg:
        return "fix_sparkless"
    if "NoneType" in msg and "create_schema" in msg:
        return "fix_sparkless"
    if "'ColumnOperation' object cannot be converted to 'PyColumn'" in msg:
        return "fix_sparkless"
    if "'AggregateFunction' object cannot be converted to 'PyColumn'" in msg:
        return "fix_sparkless"
    if "'dict' object cannot be converted to 'PyColumn'" in msg:
        return "fix_sparkless"
    if "StringType" in msg and "element_type" in msg:
        return "fix_sparkless"
    if "RobinPivotedGroupedData" in msg and "has no attribute" in msg:
        return "fix_sparkless"
    if "RobinSparkSessionBuilder" in msg and "is not callable" in msg:
        return "fix_sparkless"
    if "RobinDataFrameWriter" in msg and "has no attribute" in msg:
        return "fix_sparkless"
    if "RobinSparkSession" in msg and "has no attribute" in msg:
        return "fix_sparkless"
    if "_robin_functions_module" in msg and "missing" in msg and "positional" in msg:
        return "fix_sparkless"
    if "Cast column not found" in msg or "cast failed:" in msg:
        return "robin_sparkless"
    if "with_column failed:" in msg or "agg failed:" in msg:
        return "robin_sparkless"
    if "Expected hour=" in msg or "substr(" in msg and "failed for row" in msg:
        return "robin_sparkless"
    if "not supported between instances of 'str' and 'int'" in msg or "can only concatenate str" in msg:
        return "robin_sparkless"
    if "Expected table not found error" in msg:
        return "robin_sparkless"
    # Assertion / parity (value or type mismatch; test expected different result)
    if msg.startswith("assert ") and ("==" in msg or " is " in msg or " is not " in msg or " in " in msg or " in {" in msg or " not " in msg or " < " in msg or " > " in msg):
        return "robin_sparkless"
    if "DID NOT RAISE" in msg:
        return "robin_sparkless"
    if "argument of type 'NoneType' is not iterable" in msg:
        return "fix_sparkless"
    return "other"


def parse_results(results_path: str | Path) -> dict:
    """Parse results file and return structured data."""
    path = Path(results_path)
    text = path.read_text()
    lines = text.splitlines()

    # Summary lines: "=== 746 failed, 271 passed, 4 skipped, 118 errors in 21.24s ==="
    # Or without errors: "=== 670 failed, 229 passed, 4 skipped in 17.31s ==="
    # First occurrence = unit, second = parity.
    summary_re = re.compile(
        r"=+\s*(\d+)\s+failed,\s*(\d+)\s+passed,\s*(\d+)\s+skipped(?:,\s*(\d+)\s+errors)?"
    )
    summaries: list[dict] = []
    for line in lines:
        m = summary_re.search(line)
        if m:
            summaries.append({
                "failed": int(m.group(1)),
                "passed": int(m.group(2)),
                "skipped": int(m.group(3)),
                "errors": int(m.group(4)) if m.group(4) is not None else 0,
            })
    summary_unit = summaries[0] if len(summaries) >= 1 else {"passed": 0, "failed": 0, "errors": 0, "skipped": 0}
    summary_parity = summaries[1] if len(summaries) >= 2 else {"passed": 0, "failed": 0, "errors": 0, "skipped": 0}

    failures: list[dict] = []
    fail_re = re.compile(r"^(FAILED|ERROR)\s+(.+?)(?:\s+-\s+(.+))?$")
    for line in lines:
        line = line.strip()
        if not line.startswith("FAILED ") and not line.startswith("ERROR "):
            continue
        m = fail_re.match(line)
        if not m:
            continue
        kind = m.group(1)  # FAILED or ERROR
        test_id = m.group(2).strip()
        rest = (m.group(3) or "").strip()
        # rest is "ExceptionType: message"
        if ": " in rest:
            exc_type, _, msg = rest.partition(": ")
            exception_type = exc_type.strip()
            message = msg.strip()
        else:
            exception_type = rest or "Unknown"
            message = rest
        phase = "unit" if test_id.startswith("tests/unit/") else "parity"
        category = _categorize(message, exception_type)
        failures.append({
            "kind": kind,
            "test_id": test_id,
            "exception_type": exception_type,
            "message": message,
            "phase": phase,
            "category": category,
        })

    return {
        "results_file": str(path.resolve()),
        "summary_unit": summary_unit,
        "summary_parity": summary_parity,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse Robin pytest results and output JSON.")
    parser.add_argument("results_file", type=str, help="Path to results .txt file")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output JSON path (default: tests/robin_results_parsed.json)")
    args = parser.parse_args()
    out_path = args.output
    if not out_path:
        out_path = str(Path(__file__).resolve().parent.parent / "robin_results_parsed.json")
    data = parse_results(args.results_file)
    Path(out_path).write_text(json.dumps(data, indent=2))
    print(f"Wrote {out_path} ({len(data['failures'])} failures/errors)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
