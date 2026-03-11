"""
Produce a list of robin_sparkless parity gaps with existing issue numbers or 'create'.
Reads parsed Robin results JSON and maps error patterns to docs/upstream.md issue numbers.
Output: tests/robin_parity_gaps_for_issues.json for use by create_robin_github_issues.py.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

# Map message substrings to upstream issue number (from docs/upstream.md / robin_parity_matrix.md).
# Order matters: first match wins (more specific patterns first).
PATTERN_TO_ISSUE = [
    (r"filter predicate must be of type.*Boolean.*got.*String", 646),
    (r"casting from Utf8View to Boolean|filter predicate.*Boolean", 646),
    (r"conversion from.*str.*to.*i32|conversion from.*str.*to.*datetime|casting from f64 to i32|string to boolean failed", 649),
    (r"create_dataframe_from_rows failed:.*struct value must be object", 634),
    (r"create_dataframe_from_rows failed:.*map column", 627),  # or 633
    (r"create_dataframe_from_rows failed:.*array element|json_value_to_series", 625),
    (r"select expects Column or str", 645),
    (r"cannot convert to Column", 644),
    (r"Table or view .* not found", 629),
    (r"not found:.*ID|not found:.*Column", 636),
    (r"type String is incompatible with.*Int64|union.*coercion", 613),
    (r"cannot compare string with numeric type", 628),
    (r"cannot compare string with numeric", 635),
    (r"is_in.*cannot check for List.*String data|isin.*List\(Int64\)", 638),
    (r"isin.*empty list|isin requires a list", 637),
    (r"conversion from.*str.*to.*i32|conversion from.*str.*to.*datetime|casting from f64 to i32|string to boolean failed", 649),
    (r"invalid series dtype: expected.*Boolean", None),  # casewhen/boolean coercion - could be new or 646
    (r"arithmetic on string and numeric not allowed", 628),
    (r"Key 'avg\(.*\)' not found|agg result column", 672),
    (r"DESCRIBE DETAIL|describe_detail", 678),
    (r"dtype Unknown.*not supported|div operation not supported for dtypes", None),
]

def normalize_message(msg: str, max_len: int = 100) -> str:
    """Abbreviate for grouping."""
    if not msg:
        return "other"
    s = (msg or "").strip()
    # Normalize common prefixes for grouping
    if "create_dataframe_from_rows failed:" in s:
        # Group by next part: map column X, struct value, array element, json_value_to_series
        if "map column" in s:
            return "create_dataframe_from_rows: map column ..."
        if "struct value must be object" in s:
            return "create_dataframe_from_rows: struct value must be object ..."
        if "array element" in s or "json_value_to_series" in s:
            return "create_dataframe_from_rows: array/json ..."
    if "collect failed:" in s:
        if "filter predicate" in s:
            return "collect failed: filter predicate must be Boolean, got String"
        if "casting from Utf8View to Boolean" in s or "casting from.*View to Boolean" in s:
            return "collect failed: filter predicate / Utf8View to Boolean"
        if "conversion from" in s or "casting from f64" in s or "casting from string to boolean" in s or "to boolean failed" in s:
            return "collect failed: type conversion (str/datetime/f64/boolean)"
        if "cannot compare string with numeric" in s:
            return "collect failed: cannot compare string with numeric"
        if "invalid series dtype" in s:
            return "collect failed: invalid series dtype (expected Boolean)"
        if "arithmetic on string and numeric" in s:
            return "collect failed: arithmetic on string and numeric not allowed"
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def main() -> None:
    ap = argparse.ArgumentParser(description="List robin_sparkless gaps with issue numbers")
    ap.add_argument("input", nargs="?", default="tests/robin_results_parsed_no_skip.json", help="Parsed results JSON")
    ap.add_argument("-o", "--output", default="tests/robin_parity_gaps_for_issues.json", help="Output JSON")
    args = ap.parse_args()
    path = Path(args.input)
    if not path.exists():
        path = Path(__file__).resolve().parent.parent / path.name
    data = json.loads(path.read_text())
    failures = [f for f in data.get("failures", []) if f.get("category") == "robin_sparkless"]
    # Group by normalized message
    by_pattern: dict[str, list[dict]] = defaultdict(list)
    for f in failures:
        key = normalize_message(f.get("message", ""))
        by_pattern[key].append(f)
    # Build output: one row per pattern with existing_issue and example tests
    out = []
    for pattern, group in sorted(by_pattern.items(), key=lambda x: -len(x[1])):
        msg_sample = group[0].get("message", "")
        issue = None
        for regex, num in PATTERN_TO_ISSUE:
            if re.search(regex, msg_sample, re.IGNORECASE | re.DOTALL):
                issue = num
                break
        example_tests = list({f["test_id"] for f in group[:5]})
        short_name = pattern.replace("create_dataframe_from_rows: ", "cdf_").replace("collect failed: ", "collect_")[:60]
        out.append({
            "short_name": short_name,
            "error_pattern": pattern,
            "count": len(group),
            "example_test_ids": example_tests,
            "existing_issue": issue,
            "action": "comment" if issue else "create",
        })
    Path(args.output).write_text(json.dumps(out, indent=2))
    print(f"Wrote {len(out)} gap groups to {args.output}")


if __name__ == "__main__":
    main()
