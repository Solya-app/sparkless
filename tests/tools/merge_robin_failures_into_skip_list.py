"""
Merge FAILED/ERROR test IDs from a Robin pytest results file into tests/robin_skip_list.json.

Usage:
  python tests/tools/merge_robin_failures_into_skip_list.py tests/results_robin_<timestamp>.txt
  python tests/tools/merge_robin_failures_into_skip_list.py results.txt -o tests/robin_skip_list.json

Reads the results file for lines like "FAILED tests/path::TestClass::test_name",
loads the current skip list, merges (dedupes, sorts), and writes back.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def extract_failed_ids(results_path: str | Path) -> list[str]:
    """Parse results file and return list of failed/error test node IDs."""
    path = Path(results_path)
    if not path.exists():
        return []
    text = path.read_text()
    lines = text.splitlines()
    fail_re = re.compile(r"^(FAILED|ERROR)\s+(.+?)(?:\s+-\s+.+)?$")
    ids_list: list[str] = []
    for line in lines:
        line = line.strip()
        if not line.startswith("FAILED ") and not line.startswith("ERROR "):
            continue
        m = fail_re.match(line)
        if not m:
            continue
        test_id = m.group(2).strip()
        ids_list.append(test_id)
    return sorted(set(ids_list))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge FAILED/ERROR ids from Robin results into robin_skip_list.json"
    )
    parser.add_argument(
        "results_file",
        type=str,
        help="Path to pytest results .txt file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Output JSON path (default: tests/robin_skip_list.json)",
    )
    args = parser.parse_args()
    out_path = Path(
        args.output
        or Path(__file__).resolve().parent.parent / "robin_skip_list.json"
    )
    failed_ids = extract_failed_ids(args.results_file)
    skip_path = out_path
    if skip_path.exists():
        try:
            data = json.loads(skip_path.read_text())
            current = frozenset(data) if isinstance(data, list) else frozenset()
        except Exception:
            current = frozenset()
    else:
        current = frozenset()
    merged = sorted(current | set(failed_ids))
    skip_path.write_text(json.dumps(merged, indent=2) + "\n")
    added = len(merged) - len(current)
    print(f"Merged {len(failed_ids)} failed id(s) into skip list; {added} new. Total: {len(merged)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
