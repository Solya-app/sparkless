#!/usr/bin/env python3
"""
Create GitHub issues in eddiethedean/robin-sparkless for each distinct
robin_sparkless error pattern. Each issue includes reproduction code (from a
representative test) and PySpark snippet showing expected behavior.

Uses parsed JSON from tests/tools/parse_robin_results.py.

Run from repo root:
  python scripts/create_robin_sparkless_issues_from_parsed.py [--dry-run] [--limit N]
  python scripts/create_robin_sparkless_issues_from_parsed.py -i tests/robin_results_parsed_n9.json --dry-run
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"
TITLE_MAX = 60
PATTERN_NORMALIZE_LEN = 72


def _normalize_pattern(msg: str, max_len: int = PATTERN_NORMALIZE_LEN) -> str:
    """Normalize failure message to a stable pattern for grouping."""
    s = (msg or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _load_parsed(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _group_robin_sparkless(data: dict) -> dict[str, list[dict]]:
    """Group failures with category robin_sparkless by normalized message pattern."""
    failures = data.get("failures", [])
    robin = [f for f in failures if f.get("category") == "robin_sparkless"]
    by_pattern: dict[str, list[dict]] = defaultdict(list)
    for r in robin:
        pat = _normalize_pattern(r.get("message", ""))
        by_pattern[pat].append(r)
    return dict(by_pattern)


def extract_test_method_source(repo_root: Path, test_id: str) -> str:
    """
    Extract the source code of the test method from the test file.
    test_id format: tests/unit/.../file.py::ClassName::test_method_name
    Returns the method body (with docstring) or a fallback message.
    """
    if "::" not in test_id:
        return f"# Reproduce by running: pytest {test_id} -v"
    path_part, _, rest = test_id.partition("::")
    parts = rest.split("::")
    if len(parts) < 2:
        return f"# Reproduce by running: pytest {test_id} -v"
    class_name, method_name = parts[0], parts[1]
    file_path = repo_root / path_part
    if not file_path.exists():
        return f"# Test file not found: {path_part}\n# Reproduce by running: pytest {test_id} -v"
    try:
        source = file_path.read_text(encoding="utf-8")
    except Exception:
        return f"# Could not read {path_part}\n# Reproduce by running: pytest {test_id} -v"
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return f"# Could not parse {path_part}\n# Reproduce by running: pytest {test_id} -v"
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == method_name:
                    start = item.lineno - 1
                    end = item.end_lineno if hasattr(item, "end_lineno") and item.end_lineno else item.lineno
                    lines = source.splitlines()
                    snippet = "\n".join(lines[start:end])
                    return snippet
            break
    return f"# Method {method_name} not found in {class_name}\n# Reproduce by running: pytest {test_id} -v"


def make_title(pattern: str, seen_titles: set[str]) -> str:
    """Build issue title; ensure uniqueness by appending (2), (3) if needed."""
    short = (pattern or "Unknown error")[:TITLE_MAX].strip()
    if not short.endswith("..."):
        pass
    base = f"[Sparkless] {short}"
    if base not in seen_titles:
        seen_titles.add(base)
        return base
    n = 2
    while f"{base} ({n})" in seen_titles:
        n += 1
    title = f"{base} ({n})"
    seen_titles.add(title)
    return title


def make_body(
    pattern: str,
    message: str,
    representative_test_id: str,
    all_test_ids: list[str],
    repro_code: str,
    pyspark_snippet: str,
) -> str:
    """Build the issue body in markdown."""
    others = [t for t in all_test_ids if t != representative_test_id][:10]
    affected = f"{len(all_test_ids)} test(s). Representative: `{representative_test_id}`."
    if others:
        affected += "\nOthers: " + ", ".join(f"`{t}`" for t in others)
    return f"""## Summary

When Sparkless runs with the Robin backend, the following operation fails. PySpark accepts it and returns the expected result.

## Error (Robin)

```
{message[:1500]}
```

## Reproduction (Sparkless)

From test: `{representative_test_id}`

```python
{repro_code}
```

## Expected (PySpark)

```python
{pyspark_snippet}
```

## Affected tests

{affected}

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
SPARKLESS_TEST_BACKEND=robin pytest {representative_test_id} -v --tb=short
```
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create robin-sparkless GitHub issues from parsed robin_sparkless failures."
    )
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        default=None,
        help="Path to parsed JSON (default: tests/robin_results_parsed_n9.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print title and body only; do not call gh",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of issues to create (for testing)",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default=REPO,
        help=f"GitHub repo (default: {REPO})",
    )
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    scripts_dir = Path(__file__).resolve().parent
    snippets_path = scripts_dir / "robin_issue_pyspark_snippets.py"
    spec = importlib.util.spec_from_file_location("robin_issue_pyspark_snippets", snippets_path)
    if spec is None or spec.loader is None:
        print("Could not load robin_issue_pyspark_snippets.py", file=sys.stderr)
        return 1
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    get_pyspark_snippet = mod.get_pyspark_snippet

    input_path = args.input or (repo_root / "tests" / "robin_results_parsed_n9.json")
    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    data = _load_parsed(input_path)
    by_pattern = _group_robin_sparkless(data)
    # Sort by pattern string for stable order; then by count desc for priority
    patterns_sorted = sorted(
        by_pattern.keys(),
        key=lambda p: (-len(by_pattern[p]), p),
    )
    if args.limit is not None:
        patterns_sorted = patterns_sorted[: args.limit]

    seen_titles: set[str] = set()
    created = 0
    for pattern in patterns_sorted:
        items = by_pattern[pattern]
        rep = items[0]
        representative_test_id = rep["test_id"]
        message = rep.get("message", pattern)
        all_test_ids = [x["test_id"] for x in items]

        repro_code = extract_test_method_source(repo_root, representative_test_id)
        pyspark_snippet = get_pyspark_snippet(message)
        title = make_title(pattern, seen_titles)
        body = make_body(
            pattern,
            message,
            representative_test_id,
            all_test_ids,
            repro_code,
            pyspark_snippet,
        )

        if args.dry_run:
            print(f"[dry-run] Title: {title}")
            print(f"[dry-run] Body length: {len(body)} chars")
            print()
            continue

        body_file = repo_root / "tests" / ".robin_issue_body.txt"
        body_file.parent.mkdir(parents=True, exist_ok=True)
        body_file.write_text(body, encoding="utf-8")
        try:
            subprocess.run(
                [
                    "gh",
                    "issue",
                    "create",
                    "-R",
                    args.repo,
                    "--title",
                    title,
                    "--body-file",
                    str(body_file),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            created += 1
            print(f"Created: {title[:60]}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to create '{title[:40]}...': {e.stderr or e}", file=sys.stderr)
        finally:
            if body_file.exists():
                body_file.unlink(missing_ok=True)

    if not args.dry_run and created:
        print(f"\nCreated {created} issue(s) in {args.repo}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
