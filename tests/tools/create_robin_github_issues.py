"""
Create robin-sparkless GitHub issues from parity gap list and docs, or post 'Still reproduces' comments.

Usage:
  # Dry run: print what would be created/commented
  python tests/tools/create_robin_github_issues.py --dry-run

  # Create new issues only (from docs for gaps with action=create)
  python tests/tools/create_robin_github_issues.py --create

  # Post comment to an existing issue (e.g. 646)
  python tests/tools/create_robin_github_issues.py --comment 646 --body tests/tools/issue_646_comment.md

  # Create all new issues and optionally post comments for a few existing ones
  python tests/tools/create_robin_github_issues.py --create --comment-issues 646 634 649

Requires: gh CLI installed and authenticated; repo eddiethedean/robin-sparkless.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"
DOCS_DIR = Path(__file__).resolve().parent.parent.parent / "docs"

# Map substring of gap short_name to (title_suffix, doc_file) for "create" gaps.
NEW_ISSUE_DOCS = [
    ("invalid series dtype", "when/otherwise: invalid series dtype expected Boolean", "robin_github_issue_casewhen_boolean_dtype.md"),
    ("assert False", "Join/union type coercion (String vs Int64)", "robin_github_issue_join_union_type_coercion.md"),
    ("dtype Unknown", "casewhen / bitwise not: dtype Unknown not supported in 'not' operation", "robin_github_issue_casewhen_bitwise_not.md"),
    ("div operation not supported", "Division between string columns: div operation not supported", "robin_github_issue_string_division.md"),
]


def run_gh(args: list[str], body: str | None = None, body_file: Path | None = None) -> tuple[bool, str]:
    cmd = ["gh", "issue", "--repo", REPO] + args
    try:
        if body_file and body_file.exists():
            with open(body_file) as f:
                body = f.read()
        if body is not None:
            result = subprocess.run(cmd, input=body, capture_output=True, text=True, timeout=30)
        else:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out = (result.stdout or "").strip() or (result.stderr or "").strip()
        return result.returncode == 0, out
    except FileNotFoundError:
        return False, "gh CLI not found"
    except subprocess.TimeoutExpired:
        return False, "gh timed out"


def create_issue(title: str, body: str, dry_run: bool) -> bool:
    if dry_run:
        print(f"[DRY-RUN] Would create issue: {title}")
        print(f"  Body length: {len(body)} chars")
        return True
    ok, out = run_gh(["create", "--title", title, "--body", body])
    if ok:
        print(f"Created: {out}")
    else:
        print(f"Failed to create issue: {out}", file=sys.stderr)
    return ok


def comment_issue(issue_num: int, body: str, dry_run: bool) -> bool:
    if dry_run:
        print(f"[DRY-RUN] Would comment on #{issue_num}")
        return True
    ok, out = run_gh(["comment", str(issue_num), "--body", body])
    if ok:
        print(f"Commented on #{issue_num}")
    else:
        print(f"Failed to comment on #{issue_num}: {out}", file=sys.stderr)
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description="Create or comment on robin-sparkless GitHub issues")
    ap.add_argument("--dry-run", action="store_true", help="Print only, do not call gh")
    ap.add_argument("--create", action="store_true", help="Create new issues from docs for action=create gaps")
    ap.add_argument("--comment", type=int, metavar="ISSUE", help="Post comment to this issue number")
    ap.add_argument("--body", type=Path, help="Comment body file (for --comment)")
    ap.add_argument("--comment-issues", nargs="*", type=int, default=[], metavar="N", help="Post 'Still reproduces' to these issue numbers")
    ap.add_argument("--gaps-json", type=Path, default=Path("tests/robin_parity_gaps_for_issues.json"), help="Gaps list JSON")
    args = ap.parse_args()

    if args.comment is not None:
        body = (args.body or Path()).read_text() if (args.body and args.body.exists()) else "Still reproduces (verified with Sparkless + robin-sparkless)."
        if not comment_issue(args.comment, body, args.dry_run):
            return 1
        return 0

    if args.comment_issues and not args.dry_run:
        body = "Still reproduces (verified with Sparkless v4 + robin-sparkless 0.15.0). See docs/robin_parity_matrix.md and Sparkless test suite."
        for num in args.comment_issues:
            comment_issue(num, body, dry_run=False)
        return 0
    elif args.comment_issues and args.dry_run:
        for num in args.comment_issues:
            print(f"[DRY-RUN] Would comment on #{num}")
        return 0

    if not args.create:
        if not args.dry_run and not args.comment_issues:
            print("Use --create to create new issues, --comment N --body FILE to comment, or --comment-issues 646 634 ...")
        return 0

    # Load gaps to find which have action=create
    gaps_path = args.gaps_json
    if not gaps_path.exists():
        gaps_path = Path(__file__).resolve().parent.parent / gaps_path.name
    if not gaps_path.exists():
        print(f"Gaps file not found: {gaps_path}", file=sys.stderr)
        return 1
    gaps = json.loads(gaps_path.read_text())
    create_gaps = [g for g in gaps if g.get("action") == "create"]

    created = 0
    for g in create_gaps:
        short = g.get("short_name", "")
        doc_file = None
        title = None
        for _short, _title, _doc in NEW_ISSUE_DOCS:
            if _short in short or short in _short:
                doc_file = DOCS_DIR / _doc
                title = f"[PySpark parity] {_title}"
                break
        if not doc_file or not title:
            for _short, _title, _doc in NEW_ISSUE_DOCS:
                if _doc.replace("robin_github_issue_", "").replace(".md", "") in short:
                    doc_file = DOCS_DIR / _doc
                    title = f"[PySpark parity] {_title}"
                    break
        if not doc_file or not doc_file.exists():
            print(f"Skipping {short}: no doc found", file=sys.stderr)
            continue
        body = doc_file.read_text()
        # Remove "(to be created)" from body so issue body is clean
        body = body.replace("**Upstream issue:** (to be created)", "**Upstream issue:** (this issue)")
        if not create_issue(title, body, args.dry_run):
            return 1
        created += 1

    if args.create and created == 0 and not create_gaps:
        print("No gaps with action=create; nothing to create.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
