"""Run full-package Pyright and emit a compact Markdown trend summary."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("target", nargs="?", default="megaton")
    return parser.parse_args()


def _delta(current: int, baseline: int) -> str:
    value = current - baseline
    return f"{value:+d}"


def _relative_path(path: str) -> str:
    try:
        return str(Path(path).resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return path


def main() -> int:
    args = _args()
    completed = subprocess.run(
        [sys.executable, "-m", "pyright", args.target, "--outputjson"],
        capture_output=True,
        text=True,
        check=False,
    )
    if not completed.stdout:
        print("## Pyright informational\n\nPyright produced no JSON output.")
        if completed.stderr:
            print(f"\n```text\n{completed.stderr.strip()}\n```")
        return completed.returncode or 2

    try:
        report = json.loads(completed.stdout)
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"## Pyright informational\n\nUnable to read diagnostics: `{exc}`")
        return 2

    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    summary = report.get("summary", {})
    errors = int(summary.get("errorCount", 0))
    warnings = int(summary.get("warningCount", 0))
    baseline_errors = int(baseline.get("errorCount", 0))
    baseline_warnings = int(baseline.get("warningCount", 0))
    diagnostics = report.get("generalDiagnostics", [])
    rules = Counter(item.get("rule") or "unclassified" for item in diagnostics)
    files = Counter(_relative_path(item.get("file", "unknown")) for item in diagnostics)

    print("## Pyright informational")
    print()
    print(f"Pyright `{report.get('version', 'unknown')}`; baseline `{baseline.get('pyrightVersion', 'unknown')}`.")
    print()
    print("| Metric | Current | Baseline | Delta |")
    print("| --- | ---: | ---: | ---: |")
    print(f"| Errors | {errors} | {baseline_errors} | {_delta(errors, baseline_errors)} |")
    print(f"| Warnings | {warnings} | {baseline_warnings} | {_delta(warnings, baseline_warnings)} |")
    print(f"| Files analyzed | {summary.get('filesAnalyzed', 0)} | - | - |")

    print("\n### Top diagnostic rules\n")
    for rule, count in rules.most_common(5):
        print(f"- `{rule}`: {count}")

    print("\n### Top files\n")
    for filename, count in files.most_common(5):
        print(f"- `{filename}`: {count}")

    # Diagnostics are expected in this informational job. A parsed and written
    # report means the reporting step itself succeeded.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
