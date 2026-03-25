#!/usr/bin/env python
"""
Standalone CLI script for ingesting trade and position files
without going through the HTTP endpoint.

File type is detected automatically from file *content* – no naming
convention is required.

Usage
-----
    # Ingest all sample files at once:
    python scripts/ingest_files.py data/samples/trades_format_1.csv \\
                                   data/samples/trades_format_2.txt \\
                                   data/samples/positions.yaml

    # Any individual file:
    python scripts/ingest_files.py <path/to/file>

The script prints a data quality report to stdout for each file.
"""
import json
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app import create_app
from app.services.ingestion import detect_and_ingest


def _print_report(report_dict: dict) -> None:
    print("\n" + "=" * 60)
    print(f"  File     : {report_dict['file_name']}")
    print(f"  Type     : {report_dict['file_type']}")
    print(f"  Total    : {report_dict['rows_total']}")
    print(f"  Accepted : {report_dict['rows_accepted']}")
    print(f"  Rejected : {report_dict['rows_rejected']}")
    print(f"  Duplicate: {report_dict['rows_duplicate']}")

    errors = report_dict.get("errors", [])
    warnings = report_dict.get("warnings", [])

    if errors:
        print(f"\n  ERRORS ({len(errors)}):")
        for e in errors:
            print(f"    Row {e.get('row', '?')} | {e.get('field', '?')} | {e.get('message', '?')}")

    if warnings:
        print(f"\n  WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"    Row {w.get('row', '?')} | {w.get('field', '?')} | {w.get('message', '?')}")

    print("=" * 60)


def main(file_paths: list[str]) -> None:
    if not file_paths:
        print("Usage: python scripts/ingest_files.py <file1> [file2 ...]")
        sys.exit(1)

    app = create_app()
    with app.app_context():
        for path in file_paths:
            if not os.path.isfile(path):
                print(f"[ERROR] File not found: {path}")
                continue

            file_name = os.path.basename(path)
            print(f"\nIngesting: {path}")

            with open(path, "rb") as fh:
                content = fh.read()

            try:
                report = detect_and_ingest(content, file_name)
                _print_report(report.to_dict())
            except ValueError as exc:
                print(f"[ERROR] {exc}")


if __name__ == "__main__":
    main(sys.argv[1:])
