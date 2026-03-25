"""
POST /ingest
------------
Accepts one or more uploaded files (multipart/form-data), auto-detects
each file's type from its *content* (not its name or extension), runs
quality checks, persists valid rows, and returns a data quality report.

No file-naming convention is required or assumed.  Supported formats are
detected purely from the file content:
  - YAML with a top-level ``positions`` key  → position file
  - Pipe-delimited with REPORT_DATE header   → trade Format 2
  - Comma-delimited with TradeDate header    → trade Format 1

Alternatively, the standalone CLI script ``scripts/ingest_files.py`` can
be used to load files directly without going through HTTP.
"""
import os
from flask import Blueprint, jsonify, request

from ..services.ingestion import detect_and_ingest

ingest_bp = Blueprint("ingest", __name__)


@ingest_bp.route("/ingest", methods=["POST"])
def ingest():
    """
    Upload one or more trade/position files for ingestion.

    Accepts multipart/form-data with field name ``files``.
    File names and extensions are irrelevant – type is detected from content.
    Returns a JSON array of per-file quality reports.

    Example (curl)::

        curl -X POST http://localhost:5000/ingest \\
             -F "files=@data/samples/trades_format_1.csv" \\
             -F "files=@data/samples/trades_format_2.txt" \\
             -F "files=@data/samples/positions.yaml"
    """
    if "files" not in request.files:
        return jsonify({"error": "No files provided. Use multipart field 'files'."}), 400

    uploaded = request.files.getlist("files")
    if not uploaded:
        return jsonify({"error": "File list is empty."}), 400

    reports = []
    for f in uploaded:
        if f.filename == "":
            reports.append({"error": "One file had no filename – skipped."})
            continue

        content = f.read()
        try:
            report = detect_and_ingest(content, os.path.basename(f.filename))
            reports.append(report.to_dict())
        except ValueError as exc:
            reports.append({"file_name": f.filename, "error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            reports.append({
                "file_name": f.filename,
                "error": f"Unexpected error: {exc}",
            })

    return jsonify({"ingest_reports": reports}), 200
