"""
POST /ingest
------------
Accepts one or more uploaded files (multipart/form-data) or a JSON body
with base64-encoded content, runs quality checks, persists valid rows,
and returns a data quality report.

Alternatively, the standalone CLI script `scripts/ingest_files.py` can be
used to load files directly without going through HTTP.
"""
import os
from flask import Blueprint, jsonify, request

from ..services.ingestion import detect_and_ingest

ingest_bp = Blueprint("ingest", __name__)

ALLOWED_EXTENSIONS = {"csv", "json", "txt"}


def _allowed(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@ingest_bp.route("/ingest", methods=["POST"])
def ingest():
    """
    Upload one or more trade/position files for ingestion.

    Accepts multipart/form-data with field name ``files``.
    Returns a JSON array of per-file quality reports.

    Example (curl)::

        curl -X POST http://localhost:5000/ingest \\
             -F "files=@data/samples/trades_format_a.csv" \\
             -F "files=@data/samples/trades_format_b.json" \\
             -F "files=@data/samples/positions.csv"
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

        if not _allowed(f.filename):
            reports.append({
                "file_name": f.filename,
                "error": f"Unsupported file type. Allowed: {ALLOWED_EXTENSIONS}",
            })
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
