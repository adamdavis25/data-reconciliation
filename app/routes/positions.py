"""
GET /positions?account=ACC001&date=2026-01-15
---------------------------------------------
Returns all positions for the given account on the given date,
including cost basis and market value per holding, plus account totals.
"""
from datetime import date

from flask import Blueprint, jsonify, request
from sqlalchemy import func

from ..extensions import db
from ..models import Position

positions_bp = Blueprint("positions", __name__)


@positions_bp.route("/positions", methods=["GET"])
def get_positions():
    """
    Query parameters
    ----------------
    account : str  – account ID (required)
    date    : str  – ISO date YYYY-MM-DD (required)

    Response
    --------
    {
      "account_id": "ACC001",
      "position_date": "2026-01-15",
      "positions": [ { ...position fields... }, ... ],
      "summary": {
        "total_cost_basis": 123456.78,
        "total_market_value": 130000.00,
        "unrealised_pnl": 6543.22,
        "position_count": 7
      }
    }
    """
    account = request.args.get("account", "").strip()
    date_str = request.args.get("date", "").strip()

    if not account:
        return jsonify({"error": "Query parameter 'account' is required."}), 400
    if not date_str:
        return jsonify({"error": "Query parameter 'date' is required."}), 400

    try:
        pos_date = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Use YYYY-MM-DD."}), 400

    rows = (
        Position.query
        .filter_by(account_id=account, position_date=pos_date)
        .order_by(Position.symbol)
        .all()
    )

    if not rows:
        return jsonify({
            "account_id":    account,
            "position_date": date_str,
            "positions":     [],
            "summary": {
                "total_cost_basis":   0,
                "total_market_value": 0,
                "unrealised_pnl":     0,
                "position_count":     0,
            },
        }), 200

    positions_list = [p.to_dict() for p in rows]
    total_cost   = sum(float(p.total_cost_basis) for p in rows)
    total_mv     = sum(float(p.market_value)     for p in rows)

    return jsonify({
        "account_id":    account,
        "position_date": date_str,
        "positions":     positions_list,
        "summary": {
            "total_cost_basis":   round(total_cost, 6),
            "total_market_value": round(total_mv, 6),
            "unrealised_pnl":     round(total_mv - total_cost, 6),
            "position_count":     len(rows),
        },
    }), 200
