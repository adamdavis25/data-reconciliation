"""
GET /compliance/concentration?date=2026-01-15
---------------------------------------------
Identifies any equity position that exceeds 20 % of the total account
market value on the given date.

Algorithm
---------
1. For each (account, date) pair, compute the total market value of all
   positions.
2. For each individual position compute its weight = market_value / total_mv.
3. Flag any position where weight > 0.20 as a concentration breach.
"""
from datetime import date

from flask import Blueprint, jsonify, request
from sqlalchemy import func

from ..extensions import db
from ..models import Position

compliance_bp = Blueprint("compliance", __name__)

CONCENTRATION_THRESHOLD = 0.20  # 20 %


@compliance_bp.route("/compliance/concentration", methods=["GET"])
def concentration():
    """
    Query parameters
    ----------------
    date : str  – ISO date YYYY-MM-DD (required)

    Response
    --------
    {
      "date": "2026-01-15",
      "threshold_pct": 20.0,
      "breaches": [
        {
          "account_id": "ACC001",
          "symbol": "NVDA",
          "market_value": 66750.0,
          "account_total_market_value": 200000.0,
          "concentration_pct": 33.375,
          "excess_pct": 13.375
        },
        ...
      ],
      "accounts_checked": 3,
      "accounts_with_breaches": 2
    }
    """
    date_str = request.args.get("date", "").strip()

    if not date_str:
        return jsonify({"error": "Query parameter 'date' is required."}), 400

    try:
        pos_date = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Use YYYY-MM-DD."}), 400

    # Step 1 – compute per-account total market value
    account_totals = (
        db.session.query(
            Position.account_id,
            func.sum(Position.market_value).label("total_mv"),
        )
        .filter(Position.position_date == pos_date)
        .group_by(Position.account_id)
        .all()
    )

    if not account_totals:
        return jsonify({
            "date":                    date_str,
            "threshold_pct":           CONCENTRATION_THRESHOLD * 100,
            "breaches":                [],
            "accounts_checked":        0,
            "accounts_with_breaches":  0,
        }), 200

    total_mv_by_account = {row.account_id: float(row.total_mv) for row in account_totals}

    # Step 2 – fetch all individual positions for that date
    positions = (
        Position.query
        .filter(Position.position_date == pos_date)
        .order_by(Position.account_id, Position.symbol)
        .all()
    )

    breaches = []
    breaching_accounts = set()

    for pos in positions:
        account_total = total_mv_by_account.get(pos.account_id, 0)
        if account_total <= 0:
            continue

        mv = float(pos.market_value)
        weight = mv / account_total

        if weight > CONCENTRATION_THRESHOLD:
            concentration_pct = round(weight * 100, 4)
            excess_pct        = round((weight - CONCENTRATION_THRESHOLD) * 100, 4)
            breaches.append({
                "account_id":                pos.account_id,
                "symbol":                    pos.symbol,
                "quantity":                  float(pos.quantity),
                "closing_price":             float(pos.closing_price),
                "market_value":              round(mv, 4),
                "account_total_market_value": round(account_total, 4),
                "concentration_pct":         concentration_pct,
                "threshold_pct":             CONCENTRATION_THRESHOLD * 100,
                "excess_pct":                excess_pct,
            })
            breaching_accounts.add(pos.account_id)

    return jsonify({
        "date":                   date_str,
        "threshold_pct":          CONCENTRATION_THRESHOLD * 100,
        "breaches":               breaches,
        "accounts_checked":       len(total_mv_by_account),
        "accounts_with_breaches": len(breaching_accounts),
    }), 200
