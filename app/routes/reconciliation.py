"""
GET /reconciliation?date=2026-01-15
------------------------------------
Compares the trade file data against the position file for a given day
and surfaces discrepancies.

Reconciliation logic
--------------------
For each (account, symbol) pair that appears in either the trades table
or the positions table on the given date:

1. **Net trade quantity** – sum of all BUY quantities minus all SELL
   quantities recorded in the trades table.

2. **Position quantity** – quantity from the positions table.

3. **Discrepancy** – a row is flagged when:
   - The symbol appears in trades but not in positions  (missing position).
   - The symbol appears in positions but has no trades  (no trade activity –
     informational, not necessarily an error).
   - The symbol appears in both but the net trade quantity does not match
     the position quantity recorded by the broker.

Note: the reconciliation compares *trade activity on the day* against
*end-of-day positions*.  A position quantity may legitimately differ from
the day's net trades if the account carried a prior-day position; for this
reason we report the delta rather than treating every mismatch as an error.
"""
from datetime import date
from decimal import Decimal

from flask import Blueprint, jsonify, request
from sqlalchemy import func, case

from ..extensions import db
from ..models import Position, Trade

reconciliation_bp = Blueprint("reconciliation", __name__)


@reconciliation_bp.route("/reconciliation", methods=["GET"])
def reconciliation():
    """
    Query parameters
    ----------------
    date : str  – ISO date YYYY-MM-DD (required)

    Response
    --------
    {
      "date": "2026-01-15",
      "summary": {
        "total_pairs_checked": 20,
        "matched": 12,
        "discrepancies": 8
      },
      "discrepancies": [
        {
          "account_id": "ACC001",
          "symbol": "AAPL",
          "net_trade_quantity": 100.0,
          "position_quantity": 150.0,
          "delta": 50.0,
          "issue": "quantity_mismatch"
        },
        ...
      ],
      "matched_pairs": [ ... ]
    }
    """
    date_str = request.args.get("date", "").strip()

    if not date_str:
        return jsonify({"error": "Query parameter 'date' is required."}), 400

    try:
        recon_date = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Use YYYY-MM-DD."}), 400

    # --- Net trade quantities per (account, symbol) on the given date ---
    net_trades = (
        db.session.query(
            Trade.account_id,
            Trade.symbol,
            func.sum(
                case(
                    (Trade.side == "BUY",  Trade.quantity),
                    (Trade.side == "SELL", -Trade.quantity),
                    else_=Decimal("0"),
                )
            ).label("net_quantity"),
            func.sum(Trade.quantity).label("gross_quantity"),
            func.count(Trade.id).label("trade_count"),
        )
        .filter(Trade.trade_date == recon_date)
        .group_by(Trade.account_id, Trade.symbol)
        .all()
    )

    # --- Positions for the given date ---
    positions = (
        Position.query
        .filter(Position.position_date == recon_date)
        .all()
    )

    # Build lookup dicts
    trade_map = {
        (r.account_id, r.symbol): {
            "net_quantity":   float(r.net_quantity),
            "gross_quantity": float(r.gross_quantity),
            "trade_count":    r.trade_count,
        }
        for r in net_trades
    }

    position_map = {
        (p.account_id, p.symbol): float(p.quantity)
        for p in positions
    }

    all_keys = set(trade_map.keys()) | set(position_map.keys())

    discrepancies = []
    matched_pairs = []

    for key in sorted(all_keys):
        account_id, symbol = key
        trade_info = trade_map.get(key)
        pos_qty    = position_map.get(key)

        if trade_info is not None and pos_qty is None:
            discrepancies.append({
                "account_id":        account_id,
                "symbol":            symbol,
                "net_trade_quantity": trade_info["net_quantity"],
                "position_quantity":  None,
                "delta":             None,
                "issue":             "missing_position",
                "detail": (
                    f"{trade_info['trade_count']} trade(s) found but no "
                    f"matching position record for {account_id}/{symbol} "
                    f"on {date_str}."
                ),
            })

        elif trade_info is None and pos_qty is not None:
            # Position exists but no trades on this day – informational
            matched_pairs.append({
                "account_id":        account_id,
                "symbol":            symbol,
                "net_trade_quantity": 0.0,
                "position_quantity":  pos_qty,
                "delta":             pos_qty,
                "note":              "no_trade_activity_on_date",
            })

        else:
            # Both exist – compare
            net_qty = trade_info["net_quantity"]
            delta   = round(pos_qty - net_qty, 6)

            if abs(delta) < 1e-6:
                matched_pairs.append({
                    "account_id":        account_id,
                    "symbol":            symbol,
                    "net_trade_quantity": net_qty,
                    "position_quantity":  pos_qty,
                    "delta":             0.0,
                })
            else:
                discrepancies.append({
                    "account_id":        account_id,
                    "symbol":            symbol,
                    "net_trade_quantity": net_qty,
                    "position_quantity":  pos_qty,
                    "delta":             delta,
                    "issue":             "quantity_mismatch",
                    "detail": (
                        f"Net trade qty ({net_qty}) differs from position qty "
                        f"({pos_qty}) by {delta:+.6f}. This may reflect a "
                        f"prior-day carry-over position."
                    ),
                })

    return jsonify({
        "date": date_str,
        "summary": {
            "total_pairs_checked": len(all_keys),
            "matched":             len(matched_pairs),
            "discrepancies":       len(discrepancies),
        },
        "discrepancies":  discrepancies,
        "matched_pairs":  matched_pairs,
    }), 200
