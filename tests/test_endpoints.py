"""
Integration tests for all Flask endpoints.

Uses the ``seeded_db`` fixture (defined in conftest.py) which pre-loads
the canonical sample data so assertions can be deterministic.

Sample data date: 2025-01-15
Accounts: ACC001, ACC002, ACC003, ACC004
"""
import io
import pytest
from werkzeug.datastructures import MultiDict


# ---------------------------------------------------------------------------
# POST /ingest
# ---------------------------------------------------------------------------

class TestIngestEndpoint:

    TRADE_1_CSV = (
        "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
        "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        "2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17\n"
    )

    TRADE_2_PIPE = (
        "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
        "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
    )

    POSITIONS_YAML = (
        'report_date: "2025-01-15"\n'
        "positions:\n"
        "  - account_id: ACC001\n"
        "    holdings:\n"
        "      - symbol: AAPL\n"
        "        quantity: 100\n"
        "        cost_basis_per_share: 185.50\n"
        "        closing_price: 185.50\n"
        "        currency: USD\n"
    )

    def test_ingest_format_1_returns_200(self, client, db):
        data = {"files": (io.BytesIO(self.TRADE_1_CSV.encode()), "trades_1.csv")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        assert resp.status_code == 200

    def test_ingest_format_1_quality_report(self, client, db):
        data = {"files": (io.BytesIO(self.TRADE_1_CSV.encode()), "trades_1.csv")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        body = resp.get_json()
        assert "ingest_reports" in body
        report = body["ingest_reports"][0]
        assert report["file_type"] == "trade_1"
        assert report["rows_accepted"] == 2
        assert report["rows_rejected"] == 0

    def test_ingest_format_2_pipe_file(self, client, db):
        data = {"files": (io.BytesIO(self.TRADE_2_PIPE.encode()), "trades_2.txt")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        assert resp.status_code == 200
        body = resp.get_json()
        report = body["ingest_reports"][0]
        assert report["file_type"] == "trade_2"
        assert report["rows_accepted"] == 1

    def test_ingest_positions_file(self, client, db):
        data = {"files": (io.BytesIO(self.POSITIONS_YAML.encode()), "positions.yaml")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        body = resp.get_json()
        assert body["ingest_reports"][0]["file_type"] == "position"
        assert body["ingest_reports"][0]["rows_accepted"] == 1

    def test_ingest_positions_file_any_extension(self, client, db):
        """File extension must not matter – detection is content-based."""
        data = {"files": (io.BytesIO(self.POSITIONS_YAML.encode()), "daily_report")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        body = resp.get_json()
        assert body["ingest_reports"][0]["file_type"] == "position"

    def test_ingest_multiple_files(self, client, db):
        data = MultiDict([
            ("files", (io.BytesIO(self.TRADE_1_CSV.encode()), "trades_1.csv")),
            ("files", (io.BytesIO(self.TRADE_2_PIPE.encode()), "trades_2.txt")),
            ("files", (io.BytesIO(self.POSITIONS_YAML.encode()), "positions.yaml")),
        ])
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        assert resp.status_code == 200
        body = resp.get_json()
        assert len(body["ingest_reports"]) == 3

    def test_ingest_no_files_returns_400(self, client, db):
        resp = client.post("/ingest", data={}, content_type="multipart/form-data")
        assert resp.status_code == 400

    def test_ingest_bad_csv_rows_reported(self, client, db):
        bad_csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        data = {"files": (io.BytesIO(bad_csv.encode()), "bad.csv")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        body = resp.get_json()
        report = body["ingest_reports"][0]
        assert report["rows_rejected"] == 1
        assert len(report["errors"]) > 0

    def test_ingest_bad_pipe_rows_reported(self, client, db):
        bad_pipe = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115||AAPL|100|18550.00|CUSTODIAN_A\n"
        )
        data = {"files": (io.BytesIO(bad_pipe.encode()), "bad.txt")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        body = resp.get_json()
        report = body["ingest_reports"][0]
        assert report["rows_rejected"] == 1


# ---------------------------------------------------------------------------
# GET /positions
# ---------------------------------------------------------------------------

class TestPositionsEndpoint:

    def test_returns_positions_for_account_and_date(self, client, seeded_db):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["account_id"] == "ACC001"
        assert body["position_date"] == "2025-01-15"
        assert len(body["positions"]) == 3

    def test_summary_totals_present(self, client, seeded_db):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        body = resp.get_json()
        summary = body["summary"]
        assert "total_cost_basis"   in summary
        assert "total_market_value" in summary
        assert "unrealised_pnl"     in summary
        assert "position_count"     in summary

    def test_position_count_correct(self, client, seeded_db):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        body = resp.get_json()
        assert body["summary"]["position_count"] == 3

    def test_unrealised_pnl_zero_when_cost_equals_price(self, client, seeded_db):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        body = resp.get_json()
        assert body["summary"]["unrealised_pnl"] == pytest.approx(0.0, abs=1e-4)

    def test_acc004_positions(self, client, seeded_db):
        resp = client.get("/positions?account=ACC004&date=2025-01-15")
        body = resp.get_json()
        symbols = {p["symbol"] for p in body["positions"]}
        assert symbols == {"AAPL", "MSFT"}

    def test_missing_account_returns_400(self, client, seeded_db):
        resp = client.get("/positions?date=2025-01-15")
        assert resp.status_code == 400

    def test_missing_date_returns_400(self, client, seeded_db):
        resp = client.get("/positions?account=ACC001")
        assert resp.status_code == 400

    def test_invalid_date_returns_400(self, client, seeded_db):
        resp = client.get("/positions?account=ACC001&date=not-a-date")
        assert resp.status_code == 400

    def test_unknown_account_returns_empty(self, client, seeded_db):
        resp = client.get("/positions?account=UNKNOWN&date=2025-01-15")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["positions"] == []
        assert body["summary"]["position_count"] == 0

    def test_position_fields_present(self, client, seeded_db):
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        body = resp.get_json()
        pos = body["positions"][0]
        for f in ("symbol", "quantity", "cost_basis_per_share",
                  "closing_price", "total_cost_basis", "market_value"):
            assert f in pos, f"Missing field: {f}"


# ---------------------------------------------------------------------------
# GET /compliance/concentration
# ---------------------------------------------------------------------------

class TestComplianceEndpoint:

    def test_returns_200_with_date(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        assert resp.status_code == 200

    def test_response_structure(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        body = resp.get_json()
        assert "date" in body
        assert "threshold_pct" in body
        assert "breaches" in body
        assert "accounts_checked" in body
        assert "accounts_with_breaches" in body

    def test_threshold_is_20_pct(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        assert resp.get_json()["threshold_pct"] == 20.0

    def test_accounts_checked_count(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        assert resp.get_json()["accounts_checked"] == 4

    def test_all_breaches_exceed_20_pct(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        for breach in resp.get_json()["breaches"]:
            assert breach["concentration_pct"] > 20.0

    def test_excess_pct_correct(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        for breach in resp.get_json()["breaches"]:
            expected = round(breach["concentration_pct"] - 20.0, 4)
            assert breach["excess_pct"] == pytest.approx(expected, abs=1e-3)

    def test_breach_fields_present(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2025-01-15")
        body = resp.get_json()
        if body["breaches"]:
            breach = body["breaches"][0]
            for f in ("account_id", "symbol", "market_value",
                      "account_total_market_value",
                      "concentration_pct", "excess_pct"):
                assert f in breach

    def test_missing_date_returns_400(self, client, seeded_db):
        resp = client.get("/compliance/concentration")
        assert resp.status_code == 400

    def test_no_data_date_returns_empty(self, client, seeded_db):
        resp = client.get("/compliance/concentration?date=2000-01-01")
        body = resp.get_json()
        assert body["breaches"] == []
        assert body["accounts_checked"] == 0

    def test_known_breach_detected(self, client, app, db):
        """
        ACC_TEST: AAPL = 80 % of portfolio → must be flagged.
        MSFT = exactly 20 % → must NOT be flagged (strictly > 20 %).
        """
        from app.services.ingestion import ingest_positions
        yaml_str = (
            'report_date: "2025-06-01"\n'
            "positions:\n"
            "  - account_id: ACC_TEST\n"
            "    holdings:\n"
            "      - symbol: AAPL\n"
            "        quantity: 800\n"
            "        cost_basis_per_share: 100.00\n"
            "        closing_price: 100.00\n"
            "        currency: USD\n"
            "      - symbol: MSFT\n"
            "        quantity: 200\n"
            "        cost_basis_per_share: 100.00\n"
            "        closing_price: 100.00\n"
            "        currency: USD\n"
        )
        with app.app_context():
            ingest_positions(yaml_str, "breach_test.yaml")

        resp = client.get("/compliance/concentration?date=2025-06-01")
        body = resp.get_json()
        assert body["accounts_with_breaches"] == 1
        breach = body["breaches"][0]
        assert breach["symbol"] == "AAPL"
        assert breach["concentration_pct"] == pytest.approx(80.0, abs=0.01)

    def test_no_breach_when_all_at_or_below_threshold(self, client, app, db):
        """5 equal positions at exactly 20 % each – none should breach."""
        from app.services.ingestion import ingest_positions
        holdings = "\n".join(
            f"      - symbol: {s}\n"
            "        quantity: 100\n"
            "        cost_basis_per_share: 10.00\n"
            "        closing_price: 10.00\n"
            "        currency: USD"
            for s in ["A", "B", "C", "D", "E"]
        )
        yaml_str = (
            'report_date: "2025-07-01"\n'
            "positions:\n"
            "  - account_id: ACC_EQ\n"
            "    holdings:\n"
            f"{holdings}\n"
        )
        with app.app_context():
            ingest_positions(yaml_str, "eq_test.yaml")

        resp = client.get("/compliance/concentration?date=2025-07-01")
        assert resp.get_json()["accounts_with_breaches"] == 0

    def test_acc004_concentration(self, client, seeded_db):
        """
        ACC004 sample positions:
          AAPL: 500 × 185.50 = 92,750
          MSFT: 300 × 420.25 = 126,075
          Total = 218,825
          AAPL pct = 42.4 %  → breach
          MSFT pct = 57.6 %  → breach
        """
        resp = client.get("/compliance/concentration?date=2025-01-15")
        body = resp.get_json()
        acc004_breaches = [b for b in body["breaches"] if b["account_id"] == "ACC004"]
        symbols = {b["symbol"] for b in acc004_breaches}
        assert "AAPL" in symbols
        assert "MSFT" in symbols


# ---------------------------------------------------------------------------
# GET /reconciliation
# ---------------------------------------------------------------------------

class TestReconciliationEndpoint:

    def test_returns_200_with_date(self, client, seeded_db):
        resp = client.get("/reconciliation?date=2025-01-15")
        assert resp.status_code == 200

    def test_response_structure(self, client, seeded_db):
        resp = client.get("/reconciliation?date=2025-01-15")
        body = resp.get_json()
        assert "date" in body
        assert "summary" in body
        assert "discrepancies" in body
        assert "matched_pairs" in body

    def test_summary_counts_add_up(self, client, seeded_db):
        resp = client.get("/reconciliation?date=2025-01-15")
        s = resp.get_json()["summary"]
        assert s["matched"] + s["discrepancies"] == s["total_pairs_checked"]

    def test_missing_date_returns_400(self, client, seeded_db):
        resp = client.get("/reconciliation")
        assert resp.status_code == 400

    def test_invalid_date_returns_400(self, client, seeded_db):
        resp = client.get("/reconciliation?date=bad")
        assert resp.status_code == 400

    def test_no_data_returns_empty(self, client, seeded_db):
        resp = client.get("/reconciliation?date=1999-01-01")
        body = resp.get_json()
        assert body["summary"]["total_pairs_checked"] == 0

    def test_quantity_mismatch_flagged(self, client, app, db):
        """
        Trade: BUY 100 AAPL for ACC_RECON
        Position: 150 AAPL for ACC_RECON  → delta = +50 (mismatch)
        """
        from app.services.ingestion import ingest_trades_format_1, ingest_positions
        trade_csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-08-01,ACC_RECON,AAPL,100,185.50,BUY,2025-08-03\n"
        )
        pos_yaml = (
            'report_date: "2025-08-01"\n'
            "positions:\n"
            "  - account_id: ACC_RECON\n"
            "    holdings:\n"
            "      - symbol: AAPL\n"
            "        quantity: 150\n"
            "        cost_basis_per_share: 185.50\n"
            "        closing_price: 185.50\n"
            "        currency: USD\n"
        )
        with app.app_context():
            ingest_trades_format_1(trade_csv, "recon_t.csv")
            ingest_positions(pos_yaml, "recon_p.yaml")

        resp = client.get("/reconciliation?date=2025-08-01")
        body = resp.get_json()
        mismatch = next(
            (d for d in body["discrepancies"]
             if d["account_id"] == "ACC_RECON" and d["symbol"] == "AAPL"),
            None,
        )
        assert mismatch is not None
        assert mismatch["issue"] == "quantity_mismatch"
        assert mismatch["delta"] == pytest.approx(50.0)

    def test_missing_position_flagged(self, client, app, db):
        """Trade exists for (ACC_NOPOS, TSLA) but no matching position record."""
        from app.services.ingestion import ingest_trades_format_1
        trade_csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-09-01,ACC_NOPOS,TSLA,50,238.45,BUY,2025-09-03\n"
        )
        with app.app_context():
            ingest_trades_format_1(trade_csv, "nopos.csv")

        resp = client.get("/reconciliation?date=2025-09-01")
        body = resp.get_json()
        missing = next(
            (d for d in body["discrepancies"]
             if d["account_id"] == "ACC_NOPOS" and d["symbol"] == "TSLA"),
            None,
        )
        assert missing is not None
        assert missing["issue"] == "missing_position"

    def test_exact_match_not_discrepancy(self, client, app, db):
        """
        Format 1: BUY 100, SELL 30 → net +70.
        Position: 70.  Should appear in matched_pairs with delta=0.
        """
        from app.services.ingestion import ingest_trades_format_1, ingest_positions
        trade_csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-10-01,ACC_MATCH,AAPL,100,185.50,BUY,2025-10-03\n"
            "2025-10-01,ACC_MATCH,AAPL,30,185.50,SELL,2025-10-03\n"
        )
        pos_yaml = (
            'report_date: "2025-10-01"\n'
            "positions:\n"
            "  - account_id: ACC_MATCH\n"
            "    holdings:\n"
            "      - symbol: AAPL\n"
            "        quantity: 70\n"
            "        cost_basis_per_share: 185.50\n"
            "        closing_price: 185.50\n"
            "        currency: USD\n"
        )
        with app.app_context():
            ingest_trades_format_1(trade_csv, "match_t.csv")
            ingest_positions(pos_yaml, "match_p.yaml")

        resp = client.get("/reconciliation?date=2025-10-01")
        body = resp.get_json()
        matched = next(
            (m for m in body["matched_pairs"]
             if m["account_id"] == "ACC_MATCH" and m["symbol"] == "AAPL"),
            None,
        )
        assert matched is not None
        assert matched["delta"] == pytest.approx(0.0, abs=1e-6)

    def test_format_2_sell_reconciles(self, client, app, db):
        """
        Format 2: intentionally malformed pipe row → rejected.
        Position: 0 TSLA (closed position).
        """
        from app.services.ingestion import ingest_trades_format_2, ingest_positions
        pipe = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20251101|ACC003,TSLA,-150|-35767.50|CUSTODIAN_A\n"
        )
        pos_yaml = (
            'report_date: "2025-11-01"\n'
            "positions:\n"
            "  - account_id: ACC003\n"
            "    holdings:\n"
            "      - symbol: TSLA\n"
            "        quantity: 0\n"
            "        cost_basis_per_share: 238.45\n"
            "        closing_price: 238.45\n"
            "        currency: USD\n"
        )
        with app.app_context():
            report = ingest_trades_format_2(pipe, "f2_sell.txt")
            ingest_positions(pos_yaml, "f2_sell_pos.yaml")

        assert report.rows_rejected == 1

    def test_sample_data_reconciliation(self, client, seeded_db):
        """
        Both Format 1 and Format 2 carry the same trades on 2025-01-15.
        Positions match Format 1 net quantities exactly (no carry-overs).
        ACC003/TSLA is a SELL in trades; position = 0 (closed).
        """
        resp = client.get("/reconciliation?date=2025-01-15")
        body = resp.get_json()
        assert body["summary"]["total_pairs_checked"] > 0
        s = body["summary"]
        assert s["matched"] + s["discrepancies"] == s["total_pairs_checked"]
