"""
Unit tests for the data ingestion service.

Covers
------
- Format 1 (CSV) happy path and field mapping
- Format 2 (pipe-delimited) happy path and field mapping
- Quality-check rejection of invalid rows for both formats
- Duplicate detection for both formats
- Position file happy path
- Auto-detect dispatcher
"""
import pytest

from app.models import Trade, Position, IngestLog
from app.services.ingestion import (
    ingest_trades_format_1,
    ingest_trades_format_2,
    ingest_positions,
    detect_and_ingest,
)


# ---------------------------------------------------------------------------
# Format 1 – CSV trades
# ---------------------------------------------------------------------------

class TestIngestTradeFormat1:

    HEADER = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"

    def _row(self, **kw):
        defaults = dict(
            date="2025-01-15", account="ACC001", ticker="AAPL",
            qty=100, price=185.50, side="BUY", settle="2025-01-17",
        )
        defaults.update(kw)
        return (
            f"{defaults['date']},{defaults['account']},{defaults['ticker']},"
            f"{defaults['qty']},{defaults['price']},{defaults['side']},"
            f"{defaults['settle']}\n"
        )

    def test_happy_path_all_rows_accepted(self, db, app):
        csv = self.HEADER + self._row() + self._row(ticker="MSFT", price=420.25)
        with app.app_context():
            report = ingest_trades_format_1(csv, "t1.csv")
        assert report.rows_total == 2
        assert report.rows_accepted == 2
        assert report.rows_rejected == 0
        assert report.errors == []

    def test_field_mapping(self, db, app):
        csv = self.HEADER + self._row(
            date="2025-01-15", account="ACC002", ticker="MSFT",
            qty=50, price=420.25, side="SELL", settle="2025-01-17",
        )
        with app.app_context():
            ingest_trades_format_1(csv, "map.csv")
            trade = Trade.query.filter_by(account_id="ACC002", symbol="MSFT").first()
        assert trade is not None
        assert trade.side == "SELL"
        assert float(trade.price) == pytest.approx(420.25)
        assert float(trade.quantity) == 50.0
        assert str(trade.settlement_date) == "2025-01-17"
        assert trade.source_format == "1"
        assert trade.source_system is None

    def test_gross_value_buy_is_positive(self, db, app):
        csv = self.HEADER + self._row(qty=100, price=185.50, side="BUY")
        with app.app_context():
            ingest_trades_format_1(csv, "gv.csv")
            trade = Trade.query.first()
        assert float(trade.gross_value) == pytest.approx(18550.0)

    def test_gross_value_sell_is_negative(self, db, app):
        csv = self.HEADER + self._row(qty=150, price=238.45, side="SELL")
        with app.app_context():
            ingest_trades_format_1(csv, "gv_sell.csv")
            trade = Trade.query.first()
        assert float(trade.gross_value) == pytest.approx(-35767.50)

    def test_trade_id_is_synthesised(self, db, app):
        csv = self.HEADER + self._row()
        with app.app_context():
            ingest_trades_format_1(csv, "synth.csv")
            trade = Trade.query.first()
        assert trade.trade_id.startswith("1-synth-")

    def test_settlement_date_stored(self, db, app):
        csv = self.HEADER + self._row(settle="2025-01-17")
        with app.app_context():
            ingest_trades_format_1(csv, "settle.csv")
            trade = Trade.query.first()
        assert str(trade.settlement_date) == "2025-01-17"

    def test_missing_account_rejected(self, db, app):
        csv = self.HEADER + self._row(account="")
        with app.app_context():
            report = ingest_trades_format_1(csv, "no_acc.csv")
        assert report.rows_rejected == 1
        assert any("AccountID" in e["field"] for e in report.errors)

    def test_missing_ticker_rejected(self, db, app):
        csv = self.HEADER + self._row(ticker="")
        with app.app_context():
            report = ingest_trades_format_1(csv, "no_ticker.csv")
        assert report.rows_rejected == 1

    def test_invalid_date_rejected(self, db, app):
        csv = self.HEADER + self._row(date="not-a-date")
        with app.app_context():
            report = ingest_trades_format_1(csv, "bad_date.csv")
        assert report.rows_rejected == 1

    def test_negative_quantity_rejected(self, db, app):
        csv = self.HEADER + self._row(qty=-50)
        with app.app_context():
            report = ingest_trades_format_1(csv, "neg_qty.csv")
        assert report.rows_rejected == 1

    def test_zero_quantity_rejected(self, db, app):
        csv = self.HEADER + self._row(qty=0)
        with app.app_context():
            report = ingest_trades_format_1(csv, "zero_qty.csv")
        assert report.rows_rejected == 1

    def test_invalid_trade_type_rejected(self, db, app):
        csv = self.HEADER + self._row(side="HOLD")
        with app.app_context():
            report = ingest_trades_format_1(csv, "bad_side.csv")
        assert report.rows_rejected == 1
        assert any("TradeType" in e["field"] for e in report.errors)

    def test_missing_required_column_rejects_file(self, db, app):
        csv = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType\n" + \
              "2025-01-15,ACC001,AAPL,100,185.50,BUY\n"
        with app.app_context():
            report = ingest_trades_format_1(csv, "missing_col.csv")
        assert any("settlementdate" in str(e).lower() for e in report.errors)
        assert report.rows_accepted == 0

    def test_duplicate_row_counted(self, db, app):
        csv = self.HEADER + self._row()
        with app.app_context():
            ingest_trades_format_1(csv, "dup.csv")
            report2 = ingest_trades_format_1(csv, "dup.csv")
        assert report2.rows_duplicate == 1

    def test_mixed_valid_invalid_rows(self, db, app):
        csv = (
            self.HEADER
            + self._row()                     
            + self._row(account="")           
            + self._row(ticker="MSFT", price=420.25)  
        )
        with app.app_context():
            report = ingest_trades_format_1(csv, "mixed.csv")
        assert report.rows_total == 3
        assert report.rows_accepted == 2
        assert report.rows_rejected == 1

    def test_ingest_log_created(self, db, app):
        csv = self.HEADER + self._row()
        with app.app_context():
            ingest_trades_format_1(csv, "log_t1.csv")
            log = IngestLog.query.filter_by(file_name="log_t1.csv").first()
        assert log is not None
        assert log.file_type == "trade_1"
        assert log.rows_accepted == 1

    def test_bytes_input_decoded(self, db, app):
        csv_bytes = (self.HEADER + self._row()).encode("utf-8")
        with app.app_context():
            report = ingest_trades_format_1(csv_bytes, "bytes.csv")
        assert report.rows_accepted == 1

    def test_full_sample_file(self, db, app):
        """All 10 rows of the provided sample should be accepted."""
        from tests.conftest import TRADE_1_CSV
        with app.app_context():
            report = ingest_trades_format_1(TRADE_1_CSV, "trades_format_1.csv")
        assert report.rows_total == 10
        assert report.rows_accepted == 10
        assert report.rows_rejected == 0


# ---------------------------------------------------------------------------
# Format 2 – Pipe-delimited trades
# ---------------------------------------------------------------------------

class TestIngestTradeFormat2:

    HEADER = "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"

    def _row(self, **kw):
        defaults = dict(
            date="20250115", account="ACC001", ticker="AAPL",
            shares=100, mv=18550.00, sys="CUSTODIAN_A",
        )
        defaults.update(kw)
        return (
            f"{defaults['date']}|{defaults['account']}|{defaults['ticker']}|"
            f"{defaults['shares']}|{defaults['mv']}|{defaults['sys']}\n"
        )

    def test_happy_path_all_rows_accepted(self, db, app):
        pipe = self.HEADER + self._row() + self._row(ticker="MSFT", mv=21012.50)
        with app.app_context():
            report = ingest_trades_format_2(pipe, "t2.txt")
        assert report.rows_total == 2
        assert report.rows_accepted == 2
        assert report.rows_rejected == 0
        assert report.errors == []

    def test_field_mapping(self, db, app):
        pipe = self.HEADER + self._row(
            date="20250115", account="ACC002", ticker="NVDA",
            shares=120, mv=60636.00, sys="CUSTODIAN_B",
        )
        with app.app_context():
            ingest_trades_format_2(pipe, "map2.txt")
            trade = Trade.query.filter_by(account_id="ACC002", symbol="NVDA").first()
        assert trade is not None
        assert trade.side == "BUY"
        assert float(trade.quantity) == 120.0
        assert float(trade.gross_value) == pytest.approx(60636.0)
        assert trade.source_system == "CUSTODIAN_B"
        assert trade.source_format == "2"
        assert trade.settlement_date is None

    def test_yyyymmdd_date_parsed(self, db, app):
        pipe = self.HEADER + self._row(date="20250115")
        with app.app_context():
            ingest_trades_format_2(pipe, "date.txt")
            trade = Trade.query.first()
        assert str(trade.trade_date) == "2025-01-15"

    def test_negative_shares_becomes_sell(self, db, app):
        pipe = self.HEADER + self._row(
            ticker="TSLA", shares=-150, mv=-35767.50, sys="CUSTODIAN_A"
        )
        with app.app_context():
            ingest_trades_format_2(pipe, "sell.txt")
            trade = Trade.query.filter_by(symbol="TSLA").first()
        assert trade.side == "SELL"
        assert float(trade.quantity) == pytest.approx(150.0)
        assert float(trade.gross_value) == pytest.approx(-35767.50)

    def test_price_derived_from_market_value(self, db, app):
        pipe = self.HEADER + self._row(shares=100, mv=18550.00)
        with app.app_context():
            ingest_trades_format_2(pipe, "price.txt")
            trade = Trade.query.first()
        assert float(trade.price) == pytest.approx(185.50)

    def test_sell_price_derived_correctly(self, db, app):
        pipe = self.HEADER + self._row(
            ticker="TSLA", shares=-150, mv=-35767.50, sys="CUSTODIAN_A"
        )
        with app.app_context():
            ingest_trades_format_2(pipe, "sell_price.txt")
            trade = Trade.query.filter_by(symbol="TSLA").first()
        assert float(trade.price) == pytest.approx(238.45, rel=1e-4)

    def test_trade_id_synthesised(self, db, app):
        pipe = self.HEADER + self._row()
        with app.app_context():
            ingest_trades_format_2(pipe, "synth2.txt")
            trade = Trade.query.first()
        assert trade.trade_id.startswith("2-synth2-")

    def test_missing_account_rejected(self, db, app):
        pipe = self.HEADER + self._row(account="")
        with app.app_context():
            report = ingest_trades_format_2(pipe, "no_acc.txt")
        assert report.rows_rejected == 1

    def test_missing_ticker_rejected(self, db, app):
        pipe = self.HEADER + self._row(ticker="")
        with app.app_context():
            report = ingest_trades_format_2(pipe, "no_ticker.txt")
        assert report.rows_rejected == 1

    def test_zero_shares_rejected(self, db, app):
        pipe = self.HEADER + self._row(shares=0, mv=0)
        with app.app_context():
            report = ingest_trades_format_2(pipe, "zero.txt")
        assert report.rows_rejected == 1

    def test_invalid_date_rejected(self, db, app):
        pipe = self.HEADER + self._row(date="BADDATE")
        with app.app_context():
            report = ingest_trades_format_2(pipe, "bad_date.txt")
        assert report.rows_rejected == 1

    def test_non_numeric_shares_rejected(self, db, app):
        pipe = self.HEADER + self._row(shares="MANY")
        with app.app_context():
            report = ingest_trades_format_2(pipe, "bad_shares.txt")
        assert report.rows_rejected == 1

    def test_missing_required_column_rejects_file(self, db, app):
        bad = "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE\n" \
              "20250115|ACC001|AAPL|100|18550.00\n"
        with app.app_context():
            report = ingest_trades_format_2(bad, "missing_col.txt")
        assert any("source_system" in str(e).lower() for e in report.errors)
        assert report.rows_accepted == 0

    def test_duplicate_row_counted(self, db, app):
        pipe = self.HEADER + self._row()
        with app.app_context():
            ingest_trades_format_2(pipe, "dup2.txt")
            report2 = ingest_trades_format_2(pipe, "dup2.txt")
        assert report2.rows_duplicate == 1

    def test_ingest_log_created(self, db, app):
        pipe = self.HEADER + self._row()
        with app.app_context():
            ingest_trades_format_2(pipe, "log_t2.txt")
            log = IngestLog.query.filter_by(file_name="log_t2.txt").first()
        assert log is not None
        assert log.file_type == "trade_2"
        assert log.rows_accepted == 1

    def test_full_sample_file(self, db, app):
        """All 10 rows of the provided sample should be accepted."""
        from tests.conftest import TRADE_2_PIPE
        with app.app_context():
            report = ingest_trades_format_2(TRADE_2_PIPE, "trades_format_2.txt")
        assert report.rows_total == 10
        assert report.rows_accepted == 10
        assert report.rows_rejected == 0


# ---------------------------------------------------------------------------
# Position file – CSV
# ---------------------------------------------------------------------------

class TestIngestPositions:

    def test_happy_path(self, db, app):
        csv = (
            "account_id,symbol,position_date,quantity,cost_basis_per_share,"
            "closing_price,currency\n"
            "ACC001,AAPL,2025-01-15,100,185.50,185.50,USD\n"
            "ACC001,MSFT,2025-01-15,50,420.25,420.25,USD\n"
        )
        with app.app_context():
            report = ingest_positions(csv, "pos.csv")
        assert report.rows_accepted == 2
        assert report.rows_rejected == 0

    def test_derived_fields_computed(self, db, app):
        csv = (
            "account_id,symbol,position_date,quantity,cost_basis_per_share,"
            "closing_price,currency\n"
            "ACC001,NVDA,2025-01-15,80,505.30,505.30,USD\n"
        )
        with app.app_context():
            ingest_positions(csv, "pos2.csv")
            pos = Position.query.filter_by(account_id="ACC001", symbol="NVDA").first()
        assert pos is not None
        assert float(pos.total_cost_basis) == pytest.approx(80 * 505.30)
        assert float(pos.market_value)     == pytest.approx(80 * 505.30)

    def test_negative_quantity_rejected(self, db, app):
        csv = (
            "account_id,symbol,position_date,quantity,cost_basis_per_share,"
            "closing_price,currency\n"
            "ACC001,AAPL,2025-01-15,-10,185.50,185.50,USD\n"
        )
        with app.app_context():
            report = ingest_positions(csv, "neg.csv")
        assert report.rows_rejected == 1

    def test_zero_quantity_accepted(self, db, app):
        """Closed positions (qty=0) are still reported by the broker."""
        csv = (
            "account_id,symbol,position_date,quantity,cost_basis_per_share,"
            "closing_price,currency\n"
            "ACC003,TSLA,2025-01-15,0,238.45,238.45,USD\n"
        )
        with app.app_context():
            report = ingest_positions(csv, "zero.csv")
        assert report.rows_accepted == 1

    def test_duplicate_position_counted(self, db, app):
        csv = (
            "account_id,symbol,position_date,quantity,cost_basis_per_share,"
            "closing_price,currency\n"
            "ACC001,AAPL,2025-01-15,100,185.50,185.50,USD\n"
        )
        with app.app_context():
            ingest_positions(csv, "dup_pos.csv")
            report2 = ingest_positions(csv, "dup_pos.csv")
        assert report2.rows_duplicate == 1

    def test_missing_column_rejected(self, db, app):
        csv = (
            "account_id,symbol,position_date,quantity,closing_price,currency\n"
            "ACC001,AAPL,2025-01-15,100,185.50,USD\n"
        )
        with app.app_context():
            report = ingest_positions(csv, "missing_col.csv")
        assert any("cost_basis_per_share" in str(e) for e in report.errors)

    def test_full_sample_file(self, db, app):
        """All 10 rows of the provided sample should be accepted."""
        from tests.conftest import POSITIONS_CSV
        with app.app_context():
            report = ingest_positions(POSITIONS_CSV, "positions.csv")
        assert report.rows_total == 10
        assert report.rows_accepted == 10
        assert report.rows_rejected == 0


# ---------------------------------------------------------------------------
# Auto-detect dispatcher
# ---------------------------------------------------------------------------

class TestDetectAndIngest:

    def test_detects_format_1_csv(self, db, app):
        csv = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        with app.app_context():
            report = detect_and_ingest(csv, "trades.csv")
        assert report.file_type == "trade_1"

    def test_detects_format_2_pipe(self, db, app):
        pipe = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
        )
        with app.app_context():
            report = detect_and_ingest(pipe, "trades.txt")
        assert report.file_type == "trade_2"

    def test_detects_position_csv(self, db, app):
        csv = (
            "account_id,symbol,position_date,quantity,cost_basis_per_share,"
            "closing_price,currency\n"
            "ACC001,AAPL,2025-01-15,100,185.50,185.50,USD\n"
        )
        with app.app_context():
            report = detect_and_ingest(csv, "positions.csv")
        assert report.file_type == "position"

    def test_unrecognised_csv_raises(self, db, app):
        csv = "foo,bar,baz\n1,2,3\n"
        with app.app_context():
            with pytest.raises(ValueError, match="Cannot determine file type"):
                detect_and_ingest(csv, "unknown.csv")

    def test_bytes_input_works(self, db, app):
        pipe = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
        ).encode("utf-8")
        with app.app_context():
            report = detect_and_ingest(pipe, "trades.txt")
        assert report.file_type == "trade_2"
        assert report.rows_accepted == 1
