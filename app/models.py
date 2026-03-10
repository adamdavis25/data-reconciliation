"""
SQLAlchemy database models for the Portfolio Data Clearinghouse.

Tables
------
Trade       – normalised record of every trade, ingested from two source formats.
Position    – end-of-day position snapshot from the bank-broker feed.
IngestLog   – audit trail for every ingest run.
"""
from datetime import datetime, timezone
from .extensions import db


class Trade(db.Model):
    """
    Normalised trade record.

    Source formats
    --------------
    Format 1 (CSV)  : TradeDate, AccountID, Ticker, Quantity, Price,
                      TradeType, SettlementDate
    Format 2 (pipe) : REPORT_DATE, ACCOUNT_ID, SECURITY_TICKER, SHARES,
                      MARKET_VALUE, SOURCE_SYSTEM
                      (SELL indicated by negative SHARES / MARKET_VALUE;
                       price derived as abs(MARKET_VALUE) / abs(SHARES))
    Both formats are mapped to this single table.
    """
    __tablename__ = "trades"

    id = db.Column(db.Integer, primary_key=True)

    trade_id = db.Column(db.String(128), nullable=False, index=True)

    account_id  = db.Column(db.String(32),  nullable=False, index=True)
    symbol      = db.Column(db.String(16),  nullable=False, index=True)
    trade_date  = db.Column(db.Date,        nullable=False, index=True)
    quantity    = db.Column(db.Numeric(18, 6), nullable=False)
    price       = db.Column(db.Numeric(18, 6), nullable=True)
    side        = db.Column(db.String(4),   nullable=False)
    currency    = db.Column(db.String(3),   nullable=False, default="USD")

    gross_value = db.Column(db.Numeric(18, 6), nullable=False)

    settlement_date = db.Column(db.Date, nullable=True)

    source_system = db.Column(db.String(64), nullable=True)

    source_format = db.Column(db.String(8),  nullable=False)
    source_file   = db.Column(db.String(256), nullable=True)
    ingested_at   = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("trade_id", "source_format", name="uq_trade_source"),
    )

    def to_dict(self):
        return {
            "id":              self.id,
            "trade_id":        self.trade_id,
            "account_id":      self.account_id,
            "symbol":          self.symbol,
            "trade_date":      str(self.trade_date),
            "quantity":        float(self.quantity),
            "price":           float(self.price) if self.price is not None else None,
            "side":            self.side,
            "currency":        self.currency,
            "gross_value":     float(self.gross_value),
            "settlement_date": str(self.settlement_date) if self.settlement_date else None,
            "source_system":   self.source_system,
            "source_format":   self.source_format,
            "source_file":     self.source_file,
        }

    def __repr__(self):
        return (
            f"<Trade {self.trade_id} {self.side} {self.quantity}"
            f" {self.symbol} @ {self.price}>"
        )


class Position(db.Model):
    """
    End-of-day position snapshot received from the bank-broker.

    Source format (CSV): account_id, symbol, position_date, quantity,
                         cost_basis_per_share, closing_price, currency
    """
    __tablename__ = "positions"

    id = db.Column(db.Integer, primary_key=True)

    account_id           = db.Column(db.String(32),     nullable=False, index=True)
    symbol               = db.Column(db.String(16),     nullable=False, index=True)
    position_date        = db.Column(db.Date,           nullable=False, index=True)
    quantity             = db.Column(db.Numeric(18, 6), nullable=False)
    cost_basis_per_share = db.Column(db.Numeric(18, 6), nullable=False)
    closing_price        = db.Column(db.Numeric(18, 6), nullable=False)
    currency             = db.Column(db.String(3),      nullable=False, default="USD")

    total_cost_basis     = db.Column(db.Numeric(18, 6), nullable=False)
    market_value         = db.Column(db.Numeric(18, 6), nullable=False)

    source_file  = db.Column(db.String(256), nullable=True)
    ingested_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint(
            "account_id", "symbol", "position_date",
            name="uq_position_account_symbol_date"
        ),
    )

    def to_dict(self):
        return {
            "id":                   self.id,
            "account_id":           self.account_id,
            "symbol":               self.symbol,
            "position_date":        str(self.position_date),
            "quantity":             float(self.quantity),
            "cost_basis_per_share": float(self.cost_basis_per_share),
            "closing_price":        float(self.closing_price),
            "currency":             self.currency,
            "total_cost_basis":     float(self.total_cost_basis),
            "market_value":         float(self.market_value),
        }

    def __repr__(self):
        return (
            f"<Position {self.account_id} {self.symbol}"
            f" {self.position_date} qty={self.quantity}>"
        )


class IngestLog(db.Model):
    """
    Audit record created for every ingest run.
    Captures counts of rows accepted, rejected, and the quality report.
    """
    __tablename__ = "ingest_logs"

    id           = db.Column(db.Integer, primary_key=True)
    run_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    file_name    = db.Column(db.String(256), nullable=False)
    file_type    = db.Column(db.String(16),  nullable=False)
    rows_total   = db.Column(db.Integer, default=0)
    rows_accepted= db.Column(db.Integer, default=0)
    rows_rejected= db.Column(db.Integer, default=0)
    rows_duplicate= db.Column(db.Integer, default=0)
    errors       = db.Column(db.Text,    nullable=True)

    def to_dict(self):
        import json
        return {
            "id":              self.id,
            "run_at":          self.run_at.isoformat(),
            "file_name":       self.file_name,
            "file_type":       self.file_type,
            "rows_total":      self.rows_total,
            "rows_accepted":   self.rows_accepted,
            "rows_rejected":   self.rows_rejected,
            "rows_duplicate":  self.rows_duplicate,
            "errors":          json.loads(self.errors) if self.errors else [],
        }
