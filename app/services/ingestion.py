"""
Data ingestion service.

Supports three file types:
  - trade_1  : CSV (comma-delimited) with columns:
                TradeDate, AccountID, Ticker, Quantity, Price,
                TradeType, SettlementDate
  - trade_2  : Pipe-delimited flat file with columns:
                REPORT_DATE, ACCOUNT_ID, SECURITY_TICKER, SHARES,
                MARKET_VALUE, SOURCE_SYSTEM
                Dates are YYYYMMDD; SELL indicated by negative SHARES/MARKET_VALUE;
                price is derived as abs(MARKET_VALUE) / abs(SHARES).
  - position : CSV (comma-delimited) with columns:
                account_id, symbol, position_date, quantity,
                cost_basis_per_share, closing_price, currency

Each loader returns a QualityReport that is also persisted to IngestLog.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.exc import IntegrityError

from ..extensions import db
from ..models import IngestLog, Position, Trade

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_SIDES      = {"BUY", "SELL"}
VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"}


# ---------------------------------------------------------------------------
# Shared parsing helpers
# ---------------------------------------------------------------------------

def _parse_date_iso(value: str) -> date | None:
    """Parse YYYY-MM-DD date string."""
    try:
        return date.fromisoformat(str(value).strip())
    except (ValueError, AttributeError):
        return None


def _parse_date_compact(value: str) -> date | None:
    """Parse YYYYMMDD date string (Format 2 style)."""
    v = str(value).strip()
    try:
        return date(int(v[:4]), int(v[4:6]), int(v[6:8]))
    except (ValueError, IndexError, TypeError):
        return None


def _parse_decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError):
        return None


def _make_trade_id(source_format: str, file_name: str, row_num: int) -> str:
    """
    Synthesise a stable trade key for formats that carry no explicit trade ID.
    Pattern: <format>-<basename_no_ext>-<row_num>
    """
    base = os.path.splitext(os.path.basename(file_name))[0]
    return f"{source_format}-{base}-{row_num}"


# ---------------------------------------------------------------------------
# Quality-check result container
# ---------------------------------------------------------------------------

@dataclass
class QualityReport:
    file_name:      str
    file_type:      str
    rows_total:     int = 0
    rows_accepted:  int = 0
    rows_rejected:  int = 0
    rows_duplicate: int = 0
    errors:         list[dict] = field(default_factory=list)
    warnings:       list[dict] = field(default_factory=list)

    def add_error(self, row: int, field_name: str, message: str, raw: Any = None):
        self.errors.append({
            "row":     row,
            "field":   field_name,
            "message": message,
            "raw":     str(raw) if raw is not None else None,
        })

    def add_warning(self, row: int, field_name: str, message: str, raw: Any = None):
        self.warnings.append({
            "row":     row,
            "field":   field_name,
            "message": message,
            "raw":     str(raw) if raw is not None else None,
        })

    def to_dict(self) -> dict:
        return {
            "file_name":      self.file_name,
            "file_type":      self.file_type,
            "rows_total":     self.rows_total,
            "rows_accepted":  self.rows_accepted,
            "rows_rejected":  self.rows_rejected,
            "rows_duplicate": self.rows_duplicate,
            "errors":         self.errors,
            "warnings":       self.warnings,
        }

    def persist(self):
        """Save summary to IngestLog table."""
        log = IngestLog(
            file_name      = self.file_name,
            file_type      = self.file_type,
            rows_total     = self.rows_total,
            rows_accepted  = self.rows_accepted,
            rows_rejected  = self.rows_rejected,
            rows_duplicate = self.rows_duplicate,
            errors         = json.dumps(self.errors),
        )
        db.session.add(log)
        db.session.commit()


# ---------------------------------------------------------------------------
# Trade Format 1 – CSV
# ---------------------------------------------------------------------------
# Columns: TradeDate, AccountID, Ticker, Quantity, Price, TradeType,
#          SettlementDate
# ---------------------------------------------------------------------------

TRADE_1_REQUIRED_COLS = {
    "tradedate", "accountid", "ticker",
    "quantity", "price", "tradetype", "settlementdate",
}


def ingest_trades_format_1(file_content: str | bytes, file_name: str) -> QualityReport:
    """
    Ingest Format 1 trade file (comma-delimited CSV).

    Column mapping
    --------------
    TradeDate      → trade_date
    AccountID      → account_id
    Ticker         → symbol
    Quantity       → quantity   (always positive; side from TradeType)
    Price          → price
    TradeType      → side       (BUY / SELL)
    SettlementDate → settlement_date
    trade_id       → synthesised: "1-<basename>-<row_num>"
    """
    report = QualityReport(file_name=file_name, file_type="trade_1")

    if isinstance(file_content, bytes):
        file_content = file_content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(file_content))

    if reader.fieldnames is None:
        report.add_error(0, "file", "Empty or unparseable CSV")
        report.persist()
        return report

    # Normalise header names for comparison (strip + lower)
    norm_fields = {f.strip().lower() for f in reader.fieldnames}
    missing_cols = TRADE_1_REQUIRED_COLS - norm_fields
    if missing_cols:
        report.add_error(
            0, "columns",
            f"Missing required columns: {sorted(missing_cols)}"
        )
        report.persist()
        return report

    for row_num, row in enumerate(reader, start=2):
        report.rows_total += 1
        errors_before = len(report.errors)

        # Normalise keys so we can access regardless of original casing
        row_n = {k.strip().lower(): v for k, v in row.items()}

        account_id   = str(row_n.get("accountid", "")).strip()
        symbol       = str(row_n.get("ticker", "")).strip().upper()
        trade_date_s = str(row_n.get("tradedate", "")).strip()
        qty_raw      = row_n.get("quantity", "")
        price_raw    = row_n.get("price", "")
        side         = str(row_n.get("tradetype", "")).strip().upper()
        settle_s     = str(row_n.get("settlementdate", "")).strip()

        # --- account_id ---
        if not account_id:
            report.add_error(row_num, "AccountID", "Missing account ID")

        # --- symbol ---
        if not symbol:
            report.add_error(row_num, "Ticker", "Missing ticker symbol")

        # --- trade_date ---
        trade_date = _parse_date_iso(trade_date_s)
        if trade_date is None:
            report.add_error(row_num, "TradeDate",
                             f"Invalid date format: '{trade_date_s}'")
        elif trade_date > date.today():
            report.add_warning(row_num, "TradeDate",
                               f"Future trade date: {trade_date}")

        # --- quantity ---
        quantity = _parse_decimal(qty_raw)
        if quantity is None:
            report.add_error(row_num, "Quantity",
                             f"Non-numeric quantity: '{qty_raw}'")
        elif quantity <= 0:
            report.add_error(row_num, "Quantity",
                             f"Quantity must be positive, got {quantity}")

        # --- price ---
        price = _parse_decimal(price_raw)
        if price is None:
            report.add_error(row_num, "Price",
                             f"Non-numeric price: '{price_raw}'")
        elif price <= 0:
            report.add_error(row_num, "Price",
                             f"Price must be positive, got {price}")

        # --- side (TradeType) ---
        if side not in VALID_SIDES:
            report.add_error(row_num, "TradeType",
                             f"Invalid trade type '{side}'; expected BUY or SELL")

        # --- settlement_date (optional but validated if present) ---
        settlement_date = None
        if settle_s:
            settlement_date = _parse_date_iso(settle_s)
            if settlement_date is None:
                report.add_warning(row_num, "SettlementDate",
                                   f"Could not parse settlement date '{settle_s}'")

        if len(report.errors) > errors_before:
            report.rows_rejected += 1
            continue

        gross_value = quantity * price
        if side == "SELL":
            gross_value = -gross_value

        trade_id = _make_trade_id("1", file_name, row_num)

        trade = Trade(
            trade_id        = trade_id,
            account_id      = account_id,
            symbol          = symbol,
            trade_date      = trade_date,
            quantity        = quantity,
            price           = price,
            side            = side,
            currency        = "USD",
            gross_value     = gross_value,
            settlement_date = settlement_date,
            source_system   = None,
            source_format   = "1",
            source_file     = file_name,
        )
        db.session.add(trade)
        try:
            db.session.flush()
            report.rows_accepted += 1
        except IntegrityError:
            db.session.rollback()
            report.rows_duplicate += 1
            report.add_warning(row_num, "trade_id",
                               f"Duplicate trade key '{trade_id}' – skipped")

    db.session.commit()
    report.persist()
    return report


# ---------------------------------------------------------------------------
# Trade Format 2 – Pipe-delimited
# ---------------------------------------------------------------------------
# Columns: REPORT_DATE, ACCOUNT_ID, SECURITY_TICKER, SHARES,
#          MARKET_VALUE, SOURCE_SYSTEM
# Notes:
#   - Date is YYYYMMDD (no separators)
#   - Negative SHARES / MARKET_VALUE indicates a SELL
#   - No explicit price; derived as abs(MARKET_VALUE) / abs(SHARES)
#   - No explicit currency; assumed USD
# ---------------------------------------------------------------------------

TRADE_2_REQUIRED_COLS = {
    "report_date", "account_id", "security_ticker",
    "shares", "market_value", "source_system",
}


def ingest_trades_format_2(file_content: str | bytes, file_name: str) -> QualityReport:
    """
    Ingest Format 2 trade file (pipe-delimited).

    Column mapping
    --------------
    REPORT_DATE      → trade_date  (parsed from YYYYMMDD)
    ACCOUNT_ID       → account_id
    SECURITY_TICKER  → symbol
    SHARES           → quantity    (abs value; sign → side)
    MARKET_VALUE     → gross_value (signed; abs/qty → price)
    SOURCE_SYSTEM    → source_system
    trade_id         → synthesised: "2-<basename>-<row_num>"
    """
    report = QualityReport(file_name=file_name, file_type="trade_2")

    if isinstance(file_content, bytes):
        file_content = file_content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(file_content), delimiter="|")

    if reader.fieldnames is None:
        report.add_error(0, "file", "Empty or unparseable pipe-delimited file")
        report.persist()
        return report

    norm_fields = {f.strip().lower() for f in reader.fieldnames}
    missing_cols = TRADE_2_REQUIRED_COLS - norm_fields
    if missing_cols:
        report.add_error(
            0, "columns",
            f"Missing required columns: {sorted(missing_cols)}"
        )
        report.persist()
        return report

    for row_num, row in enumerate(reader, start=2):
        report.rows_total += 1
        errors_before = len(report.errors)

        row_n = {k.strip().lower(): v for k, v in row.items()}

        account_id    = str(row_n.get("account_id", "")).strip()
        symbol        = str(row_n.get("security_ticker", "")).strip().upper()
        date_raw      = str(row_n.get("report_date", "")).strip()
        shares_raw    = row_n.get("shares", "")
        mv_raw        = row_n.get("market_value", "")
        source_system = str(row_n.get("source_system", "")).strip() or None

        # --- account_id ---
        if not account_id:
            report.add_error(row_num, "ACCOUNT_ID", "Missing account ID")

        # --- symbol ---
        if not symbol:
            report.add_error(row_num, "SECURITY_TICKER", "Missing ticker symbol")

        # --- trade_date (YYYYMMDD) ---
        trade_date = _parse_date_compact(date_raw)
        if trade_date is None:
            report.add_error(row_num, "REPORT_DATE",
                             f"Invalid date format '{date_raw}'; expected YYYYMMDD")
        elif trade_date > date.today():
            report.add_warning(row_num, "REPORT_DATE",
                               f"Future trade date: {trade_date}")

        # --- shares ---
        shares = _parse_decimal(shares_raw)
        if shares is None:
            report.add_error(row_num, "SHARES",
                             f"Non-numeric shares: '{shares_raw}'")
        elif shares == 0:
            report.add_error(row_num, "SHARES", "Shares cannot be zero")

        # --- market_value ---
        market_value = _parse_decimal(mv_raw)
        if market_value is None:
            report.add_error(row_num, "MARKET_VALUE",
                             f"Non-numeric market value: '{mv_raw}'")

        if len(report.errors) > errors_before:
            report.rows_rejected += 1
            continue

        # Derive side from sign of shares
        if shares < 0:
            side     = "SELL"
            quantity = abs(shares)
            gross_mv = market_value  # already negative
        else:
            side     = "BUY"
            quantity = shares
            gross_mv = market_value

        # Validate sign consistency: market_value sign should match shares sign
        if shares > 0 and market_value < 0:
            report.add_warning(row_num, "MARKET_VALUE",
                               f"Positive SHARES ({shares}) but negative "
                               f"MARKET_VALUE ({market_value}); using SHARES sign")
            gross_mv = abs(market_value)
        elif shares < 0 and market_value > 0:
            report.add_warning(row_num, "MARKET_VALUE",
                               f"Negative SHARES ({shares}) but positive "
                               f"MARKET_VALUE ({market_value}); treating as SELL")
            gross_mv = -market_value

        # Derive per-share price
        abs_mv  = abs(gross_mv)
        price   = (abs_mv / quantity).quantize(Decimal("0.000001")) if quantity != 0 else None

        trade_id = _make_trade_id("2", file_name, row_num)

        trade = Trade(
            trade_id        = trade_id,
            account_id      = account_id,
            symbol          = symbol,
            trade_date      = trade_date,
            quantity        = quantity,
            price           = price,
            side            = side,
            currency        = "USD",
            gross_value     = gross_mv,
            settlement_date = None,
            source_system   = source_system,
            source_format   = "2",
            source_file     = file_name,
        )
        db.session.add(trade)
        try:
            db.session.flush()
            report.rows_accepted += 1
        except IntegrityError:
            db.session.rollback()
            report.rows_duplicate += 1
            report.add_warning(row_num, "trade_id",
                               f"Duplicate trade key '{trade_id}' – skipped")

    db.session.commit()
    report.persist()
    return report


# ---------------------------------------------------------------------------
# Position file – CSV (unchanged format)
# ---------------------------------------------------------------------------

POSITION_REQUIRED_COLS = {
    "account_id", "symbol", "position_date", "quantity",
    "cost_basis_per_share", "closing_price", "currency",
}


def ingest_positions(file_content: str | bytes, file_name: str) -> QualityReport:
    """Ingest CSV position file from the bank-broker."""
    report = QualityReport(file_name=file_name, file_type="position")

    if isinstance(file_content, bytes):
        file_content = file_content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(file_content))

    if reader.fieldnames is None:
        report.add_error(0, "file", "Empty or unparseable CSV")
        report.persist()
        return report

    actual_cols = {c.strip().lower() for c in reader.fieldnames}
    missing_cols = {c.lower() for c in POSITION_REQUIRED_COLS} - actual_cols
    if missing_cols:
        report.add_error(0, "columns",
                         f"Missing required columns: {sorted(missing_cols)}")
        report.persist()
        return report

    for row_num, row in enumerate(reader, start=2):
        report.rows_total += 1
        errors_before = len(report.errors)

        account_id   = str(row.get("account_id", "")).strip()
        symbol       = str(row.get("symbol", "")).strip().upper()
        pos_date_str = str(row.get("position_date", "")).strip()
        qty_raw      = row.get("quantity", "")
        cb_raw       = row.get("cost_basis_per_share", "")
        cp_raw       = row.get("closing_price", "")
        currency     = str(row.get("currency", "USD")).strip().upper()

        if not account_id:
            report.add_error(row_num, "account_id", "Missing account ID")
        if not symbol:
            report.add_error(row_num, "symbol", "Missing symbol")

        pos_date = _parse_date_iso(pos_date_str)
        if pos_date is None:
            report.add_error(row_num, "position_date",
                             f"Invalid date format: '{pos_date_str}'")
        elif pos_date > date.today():
            report.add_warning(row_num, "position_date",
                               f"Future position date: {pos_date}")

        quantity = _parse_decimal(qty_raw)
        if quantity is None:
            report.add_error(row_num, "quantity",
                             f"Non-numeric quantity: '{qty_raw}'")
        elif quantity < 0:
            report.add_error(row_num, "quantity",
                             f"Quantity cannot be negative, got {quantity}")

        cost_basis = _parse_decimal(cb_raw)
        if cost_basis is None:
            report.add_error(row_num, "cost_basis_per_share",
                             f"Non-numeric cost basis: '{cb_raw}'")
        elif cost_basis < 0:
            report.add_error(row_num, "cost_basis_per_share",
                             f"Cost basis cannot be negative, got {cost_basis}")

        closing_price = _parse_decimal(cp_raw)
        if closing_price is None:
            report.add_error(row_num, "closing_price",
                             f"Non-numeric closing price: '{cp_raw}'")
        elif closing_price < 0:
            report.add_error(row_num, "closing_price",
                             f"Closing price cannot be negative, got {closing_price}")

        if currency not in VALID_CURRENCIES:
            report.add_warning(row_num, "currency",
                               f"Unrecognised currency '{currency}'")

        if len(report.errors) > errors_before:
            report.rows_rejected += 1
            continue

        total_cost   = quantity * cost_basis
        market_value = quantity * closing_price

        position = Position(
            account_id           = account_id,
            symbol               = symbol,
            position_date        = pos_date,
            quantity             = quantity,
            cost_basis_per_share = cost_basis,
            closing_price        = closing_price,
            currency             = currency,
            total_cost_basis     = total_cost,
            market_value         = market_value,
            source_file          = file_name,
        )
        db.session.add(position)
        try:
            db.session.flush()
            report.rows_accepted += 1
        except IntegrityError:
            db.session.rollback()
            report.rows_duplicate += 1
            report.add_warning(
                row_num, "position",
                f"Duplicate position ({account_id}, {symbol}, {pos_date}) – skipped"
            )

    db.session.commit()
    report.persist()
    return report


# ---------------------------------------------------------------------------
# Auto-detect helper
# ---------------------------------------------------------------------------

def detect_and_ingest(file_content: str | bytes, file_name: str) -> QualityReport:
    """
    Auto-detect file type from header content and dispatch to the correct loader.

    Detection rules
    ---------------
    1. Inspect the first line of the file.
    2. Pipe-delimited header containing REPORT_DATE → trade_2
    3. Comma-delimited header containing TRADEDATE or TRADETYPE → trade_1
    4. Comma-delimited header containing POSITION_DATE or COST_BASIS → position
    5. Otherwise raise ValueError.

    Note: file extension is intentionally NOT used as the sole signal because
    both trade formats are flat text files and the extension may be .csv or .txt.
    """
    if isinstance(file_content, bytes):
        first_line = file_content.decode("utf-8-sig").split("\n")[0]
    else:
        first_line = file_content.split("\n")[0]

    first_line_upper = first_line.strip().upper()

    # Pipe-delimited → Format 2
    if "|" in first_line:
        cols = {c.strip().upper() for c in first_line_upper.split("|")}
        if "REPORT_DATE" in cols or "SECURITY_TICKER" in cols:
            return ingest_trades_format_2(file_content, file_name)
        raise ValueError(
            f"Pipe-delimited file '{file_name}' has unrecognised columns: "
            f"{sorted(cols)}"
        )

    # Comma-delimited
    cols = {c.strip().upper() for c in first_line_upper.split(",")}

    if "POSITION_DATE" in cols or "COST_BASIS_PER_SHARE" in cols:
        return ingest_positions(file_content, file_name)

    if "TRADEDATE" in cols or "TRADETYPE" in cols or "TICKER" in cols:
        return ingest_trades_format_1(file_content, file_name)

    raise ValueError(
        f"Cannot determine file type for '{file_name}' from header: "
        f"{sorted(cols)}. "
        "Expected Format 1 (TradeDate/TradeType/Ticker), "
        "Format 2 (REPORT_DATE|...) or Position (position_date/cost_basis_per_share)."
    )
