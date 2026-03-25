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
  - position : YAML file with structure:
                report_date: "YYYY-MM-DD"
                positions:
                  - account_id: <str>
                    holdings:
                      - symbol, quantity, cost_basis_per_share,
                        closing_price, currency

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

import yaml

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
# Position file – YAML
# ---------------------------------------------------------------------------
# Expected structure:
#   report_date: "YYYY-MM-DD"
#   positions:
#     - account_id: ACC001
#       holdings:
#         - symbol: AAPL
#           quantity: 100
#           cost_basis_per_share: 185.50
#           closing_price: 185.50
#           currency: USD        # optional, defaults to USD
# ---------------------------------------------------------------------------

POSITION_HOLDING_REQUIRED_KEYS = {
    "symbol", "quantity", "cost_basis_per_share", "closing_price",
}


def ingest_positions(file_content: str | bytes, file_name: str) -> QualityReport:
    """Ingest YAML position file from the bank-broker."""
    report = QualityReport(file_name=file_name, file_type="position")

    if isinstance(file_content, bytes):
        file_content = file_content.decode("utf-8-sig")

    # --- Parse YAML ---
    try:
        data = yaml.safe_load(file_content)
    except yaml.YAMLError as exc:
        report.add_error(0, "file", f"YAML parse error: {exc}")
        report.persist()
        return report

    if not isinstance(data, dict):
        report.add_error(0, "file", "YAML root must be a mapping (dict)")
        report.persist()
        return report

    # --- report_date (top-level, used as fallback when holding omits it) ---
    report_date_raw = data.get("report_date")
    report_date: date | None = None
    if report_date_raw is not None:
        report_date = _parse_date_iso(str(report_date_raw))
        if report_date is None:
            report.add_error(0, "report_date",
                             f"Invalid report_date format: '{report_date_raw}'")

    positions_list = data.get("positions")
    if not isinstance(positions_list, list) or len(positions_list) == 0:
        report.add_error(0, "positions", "No 'positions' list found in YAML")
        report.persist()
        return report

    # Flatten nested structure: iterate account blocks → holdings
    row_num = 0
    for acct_block in positions_list:
        if not isinstance(acct_block, dict):
            report.add_error(row_num, "positions",
                             "Each positions entry must be a mapping")
            continue

        account_id = str(acct_block.get("account_id", "")).strip()
        if not account_id:
            report.add_error(row_num, "account_id",
                             "Missing account_id in positions block")

        holdings = acct_block.get("holdings", [])
        if not isinstance(holdings, list):
            report.add_error(row_num, "holdings",
                             f"'holdings' for {account_id} must be a list")
            continue

        for holding in holdings:
            row_num += 1
            report.rows_total += 1
            errors_before = len(report.errors)

            if not isinstance(holding, dict):
                report.add_error(row_num, "holding", "Holding must be a mapping")
                report.rows_rejected += 1
                continue

            missing_keys = POSITION_HOLDING_REQUIRED_KEYS - {
                k.lower() for k in holding.keys()
            }
            if missing_keys:
                report.add_error(row_num, "holding",
                                 f"Missing required keys: {sorted(missing_keys)}")
                report.rows_rejected += 1
                continue

            symbol   = str(holding.get("symbol", "")).strip().upper()
            qty_raw  = holding.get("quantity")
            cb_raw   = holding.get("cost_basis_per_share")
            cp_raw   = holding.get("closing_price")
            currency = str(holding.get("currency", "USD")).strip().upper()

            # Use holding-level date if present, else fall back to report_date
            holding_date_raw = holding.get("position_date")
            if holding_date_raw is not None:
                pos_date = _parse_date_iso(str(holding_date_raw))
                if pos_date is None:
                    report.add_error(row_num, "position_date",
                                     f"Invalid date: '{holding_date_raw}'")
            else:
                pos_date = report_date

            if not account_id:
                report.add_error(row_num, "account_id", "Missing account ID")
            if not symbol:
                report.add_error(row_num, "symbol", "Missing symbol")

            if pos_date is None:
                report.add_error(row_num, "position_date",
                                 "No position_date provided in holding or report_date")
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
    Auto-detect file type from *content* and dispatch to the correct loader.

    File names and extensions are intentionally ignored — the caller should
    not need to follow any naming convention.

    Detection rules (applied in order)
    ------------------------------------
    1. If the content parses as YAML and contains a top-level ``positions``
       key → position file.
    2. If the first non-empty line is pipe-delimited and contains
       ``REPORT_DATE`` or ``SECURITY_TICKER`` → trade Format 2.
    3. If the first non-empty line is comma-delimited and contains
       ``TRADEDATE`` / ``TRADETYPE`` / ``TICKER`` → trade Format 1.
    4. Otherwise raise ValueError with a descriptive message.
    """
    if isinstance(file_content, bytes):
        text = file_content.decode("utf-8-sig")
    else:
        text = file_content

    # --- Try YAML position file first ---
    # YAML is structurally distinct from flat delimited text, so we attempt a
    # lightweight parse.  We only accept it as a position file when the parsed
    # result is a dict that contains a "positions" key – this avoids
    # misidentifying a plain CSV as YAML (yaml.safe_load happily parses a
    # single-column CSV as a list of strings).
    try:
        parsed = yaml.safe_load(text)
        if isinstance(parsed, dict) and "positions" in parsed:
            return ingest_positions(file_content, file_name)
    except yaml.YAMLError:
        pass  # Not valid YAML – fall through to delimited-text detection

    # --- Delimited text detection ---
    first_line = ""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            first_line = stripped
            break

    first_line_upper = first_line.upper()

    # Pipe-delimited → Format 2
    if "|" in first_line:
        cols = {c.strip().upper() for c in first_line_upper.split("|")}
        if "REPORT_DATE" in cols or "SECURITY_TICKER" in cols:
            return ingest_trades_format_2(file_content, file_name)
        raise ValueError(
            f"Pipe-delimited file '{file_name}' has unrecognised columns: "
            f"{sorted(cols)}"
        )

    # Comma-delimited → Format 1
    cols = {c.strip().upper() for c in first_line_upper.split(",")}
    if "TRADEDATE" in cols or "TRADETYPE" in cols or "TICKER" in cols:
        return ingest_trades_format_1(file_content, file_name)

    raise ValueError(
        f"Cannot determine file type for '{file_name}' from content. "
        "Expected: YAML with 'positions' key (position file), "
        "pipe-delimited with REPORT_DATE/SECURITY_TICKER (trade Format 2), "
        "or comma-delimited with TradeDate/TradeType/Ticker (trade Format 1)."
    )
