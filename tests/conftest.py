"""
Pytest fixtures shared across all test modules.
"""
import pytest

from app import create_app
from app.config import TestingConfig
from app.extensions import db as _db
from app.models import Trade, Position


# ---------------------------------------------------------------------------
# Application & database fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def app():
    """Create the Flask application configured for testing (in-memory SQLite)."""
    application = create_app(TestingConfig)
    with application.app_context():
        _db.create_all()
        yield application
        _db.drop_all()


@pytest.fixture(scope="function")
def db(app):
    """
    Yield the database object and roll back after every test so each test
    starts with a clean slate.
    """
    with app.app_context():
        yield _db
        _db.session.rollback()
        for table in reversed(_db.metadata.sorted_tables):
            _db.session.execute(table.delete())
        _db.session.commit()


@pytest.fixture(scope="function")
def client(app, db):
    """Flask test client with an active app context."""
    return app.test_client()


# ---------------------------------------------------------------------------
# Canonical sample data (mirrors the provided file examples)
# ---------------------------------------------------------------------------

TRADE_1_CSV = """\
TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17
2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17
2025-01-15,ACC002,GOOGL,75,142.80,BUY,2025-01-17
2025-01-15,ACC002,AAPL,200,185.50,BUY,2025-01-17
2025-01-15,ACC003,TSLA,150,238.45,SELL,2025-01-17
2025-01-15,ACC003,NVDA,80,505.30,BUY,2025-01-17
2025-01-15,ACC001,GOOGL,100,142.80,BUY,2025-01-17
2025-01-15,ACC004,AAPL,500,185.50,BUY,2025-01-17
2025-01-15,ACC004,MSFT,300,420.25,BUY,2025-01-17
2025-01-15,ACC002,NVDA,120,505.30,BUY,2025-01-17
"""

TRADE_2_PIPE = """\
REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM
20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A
20250115|ACC001|MSFT|50|21012.50|CUSTODIAN_A
20250115|ACC001|GOOGL|100|14280.00|CUSTODIAN_A
20250115|ACC002|GOOGL|75|10710.00|CUSTODIAN_B
20250115|ACC002|AAPL|200|37100.00|CUSTODIAN_B
20250115|ACC002|NVDA|120|60636.00|CUSTODIAN_B
20250115|ACC003|TSLA|-150|-35767.50|CUSTODIAN_A
20250115|ACC003|NVDA|80|40424.00|CUSTODIAN_A
20250115|ACC004|AAPL|500|92750.00|CUSTODIAN_C
20250115|ACC004|MSFT|300|126075.00|CUSTODIAN_C
"""

POSITIONS_YAML = """\
report_date: "2025-01-15"
positions:
  - account_id: ACC001
    holdings:
      - symbol: AAPL
        quantity: 100
        cost_basis_per_share: 185.50
        closing_price: 185.50
        currency: USD
      - symbol: MSFT
        quantity: 50
        cost_basis_per_share: 420.25
        closing_price: 420.25
        currency: USD
      - symbol: GOOGL
        quantity: 100
        cost_basis_per_share: 142.80
        closing_price: 142.80
        currency: USD
  - account_id: ACC002
    holdings:
      - symbol: GOOGL
        quantity: 75
        cost_basis_per_share: 142.80
        closing_price: 142.80
        currency: USD
      - symbol: AAPL
        quantity: 200
        cost_basis_per_share: 185.50
        closing_price: 185.50
        currency: USD
      - symbol: NVDA
        quantity: 120
        cost_basis_per_share: 505.30
        closing_price: 505.30
        currency: USD
  - account_id: ACC003
    holdings:
      - symbol: TSLA
        quantity: 0
        cost_basis_per_share: 238.45
        closing_price: 238.45
        currency: USD
      - symbol: NVDA
        quantity: 80
        cost_basis_per_share: 505.30
        closing_price: 505.30
        currency: USD
  - account_id: ACC004
    holdings:
      - symbol: AAPL
        quantity: 500
        cost_basis_per_share: 185.50
        closing_price: 185.50
        currency: USD
      - symbol: MSFT
        quantity: 300
        cost_basis_per_share: 420.25
        closing_price: 420.25
        currency: USD
"""


@pytest.fixture()
def seeded_db(db, app):
    """
    Populate the in-memory database with the canonical sample data so that
    endpoint tests have deterministic data to query.
    """
    from app.services.ingestion import (
        ingest_trades_format_1,
        ingest_trades_format_2,
        ingest_positions,
    )
    with app.app_context():
        ingest_trades_format_1(TRADE_1_CSV, "trades_format_1.csv")
        ingest_trades_format_2(TRADE_2_PIPE, "trades_format_2.txt")
        ingest_positions(POSITIONS_YAML, "positions.yaml")
    return db
