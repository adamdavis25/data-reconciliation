# Portfolio Data Clearinghouse

A simplified portfolio data reconciliation system built with Python / Flask.

The service ingests daily trade and position files from multiple sources, stores
them in a relational database, and exposes REST endpoints for querying positions,
detecting compliance violations, and reconciling trade activity against
broker-reported positions.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Database Schema](#database-schema)
4. [Setup & Installation](#setup--installation)
5. [Running the Application](#running-the-application)
6. [Data Ingestion](#data-ingestion)
7. [API Endpoints](#api-endpoints)
8. [Running the Tests](#running-the-tests)
9. [Sample Test Queries & Validation Notes](#sample-test-queries--validation-notes)
10. [Design Decisions](#design-decisions)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                   Flask Application                   │
│                                                        │
│  POST /ingest          ← ingestion service             │
│  GET  /positions       ← position query                │
│  GET  /compliance/concentration  ← 20 % rule check    │
│  GET  /reconciliation  ← trade vs position diff        │
└────────────────────────┬─────────────────────────────┘
                         │ SQLAlchemy ORM
                ┌────────▼────────┐
                │  SQLite (dev)   │
                │  (any RDBMS in  │
                │   production)   │
                └─────────────────┘
```

---

## Project Structure

```
portfolio-data-clearinghous/
├── app/
│   ├── __init__.py          # Application factory
│   ├── config.py            # Config classes (Config, TestingConfig)
│   ├── extensions.py        # SQLAlchemy instance
│   ├── models.py            # Trade, Position, IngestLog models
│   ├── routes/
│   │   ├── ingest.py        # POST /ingest
│   │   ├── positions.py     # GET  /positions
│   │   ├── compliance.py    # GET  /compliance/concentration
│   │   └── reconciliation.py# GET  /reconciliation
│   └── services/
│       └── ingestion.py     # Ingestion logic + quality checks
├── data/
│   └── samples/
│       ├── trades_format_a.csv   # Trade source A (CSV)
│       ├── trades_format_b.json  # Trade source B (JSON)
│       └── positions.csv         # Bank-broker position snapshot
├── scripts/
│   └── ingest_files.py      # Standalone CLI ingestion script
├── tests/
│   ├── conftest.py          # Fixtures (app, db, client, seeded_db)
│   ├── test_ingestion.py    # Unit tests for ingestion service
│   └── test_endpoints.py    # Integration tests for all endpoints
├── run.py                   # Development server entry point
└── requirements.txt
```

---

## Database Schema

### `trades` table

| Column         | Type          | Notes                                      |
|----------------|---------------|--------------------------------------------|
| id             | Integer PK    | Auto-increment                             |
| trade_id       | String(64)    | Source trade identifier                    |
| account_id     | String(32)    | Account reference                          |
| symbol         | String(16)    | Equity ticker                              |
| trade_date     | Date          |                                            |
| quantity       | Numeric(18,6) | Always positive                            |
| price          | Numeric(18,6) | Per-share price                            |
| side           | String(4)     | `BUY` or `SELL`                            |
| currency       | String(3)     | ISO currency code                          |
| gross_value    | Numeric(18,6) | `quantity × price`; negative for SELL      |
| source_format  | String(8)     | `A` (CSV) or `B` (JSON)                    |
| source_file    | String(256)   | Original filename                          |
| ingested_at    | DateTime      | UTC timestamp                              |

Unique constraint: `(trade_id, source_format)`

### `positions` table

| Column               | Type          | Notes                        |
|----------------------|---------------|------------------------------|
| id                   | Integer PK    | Auto-increment               |
| account_id           | String(32)    |                              |
| symbol               | String(16)    |                              |
| position_date        | Date          |                              |
| quantity             | Numeric(18,6) | ≥ 0                          |
| cost_basis_per_share | Numeric(18,6) |                              |
| closing_price        | Numeric(18,6) |                              |
| currency             | String(3)     |                              |
| total_cost_basis     | Numeric(18,6) | `quantity × cost_basis`      |
| market_value         | Numeric(18,6) | `quantity × closing_price`   |
| source_file          | String(256)   |                              |
| ingested_at          | DateTime      |                              |

Unique constraint: `(account_id, symbol, position_date)`

### `ingest_logs` table

Audit trail for every ingest run – row counts, errors, timestamps.

---

## Setup & Installation

### Prerequisites

- Python 3.11+
- pip

### Steps

```bash
# 1. Clone the repository
git clone <repo-url>
cd portfolio-data-clearinghous

# 2. Create and activate a virtual environment
python -m venv venv
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Running the Application

```bash
python run.py
```

The development server starts at **http://localhost:5000**.

The SQLite database file (`portfolio.db`) is created automatically in the
project root on first run.

---

## Data Ingestion

### Via the REST endpoint

```bash
# Ingest all three sample files in one request
curl -X POST http://localhost:5000/ingest \
     -F "files=@data/samples/trades_format_a.csv" \
     -F "files=@data/samples/trades_format_b.json" \
     -F "files=@data/samples/positions.csv"
```

### Via the standalone CLI script

```bash
python scripts/ingest_files.py data/samples/trades_format_a.csv \
                               data/samples/trades_format_b.json \
                               data/samples/positions.csv
```

### Supported file formats

| Format    | Extension | Description                                              |
|-----------|-----------|----------------------------------------------------------|
| Trade A   | `.csv`    | `trade_id, account_id, symbol, trade_date, quantity, price, side, currency` |
| Trade B   | `.json`   | `id, account, ticker, date, shares, unit_price, action, ccy` |
| Positions | `.csv`    | `account_id, symbol, position_date, quantity, cost_basis_per_share, closing_price, currency` |

The auto-detect logic inspects the file extension and header columns to
dispatch to the correct loader automatically.

### Quality checks performed

| Check                        | Action on failure |
|------------------------------|-------------------|
| Required columns present     | Reject entire file|
| Non-empty trade_id / account | Reject row        |
| Non-empty symbol             | Reject row        |
| Valid ISO date               | Reject row        |
| Quantity > 0 (trades)        | Reject row        |
| Quantity ≥ 0 (positions)     | Reject row        |
| Price > 0                    | Reject row        |
| Side is BUY or SELL          | Reject row        |
| Recognised currency code     | Warning only      |
| Future trade/position date   | Warning only      |
| Duplicate key                | Skip + count      |

---

## API Endpoints

### `POST /ingest`

Load one or more files and receive a data quality report.

**Request:** `multipart/form-data` with field `files` (repeatable).

**Response:**
```json
{
  "ingest_reports": [
    {
      "file_name": "trades_format_a.csv",
      "file_type": "trade_a",
      "rows_total": 12,
      "rows_accepted": 12,
      "rows_rejected": 0,
      "rows_duplicate": 0,
      "errors": [],
      "warnings": []
    }
  ]
}
```

---

### `GET /positions?account=ACC001&date=2026-01-15`

Returns all positions for an account on a given date, with cost basis,
market value, and a portfolio summary.

**Parameters:**

| Name    | Required | Description          |
|---------|----------|----------------------|
| account | Yes      | Account ID           |
| date    | Yes      | ISO date YYYY-MM-DD  |

**Response:**
```json
{
  "account_id": "ACC001",
  "position_date": "2026-01-15",
  "positions": [
    {
      "symbol": "AAPL",
      "quantity": 150.0,
      "cost_basis_per_share": 181.25,
      "closing_price": 183.0,
      "total_cost_basis": 27187.5,
      "market_value": 27450.0,
      "currency": "USD"
    }
  ],
  "summary": {
    "total_cost_basis": 123456.78,
    "total_market_value": 130000.00,
    "unrealised_pnl": 6543.22,
    "position_count": 7
  }
}
```

---

### `GET /compliance/concentration?date=2026-01-15`

Identifies any equity position exceeding **20 %** of the account's total
market value.

**Parameters:**

| Name | Required | Description         |
|------|----------|---------------------|
| date | Yes      | ISO date YYYY-MM-DD |

**Response:**
```json
{
  "date": "2026-01-15",
  "threshold_pct": 20.0,
  "breaches": [
    {
      "account_id": "ACC001",
      "symbol": "NVDA",
      "quantity": 75.0,
      "closing_price": 890.0,
      "market_value": 66750.0,
      "account_total_market_value": 200000.0,
      "concentration_pct": 33.375,
      "threshold_pct": 20.0,
      "excess_pct": 13.375
    }
  ],
  "accounts_checked": 3,
  "accounts_with_breaches": 1
}
```

A position is flagged when `market_value / account_total_market_value > 0.20`
(strictly greater than, so an exactly 20 % position is not a violation).

---

### `GET /reconciliation?date=2026-01-15`

Compares net trade activity against broker-reported positions.

**Parameters:**

| Name | Required | Description         |
|------|----------|---------------------|
| date | Yes      | ISO date YYYY-MM-DD |

**Response:**
```json
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
      "issue": "quantity_mismatch",
      "detail": "Net trade qty (100.0) differs from position qty (150.0) by +50.0..."
    }
  ],
  "matched_pairs": [ ... ]
}
```

**Discrepancy issue codes:**

| Code               | Meaning                                                  |
|--------------------|----------------------------------------------------------|
| `quantity_mismatch`| Net trade qty ≠ position qty (may be a prior-day carry)  |
| `missing_position` | Trades exist but no matching position record             |

Positions with no trade activity on the date are reported in `matched_pairs`
with `note: "no_trade_activity_on_date"`.

---

## Running the Tests

```bash
pytest -v
```

All tests use an **in-memory SQLite database** and are fully isolated –
each test function gets a clean database state via the `db` fixture.

Expected output:

```
tests/test_ingestion.py::TestIngestTradeFormatA::test_happy_path_all_rows_accepted PASSED
tests/test_ingestion.py::TestIngestTradeFormatA::test_trades_persisted_to_db PASSED
...
tests/test_endpoints.py::TestComplianceEndpoint::test_known_breach_detected PASSED
...
== N passed in X.XXs ==
```

---

## Sample Test Queries & Validation Notes

After ingesting the sample files, the following queries validate the
reconciliation and compliance logic.

### 1. Positions for ACC001 on 2026-01-15

```bash
curl "http://localhost:5000/positions?account=ACC001&date=2026-01-15"
```

Expected: 7 positions (AAPL, MSFT, NVDA, AMZN, GOOGL, TSLA, META).
TSLA quantity = 0 (closed position still reported by broker).

### 2. Compliance concentration check for 2026-01-15

```bash
curl "http://localhost:5000/compliance/concentration?date=2026-01-15"
```

With the sample data, NVDA in ACC001 (75 shares × $890 = $66,750) should
breach the 20 % threshold depending on the total portfolio value of that
account. The response lists each breach with exact concentration and excess
percentages.

### 3. Reconciliation for 2026-01-15

```bash
curl "http://localhost:5000/reconciliation?date=2026-01-15"
```

The sample data is designed so that:
- Positions where trades match net quantity appear in `matched_pairs`.
- Positions where the broker reports a different quantity (e.g. carry-over
  from prior day) appear in `discrepancies` with issue `quantity_mismatch`.
- Any symbol traded but absent from the position file appears with
  issue `missing_position`.

### 4. Data quality report (intentional bad rows in Format B)

The sample `trades_format_b.json` contains two deliberately invalid records:
- `B2011`: negative shares (`-10`) → rejected.
- `B2012`: empty ticker → rejected.

The ingest report will show `rows_rejected: 2` and list the specific errors.

---

## Design Decisions

- **Single `trades` table for both formats.** Both CSV and JSON trade sources
  are normalised into one table. The `source_format` column preserves
  provenance. The unique constraint on `(trade_id, source_format)` allows the
  same logical trade to exist in both sources without collision (useful for
  cross-source reconciliation).

- **Derived columns stored at ingest time.** `gross_value`, `total_cost_basis`,
  and `market_value` are computed once on write. This keeps query logic simple
  and avoids repeated floating-point arithmetic at read time.

- **Strict > 20 % threshold.** The compliance rule flags positions where
  concentration is *strictly greater than* 20 %. An exactly 20 % position is
  not a violation.

- **Reconciliation reports delta, not pass/fail.** Because positions reflect
  end-of-day holdings (including prior-day carry-overs), a delta between net
  daily trades and position quantity is expected and informational. The
  endpoint reports the delta and lets the consumer decide on materiality.

- **SQLite for development; any SQLAlchemy-supported RDBMS in production.**
  Set the `DATABASE_URL` environment variable to switch to PostgreSQL, MySQL,
  etc. without any code changes.
