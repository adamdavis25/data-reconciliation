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
│       ├── trades_format_1.csv   # Trade Format 1 (comma-delimited CSV)
│       ├── trades_format_2.txt   # Trade Format 2 (pipe-delimited)
│       └── positions.yaml        # Bank-broker position snapshot (YAML)
├── scripts/
│   └── ingest_files.py      # Standalone CLI ingestion script
├── tests/
│   ├── conftest.py          # Fixtures (app, db, client, seeded_db)
│   ├── test_ingestion.py    # Unit tests for ingestion service
│   └── test_endpoints.py    # Integration tests for all endpoints
├── .gitignore
├── run.py                   # Development server entry point
└── requirements.txt
```

---

## Database Schema

### `trades` table

| Column          | Type          | Notes                                        |
|-----------------|---------------|----------------------------------------------|
| id              | Integer PK    | Auto-increment                               |
| trade_id        | String(128)   | Synthesised key: `<format>-<basename>-<row>` |
| account_id      | String(32)    | Account reference                            |
| symbol          | String(16)    | Equity ticker                                |
| trade_date      | Date          |                                              |
| quantity        | Numeric(18,6) | Always positive                              |
| price           | Numeric(18,6) | Per-share price (nullable for Format 2)      |
| side            | String(4)     | `BUY` or `SELL`                              |
| currency        | String(3)     | ISO currency code                            |
| gross_value     | Numeric(18,6) | `quantity × price`; negative for SELL        |
| settlement_date | Date          | Format 1 only; nullable                      |
| source_system   | String(64)    | Format 2 only; nullable                      |
| source_format   | String(8)     | `1` (CSV) or `2` (pipe-delimited)            |
| source_file     | String(256)   | Original filename                            |
| ingested_at     | DateTime      | UTC timestamp                                |

### `positions` table

| Column               | Type          | Notes                        |
|----------------------|---------------|------------------------------|
| id                   | Integer PK    | Auto-increment               |
| account_id           | String(32)    |                              |
| symbol               | String(16)    |                              |
| position_date        | Date          |                              |
| quantity             | Numeric(18,6) | ≥ 0 (0 = closed position)    |
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
project root on first run.  This file is excluded from version control via
`.gitignore` — it is a runtime artefact, not source code.

---

## Data Ingestion

### Supported file formats

| Format     | Description                                                                                 |
|------------|---------------------------------------------------------------------------------------------|
| Trade 1    | Comma-delimited CSV. Columns: `TradeDate, AccountID, Ticker, Quantity, Price, TradeType, SettlementDate` |
| Trade 2    | Pipe-delimited flat file. Columns: `REPORT_DATE\|ACCOUNT_ID\|SECURITY_TICKER\|SHARES\|MARKET_VALUE\|SOURCE_SYSTEM`. Dates are `YYYYMMDD`; negative `SHARES` = SELL. |
| Positions  | YAML. Top-level keys: `report_date` and `positions` (list of account blocks, each with a `holdings` list). |

**No file-naming convention is required.** The ingestion service detects file
type purely from the file's content:

1. YAML with a top-level `positions` key → position file
2. Pipe-delimited first line containing `REPORT_DATE` or `SECURITY_TICKER` → Trade Format 2
3. Comma-delimited first line containing `TradeDate` / `TradeType` / `Ticker` → Trade Format 1

### Sample position YAML structure

```yaml
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
```

### Via the REST endpoint

```bash
# Ingest all three sample files in one request
curl -X POST http://localhost:5000/ingest \
     -F "files=@data/samples/trades_format_1.csv" \
     -F "files=@data/samples/trades_format_2.txt" \
     -F "files=@data/samples/positions.yaml"
```

### Via the standalone CLI script

```bash
python scripts/ingest_files.py data/samples/trades_format_1.csv \
                               data/samples/trades_format_2.txt \
                               data/samples/positions.yaml
```

### Quality checks performed

| Check                              | Action on failure  |
|------------------------------------|--------------------|
| Required columns / keys present    | Reject entire file |
| Non-empty account_id               | Reject row         |
| Non-empty symbol                   | Reject row         |
| Valid ISO date                     | Reject row         |
| Quantity > 0 (trades)              | Reject row         |
| Quantity ≥ 0 (positions)           | Reject row         |
| Price > 0                          | Reject row         |
| Side is BUY or SELL                | Reject row         |
| Recognised currency code           | Warning only       |
| Future trade/position date         | Warning only       |
| Duplicate key                      | Skip + count       |
| Valid YAML structure (positions)   | Reject entire file |

---

## API Endpoints

### `POST /ingest`

Load one or more files and receive a data quality report.

**Request:** `multipart/form-data` with field `files` (repeatable).  
File names and extensions are irrelevant — type is detected from content.

**Response:**
```json
{
  "ingest_reports": [
    {
      "file_name": "trades_format_1.csv",
      "file_type": "trade_1",
      "rows_total": 10,
      "rows_accepted": 10,
      "rows_rejected": 0,
      "rows_duplicate": 0,
      "errors": [],
      "warnings": []
    }
  ]
}
```

---

### `GET /positions?account=ACC001&date=2025-01-15`

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
  "position_date": "2025-01-15",
  "positions": [
    {
      "symbol": "AAPL",
      "quantity": 100.0,
      "cost_basis_per_share": 185.50,
      "closing_price": 185.50,
      "total_cost_basis": 18550.0,
      "market_value": 18550.0,
      "currency": "USD"
    }
  ],
  "summary": {
    "total_cost_basis": 53717.5,
    "total_market_value": 53717.5,
    "unrealised_pnl": 0.0,
    "position_count": 3
  }
}
```

---

### `GET /compliance/concentration?date=2025-01-15`

Identifies any equity position exceeding **20 %** of the account's total
market value.

**Parameters:**

| Name | Required | Description         |
|------|----------|---------------------|
| date | Yes      | ISO date YYYY-MM-DD |

**Response:**
```json
{
  "date": "2025-01-15",
  "threshold_pct": 20.0,
  "breaches": [
    {
      "account_id": "ACC004",
      "symbol": "AAPL",
      "quantity": 500.0,
      "closing_price": 185.50,
      "market_value": 92750.0,
      "account_total_market_value": 218825.0,
      "concentration_pct": 42.38,
      "threshold_pct": 20.0,
      "excess_pct": 22.38
    }
  ],
  "accounts_checked": 4,
  "accounts_with_breaches": 2
}
```

A position is flagged when `market_value / account_total_market_value > 0.20`
(strictly greater than — an exactly 20 % position is not a violation).

---

### `GET /reconciliation?date=2025-01-15`

Compares net trade activity against broker-reported positions.

**Parameters:**

| Name | Required | Description         |
|------|----------|---------------------|
| date | Yes      | ISO date YYYY-MM-DD |

**Response:**
```json
{
  "date": "2025-01-15",
  "summary": {
    "total_pairs_checked": 10,
    "matched": 9,
    "discrepancies": 1
  },
  "discrepancies": [
    {
      "account_id": "ACC003",
      "symbol": "TSLA",
      "net_trade_quantity": -150.0,
      "position_quantity": 0.0,
      "delta": 150.0,
      "issue": "quantity_mismatch",
      "detail": "..."
    }
  ],
  "matched_pairs": [ "..." ]
}
```

**Discrepancy issue codes:**

| Code                | Meaning                                                  |
|---------------------|----------------------------------------------------------|
| `quantity_mismatch` | Net trade qty ≠ position qty (may be a prior-day carry)  |
| `missing_position`  | Trades exist but no matching position record             |

---

## Running the Tests

```bash
pytest -v
```

All tests use an **in-memory SQLite database** and are fully isolated —
each test function gets a clean database state via the `db` fixture.

Expected output:

```
92 passed in ~1s
```

---

## Sample Test Queries & Validation Notes

After ingesting the sample files with the CLI script, the following queries
validate the reconciliation and compliance logic.

### 1. Load all sample files

```bash
python scripts/ingest_files.py data/samples/trades_format_1.csv \
                               data/samples/trades_format_2.txt \
                               data/samples/positions.yaml
```

### 2. Positions for ACC001 on 2025-01-15

```bash
curl "http://localhost:5000/positions?account=ACC001&date=2025-01-15"
```

Expected: 3 positions (AAPL, MSFT, GOOGL).

### 3. Compliance concentration check for 2025-01-15

```bash
curl "http://localhost:5000/compliance/concentration?date=2025-01-15"
```

With the sample data, ACC004 holds AAPL (≈ 42 %) and MSFT (≈ 58 %) —
both breach the 20 % threshold.

### 4. Reconciliation for 2025-01-15

```bash
curl "http://localhost:5000/reconciliation?date=2025-01-15"
```

The sample data is designed so that positions match net trade quantities.
ACC003/TSLA is a SELL in the trade file; the position file reports 0 shares
(closed position), which produces a `quantity_mismatch` discrepancy because
net trade quantity is negative (−150) while the broker reports 0.

---

## Design Decisions

- **Single `trades` table for both formats.** Both trade sources are
  normalised into one table. The `source_format` column preserves provenance.
  The unique constraint prevents exact duplicate ingestion while allowing the
  same symbol to appear across formats.

- **Position file is YAML.** The bank-broker delivers positions as a
  structured YAML document with nested account → holdings hierarchy. This
  is distinct from the flat delimited trade files and reflects a realistic
  difference in data sources.

- **Content-based file type detection; no naming convention assumed.**
  The ingestion service inspects file content (YAML structure, delimiter,
  header column names) to determine file type. File names and extensions
  are ignored, so operators can name files however they wish.

- **Derived columns stored at ingest time.** `gross_value`, `total_cost_basis`,
  and `market_value` are computed once on write. This keeps query logic simple
  and avoids repeated arithmetic at read time.

- **Strict > 20 % threshold.** The compliance rule flags positions where
  concentration is *strictly greater than* 20 %. An exactly 20 % position is
  not a violation.

- **Reconciliation reports delta, not pass/fail.** Because positions reflect
  end-of-day holdings (including prior-day carry-overs), a delta between net
  daily trades and position quantity is expected and informational. The
  endpoint reports the delta and lets the consumer decide on materiality.

- **`portfolio.db` excluded from version control.** The SQLite database is a
  runtime artefact generated on first run. It is listed in `.gitignore` and
  should never be committed. Use `python run.py` to create it locally.

- **SQLite for development; any SQLAlchemy-supported RDBMS in production.**
  Set the `DATABASE_URL` environment variable to switch to PostgreSQL, MySQL,
  etc. without any code changes.
