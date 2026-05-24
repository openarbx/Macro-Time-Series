# Macro FRED/ALFRED Scraper

Production-style scraper and ingestion pipeline for U.S. macroeconomic time series from FRED/ALFRED.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Data Models](#data-models)
- [Storage](#storage)
- [Logging](#logging)
- [Data Readers](#data-readers)
- [Error Handling & Safety](#error-handling--safety)
- [Project Structure](#project-structure)

---

## Overview

`main.py` is a robust, production-oriented Python pipeline that fetches macroeconomic data from the St. Louis Fed FRED/ALFRED APIs, validates it, and persists it to both local files and MongoDB.

### Targets

| Source | Data | API |
|--------|------|-----|
| FRED | Latest-vintage macro time series | [FRED API](https://fred.stlouisfed.org/docs/api/fred/) |
| ALFRED | Historical revision/vintage panels | [ALFRED API](https://alfred.stlouisfed.org/) |

### Design Goals

- **Safe HTTP reads**: Exponential-backoff retries, timeouts, bounded pagination, rate limiting, custom user-agent.
- **Safe parsing**: Pydantic schema validation, date parsing, numeric coercion, missing-value handling.
- **Safe writes**: Atomic local file writes, MongoDB upserts, run-level audit records.
- **Safe logging**: Daily folder rotation, 5,000-line log file caps.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   CLI / main()  │────▶│ IngestionPipeline│────▶│  FredClient     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                        │
                              ▼                        ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │   MongoMacroStore│     │   CsvStore      │
                        │   (MongoDB)      │     │   (local CSV)   │
                        └─────────────────┘     └─────────────────┘
                              │
                              ▼
                        ┌─────────────────┐
                        │   DataReader    │
                        │ (read-back API) │
                        └─────────────────┘
```

### Core Classes

| Class | Responsibility |
|-------|-------------|
| `RotatingLineLogger` | JSON-structured logs with daily folders and 5,000-line rotation |
| `FileWriter` | Atomic file writes (bytes, JSON, CSV) with SHA-256 checksums |
| `SafeParser` | Defensive parsing of dates, floats, and DataFrame column checks |
| `FredClient` | FRED/ALFRED API adapter with pagination and retries |
| `DataCleaner` | Transforms raw API rows into validated DataFrames |
| `CsvStore` | Persists DataFrames to `data/raw/<source>/<category>/` |
| `MongoMacroStore` | MongoDB connection, index management, bulk upserts, run tracking |
| `MacroFredAlfredScraper` | Orchestrates fetch→validate→store workflow per series |
| `DataReader` | Safe downstream readers for MongoDB with shape validation |

---

## Installation

### Requirements

- Python 3.10+
- MongoDB (optional; pipeline works locally without it, but expects connection)
- FRED API key (required)

### Dependencies

```bash
pip install -r requirements.txt
```

---

## Configuration

Create a `.env` file in the project root:

```dotenv
# MongoDB (optional — defaults to localhost)
MONGO_URI=mongodb://localhost:27017
MONGO_DB=macro_research

# FRED API key (required — get one at https://fred.stlouisfed.org/docs/api/api_key.html)
FRED_API_KEY=your_fred_api_key_here
```

Series are defined in `macro_series.yml` at the project root.

---

## Usage

### Basic Run

```bash
python main.py --mode all --vintage-stride 1
```

### Latest FRED data only

```bash
python main.py --mode latest
```

### Limit to one category

```bash
python main.py --mode all --category labor
```

### Use explicit vintage dates

```bash
python main.py --mode all --vintage-dates 2007-12-01,2008-09-15,2020-03-01
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `macro_series.yml` | Series registry YAML path |
| `--mode` | `all` | `latest`, `alfred`, or `all` |
| `--category` | `None` | Filter to a single category |
| `--observation-start` | `1776-07-04` | Start date for observations |
| `--observation-end` | `9999-12-31` | End date for observations |
| `--vintage-dates` | `None` | Comma-separated YYYY-MM-DD vintage dates |
| `--vintage-stride` | `1` | Use every nth vintage date |
| `--sleep` | `0.15` | Seconds to sleep between API requests |

---

## Data Sources

### FRED — Latest Vintage

Fetched series include CPI, core CPI, PCE inflation, unemployment, payrolls, initial claims, industrial production, capacity utilization, retail sales, credit spreads, money supply, and financial conditions indices.

### ALFRED — Historical Revisions

When `alfred: true` in `macro_series.yml`, the pipeline downloads every vintage date (or every nth with `--vintage-stride`) and stores each revision window. This avoids look-ahead bias in backtests.

**Note**: `vintage-stride=1` fetches every vintage date. This is clean but can be slow.

---

## Data Models

All data is validated with **Pydantic v2** before storage.

### `MacroObservation`

| Field | Type | Notes |
|-------|------|-------|
| `series_id` | `str` | e.g. `CPIAUCSL` |
| `series_name` | `str` | Human-readable label |
| `category` | `str` | e.g. `inflation` |
| `date` | `date` | Observation date |
| `value` | `Optional[float]` | `None` for missing values |
| `realtime_start` | `Optional[date]` | Vintage window start |
| `realtime_end` | `Optional[date]` | Vintage window end |
| `units` | `str` | FRED transformation code |
| `frequency_requested` | `Optional[str]` | e.g. `m`, `w`, `d` |
| `source` | `str` | `fred` or `alfred` |
| `observation_key` | `str` | SHA-256 hash for idempotent upserts |
| `ingested_at_utc` | `datetime` | Ingestion timestamp |

---

## Storage

### Local Files

| Directory | Content |
|-----------|---------|
| `data/raw/fred/<category>/` | Cleaned CSVs for latest observations |
| `data/raw/alfred/<category>/` | Cleaned CSVs for vintage panels |
| `data/processed/` | Reserved for aggregated outputs |
| `logs/YYYY-MM-DD/` | Daily log folders with `log-0001.log`, `log-0002.log`, ... |

All local writes are **atomic** (written to `.tmp` then renamed) to prevent partial files on crashes.

### MongoDB Collections

| Collection | Index | Purpose |
|------------|-------|---------|
| `macro_observations_latest` | `{series_id: 1, date: 1, realtime_start: 1}` (unique) | Latest FRED observations |
| `macro_observations_vintage` | `{series_id: 1, date: 1, realtime_start: 1, realtime_end: 1}` (unique) | ALFRED vintage panels |
| `macro_series_metadata` | `{series_id: 1}` (unique) | Series metadata |
| `macro_ingestion_runs` | `{run_id: 1}` (unique) | Pipeline run audit records |

Dates are stored as ISO strings in MongoDB for predictable querying across drivers.

---

## Logging

The pipeline uses a custom `RotatingLineLogger` that writes **JSON Lines** (one JSON object per line):

```json
{"ts": "2026-05-24T20:53:24.836816+00:00", "level": "INFO", "event": "ingestion_started", "run_id": "..."}
```

- **Daily folders**: `logs/2026-05-24/`
- **5,000-line rotation**: `log-0001.log`, `log-0002.log`, ...
- **Auto-continuation**: If restarted, picks up the next file index.

Every pipeline run produces an audit record in MongoDB (`macro_ingestion_runs`) capturing:
- `run_id`, `started_at`, `ended_at`
- `status`: `success`, `partial_failure`, or `fatal`
- `records_inserted_or_matched`, `failure_count`
- `sources` list and any errors

---

## Data Readers

After ingestion, use `DataReader` to query the database safely:

```python
from main import MongoMacroStore, DataReader

mongo = MongoMacroStore("mongodb://localhost:27017", "macro_research")
reader = DataReader(mongo)

# Read latest series
cpi = reader.read_latest_series("CPIAUCSL", start="2020-01-01", end="2026-05-24")
print(cpi.head())

# Read vintage panel (point-in-time)
vintage_cpi = reader.read_vintage_series("CPIAUCSL", vintage_date="2020-03-15")
print(vintage_cpi.head())
```

All readers enforce:
- Column existence checks
- Date parsing and numeric coercion
- Deduplication (`keep="last"`)
- Sorted output

---

## Error Handling & Safety

| Layer | Safeguard |
|-------|-----------|
| **HTTP** | Exponential backoff with jitter (1–30s), 5 retries, rate limiting (0.15s between requests) |
| **Pagination** | Bounded offset/limit pagination with page logging |
| **Parsing** | Defensive date/float parsing; missing/invalid values skipped with warnings |
| **Validation** | Pydantic models reject invalid rows |
| **Storage** | Atomic file writes; MongoDB bulk upserts with `ordered=False` |
| **Resilience** | Per-series error isolation; one failure does not abort the entire pipeline |
| **MongoDB** | Connection timeout (5s), ping check, detailed error logging per operation |
| **Config** | YAML schema validation with detailed error messages |
| **Audit** | Every run recorded in `macro_ingestion_runs` with status and counts |

---

## Project Structure

```
Macro-Time-Series/
├── main.py              # Pipeline script
├── macro_series.yml     # Series registry
├── README.md            # This file
├── .env                 # Environment variables (not committed)
├── .gitignore           # Git ignore rules
├── LICENSE
├── requirements.txt
├── data/
│   ├── processed/       # Aggregated outputs
│   └── raw/             # Cleaned CSVs
│       ├── fred/
│       └── alfred/
└── logs/
    └── YYYY-MM-DD/      # Daily JSON log files
```

---

## License

See [LICENSE](./LICENSE).
