"""
macro_fred_alfred_scraper.py

Production-style scraper/ingestion pipeline for macroeconomic time series from FRED/ALFRED.

Targets:
- St. Louis Fed FRED API for latest-vintage observations.
- St. Louis Fed ALFRED API for historical revision/vintage panels.

Design goals:
- Safe HTTP reads: retries, timeouts, bounded pagination, rate limiting, user agent.
- Safe parsing: schema validation, date parsing, numeric coercion, missing-value handling.
- Safe writes: atomic local writes, MongoDB upserts, run-level audit records.
- Safe logging: daily folder, one log file per 5,000 lines.

Install:
    pip install requests pandas pymongo python-dotenv pyyaml pydantic tenacity

Environment variables:
    MONGO_URI=mongodb://localhost:27017
    MONGO_DB=macro_research
    FRED_API_KEY=your_fred_api_key

Run:
    python main.py --mode all --vintage-stride 1
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import (
    ConnectionFailure,
    ServerSelectionTimeoutError,
    BulkWriteError,
    OperationFailure,
    ConfigurationError,
)
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter


# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = BASE_DIR / "logs"

for folder in [RAW_DIR / "fred", RAW_DIR / "alfred", PROCESSED_DIR, LOG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


# ============================================================
# JSON-structured rotating logger
# ============================================================

class RotatingLineLogger:
    def __init__(self, root_dir: Path, max_lines: int = 5000) -> None:
        self.root_dir = root_dir
        self.max_lines = max_lines
        self.current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.date_dir = self.root_dir / self.current_date
        self.date_dir.mkdir(parents=True, exist_ok=True)
        self.file_index = self._next_file_index()
        self.line_count = 0
        self.file_path = self.date_dir / f"log-{self.file_index:04d}.log"

    def _next_file_index(self) -> int:
        existing = sorted(self.date_dir.glob("log-*.log"))
        if not existing:
            return 1
        last = existing[-1].stem.split("-")[-1]
        try:
            return int(last) + 1
        except ValueError:
            return 1

    def _roll_if_needed(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.current_date:
            self.current_date = today
            self.date_dir = self.root_dir / today
            self.date_dir.mkdir(parents=True, exist_ok=True)
            self.file_index = self._next_file_index()
            self.line_count = 0
            self.file_path = self.date_dir / f"log-{self.file_index:04d}.log"
            return

        if self.line_count >= self.max_lines:
            self.file_index += 1
            self.line_count = 0
            self.file_path = self.date_dir / f"log-{self.file_index:04d}.log"

    def log(self, level: str, event: str, **kwargs: Any) -> None:
        self._roll_if_needed()
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level.upper(),
            "event": event,
            **kwargs,
        }
        try:
            with self.file_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
            self.line_count += 1
        except Exception as exc:
            sys.stderr.write(f"LOGGING FAILURE: {exc}\n")

    def info(self, event: str, **kwargs: Any) -> None:
        self.log("INFO", event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self.log("WARNING", event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self.log("ERROR", event, **kwargs)

    def critical(self, event: str, **kwargs: Any) -> None:
        self.log("CRITICAL", event, **kwargs)


LOGGER = RotatingLineLogger(LOG_DIR)


# ============================================================
# Validation models
# ============================================================

class MacroObservation(BaseModel):
    series_id: str
    series_name: str
    category: str
    date: date
    value: Optional[float] = None
    realtime_start: Optional[date] = None
    realtime_end: Optional[date] = None
    units: str = "lin"
    frequency_requested: Optional[str] = None
    source: str
    ingested_at_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    observation_key: str

    @field_validator("value")
    @classmethod
    def check_finite(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and pd.isna(v):
            return None
        return v


class SeriesMetadataDoc(BaseModel):
    series_id: str
    configured_category: str
    configured_name: str
    configured_units: str
    configured_frequency: Optional[str]
    alfred_enabled: bool
    metadata_ingested_at_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IngestionRun(BaseModel):
    run_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    status: str = "running"
    records_inserted_or_matched: int = 0
    failure_count: int = 0
    sources: List[str] = Field(default_factory=list)
    args: Dict[str, Any] = Field(default_factory=dict)
    errors: List[Dict[str, Any]] = Field(default_factory=list)


# ============================================================
# Safe file writer
# ============================================================

class FileWriter:
    @staticmethod
    def checksum_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def atomic_write_bytes(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        tmp.write_bytes(data)
        tmp.replace(path)

    @staticmethod
    def atomic_write_json(path: Path, payload: Any) -> None:
        data = json.dumps(payload, default=str, ensure_ascii=False, indent=2).encode("utf-8")
        FileWriter.atomic_write_bytes(path, data)

    @staticmethod
    def write_dataframe_csv(path: Path, df: pd.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        df.to_csv(tmp, index=False, quoting=csv.QUOTE_MINIMAL)
        tmp.replace(path)


# ============================================================
# Safe parser
# ============================================================

class SafeParser:
    @staticmethod
    def parse_date(value: Any) -> Optional[date]:
        if value is None or pd.isna(value):
            return None
        try:
            return pd.to_datetime(value, errors="raise").date()
        except Exception:
            return None

    @staticmethod
    def parse_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if value in {"", ".", "NA", "N/A", "null", "None"}:
                return None
            value = value.replace(",", "")
        try:
            f = float(value)
        except Exception:
            return None
        if pd.isna(f):
            return None
        return f

    @staticmethod
    def require_columns(df: pd.DataFrame, required: Sequence[str], context: str) -> None:
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"{context}: missing required columns: {missing}; actual={list(df.columns)}")


# ============================================================
# Config dataclasses
# ============================================================

@dataclass(frozen=True)
class SeriesSpec:
    series_id: str
    category: str
    name: str
    units: str = "lin"
    frequency: Optional[str] = None
    alfred: bool = True


@dataclass
class ScrapeConfig:
    api_key: str
    mongo_uri: str
    mongo_db: str
    observation_start: str
    observation_end: str
    request_sleep_seconds: float = 0.15


class MacroSeriesRegistry:
    def __init__(self, config_path: Path):
        self.config_path = config_path

    def load(self) -> List[SeriesSpec]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML in config file: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to read config file: {exc}") from exc

        if not isinstance(raw, dict) or "series" not in raw:
            raise ValueError("Config file must contain a 'series' key at top level")

        specs = []
        for sid, item in raw["series"].items():
            if not isinstance(item, dict):
                LOGGER.warning("config_invalid_series_item", series_id=sid, item_type=type(item).__name__)
                continue
            try:
                specs.append(
                    SeriesSpec(
                        series_id=sid,
                        category=item["category"],
                        name=item["name"],
                        units=item.get("units", "lin"),
                        frequency=item.get("frequency"),
                        alfred=bool(item.get("alfred", True)),
                    )
                )
            except Exception as exc:
                LOGGER.error("config_series_parse_failed", series_id=sid, error=str(exc))
                raise ValueError(f"Failed to parse series '{sid}': {exc}") from exc
        return specs


# ============================================================
# FRED API client
# ============================================================

class FredApiError(RuntimeError):
    pass


class FredClient:
    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, api_key: str, sleep_seconds: float = 0.15):
        self.api_key = api_key
        self.sleep_seconds = sleep_seconds
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "macro-fred-alfred-scraper/1.0 (contact: local-research)",
            "Accept": "application/json",
        })

    @retry(
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError, requests.HTTPError, FredApiError)),
        wait=wait_exponential_jitter(initial=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"  # type: ignore[union-attr]
        request_params = {**params, "api_key": self.api_key, "file_type": "json"}

        redacted = dict(request_params)
        redacted["api_key"] = "***"
        LOGGER.info("fred_api_request", endpoint=endpoint, params=redacted)

        try:
            response = self.session.get(url, params=request_params, timeout=45)
        except requests.Timeout as exc:
            LOGGER.error("fred_api_timeout", endpoint=endpoint, params=redacted)
            raise
        except requests.ConnectionError as exc:
            LOGGER.error("fred_api_connection_error", endpoint=endpoint, error=str(exc))
            raise

        time.sleep(self.sleep_seconds)

        if response.status_code in {429, 500, 502, 503, 504}:
            LOGGER.warning("fred_api_retryable_status", endpoint=endpoint, status_code=response.status_code)
            response.raise_for_status()

        if response.status_code >= 400:
            LOGGER.error("fred_api_bad_status", endpoint=endpoint, status_code=response.status_code, text=response.text[:500])
            raise FredApiError(f"HTTP {response.status_code}: {response.text[:1000]}")

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            LOGGER.error("fred_api_invalid_json", endpoint=endpoint, text=response.text[:500])
            raise FredApiError(f"Invalid JSON response: {response.text[:500]}") from exc

        if "error_code" in payload:
            LOGGER.error("fred_api_error", endpoint=endpoint, error_code=payload.get("error_code"), error_message=payload.get("error_message"))
            raise FredApiError(f"FRED error {payload.get('error_code')}: {payload.get('error_message')}")

        return payload

    def get_series_metadata(self, series_id: str) -> Dict[str, Any]:
        payload = self._get("series", {"series_id": series_id})
        seriess = payload.get("seriess", [])
        if not seriess:
            raise FredApiError(f"No metadata returned for {series_id}")
        return seriess[0]

    def get_observations(
        self,
        series_id: str,
        observation_start: str,
        observation_end: str,
        units: str = "lin",
        frequency: Optional[str] = None,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None,
        vintage_dates: Optional[Sequence[str]] = None,
        output_type: int = 1,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "series_id": series_id,
            "observation_start": observation_start,
            "observation_end": observation_end,
            "units": units,
            "limit": 100000,
            "offset": 0,
            "sort_order": "asc",
            "output_type": output_type,
        }
        if frequency:
            params["frequency"] = frequency
        if realtime_start:
            params["realtime_start"] = realtime_start
        if realtime_end:
            params["realtime_end"] = realtime_end
        if vintage_dates:
            params["vintage_dates"] = ",".join(vintage_dates)

        all_rows: List[Dict[str, Any]] = []
        page = 1
        while True:
            try:
                payload = self._get("series/observations", params)
            except Exception as exc:
                LOGGER.error("fred_observations_page_failed", series_id=series_id, page=page, error=str(exc))
                raise

            rows = payload.get("observations", [])
            if not isinstance(rows, list):
                raise FredApiError(f"FRED observations is not a list for {series_id}")

            all_rows.extend(rows)

            count = int(payload.get("count", len(rows)))
            offset = int(payload.get("offset", params["offset"]))
            limit = int(payload.get("limit", params["limit"]))

            LOGGER.info("fred_observations_page", series_id=series_id, page=page, count=count, offset=offset, rows_fetched=len(rows))

            if offset + limit >= count:
                break
            params["offset"] = offset + limit
            page += 1

        return all_rows

    def get_vintage_dates(
        self,
        series_id: str,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None,
    ) -> List[str]:
        params: Dict[str, Any] = {
            "series_id": series_id,
            "limit": 100000,
            "offset": 0,
            "sort_order": "asc",
        }
        if realtime_start:
            params["realtime_start"] = realtime_start
        if realtime_end:
            params["realtime_end"] = realtime_end

        out: List[str] = []
        page = 1
        while True:
            try:
                payload = self._get("series/vintagedates", params)
            except Exception as exc:
                LOGGER.error("fred_vintagedates_page_failed", series_id=series_id, page=page, error=str(exc))
                raise

            rows = payload.get("vintage_dates", [])
            if not isinstance(rows, list):
                raise FredApiError(f"FRED vintage_dates is not a list for {series_id}")
            out.extend(rows)

            count = int(payload.get("count", len(rows)))
            offset = int(payload.get("offset", params["offset"]))
            limit = int(payload.get("limit", params["limit"]))

            LOGGER.info("fred_vintagedates_page", series_id=series_id, page=page, count=count, offset=offset, dates_fetched=len(rows))

            if offset + limit >= count:
                break
            params["offset"] = offset + limit
            page += 1

        return out


# ============================================================
# Data cleaner
# ============================================================

class DataCleaner:
    @staticmethod
    def clean_observations(
        rows: List[Dict[str, Any]],
        spec: SeriesSpec,
        source: str,
        ingestion_time: datetime,
    ) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()

        try:
            df = pd.DataFrame(rows)
        except Exception as exc:
            LOGGER.error("dataframe_creation_failed", series_id=spec.series_id, source=source, error=str(exc))
            raise ValueError(f"Failed to create DataFrame: {exc}") from exc

        SafeParser.require_columns(df, ["date", "value"], f"observations for {spec.series_id}")

        df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["realtime_start"] = pd.to_datetime(df.get("realtime_start", pd.NaT), errors="coerce").dt.date
        df["realtime_end"] = pd.to_datetime(df.get("realtime_end", pd.NaT), errors="coerce").dt.date

        df["series_id"] = spec.series_id
        df["series_name"] = spec.name
        df["category"] = spec.category
        df["units"] = spec.units
        df["frequency_requested"] = spec.frequency
        df["source"] = source
        df["ingested_at_utc"] = ingestion_time.isoformat()

        def _make_key(row: pd.Series) -> str:
            rs = row.get("realtime_start")
            re = row.get("realtime_end")
            rs_str = str(rs) if pd.notna(rs) else ""
            re_str = str(re) if pd.notna(re) else ""
            return hashlib.sha256(
                f"{row['series_id']}|{row['date']}|{rs_str}|{re_str}|{row['source']}".encode()
            ).hexdigest()

        df["observation_key"] = df.apply(_make_key, axis=1)

        valid_records = []
        rejected = 0
        for _, row in df.iterrows():
            try:
                rec = MacroObservation(
                    series_id=str(row["series_id"]),
                    series_name=str(row["series_name"]),
                    category=str(row["category"]),
                    date=row["date"],
                    value=None if pd.isna(row["value"]) else float(row["value"]),
                    realtime_start=row["realtime_start"] if pd.notna(row["realtime_start"]) else None,
                    realtime_end=row["realtime_end"] if pd.notna(row["realtime_end"]) else None,
                    units=str(row["units"]),
                    frequency_requested=str(row["frequency_requested"]) if pd.notna(row["frequency_requested"]) else None,
                    source=str(row["source"]),
                    observation_key=str(row["observation_key"]),
                )
                valid_records.append(rec.model_dump())
            except ValidationError as exc:
                rejected += 1
                LOGGER.warning("observation_validation_rejected", series_id=spec.series_id, date=str(row.get("date")), error=str(exc))

        LOGGER.info("observations_cleaned", series_id=spec.series_id, source=source, valid=len(valid_records), rejected=rejected)
        return pd.DataFrame(valid_records)

    @staticmethod
    def to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []
        out = []
        for rec in df.to_dict("records"):
            for k in ["date", "realtime_start", "realtime_end"]:
                if rec.get(k) is not None and not pd.isna(rec.get(k)):
                    rec[k] = str(rec[k])
            if pd.isna(rec.get("value")):
                rec["value"] = None
            out.append(rec)
        return out


# ============================================================
# CSV store
# ============================================================

class CsvStore:
    def __init__(self, data_root: Path):
        self.data_root = data_root

    def save(self, df: pd.DataFrame, source: str, category: str, file_name: str) -> Optional[Path]:
        if df.empty:
            LOGGER.warning("csv_save_skipped_empty", source=source, category=category, file_name=file_name)
            return None
        out_dir = self.data_root / "raw" / source / category
        out_path = out_dir / file_name
        try:
            FileWriter.write_dataframe_csv(out_path, df)
            LOGGER.info("csv_saved", path=str(out_path), rows=len(df))
            return out_path
        except Exception as exc:
            LOGGER.error("csv_save_failed", path=str(out_path), error=str(exc))
            raise


# ============================================================
# MongoDB store
# ============================================================

class MongoMacroStore:
    def __init__(self, mongo_uri: str, db_name: str):
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            self.db = self.client[db_name]
            self.latest: Collection = self.db["macro_observations_latest"]
            self.vintage: Collection = self.db["macro_observations_vintage"]
            self.metadata: Collection = self.db["macro_series_metadata"]
            self.runs: Collection = self.db["macro_ingestion_runs"]
            self._ensure_indexes()
            LOGGER.info("mongodb_connected", uri=mongo_uri, db=db_name)
        except ConnectionFailure as exc:
            LOGGER.critical("mongodb_connection_failed", uri=mongo_uri, error=str(exc))
            raise
        except ServerSelectionTimeoutError as exc:
            LOGGER.critical("mongodb_server_selection_timeout", uri=mongo_uri, error=str(exc))
            raise
        except ConfigurationError as exc:
            LOGGER.critical("mongodb_configuration_error", uri=mongo_uri, error=str(exc))
            raise
        except Exception as exc:
            LOGGER.critical("mongodb_unknown_error", uri=mongo_uri, error=str(exc))
            raise

    def _ensure_indexes(self) -> None:
        try:
            self.latest.create_index(
                [("series_id", ASCENDING), ("date", ASCENDING), ("realtime_start", ASCENDING)],
                unique=True,
                name="uniq_latest_series_date_realtime",
            )
            self.vintage.create_index(
                [
                    ("series_id", ASCENDING),
                    ("date", ASCENDING),
                    ("realtime_start", ASCENDING),
                    ("realtime_end", ASCENDING),
                ],
                unique=True,
                name="uniq_vintage_series_date_realtime_window",
            )
            self.vintage.create_index(
                [("series_id", ASCENDING), ("realtime_start", ASCENDING), ("date", ASCENDING)],
                name="query_point_in_time_panel",
            )
            self.metadata.create_index([("series_id", ASCENDING)], unique=True, name="uniq_series_metadata")
            self.runs.create_index([("run_id", ASCENDING)], unique=True, name="uniq_run_id")
        except OperationFailure as exc:
            LOGGER.error("mongodb_index_creation_failed", error=str(exc))
            raise

    def upsert_metadata(self, series_id: str, metadata: Dict[str, Any], spec: SeriesSpec) -> None:
        doc = {
            **metadata,
            "series_id": series_id,
            "configured_category": spec.category,
            "configured_name": spec.name,
            "configured_units": spec.units,
            "configured_frequency": spec.frequency,
            "alfred_enabled": spec.alfred,
            "metadata_ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.metadata.update_one({"series_id": series_id}, {"$set": doc}, upsert=True)
            LOGGER.info("mongo_metadata_upserted", series_id=series_id)
        except Exception as exc:
            LOGGER.error("mongo_metadata_upsert_failed", series_id=series_id, error=str(exc))
            raise

    def upsert_observations(self, df: pd.DataFrame, vintage: bool) -> int:
        records = DataCleaner.to_records(df)
        if not records:
            return 0

        coll = self.vintage if vintage else self.latest
        ops = []
        for rec in records:
            key = {
                "series_id": rec["series_id"],
                "date": rec["date"],
                "realtime_start": rec["realtime_start"],
            }
            if vintage:
                key["realtime_end"] = rec["realtime_end"]
            ops.append(UpdateOne(key, {"$set": rec}, upsert=True))

        try:
            result = coll.bulk_write(ops, ordered=False)
            changed = result.upserted_count + result.modified_count
            LOGGER.info("mongo_observations_upserted", collection=coll.name, rows=len(records), changed=changed)
            return changed
        except BulkWriteError as exc:
            LOGGER.error("mongo_bulk_write_error", collection=coll.name, error=str(exc), details=exc.details)
            raise
        except Exception as exc:
            LOGGER.error("mongo_observations_upsert_failed", collection=coll.name, rows=len(records), error=str(exc))
            raise

    def start_run(self, run_id: str, args: Dict[str, Any]) -> None:
        doc = IngestionRun(
            run_id=run_id,
            started_at=datetime.now(timezone.utc),
            args=args,
        ).model_dump()
        try:
            self.runs.update_one({"run_id": run_id}, {"$set": doc}, upsert=True)
            LOGGER.info("mongo_run_started", run_id=run_id)
        except Exception as exc:
            LOGGER.error("mongo_run_start_failed", run_id=run_id, error=str(exc))
            raise

    def finish_run(self, run_id: str, status: str, stats: Dict[str, Any]) -> None:
        try:
            self.runs.update_one(
                {"run_id": run_id},
                {
                    "$set": {
                        "ended_at": datetime.now(timezone.utc),
                        "status": status,
                        "records_inserted_or_matched": stats.get("records_inserted_or_matched", 0),
                        "failure_count": stats.get("failure_count", 0),
                        "sources": stats.get("sources", []),
                        "errors": stats.get("errors", []),
                    }
                },
                upsert=True,
            )
            LOGGER.info("mongo_run_finished", run_id=run_id, status=status)
        except Exception as exc:
            LOGGER.error("mongo_run_finish_failed", run_id=run_id, error=str(exc))
            raise


# ============================================================
# Main scraper
# ============================================================

class MacroFredAlfredScraper:
    def __init__(
        self,
        config: ScrapeConfig,
        specs: List[SeriesSpec],
        client: FredClient,
        csv_store: CsvStore,
        mongo_store: MongoMacroStore,
    ):
        self.config = config
        self.specs = specs
        self.client = client
        self.csv_store = csv_store
        self.mongo = mongo_store

    def scrape_latest(self, spec: SeriesSpec) -> int:
        ingestion_time = datetime.now(timezone.utc)
        try:
            metadata = self.client.get_series_metadata(spec.series_id)
            self.mongo.upsert_metadata(spec.series_id, metadata, spec)
        except Exception as exc:
            LOGGER.error("latest_metadata_failed", series_id=spec.series_id, error=str(exc))
            raise

        try:
            rows = self.client.get_observations(
                series_id=spec.series_id,
                observation_start=self.config.observation_start,
                observation_end=self.config.observation_end,
                units=spec.units,
                frequency=spec.frequency,
                output_type=1,
            )
        except Exception as exc:
            LOGGER.error("latest_observations_failed", series_id=spec.series_id, error=str(exc))
            raise

        try:
            df = DataCleaner.clean_observations(rows, spec, source="fred", ingestion_time=ingestion_time)
            self.csv_store.save(df, "fred", spec.category, f"{spec.series_id}.csv")
            self.mongo.upsert_observations(df, vintage=False)
            return len(df)
        except Exception as exc:
            LOGGER.error("latest_process_failed", series_id=spec.series_id, error=str(exc))
            raise

    def scrape_alfred_vintages(
        self,
        spec: SeriesSpec,
        explicit_vintage_dates: Optional[List[str]] = None,
        vintage_stride: int = 1,
    ) -> int:
        if not spec.alfred:
            LOGGER.info("alfred_skipped", series_id=spec.series_id, reason="alfred=false")
            return 0

        ingestion_time = datetime.now(timezone.utc)

        try:
            if explicit_vintage_dates:
                vintage_dates = explicit_vintage_dates
            else:
                all_vintage_dates = self.client.get_vintage_dates(spec.series_id)
                vintage_stride = max(1, vintage_stride)
                vintage_dates = all_vintage_dates[::vintage_stride]
        except Exception as exc:
            LOGGER.error("alfred_vintage_dates_failed", series_id=spec.series_id, error=str(exc))
            raise

        if not vintage_dates:
            LOGGER.warning("alfred_no_vintage_dates", series_id=spec.series_id)
            return 0

        chunks = [vintage_dates[i:i + 1000] for i in range(0, len(vintage_dates), 1000)]
        frames = []
        total_valid = 0

        for idx, chunk in enumerate(chunks, start=1):
            LOGGER.info("alfred_chunk_start", series_id=spec.series_id, chunk=idx, total_chunks=len(chunks), dates=len(chunk))
            try:
                rows = self.client.get_observations(
                    series_id=spec.series_id,
                    observation_start=self.config.observation_start,
                    observation_end=self.config.observation_end,
                    units=spec.units,
                    frequency=spec.frequency,
                    vintage_dates=chunk,
                    output_type=2,
                )
                df_chunk = DataCleaner.clean_observations(
                    rows, spec, source="alfred", ingestion_time=ingestion_time
                )
                frames.append(df_chunk)
                inserted = self.mongo.upsert_observations(df_chunk, vintage=True)
                total_valid += inserted
            except Exception as exc:
                LOGGER.error("alfred_chunk_failed", series_id=spec.series_id, chunk=idx, error=str(exc))
                raise

        try:
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            if not df.empty:
                df = df.drop_duplicates(
                    subset=["series_id", "date", "realtime_start", "realtime_end", "source"]
                )
            self.csv_store.save(df, "alfred", spec.category, f"{spec.series_id}_vintages.csv")
            return len(df)
        except Exception as exc:
            LOGGER.error("alfred_process_failed", series_id=spec.series_id, error=str(exc))
            raise

    def run(
        self,
        mode: str,
        category: Optional[str] = None,
        explicit_vintage_dates: Optional[List[str]] = None,
        vintage_stride: int = 1,
    ) -> Dict[str, Any]:
        filtered = [s for s in self.specs if category is None or s.category == category]
        stats = {
            "series_attempted": len(filtered),
            "latest_rows": 0,
            "vintage_rows": 0,
            "records_inserted_or_matched": 0,
            "failure_count": 0,
            "sources": [],
            "errors": [],
        }

        for spec in filtered:
            try:
                LOGGER.info("series_start", series_id=spec.series_id, category=spec.category, mode=mode)

                if mode in {"latest", "all"}:
                    try:
                        rows = self.scrape_latest(spec)
                        stats["latest_rows"] += rows
                        stats["records_inserted_or_matched"] += rows
                        if "fred" not in stats["sources"]:
                            stats["sources"].append("fred")
                    except Exception as exc:
                        stats["failure_count"] += 1
                        stats["errors"].append({"series_id": spec.series_id, "phase": "latest", "error": str(exc)})
                        LOGGER.error("series_latest_failed", series_id=spec.series_id, error=str(exc))
                        continue

                if mode in {"alfred", "all"}:
                    try:
                        rows = self.scrape_alfred_vintages(
                            spec,
                            explicit_vintage_dates=explicit_vintage_dates,
                            vintage_stride=vintage_stride,
                        )
                        stats["vintage_rows"] += rows
                        stats["records_inserted_or_matched"] += rows
                        if "alfred" not in stats["sources"]:
                            stats["sources"].append("alfred")
                    except Exception as exc:
                        stats["failure_count"] += 1
                        stats["errors"].append({"series_id": spec.series_id, "phase": "alfred", "error": str(exc)})
                        LOGGER.error("series_alfred_failed", series_id=spec.series_id, error=str(exc))
                        continue

                LOGGER.info("series_done", series_id=spec.series_id)
            except Exception as exc:
                stats["failure_count"] += 1
                stats["errors"].append({"series_id": spec.series_id, "phase": "unknown", "error": str(exc)})
                LOGGER.error("series_fatal", series_id=spec.series_id, error=str(exc))

        return stats


# ============================================================
# Data reader
# ============================================================

class DataReader:
    def __init__(self, mongo_store: MongoMacroStore):
        self.mongo = mongo_store

    def read_latest_series(self, series_id: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        query: Dict[str, Any] = {"series_id": series_id}
        if start or end:
            date_filter: Dict[str, str] = {}
            if start:
                date_filter["$gte"] = start
            if end:
                date_filter["$lte"] = end
            query["date"] = date_filter
        try:
            rows = list(self.mongo.latest.find(query, {"_id": 0}).sort("date", 1))
        except Exception as exc:
            LOGGER.error("read_latest_series_query_failed", series_id=series_id, error=str(exc))
            raise

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["date", "value"])

        SafeParser.require_columns(df, ["date", "value", "series_id"], f"latest series {series_id}")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date")
        df = df.drop_duplicates(subset=["date"], keep="last")
        return df[["date", "value", "series_id", "source", "units", "category"]]

    def read_vintage_series(self, series_id: str, vintage_date: Optional[str] = None) -> pd.DataFrame:
        query: Dict[str, Any] = {"series_id": series_id}
        if vintage_date:
            query["realtime_start"] = {"$lte": vintage_date}
            query["realtime_end"] = {"$gte": vintage_date}
        try:
            rows = list(self.mongo.vintage.find(query, {"_id": 0}).sort("date", 1))
        except Exception as exc:
            LOGGER.error("read_vintage_series_query_failed", series_id=series_id, error=str(exc))
            raise

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["date", "value", "realtime_start", "realtime_end"])

        SafeParser.require_columns(df, ["date", "value", "series_id", "realtime_start", "realtime_end"], f"vintage series {series_id}")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["realtime_start"] = pd.to_datetime(df["realtime_start"], errors="coerce")
        df["realtime_end"] = pd.to_datetime(df["realtime_end"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values(["date", "realtime_start"])
        return df[["date", "value", "series_id", "realtime_start", "realtime_end", "source", "units", "category"]]


# ============================================================
# CLI
# ============================================================

def valid_date_string(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD")
    return value


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Scrape macro data from FRED/ALFRED into CSV and MongoDB.")
    parser.add_argument("--config", default="macro_series.yml")
    parser.add_argument("--mode", choices=["latest", "alfred", "all"], default="all")
    parser.add_argument("--category", default=None)
    parser.add_argument("--observation-start", default="1776-07-04")
    parser.add_argument("--observation-end", default="9999-12-31")
    parser.add_argument("--vintage-dates", default=None, help="Comma-separated YYYY-MM-DD dates.")
    parser.add_argument("--vintage-stride", type=int, default=1, help="Use every nth vintage date.")
    parser.add_argument("--sleep", type=float, default=0.15)
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
    LOGGER.info("process_start", run_id=run_id, args=vars(args))

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        LOGGER.critical("missing_fred_api_key")
        print("Missing FRED_API_KEY. Put it in .env or export it.", file=sys.stderr)
        return 1

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "macro_research")

    try:
        config = ScrapeConfig(
            api_key=api_key,
            mongo_uri=mongo_uri,
            mongo_db=mongo_db,
            observation_start=args.observation_start,
            observation_end=args.observation_end,
            request_sleep_seconds=args.sleep,
        )
    except Exception as exc:
        LOGGER.critical("config_build_failed", error=str(exc))
        return 1

    config_path = BASE_DIR / args.config
    try:
        registry = MacroSeriesRegistry(config_path)
        specs = registry.load()
    except FileNotFoundError as exc:
        LOGGER.critical("config_file_not_found", path=str(config_path), error=str(exc))
        return 1
    except (ValueError, RuntimeError) as exc:
        LOGGER.critical("config_load_failed", path=str(config_path), error=str(exc))
        return 1
    except Exception as exc:
        LOGGER.critical("config_unknown_error", path=str(config_path), error=str(exc))
        return 1

    try:
        client = FredClient(api_key=config.api_key, sleep_seconds=config.request_sleep_seconds)
        csv_store = CsvStore(DATA_DIR)
        mongo_store = MongoMacroStore(config.mongo_uri, config.mongo_db)
    except Exception as exc:
        LOGGER.critical("client_init_failed", error=str(exc))
        return 1

    try:
        mongo_store.start_run(run_id, vars(args))
    except Exception as exc:
        LOGGER.critical("run_start_failed", run_id=run_id, error=str(exc))
        return 1

    scraper = MacroFredAlfredScraper(config, specs, client, csv_store, mongo_store)

    explicit_vintage_dates = (
        [x.strip() for x in args.vintage_dates.split(",") if x.strip()]
        if args.vintage_dates
        else None
    )

    try:
        stats = scraper.run(
            mode=args.mode,
            category=args.category,
            explicit_vintage_dates=explicit_vintage_dates,
            vintage_stride=args.vintage_stride,
        )
        status = "partial_failure" if stats["failure_count"] > 0 else "success"
    except Exception as exc:
        status = "fatal"
        stats = {"failure_count": 1, "errors": [{"phase": "pipeline", "error": str(exc)}]}
        LOGGER.critical("pipeline_fatal", run_id=run_id, error=str(exc))

    try:
        mongo_store.finish_run(run_id, status, stats)
    except Exception as exc:
        LOGGER.critical("run_finish_failed", run_id=run_id, error=str(exc))

    LOGGER.info("process_end", run_id=run_id, status=status, stats=stats)

    if status == "success":
        return 0
    elif status == "partial_failure":
        return 1
    else:
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
