"""
Claims CSV loader: validates records and bulk-inserts clean rows into PostgreSQL.

Usage:
    source venv/bin/activate
    python src/ingestion/loader.py
"""

import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Column, Date, MetaData, Numeric, String, Table, create_engine, func
from sqlalchemy.dialects.postgresql import TIMESTAMP, insert
from sqlalchemy.engine import URL

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH  = REPO_ROOT / "data" / "raw" / "claims.csv"
LOG_DIR   = REPO_ROOT / "logs"

EXPECTED_FIELDS = [
    "claim_id", "patient_id", "provider_id", "service_date",
    "diagnosis_code", "procedure_code", "claim_amount", "paid_amount",
    "claim_status", "insurance_type", "member_dob", "member_gender",
]

VALID_STATUSES   = {"Paid", "Denied", "Pending", "Appealed", "Adjusted"}
VALID_INSURANCES = {"Commercial", "Medicare", "Medicaid", "Medicare Advantage", "Self-Pay"}
VALID_GENDERS    = {"M", "F"}
DATE_FORMAT      = "%Y-%m-%d"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ingestion")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = logging.FileHandler(LOG_DIR / "ingestion.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


# ---------------------------------------------------------------------------
# Config / DB
# ---------------------------------------------------------------------------

def load_env_config() -> dict:
    load_dotenv(REPO_ROOT / ".env")
    required = {"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER"}
    missing = required - os.environ.keys()
    if missing:
        raise RuntimeError(f"Missing required environment variables: {missing}")
    return {
        "host":     os.environ["DB_HOST"],
        "port":     int(os.environ["DB_PORT"]),
        "dbname":   os.environ["DB_NAME"],
        "user":     os.environ["DB_USER"],
        "password": os.environ.get("DB_PASSWORD", ""),
    }


def build_engine(config: dict):
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        database=config["dbname"],
    )
    return create_engine(url, pool_pre_ping=True)


def ensure_table(engine) -> Table:
    meta = MetaData()
    table = Table(
        "raw_claims", meta,
        Column("claim_id",       String(20),             primary_key=True),
        Column("patient_id",     String(10),             nullable=False),
        Column("provider_id",    String(10),             nullable=False),
        Column("service_date",   Date(),                 nullable=False),
        Column("diagnosis_code", String(10),             nullable=False),
        Column("procedure_code", String(10),             nullable=False),
        Column("claim_amount",   Numeric(12, 2),         nullable=False),
        Column("paid_amount",    Numeric(12, 2),         nullable=False),
        Column("claim_status",   String(20),             nullable=False),
        Column("insurance_type", String(30),             nullable=False),
        Column("member_dob",     Date(),                 nullable=False),
        Column("member_gender",  String(1),              nullable=False),
        Column("loaded_at",      TIMESTAMP(timezone=True),
               nullable=False, server_default=func.now()),
    )
    meta.create_all(engine, checkfirst=True)
    return table


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_row(row_num: int, row: dict, logger: logging.Logger) -> tuple[dict | None, bool]:
    errors = []

    # 1. Null / empty check on all expected fields
    for field in EXPECTED_FIELDS:
        if not row.get(field, "").strip():
            errors.append(f"Row {row_num}: '{field}' is null or empty")

    if errors:
        for e in errors:
            logger.warning(e)
        return None, False

    # 2. Date parse
    service_date = None
    member_dob   = None
    for field in ("service_date", "member_dob"):
        try:
            parsed = datetime.strptime(row[field].strip(), DATE_FORMAT).date()
            if field == "service_date":
                service_date = parsed
            else:
                member_dob = parsed
        except ValueError:
            errors.append(f"Row {row_num}: '{field}' has invalid date '{row[field]}'")

    # 3. Non-negative float check
    claim_amount = None
    paid_amount  = None
    for field in ("claim_amount", "paid_amount"):
        try:
            val = float(row[field].strip())
        except ValueError:
            errors.append(f"Row {row_num}: '{field}' is not a number ('{row[field]}')")
            continue
        if val < 0:
            errors.append(f"Row {row_num}: '{field}' is negative ({val})")
            continue
        if field == "claim_amount":
            claim_amount = val
        else:
            paid_amount = val

    # 4. Enum membership
    status = row["claim_status"].strip()
    if status not in VALID_STATUSES:
        errors.append(f"Row {row_num}: 'claim_status' has invalid value '{status}'")

    insurance = row["insurance_type"].strip()
    if insurance not in VALID_INSURANCES:
        errors.append(f"Row {row_num}: 'insurance_type' has invalid value '{insurance}'")

    gender = row["member_gender"].strip()
    if gender not in VALID_GENDERS:
        errors.append(f"Row {row_num}: 'member_gender' has invalid value '{gender}'")

    if errors:
        for e in errors:
            logger.warning(e)
        return None, False

    return {
        "claim_id":       row["claim_id"].strip(),
        "patient_id":     row["patient_id"].strip(),
        "provider_id":    row["provider_id"].strip(),
        "service_date":   service_date,
        "diagnosis_code": row["diagnosis_code"].strip(),
        "procedure_code": row["procedure_code"].strip(),
        "claim_amount":   claim_amount,
        "paid_amount":    paid_amount,
        "claim_status":   status,
        "insurance_type": insurance,
        "member_dob":     member_dob,
        "member_gender":  gender,
    }, True


def read_and_validate_csv(
    csv_path: Path, logger: logging.Logger
) -> tuple[list[dict], int, int]:
    valid_rows    = []
    invalid_count = 0
    total         = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # row 1 is the header
            total += 1
            cleaned, ok = validate_row(row_num, row, logger)
            if ok:
                valid_rows.append(cleaned)
            else:
                invalid_count += 1

    logger.info(f"CSV read complete: {total} rows read, {invalid_count} invalid")
    return valid_rows, total, invalid_count


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

def bulk_insert(engine, table: Table, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = insert(table).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=["claim_id"])
    with engine.begin() as conn:
        result = conn.execute(stmt)
    return result.rowcount


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def log_summary(logger: logging.Logger, total: int, valid: int,
                invalid: int, inserted: int) -> None:
    skipped = valid - inserted
    lines = [
        "=== Ingestion Summary ===",
        f"  Total rows read : {total}",
        f"  Valid rows      : {valid}",
        f"  Invalid rows    : {invalid}",
        f"  Rows inserted   : {inserted}",
        f"  Rows skipped    : {skipped}  (already existed)",
    ]
    summary = "\n".join(lines)
    logger.info("\n" + summary)
    print(summary)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger = configure_logging()
    logger.info("Starting claims ingestion")

    config = load_env_config()
    engine = build_engine(config)

    table = ensure_table(engine)
    logger.info("Table 'raw_claims' ready")

    valid_rows, total, invalid_count = read_and_validate_csv(CSV_PATH, logger)

    inserted = bulk_insert(engine, table, valid_rows)

    log_summary(logger, total, len(valid_rows), invalid_count, inserted)
    engine.dispose()


if __name__ == "__main__":
    main()
