"""
Star schema transformation: reads raw_claims and loads a dimensional model.

Tables created:
  dim_patient    – patient demographics and age group
  dim_provider   – provider reference
  dim_date       – calendar date attributes
  dim_diagnosis  – ICD-10 code lookup
  dim_procedure  – CPT code lookup with base price
  fact_claims    – central fact table with FK references

Usage:
    source venv/bin/activate
    python src/transformation/star_schema.py
"""

import logging
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Column, Date, ForeignKey, Integer, MetaData, Numeric,
    String, Table, create_engine, text,
)
from sqlalchemy.engine import URL

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR   = REPO_ROOT / "logs"

# ICD-10 descriptions (subset matching generate_data.py)
DIAGNOSIS_DESCRIPTIONS = {
    "E11.9":    "Type 2 diabetes without complications",
    "I10":      "Essential hypertension",
    "J06.9":    "Acute upper respiratory infection",
    "M54.5":    "Low back pain",
    "F32.9":    "Major depressive disorder",
    "J18.9":    "Pneumonia",
    "N39.0":    "Urinary tract infection",
    "K21.0":    "GERD with esophagitis",
    "I25.10":   "Coronary artery disease",
    "J44.1":    "COPD with acute exacerbation",
    "E78.5":    "Hyperlipidemia",
    "M17.11":   "Primary osteoarthritis, right knee",
    "F41.1":    "Generalized anxiety disorder",
    "Z23":      "Immunization encounter",
    "Z00.00":   "General adult medical exam",
    "G43.909":  "Migraine",
    "R05.9":    "Cough",
    "R51.9":    "Headache",
    "Z12.31":   "Encounter for colorectal cancer screening",
    "S62.001A": "Fracture of navicular bone of wrist",
}

# CPT descriptions and base prices matching generate_data.py
PROCEDURE_INFO = {
    "99213": ("Office visit, established, low complexity",    150),
    "99214": ("Office visit, established, moderate complexity", 220),
    "99203": ("Office visit, new, low complexity",            180),
    "99204": ("Office visit, new, moderate complexity",       260),
    "99283": ("ED visit, moderate severity",                  800),
    "93000": ("ECG with interpretation",                       90),
    "85025": ("Complete blood count (CBC)",                    30),
    "80053": ("Comprehensive metabolic panel",                  50),
    "71046": ("Chest X-ray, 2 views",                         200),
    "45378": ("Colonoscopy, diagnostic",                      1200),
    "36415": ("Routine venipuncture",                          25),
    "90686": ("Influenza vaccine, quadrivalent",               45),
    "99232": ("Subsequent hospital care",                     350),
    "27447": ("Total knee arthroplasty",                    18000),
    "43239": ("Upper GI endoscopy with biopsy",             1500),
    "70553": ("MRI brain with contrast",                    2200),
    "72148": ("MRI lumbar spine without contrast",          1800),
    "99395": ("Preventive visit, established, 18-39 years",  250),
    "99396": ("Preventive visit, established, 40-64 years",  280),
    "81003": ("Urinalysis, automated",                         20),
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("star_schema")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_DIR / "transformation.log", mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def build_engine():
    load_dotenv(REPO_ROOT / ".env")
    required = {"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER"}
    missing = required - os.environ.keys()
    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["DB_USER"],
        password=os.environ.get("DB_PASSWORD", ""),
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        database=os.environ["DB_NAME"],
    )
    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------

def define_schema(meta: MetaData) -> dict[str, Table]:
    dim_patient = Table(
        "dim_patient", meta,
        Column("patient_id",  String(10), primary_key=True),
        Column("member_dob",  Date(),     nullable=False),
        Column("member_gender", String(1), nullable=False),
        Column("age_group",   String(20), nullable=False),
    )

    dim_provider = Table(
        "dim_provider", meta,
        Column("provider_id", String(10), primary_key=True),
    )

    dim_date = Table(
        "dim_date", meta,
        Column("date_id",   Integer,  primary_key=True, autoincrement=False),
        Column("full_date", Date(),   nullable=False, unique=True),
        Column("year",      Integer,  nullable=False),
        Column("month",     Integer,  nullable=False),
        Column("day",       Integer,  nullable=False),
        Column("quarter",   Integer,  nullable=False),
    )

    dim_diagnosis = Table(
        "dim_diagnosis", meta,
        Column("diagnosis_code", String(15), primary_key=True),
        Column("description",    String(100), nullable=False),
    )

    dim_procedure = Table(
        "dim_procedure", meta,
        Column("procedure_code", String(10),    primary_key=True),
        Column("description",    String(100),   nullable=False),
        Column("base_price",     Numeric(10, 2), nullable=False),
    )

    fact_claims = Table(
        "fact_claims", meta,
        Column("claim_id",       String(20),    primary_key=True),
        Column("patient_id",     String(10),    ForeignKey("dim_patient.patient_id"),   nullable=False),
        Column("provider_id",    String(10),    ForeignKey("dim_provider.provider_id"), nullable=False),
        Column("date_id",        Integer,       ForeignKey("dim_date.date_id"),         nullable=False),
        Column("diagnosis_code", String(15),    ForeignKey("dim_diagnosis.diagnosis_code"), nullable=False),
        Column("procedure_code", String(10),    ForeignKey("dim_procedure.procedure_code"), nullable=False),
        Column("claim_amount",   Numeric(12, 2), nullable=False),
        Column("paid_amount",    Numeric(12, 2), nullable=False),
        Column("claim_status",   String(20),    nullable=False),
        Column("insurance_type", String(30),    nullable=False),
    )

    return {
        "dim_patient":   dim_patient,
        "dim_provider":  dim_provider,
        "dim_date":      dim_date,
        "dim_diagnosis": dim_diagnosis,
        "dim_procedure": dim_procedure,
        "fact_claims":   fact_claims,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def age_group(dob: date, reference_date: date) -> str:
    age = (reference_date - dob.date() if hasattr(dob, "date") else reference_date - dob).days // 365
    if age < 18:
        return "0-17"
    if age < 35:
        return "18-34"
    if age < 50:
        return "35-49"
    if age < 65:
        return "50-64"
    if age < 75:
        return "65-74"
    return "75+"


def date_to_id(d: date) -> int:
    """Compact YYYYMMDD integer key."""
    return d.year * 10000 + d.month * 100 + d.day


def upsert(conn, table: Table, rows: list[dict], conflict_col: str) -> int:
    """Insert rows, skipping duplicates on conflict_col."""
    if not rows:
        return 0
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    stmt = pg_insert(table).values(rows).on_conflict_do_nothing(
        index_elements=[conflict_col]
    )
    result = conn.execute(stmt)
    return result.rowcount


# ---------------------------------------------------------------------------
# Transform & load
# ---------------------------------------------------------------------------

def transform_and_load(engine, tables: dict[str, Table], logger: logging.Logger) -> None:
    logger.info("Reading raw_claims …")
    raw = pd.read_sql_table("raw_claims", engine)
    logger.info(f"  {len(raw)} rows loaded")

    today = date.today()

    # -- dim_patient ----------------------------------------------------------
    patients = (
        raw[["patient_id", "member_dob", "member_gender"]]
        .drop_duplicates("patient_id")
        .copy()
    )
    patients["age_group"] = patients["member_dob"].apply(
        lambda d: age_group(d, today)
    )
    patient_rows = patients.to_dict("records")

    # -- dim_provider ---------------------------------------------------------
    provider_rows = (
        raw[["provider_id"]]
        .drop_duplicates()
        .to_dict("records")
    )

    # -- dim_date -------------------------------------------------------------
    unique_dates = raw["service_date"].drop_duplicates()
    date_rows = []
    for d in unique_dates:
        date_rows.append({
            "date_id":   date_to_id(d),
            "full_date": d,
            "year":      d.year,
            "month":     d.month,
            "day":       d.day,
            "quarter":   (d.month - 1) // 3 + 1,
        })

    # -- dim_diagnosis --------------------------------------------------------
    unique_codes = raw["diagnosis_code"].unique()
    diagnosis_rows = [
        {
            "diagnosis_code": code,
            "description":    DIAGNOSIS_DESCRIPTIONS.get(code, "Unknown"),
        }
        for code in unique_codes
    ]

    # -- dim_procedure --------------------------------------------------------
    unique_procs = raw["procedure_code"].unique()
    procedure_rows = [
        {
            "procedure_code": code,
            "description":    PROCEDURE_INFO.get(code, ("Unknown", 0))[0],
            "base_price":     PROCEDURE_INFO.get(code, ("Unknown", 0))[1],
        }
        for code in unique_procs
    ]

    # -- fact_claims ----------------------------------------------------------
    fact = raw[
        ["claim_id", "patient_id", "provider_id", "service_date",
         "diagnosis_code", "procedure_code", "claim_amount",
         "paid_amount", "claim_status", "insurance_type"]
    ].copy()
    fact["date_id"] = fact["service_date"].apply(date_to_id)
    fact = fact.drop(columns=["service_date"])
    fact_rows = fact.to_dict("records")

    # -- Write to DB in FK-safe order ----------------------------------------
    with engine.begin() as conn:
        n_pat  = upsert(conn, tables["dim_patient"],   patient_rows,   "patient_id")
        n_prv  = upsert(conn, tables["dim_provider"],  provider_rows,  "provider_id")
        n_dt   = upsert(conn, tables["dim_date"],      date_rows,      "date_id")
        n_dx   = upsert(conn, tables["dim_diagnosis"], diagnosis_rows, "diagnosis_code")
        n_px   = upsert(conn, tables["dim_procedure"], procedure_rows, "procedure_code")
        n_fact = upsert(conn, tables["fact_claims"],   fact_rows,      "claim_id")

    logger.info(
        "Load complete:\n"
        f"  dim_patient   : {n_pat} new rows  (total unique patients: {len(patient_rows)})\n"
        f"  dim_provider  : {n_prv} new rows  (total unique providers: {len(provider_rows)})\n"
        f"  dim_date      : {n_dt} new rows  (total unique dates: {len(date_rows)})\n"
        f"  dim_diagnosis : {n_dx} new rows  (total unique dx codes: {len(diagnosis_rows)})\n"
        f"  dim_procedure : {n_px} new rows  (total unique proc codes: {len(procedure_rows)})\n"
        f"  fact_claims   : {n_fact} new rows  (total claims: {len(fact_rows)})"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger = configure_logging()
    logger.info("Starting star schema transformation")

    engine = build_engine()

    meta   = MetaData()
    tables = define_schema(meta)
    meta.create_all(engine, checkfirst=True)
    logger.info("Star schema tables ready")

    transform_and_load(engine, tables, logger)
    engine.dispose()
    logger.info("Done")


if __name__ == "__main__":
    main()
