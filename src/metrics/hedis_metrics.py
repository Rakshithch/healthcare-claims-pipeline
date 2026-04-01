"""
HEDIS-inspired metrics computation over the healthcare claims star schema.

Computes five measures and writes results to data/processed/hedis_metrics.json:
  1. Diabetes care rate (% of diabetic patients with claims)
  2. Denial rate by payer (% denied per insurance_type)
  3. Average age at service by age group (days)
  4. Top 10 procedures by volume and total cost
  5. Provider performance (avg claim amount and denial rate per provider)

Usage:
    source venv/bin/activate
    python src/metrics/hedis_metrics.py
"""

import json
import logging
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT   = Path(__file__).resolve().parents[2]
LOG_DIR     = REPO_ROOT / "logs"
OUTPUT_PATH = REPO_ROOT / "data" / "processed" / "hedis_metrics.json"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("hedis_metrics")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_DIR / "metrics.log", mode="a", encoding="utf-8")
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
# JSON serialization
# ---------------------------------------------------------------------------

def json_serializer(obj):
    """Convert Decimal and date objects for json.dumps."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# ---------------------------------------------------------------------------
# Metric functions
# ---------------------------------------------------------------------------

def get_diabetes_care_rate(conn) -> list[dict]:
    """% of distinct patients with at least one E11.9 (Type 2 diabetes) claim."""
    sql = text("""
        SELECT
            COUNT(DISTINCT CASE WHEN f.diagnosis_code = 'E11.9' THEN f.patient_id END)::numeric
                * 100.0 / NULLIF(COUNT(DISTINCT f.patient_id), 0)  AS diabetes_care_rate_pct,
            COUNT(DISTINCT CASE WHEN f.diagnosis_code = 'E11.9' THEN f.patient_id END)
                                                                    AS diabetic_patient_count,
            COUNT(DISTINCT f.patient_id)                            AS total_patient_count
        FROM fact_claims f
    """)
    result = conn.execute(sql)
    return [dict(row._mapping) for row in result]


def get_denial_rate_by_payer(conn) -> list[dict]:
    """Denied claim % per insurance_type."""
    sql = text("""
        SELECT
            f.insurance_type,
            COUNT(*)                                                        AS total_claims,
            COUNT(*) FILTER (WHERE f.claim_status = 'Denied')              AS denied_claims,
            ROUND(
                COUNT(*) FILTER (WHERE f.claim_status = 'Denied')::numeric
                    * 100.0 / NULLIF(COUNT(*), 0),
                2
            )                                                               AS denial_rate_pct
        FROM fact_claims f
        GROUP BY f.insurance_type
        ORDER BY f.insurance_type
    """)
    result = conn.execute(sql)
    return [dict(row._mapping) for row in result]


def get_avg_age_at_service_by_age_group(conn) -> list[dict]:
    """Average age at time of service (in days) per patient age group.

    PostgreSQL date subtraction (date - date) yields an integer number of days,
    so AVG() returns numeric directly — no EXTRACT or EPOCH needed.
    """
    sql = text("""
        SELECT
            dp.age_group,
            ROUND(AVG(dd.full_date - dp.member_dob), 2)  AS avg_age_at_service_days,
            COUNT(*)                                       AS claim_count
        FROM fact_claims f
        JOIN dim_patient dp ON f.patient_id = dp.patient_id
        JOIN dim_date   dd  ON f.date_id    = dd.date_id
        GROUP BY dp.age_group
        ORDER BY dp.age_group
    """)
    result = conn.execute(sql)
    return [dict(row._mapping) for row in result]


def get_top_10_procedures_by_volume(conn) -> list[dict]:
    """Top 10 CPT procedure codes ranked by claim count, with total billed amount."""
    sql = text("""
        SELECT
            p.procedure_code,
            p.description,
            p.base_price,
            COUNT(f.claim_id)   AS total_claims,
            SUM(f.claim_amount) AS total_claim_amount
        FROM fact_claims f
        JOIN dim_procedure p ON f.procedure_code = p.procedure_code
        GROUP BY p.procedure_code, p.description, p.base_price
        ORDER BY total_claims DESC
        LIMIT 10
    """)
    result = conn.execute(sql)
    return [dict(row._mapping) for row in result]


def get_provider_performance(conn) -> list[dict]:
    """Per-provider: average claim amount, total claims, denied claims, denial rate."""
    sql = text("""
        SELECT
            f.provider_id,
            ROUND(AVG(f.claim_amount), 2)                                    AS avg_claim_amount,
            COUNT(*)                                                          AS total_claims,
            COUNT(*) FILTER (WHERE f.claim_status = 'Denied')                AS denied_claims,
            ROUND(
                COUNT(*) FILTER (WHERE f.claim_status = 'Denied')::numeric
                    * 100.0 / NULLIF(COUNT(*), 0),
                2
            )                                                                 AS denial_rate_pct
        FROM fact_claims f
        GROUP BY f.provider_id
        ORDER BY f.provider_id
    """)
    result = conn.execute(sql)
    return [dict(row._mapping) for row in result]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_all_metrics(engine) -> dict:
    logger = logging.getLogger("hedis_metrics")
    logger.info("Starting HEDIS metrics computation")

    with engine.connect() as conn:
        logger.info("Metric 1: diabetes care rate")
        diabetes = get_diabetes_care_rate(conn)

        logger.info("Metric 2: denial rate by payer")
        denial_by_payer = get_denial_rate_by_payer(conn)

        logger.info("Metric 3: avg age at service by age group")
        age_at_service = get_avg_age_at_service_by_age_group(conn)

        logger.info("Metric 4: top 10 procedures by volume")
        top_procedures = get_top_10_procedures_by_volume(conn)

        logger.info("Metric 5: provider performance")
        provider_perf = get_provider_performance(conn)

    logger.info("All metrics computed")
    return {
        "diabetes_care_rate":              diabetes,
        "denial_rate_by_payer":            denial_by_payer,
        "avg_age_at_service_by_age_group": age_at_service,
        "top_10_procedures_by_volume":     top_procedures,
        "provider_performance":            provider_perf,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger = configure_logging()
    logger.info("hedis_metrics starting")

    engine = build_engine()
    output = run_all_metrics(engine)
    engine.dispose()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=json_serializer)

    logger.info(f"Metrics written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
