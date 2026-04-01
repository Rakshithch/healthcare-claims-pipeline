# Healthcare Claims Analytics Pipeline

An end-to-end data engineering pipeline that ingests synthetic medical claims, builds a PostgreSQL star schema, computes HEDIS-inspired quality measures, and renders a self-contained HTML analytics report — all containerised with Docker.

Built to demonstrate production-grade data engineering patterns in the healthcare payer/provider domain.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Pipeline Steps](#pipeline-steps)
- [Project Structure](#project-structure)
- [Local Setup](#local-setup)
- [Docker](#docker)
- [Sample Output](#sample-output)
- [HEDIS Metrics Reference](#hedis-metrics-reference)

---

## Overview

Healthcare organisations process millions of medical claims each year — yet turning raw claim transactions into actionable quality metrics is rarely straightforward. This project demonstrates a complete analytics pipeline modelled on real-world payer data workflows:

| Concern | Approach |
|---|---|
| **Data generation** | Realistic synthetic claims (ICD-10 dx codes, CPT procedure codes, payer mix, age/gender distributions) seeded for reproducibility |
| **Data quality** | Row-level validation with per-field error logging and upsert-safe bulk loading |
| **Data modelling** | Kimball-style star schema — one fact table, five dimension tables — enabling efficient analytical queries |
| **Quality measurement** | Five HEDIS-inspired measures: diabetes care rate, denial rate by payer, age-at-service, top procedures, provider performance |
| **Reporting** | Matplotlib charts (bar, horizontal bar, scatter) embedded in a single self-contained HTML report |
| **Operationalisation** | Multi-stage Docker image, Docker Compose orchestration with health-check startup ordering, per-step logging |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SOURCE LAYER                                     │
│                                                                         │
│   generate_data.py                                                      │
│   ├── 1 000 synthetic claims (seed=42, reproducible)                    │
│   ├── 200 patients  ·  50 providers  ·  20 ICD-10 codes  ·  20 CPTs    │
│   └── data/raw/claims.csv                                               │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ CSV
┌────────────────────────────▼────────────────────────────────────────────┐
│                        INGESTION LAYER                                  │
│                                                                         │
│   loader.py                                                             │
│   ├── Field-level validation (nulls, dates, amounts, enums)             │
│   ├── Structured rejection logging                                      │
│   └── Upsert (ON CONFLICT DO NOTHING) → PostgreSQL: raw_claims          │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ raw_claims table
┌────────────────────────────▼────────────────────────────────────────────┐
│                     TRANSFORMATION LAYER                                │
│                                                                         │
│   star_schema.py  (Kimball dimensional model)                           │
│                                                                         │
│   ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐               │
│   │ dim_patient │  │ dim_provider │  │    dim_date      │               │
│   │ patient_id  │  │ provider_id  │  │ date_id (YYYYMMDD│               │
│   │ member_dob  │  └──────┬───────┘  │ year/month/day  │               │
│   │ member_gender│        │          │ quarter          │               │
│   │ age_group   │        │          └────────┬─────────┘               │
│   └──────┬──────┘        │                   │                         │
│          │               │    ┌──────────────▼──────────────┐          │
│          │               │    │        fact_claims           │          │
│          └───────────────┼───►│  claim_id   claim_amount    │          │
│                          │    │  patient_id  paid_amount    │◄─────┐   │
│   ┌──────────────────┐   └───►│  provider_id claim_status  │      │   │
│   │  dim_diagnosis   │        │  date_id    insurance_type │      │   │
│   │  diagnosis_code  │───────►│  diagnosis_code            │      │   │
│   │  description     │        │  procedure_code            │      │   │
│   └──────────────────┘        └────────────────────────────┘      │   │
│                                                                    │   │
│   ┌──────────────────┐                                             │   │
│   │  dim_procedure   │─────────────────────────────────────────────┘   │
│   │  procedure_code  │                                                  │
│   │  description     │                                                  │
│   │  base_price      │                                                  │
│   └──────────────────┘                                                  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ star schema tables
┌────────────────────────────▼────────────────────────────────────────────┐
│                        METRICS LAYER                                    │
│                                                                         │
│   hedis_metrics.py                                                      │
│   ├── Diabetes care rate       (E11.9 diagnosis prevalence)             │
│   ├── Denial rate by payer     (per insurance_type)                     │
│   ├── Avg age at service       (per age group, in days)                 │
│   ├── Top 10 procedures        (by volume + total billed)               │
│   └── Provider performance     (avg claim $ + denial rate)              │
│                          │                                              │
│                          └──► data/processed/hedis_metrics.json         │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ hedis_metrics.json
┌────────────────────────────▼────────────────────────────────────────────┐
│                       REPORTING LAYER                                   │
│                                                                         │
│   generate_report.py                                                    │
│   ├── Chart 1: Denial rate bar chart by payer        (matplotlib)       │
│   ├── Chart 2: Top 10 procedures horizontal bar      (matplotlib)       │
│   ├── Chart 3: Provider performance scatter plot     (matplotlib)       │
│   └── Self-contained HTML report (charts base64-embedded)               │
│                                                                         │
│   data/processed/denial_rate_by_payer.png                               │
│   data/processed/top_10_procedures.png                                  │
│   data/processed/provider_performance.png                               │
│   data/processed/report.html                                            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Language** | Python 3.12 | All pipeline stages |
| **Database** | PostgreSQL 16 | Raw storage + dimensional model |
| **ORM / SQL** | SQLAlchemy 2.0 | Engine, DDL, `text()` queries — no ORM magic |
| **Data driver** | psycopg2-binary 2.9 | PostgreSQL adapter |
| **Data processing** | pandas 3.0 | Star schema transformation, deduplication |
| **Visualisation** | matplotlib 3.10 | Bar, horizontal bar, scatter charts |
| **Config** | python-dotenv 1.2 | `.env`-based credential management |
| **Containerisation** | Docker + Docker Compose | Multi-stage image, service orchestration |
| **Data standard** | ICD-10-CM / CPT-4 | Industry-standard medical coding |
| **Quality framework** | HEDIS-inspired | National healthcare quality measure methodology |

---

## Pipeline Steps

### Step 1 — Data Generation (`src/ingestion/generate_data.py`)

Produces `data/raw/claims.csv` with 1 000 realistic synthetic medical claims.

- **200 patients** with demographically consistent date-of-birth/insurance-type pairings (e.g. Medicare patients age 65–89)
- **50 providers** drawn randomly per claim
- **20 ICD-10 diagnosis codes** covering common chronic and acute conditions
- **20 CPT procedure codes** with realistic base prices ($20 → $18 000)
- **5 payer types** with weighted distribution: Commercial 45%, Medicare 25%, Medicaid 15%, Medicare Advantage 10%, Self-Pay 5%
- **5 claim statuses** with weighted distribution: Paid 60%, Denied 15%, Pending 12%, Appealed 7%, Adjusted 6%
- Paid amounts computed as a realistic fraction of billed amounts (0% for Denied/Pending, 70–95% for Paid)

### Step 2 — Ingestion & Validation (`src/ingestion/loader.py`)

Validates every CSV row and bulk-loads clean records into PostgreSQL.

- **Null / empty checks** across all 12 expected fields
- **Date parsing** — rejects malformed `YYYY-MM-DD` values
- **Non-negative numeric validation** on `claim_amount` and `paid_amount`
- **Enum membership checks** for `claim_status`, `insurance_type`, `member_gender`
- **Upsert strategy** — `ON CONFLICT DO NOTHING` on `claim_id` makes reruns safe
- Rejection details written to `logs/ingestion.log` with row-level context

### Step 3 — Star Schema Transformation (`src/transformation/star_schema.py`)

Reads `raw_claims` and populates a Kimball-style dimensional model.

| Table | Grain | Key Design Notes |
|---|---|---|
| `dim_patient` | One row per patient | `age_group` computed at load time from `member_dob` |
| `dim_provider` | One row per provider | Extensible — add specialty, NPI, address columns |
| `dim_date` | One row per calendar date | `date_id` is a compact `YYYYMMDD` integer; includes `year`, `month`, `day`, `quarter` |
| `dim_diagnosis` | One row per ICD-10 code | Includes human-readable `description` |
| `dim_procedure` | One row per CPT code | Includes `description` and reference `base_price` |
| `fact_claims` | One row per claim | Foreign keys to all five dimensions; additive `claim_amount` and `paid_amount` measures |

All dimension tables loaded before the fact table. Every load is upsert-safe.

### Step 4 — HEDIS Metrics (`src/metrics/hedis_metrics.py`)

Runs five analytical SQL queries over the star schema and saves results to `data/processed/hedis_metrics.json`.

| Measure | SQL Pattern |
|---|---|
| Diabetes care rate | `COUNT(DISTINCT patient_id WHERE dx = 'E11.9') / COUNT(DISTINCT patient_id)` |
| Denial rate by payer | `COUNT(*) FILTER (WHERE claim_status = 'Denied')` grouped by `insurance_type` |
| Avg age at service | `AVG(full_date - member_dob)` — PostgreSQL date subtraction yields integer days |
| Top 10 procedures | `COUNT` + `SUM(claim_amount)` joined to `dim_procedure`, `ORDER BY count DESC LIMIT 10` |
| Provider performance | `AVG(claim_amount)` + denial rate per `provider_id` |

All `Decimal` and `date` values serialised to JSON-safe `float` and ISO-8601 strings.

### Step 5 — Report Generation (`src/reporting/generate_report.py`)

Reads the metrics JSON and renders three Matplotlib charts, then assembles a fully self-contained HTML report with all charts embedded as base64 data URIs (no external dependencies at view time).

| Output | Type | Contents |
|---|---|---|
| `denial_rate_by_payer.png` | Bar chart | Rates coloured red/green relative to the cross-payer mean; labelled with denial % and sample size |
| `top_10_procedures.png` | Horizontal bar | YlOrRd colour gradient by total billed amount; annotated with claim count and dollars |
| `provider_performance.png` | Scatter plot | X = total claims, Y = denial rate, bubble size = avg claim amount; outliers labelled |
| `report.html` | Self-contained HTML | KPI summary cards + all three charts + denial-by-payer detail table |

---

## Project Structure

```
healthcare-claims-pipeline/
│
├── src/
│   ├── run_pipeline.py              # Orchestrator — runs all 5 steps in order
│   ├── ingestion/
│   │   ├── generate_data.py         # Step 1: synthetic claims CSV
│   │   └── loader.py                # Step 2: validate + load → raw_claims
│   ├── transformation/
│   │   └── star_schema.py           # Step 3: raw_claims → star schema
│   ├── metrics/
│   │   └── hedis_metrics.py         # Step 4: HEDIS measures → JSON
│   └── reporting/
│       └── generate_report.py       # Step 5: charts + HTML report
│
├── data/
│   ├── raw/
│   │   └── claims.csv               # Generated by step 1
│   └── processed/
│       ├── hedis_metrics.json       # Generated by step 4
│       ├── denial_rate_by_payer.png # Generated by step 5
│       ├── top_10_procedures.png    # Generated by step 5
│       ├── provider_performance.png # Generated by step 5
│       └── report.html              # Generated by step 5 (self-contained)
│
├── logs/
│   ├── pipeline.log                 # Master pipeline log
│   ├── ingestion.log                # Step 2 row-level validation log
│   ├── transformation.log           # Step 3 load summary log
│   ├── metrics.log                  # Step 4 query log
│   └── reporting.log                # Step 5 render log
│
├── tests/                           # Test suite (pytest)
│
├── Dockerfile                       # Multi-stage image (builder + runtime)
├── docker-compose.yml               # postgres + pipeline services
├── .env                             # Local DB credentials (not committed)
├── .env.docker                      # Docker DB credentials (postgres service)
├── .dockerignore
└── requirements.txt                 # Pinned dependencies
```

---

## Local Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 16 running locally
- `psql` available on `PATH`

### 1. Clone and create virtualenv

```bash
git clone https://github.com/your-username/healthcare-claims-pipeline.git
cd healthcare-claims-pipeline

python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Create the database

```bash
createdb healthcare_claims
```

### 3. Configure credentials

```bash
cp .env.example .env
# Edit .env with your local PostgreSQL credentials
```

`.env` format:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=healthcare_claims
DB_USER=your_user
DB_PASSWORD=your_password
```

### 4. Run the full pipeline

```bash
python src/run_pipeline.py
```

Or run individual steps:

```bash
python src/ingestion/generate_data.py     # Step 1
python src/ingestion/loader.py            # Step 2
python src/transformation/star_schema.py  # Step 3
python src/metrics/hedis_metrics.py       # Step 4
python src/reporting/generate_report.py   # Step 5
```

### 5. View the report

```bash
open data/processed/report.html           # macOS
xdg-open data/processed/report.html      # Linux
start data/processed/report.html         # Windows
```

---

## Docker

No local PostgreSQL or Python installation required.

### Run the full pipeline

```bash
docker compose up --build
```

This will:
1. Pull `postgres:16-alpine` and start the database
2. Build the pipeline image (multi-stage, ~200 MB)
3. Wait until PostgreSQL passes its health check (`pg_isready`)
4. Execute all 5 pipeline steps in order
5. Write outputs to `./data/processed/` and `./logs/` on your host machine

### Run a single step

```bash
docker compose run --rm pipeline python src/ingestion/generate_data.py
```

### Connect to the database

```bash
# PostgreSQL is exposed on host port 5433 to avoid clashing with local Postgres
psql -h localhost -p 5433 -U pipeline -d healthcare_claims
```

### Useful queries once the pipeline has run

```sql
-- Claim volume by insurance type and status
SELECT insurance_type, claim_status, COUNT(*) AS claims
FROM fact_claims
GROUP BY 1, 2
ORDER BY 1, 2;

-- Top 5 highest-cost providers
SELECT f.provider_id, ROUND(AVG(f.claim_amount), 2) AS avg_claim
FROM fact_claims f
GROUP BY f.provider_id
ORDER BY avg_claim DESC
LIMIT 5;

-- Monthly claim trend
SELECT d.year, d.month, COUNT(*) AS claims, SUM(f.claim_amount) AS total_billed
FROM fact_claims f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

## Sample Output

### Pipeline log (abridged)

```
2026-04-01 15:24:55  INFO  Healthcare Claims Pipeline — starting
2026-04-01 15:24:55  INFO  Running 5 steps
2026-04-01 15:24:55  INFO  ============================================================
2026-04-01 15:24:55  INFO  Step 1: generate_data
2026-04-01 15:24:55  INFO  ============================================================
Generated 1000 claims → data/raw/claims.csv
2026-04-01 15:24:55  INFO  Step 1 (generate_data) completed in 0.0s
...
2026-04-01 15:24:56  INFO  Step 5 (generate_report) completed in 0.4s
2026-04-01 15:24:56  INFO  Pipeline finished successfully in 1.2s
```

### HEDIS Metrics (`hedis_metrics.json`)

**Diabetes Care Rate**
```
16.8% of patients have at least one Type 2 diabetes (E11.9) claim
  → 33 diabetic patients out of 197 total
```

**Denial Rate by Payer**

| Payer | Total Claims | Denied | Denial Rate |
|---|---|---|---|
| Medicare | 532 | 90 | **16.9%** |
| Commercial | 902 | 148 | 16.4% |
| Medicare Advantage | 176 | 26 | 14.8% |
| Self-Pay | 98 | 12 | 12.2% |
| Medicaid | 292 | 24 | **8.2%** |

**Top 3 Procedures by Volume**

| Rank | CPT | Description | Claims | Total Billed |
|---|---|---|---|---|
| 1 | 99396 | Preventive visit, established, 40–64 yrs | 124 | $36,700 |
| 2 | 80053 | Comprehensive metabolic panel | 122 | $6,420 |
| 3 | 90686 | Influenza vaccine, quadrivalent | 118 | $5,582 |

**Provider Performance**
```
50 providers analysed
  Denial rate range : 0.0% – 31.3%
  Avg claim range   : $332 – $3,961
  Overall denial    : 15.0% (300 of 2 000 claims)
```

**Average Age at Service by Age Group**

| Age Group | Avg Age at Service | Claims |
|---|---|---|
| 0–17 | ~6.6 years | 19 |
| 18–34 | ~25.3 years | 219 |
| 35–49 | ~40.6 years | 160 |
| 50–64 | ~56.3 years | 235 |
| 65–74 | ~66.8 years | 223 |
| 75+ | ~81.4 years | 144 |

---

## HEDIS Metrics Reference

[HEDIS](https://www.ncqa.org/hedis/) (Healthcare Effectiveness Data and Information Set) is the most widely used quality measurement tool in U.S. managed care. This pipeline implements a subset of HEDIS-inspired measures aligned with common payer analytics workflows:

| This Pipeline | Real HEDIS Analogue | Relevance |
|---|---|---|
| Diabetes care rate | CDC (Comprehensive Diabetes Care) | % of diabetic members receiving appropriate care |
| Denial rate by payer | Plan All-Cause Readmissions (PAR) | Operational quality — high denial rates signal claims friction |
| Avg age at service | Age/sex stratification (all measures) | Used for risk adjustment and benchmark stratification |
| Top procedures by volume | Utilisation Management metrics | Identifies high-frequency services for contract review |
| Provider performance | HEDIS Physician Measurement | Flags outlier providers for quality improvement programmes |

---

## Extending the Pipeline

Some natural next steps for a production deployment:

- **Incremental loads** — partition `raw_claims` by `loaded_at` date and add watermark logic to `loader.py`
- **dbt models** — replace the hand-written SQL in `star_schema.py` and `hedis_metrics.py` with versioned, tested dbt models
- **Airflow DAG** — wrap `run_pipeline.py` steps as individual Airflow tasks for scheduling, retries, and observability
- **Great Expectations** — add data quality contracts at the CSV ingestion boundary and after star schema population
- **Redshift / BigQuery** — swap the SQLAlchemy URL to target a cloud warehouse; the query layer is already ANSI SQL
- **CI/CD** — GitHub Actions workflow to run `pytest` and rebuild the Docker image on every push

---

*Built with Python · PostgreSQL · Docker · matplotlib · SQLAlchemy*
