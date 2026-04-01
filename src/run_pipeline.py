"""
Pipeline runner: executes all five ETL/reporting steps in order.

Steps:
  1. generate_data.py   — synthesise 1 000 claims → data/raw/claims.csv
  2. loader.py          — validate CSV and bulk-insert → raw_claims table
  3. star_schema.py     — transform raw_claims → dimensional star schema
  4. hedis_metrics.py   — compute HEDIS measures → data/processed/hedis_metrics.json
  5. generate_report.py — render charts + HTML report → data/processed/

Usage:
    python src/run_pipeline.py
"""

import importlib
import logging
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR   = REPO_ROOT / "logs"

def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_DIR / "pipeline.log", mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# Step runner
# ---------------------------------------------------------------------------

def run_step(step_num: int, module_path: str, logger: logging.Logger) -> None:
    """Import a pipeline module and call its main() function."""
    label = module_path.split(".")[-1]
    logger.info(f"{'='*60}")
    logger.info(f"Step {step_num}: {label}")
    logger.info(f"{'='*60}")

    t0 = time.monotonic()
    try:
        module = importlib.import_module(module_path)
        module.main()
    except Exception as exc:
        logger.exception(f"Step {step_num} ({label}) FAILED: {exc}")
        raise SystemExit(1) from exc

    elapsed = time.monotonic() - t0
    logger.info(f"Step {step_num} ({label}) completed in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Pipeline definition
# ---------------------------------------------------------------------------

STEPS = [
    (1, "ingestion.generate_data"),
    (2, "ingestion.loader"),
    (3, "transformation.star_schema"),
    (4, "metrics.hedis_metrics"),
    (5, "reporting.generate_report"),
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger = configure_logging()

    logger.info("Healthcare Claims Pipeline — starting")
    logger.info(f"Running {len(STEPS)} steps")

    total_start = time.monotonic()

    # Add src/ to sys.path so step modules resolve correctly when this script
    # is run from the repo root (python src/run_pipeline.py) or from inside src/
    src_dir = Path(__file__).resolve().parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    for step_num, module_path in STEPS:
        run_step(step_num, module_path, logger)

    total_elapsed = time.monotonic() - total_start
    logger.info(f"{'='*60}")
    logger.info(f"Pipeline finished successfully in {total_elapsed:.1f}s")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
