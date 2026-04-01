# ── Healthcare Claims Pipeline ──────────────────────────────────────────────
# Multi-stage build: slim final image with only runtime dependencies.

# ---------------------------------------------------------------------------
# Stage 1 — dependency builder
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build tools needed for psycopg2 compilation (if wheel unavailable)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ---------------------------------------------------------------------------
# Stage 2 — runtime image
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="healthcare-claims-pipeline" \
      org.opencontainers.image.description="Synthetic healthcare claims ETL pipeline"

# Runtime libpq only (no build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy source and create writable output directories
COPY src/       ./src/
COPY .env.docker .env

RUN mkdir -p data/raw data/processed logs

# Add src/ to PYTHONPATH so modules import without relative-path gymnastics
ENV PYTHONPATH=/app/src

# Default: run the full pipeline via the runner script.
# Override CMD in docker-compose to run individual steps.
CMD ["python", "src/run_pipeline.py"]
