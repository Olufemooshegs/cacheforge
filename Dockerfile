# syntax=docker/dockerfile:1

# --- Stage 1: Build dependencies ---
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies if needed (e.g., for asyncpg)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

# --- Stage 2: Runtime ---
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy the application code
COPY src/ ./src/
COPY demo/ ./demo/
COPY migrations/ ./migrations/
COPY alembic.ini .

# Run as a non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Default command
CMD ["uvicorn", "demo.api:app", "--host", "0.0.0.0", "--port", "8000"]