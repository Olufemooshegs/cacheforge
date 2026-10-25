#!/usr/bin/env bash
# Usage:
#   ./benchmarks/k6/run.sh baseline        # cache bypass, DB on every read
#   ./benchmarks/k6/run.sh cold            # cache empty, populate as we go
#   ./benchmarks/k6/run.sh warm            # cache pre-populated, then reads
#   ./benchmarks/k6/run.sh hot             # 50 VUs, 5 hot keys only
#
# Requires: docker compose up -d, seeded DB, uvicorn on :8000
# Overrides: VUS=100 DURATION=60s DISTRIBUTION=uniform

set -euo pipefail

MODE="${1:-warm}"
VUS="${VUS:-50}"
DURATION="${DURATION:-30s}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
PRODUCT_COUNT="${PRODUCT_COUNT:-1000}"
RESULT_DIR="benchmarks/k6/results"
mkdir -p "${RESULT_DIR}"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT="${RESULT_DIR}/${MODE}_${VUS}vu_${TIMESTAMP}.json"

case "$MODE" in
  baseline)
    # Cache is bypassed — but that requires a running uvicorn with
    # CACHEFORGE_CACHE_BYPASS=true. We don't control that from here;
    # the operator must set it before running.
    echo "== baseline =="
    echo "  Assumes uvicorn started with CACHEFORGE_CACHE_BYPASS=true"
    DIST="uniform"
    ;;
  cold)
    echo "== cold =="
    echo "  Flushing cache before workload"
    docker exec -i cacheforge-redis-1 redis-cli FLUSHDB >/dev/null
    DIST="zipf"
    ;;
  warm)
    echo "== warm =="
    echo "  Warming cache with 100 products"
    curl -s -X POST "${BASE_URL}/admin/cache/warm?limit=100" >/dev/null
    DIST="zipf"
    ;;
  hot)
    echo "== hot =="
    echo "  Hot-key workload: 5 keys, high concurrency"
    PRODUCT_COUNT=5
    DIST="uniform"
    VUS="${VUS:-100}"
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    exit 1
    ;;
esac

echo "  VUs=${VUS}  duration=${DURATION}  distribution=${DIST}  products=${PRODUCT_COUNT}"
echo "  writing ${OUT}"

k6 run \
  --summary-export "${OUT}" \
  -e "VUS=${VUS}" \
  -e "DURATION=${DURATION}" \
  -e "BASE_URL=${BASE_URL}" \
  -e "PRODUCT_COUNT=${PRODUCT_COUNT}" \
  -e "DISTRIBUTION=${DIST}" \
  benchmarks/k6/scenarios/read_workload.js