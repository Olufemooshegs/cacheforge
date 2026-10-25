from prometheus_client import Counter, Gauge, Histogram

CACHE_REQUESTS = Counter(
    "cache_requests_total",
    "Cache operations by operation and result",
    ["operation", "result"],
)

CACHE_DURATION = Histogram(
    "cache_operation_duration_seconds",
    "Cache operation duration (redis call only)",
    ["operation"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

CACHE_LOAD_DURATION = Histogram(
    "cache_load_duration_seconds",
    "Data loader duration by source",
    ["source"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

CACHE_FALLBACK = Counter(
    "cache_fallback_total",
    "Fallback to durable source by reason",
    ["reason"],
)

CACHE_INVALIDATIONS = Counter(
    "cache_invalidation_total",
    "Invalidation outcomes; source=direct (in request path) or source=outbox (worker)",
    ["result", "source"],
)

CACHE_WARMING_TOTAL = Counter(
    "cache_warming_total",
    "Cache warming outcomes by result",
    ["result"],
)

CACHE_WARMING_DURATION = Histogram(
    "cache_warming_duration_seconds",
    "Duration of a full cache warming run",
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

OUTBOX_PROCESSED = Counter(
    "outbox_processed_total",
    "Outbox events successfully processed by the worker",
)

OUTBOX_FAILED = Counter(
    "outbox_failed_total",
    "Outbox events that failed processing and will retry",
    ["reason"],
)

OUTBOX_DELAY = Histogram(
    "outbox_processing_delay_seconds",
    "Time from outbox event creation to worker processing",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

OUTBOX_QUEUE_DEPTH = Gauge(
    "outbox_queue_depth",
    "Number of unprocessed outbox events observed on the last poll",
)
