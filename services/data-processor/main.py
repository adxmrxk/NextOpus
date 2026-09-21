"""
NextOpus Data Processor Service
Processes incoming data events, provides API for querying, and exposes metrics.
"""

import asyncio
import logging
import os
import signal
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from tracing import get_tracer, instrument_app, setup_tracing

def _utcnow() -> datetime:
    """Timezone-aware UTC now.

    Incoming events carry tz-aware timestamps (the Go generator sends RFC3339
    UTC), so every datetime we compare them against must be aware too.
    """
    return datetime.now(timezone.utc)


def _as_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Treat a naive datetime as UTC so comparisons never raise."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==============================================================================
# Configuration
# ==============================================================================

@dataclass
class Config:
    port: int = int(os.getenv("PORT", "8080"))
    max_events: int = int(os.getenv("MAX_EVENTS", "100000"))
    retention_hours: int = int(os.getenv("RETENTION_HOURS", "24"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()


# ==============================================================================
# Prometheus Metrics
# ==============================================================================

EVENTS_RECEIVED = Counter(
    'nextopus_processor_events_received_total',
    'Total events received',
    ['type', 'source']
)

EVENTS_PROCESSED = Counter(
    'nextopus_processor_events_processed_total',
    'Total events processed'
)

PROCESSING_ERRORS = Counter(
    'nextopus_processor_errors_total',
    'Total processing errors',
    ['error_type']
)

PROCESSING_LATENCY = Histogram(
    'nextopus_processor_latency_seconds',
    'Event processing latency',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

BATCH_SIZE = Histogram(
    'nextopus_processor_batch_size',
    'Size of incoming batches',
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000]
)

EVENTS_IN_MEMORY = Gauge(
    'nextopus_processor_events_in_memory',
    'Current number of events in memory'
)

HEALTH_STATUS = Gauge(
    'nextopus_processor_health',
    'Health status (1 = healthy, 0 = unhealthy)'
)

AGGREGATIONS_COMPUTED = Counter(
    'nextopus_processor_aggregations_computed_total',
    'Total aggregations computed'
)


# ==============================================================================
# Data Models
# ==============================================================================

class DataEvent(BaseModel):
    id: str
    timestamp: datetime
    type: str
    source: str
    data: Dict[str, Any]
    metadata: Optional[Dict[str, str]] = None


class AggregationResult(BaseModel):
    metric: str
    value: float
    count: int
    timestamp: datetime


class ProcessorStats(BaseModel):
    total_events: int
    events_by_type: Dict[str, int]
    events_by_source: Dict[str, int]
    oldest_event: Optional[datetime]
    newest_event: Optional[datetime]
    uptime_seconds: float


# ==============================================================================
# Event Store
# ==============================================================================

class EventStore:
    """In-memory event store with time-based eviction.

    Events are appended in arrival order and only ever evicted from the front,
    which makes every index a FIFO queue too: the oldest event of a given type
    is always at the head of that type's deque. That invariant is what lets
    eviction and retention be O(1) per event instead of scanning.
    """

    def __init__(self, max_events: int = 100000, retention_hours: int = 24):
        self.events: Deque[DataEvent] = deque()
        self.events_by_type: Dict[str, Deque[DataEvent]] = defaultdict(deque)
        self.events_by_source: Dict[str, Deque[DataEvent]] = defaultdict(deque)
        # Direct lookup for the by-id endpoint, which otherwise scans.
        self._by_id: Dict[str, DataEvent] = {}
        self.max_events = max_events
        self.retention_hours = retention_hours
        self.start_time = _utcnow()
        self._lock = asyncio.Lock()
        # Aggregating is a full pass over a type; cache until the data moves.
        self._version = 0
        self._agg_cache: Dict[str, tuple] = {}

    def _evict_oldest(self) -> None:
        """Drop the front event. O(1): it heads every index."""
        old = self.events.popleft()
        by_type = self.events_by_type.get(old.type)
        if by_type:
            by_type.popleft()
            if not by_type:
                del self.events_by_type[old.type]
        by_source = self.events_by_source.get(old.source)
        if by_source:
            by_source.popleft()
            if not by_source:
                del self.events_by_source[old.source]
        # Only drop the id mapping if it still points at this event; a reused
        # id may already have been overwritten by a newer arrival.
        if self._by_id.get(old.id) is old:
            del self._by_id[old.id]

    async def add_events(self, events: List[DataEvent]) -> int:
        """Add events to the store."""
        async with self._lock:
            added = 0
            for event in events:
                event.timestamp = _as_aware(event.timestamp)
                if len(self.events) >= self.max_events:
                    self._evict_oldest()

                self.events.append(event)
                self.events_by_type[event.type].append(event)
                self.events_by_source[event.source].append(event)
                self._by_id[event.id] = event
                added += 1

            if added:
                self._version += 1
            EVENTS_IN_MEMORY.set(len(self.events))
            return added

    async def get_by_id(self, event_id: str) -> Optional[DataEvent]:
        """Direct lookup rather than scanning every event."""
        async with self._lock:
            return self._by_id.get(event_id)

    async def query(
        self,
        event_type: Optional[str] = None,
        source: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[DataEvent]:
        """Query events with filters."""
        start_time = _as_aware(start_time)
        end_time = _as_aware(end_time)
        async with self._lock:
            # Start with appropriate subset
            if event_type:
                candidates = self.events_by_type.get(event_type, ())
            elif source:
                candidates = self.events_by_source.get(source, ())
            else:
                candidates = self.events

            results = []
            for event in reversed(candidates):  # Most recent first
                ts = _as_aware(event.timestamp)
                if start_time and ts < start_time:
                    continue
                if end_time and ts > end_time:
                    continue
                if event_type and event.type != event_type:
                    continue
                if source and event.source != source:
                    continue

                results.append(event)
                if len(results) >= limit:
                    break

            return results

    async def get_stats(self) -> ProcessorStats:
        """Get store statistics."""
        async with self._lock:
            events_by_type = {k: len(v) for k, v in self.events_by_type.items()}
            events_by_source = {k: len(v) for k, v in self.events_by_source.items()}

            oldest = self.events[0].timestamp if self.events else None
            newest = self.events[-1].timestamp if self.events else None

            return ProcessorStats(
                total_events=len(self.events),
                events_by_type=events_by_type,
                events_by_source=events_by_source,
                oldest_event=oldest,
                newest_event=newest,
                uptime_seconds=(_utcnow() - self.start_time).total_seconds()
            )

    async def aggregate_metrics(self, event_type: str = "metric") -> List[AggregationResult]:
        """Aggregate numeric metrics from events."""
        async with self._lock:
            # A full pass over the type. Nothing changes between writes, so
            # repeated dashboard polls reuse the previous answer.
            cached = self._agg_cache.get(event_type)
            if cached and cached[0] == self._version:
                return cached[1]

            events = self.events_by_type.get(event_type, ())
            if not events:
                return []

            # Aggregate numeric fields
            aggregations: Dict[str, Dict[str, float]] = defaultdict(
                lambda: {"sum": 0, "count": 0, "min": float("inf"), "max": float("-inf")}
            )

            for event in events:
                for key, value in event.data.items():
                    if isinstance(value, (int, float)):
                        agg = aggregations[key]
                        agg["sum"] += value
                        agg["count"] += 1
                        agg["min"] = min(agg["min"], value)
                        agg["max"] = max(agg["max"], value)

            results = []
            for metric, agg in aggregations.items():
                if agg["count"] > 0:
                    results.append(AggregationResult(
                        metric=metric,
                        value=agg["sum"] / agg["count"],  # Average
                        count=int(agg["count"]),
                        timestamp=_utcnow()
                    ))

            AGGREGATIONS_COMPUTED.inc()
            self._agg_cache[event_type] = (self._version, results)
            return results

    async def cleanup_old_events(self):
        """Remove events older than the retention period.

        Events are ordered by arrival, so everything expired sits at the front.
        Popping from the head is O(1) each; the previous version rebuilt both
        index lists and then searched them for every removed event.
        """
        async with self._lock:
            cutoff = _utcnow() - timedelta(hours=self.retention_hours)

            removed = 0
            while self.events and _as_aware(self.events[0].timestamp) < cutoff:
                self._evict_oldest()
                removed += 1

            if removed:
                self._version += 1
                logger.info(f"Cleaned up {removed} old events")
                EVENTS_IN_MEMORY.set(len(self.events))


# ==============================================================================
# Application
# ==============================================================================

store = EventStore(config.max_events, config.retention_hours)
shutdown_event = asyncio.Event()


setup_tracing()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    HEALTH_STATUS.set(1)
    logger.info("Data Processor starting up...")

    # Start background cleanup task
    cleanup_task = asyncio.create_task(periodic_cleanup())

    yield

    # Shutdown
    HEALTH_STATUS.set(0)
    shutdown_event.set()
    cleanup_task.cancel()
    logger.info("Data Processor shutting down...")


app = FastAPI(
    title="NextOpus Data Processor",
    description="Event processing and aggregation service",
    version="1.0.0",
    lifespan=lifespan
)

instrument_app(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def periodic_cleanup():
    """Periodically clean up old events."""
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            await store.cleanup_old_events()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            PROCESSING_ERRORS.labels(error_type="cleanup").inc()


# ==============================================================================
# API Endpoints
# ==============================================================================

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": _utcnow().isoformat()}


@app.get("/ready")
async def ready():
    """Readiness check endpoint."""
    return {"status": "ready"}


@app.post("/ingest")
async def ingest_events(events: List[DataEvent]):
    """Ingest a batch of events."""
    start_time = time.time()

    tracer = get_tracer()
    try:
        BATCH_SIZE.observe(len(events))

        with tracer.start_as_current_span("count_events") as span:
            span.set_attribute("batch.size", len(events))
            for event in events:
                EVENTS_RECEIVED.labels(type=event.type, source=event.source).inc()

        with tracer.start_as_current_span("store_events") as span:
            added = await store.add_events(events)
            span.set_attribute("events.added", added)
            span.set_attribute("store.total", len(store.events))
        EVENTS_PROCESSED.inc(added)

        PROCESSING_LATENCY.observe(time.time() - start_time)

        return {
            "status": "accepted",
            "count": added,
            "latency_ms": (time.time() - start_time) * 1000
        }
    except Exception as e:
        logger.error(f"Ingestion error: {e}")
        PROCESSING_ERRORS.labels(error_type="ingestion").inc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events")
async def query_events(
    type: Optional[str] = Query(None, description="Event type filter"),
    source: Optional[str] = Query(None, description="Event source filter"),
    start: Optional[datetime] = Query(None, description="Start time"),
    end: Optional[datetime] = Query(None, description="End time"),
    limit: int = Query(100, ge=1, le=1000, description="Max results")
):
    """Query events with filters."""
    tracer = get_tracer()
    try:
        with tracer.start_as_current_span("query_events") as span:
            span.set_attribute("query.limit", limit)
            if type:
                span.set_attribute("query.type", type)
            if source:
                span.set_attribute("query.source", source)
            events = await store.query(
                event_type=type,
                source=source,
                start_time=start,
                end_time=end,
                limit=limit
            )
            span.set_attribute("query.results", len(events))
        return {"events": events, "count": len(events)}
    except Exception as e:
        logger.error(f"Query error: {e}")
        PROCESSING_ERRORS.labels(error_type="query").inc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/{event_id}")
async def get_event(event_id: str):
    """Get a specific event by ID."""
    event = await store.get_by_id(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.get("/stats")
async def get_stats():
    """Get processor statistics."""
    try:
        stats = await store.get_stats()
        return stats
    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/aggregations")
async def get_aggregations(
    type: str = Query("metric", description="Event type to aggregate")
):
    """Get aggregated metrics."""
    try:
        aggregations = await store.aggregate_metrics(type)
        return {"aggregations": aggregations, "event_type": type}
    except Exception as e:
        logger.error(f"Aggregation error: {e}")
        PROCESSING_ERRORS.labels(error_type="aggregation").inc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


# ==============================================================================
# Main
# ==============================================================================

def handle_signal(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}")
    shutdown_event.set()


if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(f"Starting Data Processor on port {config.port}")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.port,
        log_level=config.log_level.lower(),
        access_log=True
    )
