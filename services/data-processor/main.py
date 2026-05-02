"""
NextOpus Data Processor Service
Processes incoming data events, provides API for querying, and exposes metrics.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

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
    metrics_port: int = int(os.getenv("METRICS_PORT", "9090"))
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


class EventQuery(BaseModel):
    type: Optional[str] = None
    source: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 100


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
    """In-memory event store with time-based eviction."""

    def __init__(self, max_events: int = 100000, retention_hours: int = 24):
        self.events: List[DataEvent] = []
        self.events_by_type: Dict[str, List[DataEvent]] = defaultdict(list)
        self.events_by_source: Dict[str, List[DataEvent]] = defaultdict(list)
        self.max_events = max_events
        self.retention_hours = retention_hours
        self.start_time = datetime.utcnow()
        self._lock = asyncio.Lock()

    async def add_events(self, events: List[DataEvent]) -> int:
        """Add events to the store."""
        async with self._lock:
            added = 0
            for event in events:
                if len(self.events) >= self.max_events:
                    # Remove oldest event
                    old_event = self.events.pop(0)
                    self.events_by_type[old_event.type].remove(old_event)
                    self.events_by_source[old_event.source].remove(old_event)

                self.events.append(event)
                self.events_by_type[event.type].append(event)
                self.events_by_source[event.source].append(event)
                added += 1

            EVENTS_IN_MEMORY.set(len(self.events))
            return added

    async def query(
        self,
        event_type: Optional[str] = None,
        source: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[DataEvent]:
        """Query events with filters."""
        async with self._lock:
            # Start with appropriate subset
            if event_type:
                candidates = self.events_by_type.get(event_type, [])
            elif source:
                candidates = self.events_by_source.get(source, [])
            else:
                candidates = self.events

            results = []
            for event in reversed(candidates):  # Most recent first
                if start_time and event.timestamp < start_time:
                    continue
                if end_time and event.timestamp > end_time:
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
                uptime_seconds=(datetime.utcnow() - self.start_time).total_seconds()
            )

    async def aggregate_metrics(self, event_type: str = "metric") -> List[AggregationResult]:
        """Aggregate numeric metrics from events."""
        async with self._lock:
            events = self.events_by_type.get(event_type, [])
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
                        timestamp=datetime.utcnow()
                    ))

            AGGREGATIONS_COMPUTED.inc()
            return results

    async def cleanup_old_events(self):
        """Remove events older than retention period."""
        async with self._lock:
            cutoff = datetime.utcnow() - timedelta(hours=self.retention_hours)

            # Find cutoff index
            cutoff_idx = 0
            for i, event in enumerate(self.events):
                if event.timestamp >= cutoff:
                    cutoff_idx = i
                    break

            if cutoff_idx > 0:
                removed_events = self.events[:cutoff_idx]
                self.events = self.events[cutoff_idx:]

                # Update indexes
                for event in removed_events:
                    if event in self.events_by_type[event.type]:
                        self.events_by_type[event.type].remove(event)
                    if event in self.events_by_source[event.source]:
                        self.events_by_source[event.source].remove(event)

                logger.info(f"Cleaned up {len(removed_events)} old events")
                EVENTS_IN_MEMORY.set(len(self.events))


# ==============================================================================
# Application
# ==============================================================================

store = EventStore(config.max_events, config.retention_hours)
shutdown_event = asyncio.Event()


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
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/ready")
async def ready():
    """Readiness check endpoint."""
    return {"status": "ready"}


@app.post("/ingest")
async def ingest_events(events: List[DataEvent]):
    """Ingest a batch of events."""
    start_time = time.time()

    try:
        BATCH_SIZE.observe(len(events))

        for event in events:
            EVENTS_RECEIVED.labels(type=event.type, source=event.source).inc()

        added = await store.add_events(events)
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
    try:
        events = await store.query(
            event_type=type,
            source=source,
            start_time=start,
            end_time=end,
            limit=limit
        )
        return {"events": events, "count": len(events)}
    except Exception as e:
        logger.error(f"Query error: {e}")
        PROCESSING_ERRORS.labels(error_type="query").inc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/{event_id}")
async def get_event(event_id: str):
    """Get a specific event by ID."""
    events = await store.query(limit=10000)
    for event in events:
        if event.id == event_id:
            return event
    raise HTTPException(status_code=404, detail="Event not found")


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
