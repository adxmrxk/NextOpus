"""Tests for tracing setup.

Tracing must be entirely optional: with no collector configured the service
starts normally and every span is a no-op.
"""

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main  # noqa: E402
import tracing  # noqa: E402


@pytest.fixture(autouse=True)
def clear_otel_env(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)


def test_tracing_disabled_without_endpoint():
    assert tracing.tracing_enabled() is False


def test_tracing_enabled_when_endpoint_set(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger-collector:4318")
    assert tracing.tracing_enabled() is True


def test_setup_tracing_is_noop_without_endpoint():
    """Must not raise, and must not require a reachable collector."""
    tracing.setup_tracing()


def test_instrument_app_is_noop_without_endpoint():
    from fastapi import FastAPI

    tracing.instrument_app(FastAPI())


def test_get_tracer_returns_usable_tracer_when_disabled():
    """Spans are no-ops when tracing is off, but the API must still work."""
    tracer = tracing.get_tracer()
    with tracer.start_as_current_span("test-span") as span:
        span.set_attribute("k", "v")


def test_probe_endpoints_are_excluded_from_tracing():
    for path in ("health", "ready", "metrics"):
        assert path in tracing.EXCLUDED_URLS


def test_ingest_still_works_with_tracing_disabled():
    """The spans added to /ingest must not break it when tracing is off."""
    from datetime import datetime, timedelta, timezone

    main.store = main.EventStore(max_events=1000, retention_hours=24)
    now = datetime.now(timezone.utc)
    batch = [
        {
            "id": f"evt-{i}",
            "timestamp": (now - timedelta(seconds=i)).isoformat().replace("+00:00", "Z"),
            "type": "metric",
            "source": "api-gateway",
            "data": {"cpu_usage": float(i)},
        }
        for i in range(3)
    ]

    with TestClient(main.app) as client:
        assert client.post("/ingest", json=batch).json()["count"] == 3
        assert client.get("/events", params={"type": "metric"}).json()["count"] == 3
