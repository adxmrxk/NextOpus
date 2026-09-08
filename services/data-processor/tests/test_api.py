"""End-to-end tests for the HTTP API, driven through FastAPI's TestClient."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main  # noqa: E402


def batch(n=3, etype="metric", source="api-gateway"):
    now = datetime.now(timezone.utc)
    return [
        {
            "id": f"evt-{i}",
            "timestamp": (now - timedelta(seconds=i)).isoformat().replace("+00:00", "Z"),
            "type": etype,
            "source": source,
            "data": {"cpu_usage": float(i)},
            "metadata": {"region": "local"},
        }
        for i in range(n)
    ]


@pytest.fixture
def client():
    main.store = main.EventStore(max_events=1000, retention_hours=24)
    with TestClient(main.app) as c:
        yield c


def test_health_and_ready(client):
    assert client.get("/health").json()["status"] == "healthy"
    assert client.get("/ready").json()["status"] == "ready"


def test_ingest_accepts_generator_shaped_batch(client):
    r = client.post("/ingest", json=batch(5))
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "accepted"
    assert body["count"] == 5


def test_ingest_rejects_malformed_events(client):
    assert client.post("/ingest", json=[{"id": "x"}]).status_code == 422


def test_ingested_events_are_queryable(client):
    client.post("/ingest", json=batch(4, etype="log", source="auth-service"))
    body = client.get("/events", params={"type": "log", "limit": 10}).json()
    assert body["count"] == 4
    assert {e["source"] for e in body["events"]} == {"auth-service"}


def test_events_query_accepts_naive_timestamp(client):
    """This returned HTTP 500 before naive bounds were coerced to UTC."""
    client.post("/ingest", json=batch(2))
    r = client.get("/events", params={"start": "2020-01-01T00:00:00", "limit": 5})
    assert r.status_code == 200
    assert r.json()["count"] == 2


def test_events_query_future_start_returns_nothing(client):
    client.post("/ingest", json=batch(2))
    future = (datetime.now(timezone.utc) + timedelta(days=1)).replace(tzinfo=None).isoformat()
    assert client.get("/events", params={"start": future}).json()["count"] == 0


def test_get_event_by_id_and_404(client):
    client.post("/ingest", json=batch(2))
    assert client.get("/events/evt-0").status_code == 200
    assert client.get("/events/does-not-exist").status_code == 404


def test_stats_reports_counts(client):
    client.post("/ingest", json=batch(3, etype="trace"))
    body = client.get("/stats").json()
    assert body["total_events"] == 3
    assert body["events_by_type"] == {"trace": 3}


def test_aggregations_average_numeric_fields(client):
    client.post("/ingest", json=batch(3))  # cpu_usage 0,1,2
    aggs = {a["metric"]: a for a in client.get("/aggregations", params={"type": "metric"}).json()["aggregations"]}
    assert aggs["cpu_usage"]["value"] == pytest.approx(1.0)


def test_metrics_endpoint_is_prometheus_text(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
    assert "nextopus_processor_events_processed_total" in r.text
