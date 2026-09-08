"""Tests for the in-memory event store: ingest, query, retention, aggregation."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main  # noqa: E402


def ev(eid, *, hours_ago=0, etype="metric", source="api-gateway", data=None, naive=False):
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    if naive:
        ts = ts.replace(tzinfo=None)
    return main.DataEvent(id=eid, timestamp=ts, type=etype, source=source, data=data or {})


@pytest.fixture
def store():
    return main.EventStore(max_events=1000, retention_hours=24)


@pytest.mark.asyncio
async def test_add_events_indexes_by_type_and_source(store):
    await store.add_events([ev("a", etype="log", source="auth-service"), ev("b", etype="metric")])
    stats = await store.get_stats()
    assert stats.total_events == 2
    assert stats.events_by_type == {"log": 1, "metric": 1}
    assert stats.events_by_source == {"auth-service": 1, "api-gateway": 1}


@pytest.mark.asyncio
async def test_max_events_evicts_oldest_and_keeps_indexes_consistent():
    store = main.EventStore(max_events=50, retention_hours=24)
    await store.add_events([ev(f"e{i}") for i in range(200)])
    assert len(store.events) == 50
    assert store.events[0].id == "e150"
    assert sum(len(v) for v in store.events_by_type.values()) == 50
    assert sum(len(v) for v in store.events_by_source.values()) == 50


@pytest.mark.asyncio
async def test_query_filters_by_type_and_source(store):
    await store.add_events([
        ev("a", etype="log", source="auth-service"),
        ev("b", etype="metric", source="auth-service"),
        ev("c", etype="log", source="api-gateway"),
    ])
    assert {e.id for e in await store.query(event_type="log")} == {"a", "c"}
    assert {e.id for e in await store.query(source="auth-service")} == {"a", "b"}


@pytest.mark.asyncio
async def test_query_respects_limit_and_returns_newest_first(store):
    await store.add_events([ev(f"e{i}") for i in range(10)])
    got = await store.query(limit=3)
    assert [e.id for e in got] == ["e9", "e8", "e7"]


@pytest.mark.asyncio
async def test_query_accepts_naive_bounds(store):
    """A naive bound used to raise TypeError against tz-aware event timestamps."""
    await store.add_events([ev("recent")])
    assert len(await store.query(start_time=datetime.utcnow() - timedelta(hours=1))) == 1
    assert len(await store.query(start_time=datetime.utcnow() + timedelta(hours=1))) == 0


@pytest.mark.asyncio
async def test_retention_drops_expired_keeps_fresh():
    store = main.EventStore(max_events=1000, retention_hours=1)
    await store.add_events([ev("old", hours_ago=5), ev("new")])
    await store.cleanup_old_events()
    assert [e.id for e in store.events] == ["new"]
    assert sum(len(v) for v in store.events_by_type.values()) == 1


@pytest.mark.asyncio
async def test_retention_drops_everything_when_all_expired():
    """The cutoff scan defaulted to 0, so a fully expired store evicted nothing."""
    store = main.EventStore(max_events=1000, retention_hours=1)
    await store.add_events([ev(f"o{i}", hours_ago=9 - i) for i in range(3)])
    await store.cleanup_old_events()
    assert store.events == []


@pytest.mark.asyncio
async def test_retention_keeps_everything_when_none_expired():
    store = main.EventStore(max_events=1000, retention_hours=24)
    await store.add_events([ev(f"n{i}") for i in range(4)])
    await store.cleanup_old_events()
    assert len(store.events) == 4


@pytest.mark.asyncio
async def test_retention_handles_naive_timestamps():
    store = main.EventStore(max_events=1000, retention_hours=1)
    await store.add_events([ev("naive_old", hours_ago=4, naive=True)])
    await store.cleanup_old_events()
    assert store.events == []


@pytest.mark.asyncio
async def test_aggregate_metrics_averages_numeric_fields(store):
    await store.add_events([
        ev("a", data={"cpu_usage": 10.0, "label": "ignored"}),
        ev("b", data={"cpu_usage": 20.0}),
    ])
    by_metric = {a.metric: a for a in await store.aggregate_metrics("metric")}
    assert by_metric["cpu_usage"].value == pytest.approx(15.0)
    assert by_metric["cpu_usage"].count == 2
    assert "label" not in by_metric


@pytest.mark.asyncio
async def test_aggregate_metrics_empty_for_unknown_type(store):
    assert await store.aggregate_metrics("nope") == []
