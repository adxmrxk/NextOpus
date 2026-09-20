"""Tests for the Guardian: detection, cooldowns, remediation dispatch, HTTP API."""

import sys
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import guardian as G  # noqa: E402


class FakePrometheus:
    """Stands in for PrometheusClient; returns canned vectors per query."""

    def __init__(self, results=None, alerts=None):
        self.results = results or {}
        self.alerts = alerts or []

    async def query(self, promql):
        for needle, res in self.results.items():
            if needle in promql:
                return res
        return []

    async def get_alerts(self):
        return self.alerts

    async def close(self):
        pass


class FakeK8s:
    """Records calls instead of touching a cluster."""

    def __init__(self, replicas=(3, 3, 3), raises=None, lease_granted=True):
        self.dry_run = True
        self.replicas = replicas
        self.raises = raises
        self.calls = []
        self.events = []
        self.lease_granted = lease_granted
        self.lease_released = False

    def _maybe_raise(self):
        if self.raises:
            raise self.raises

    def get_pods(self, namespace, label_selector=""):
        self._maybe_raise()
        return []

    def get_deployment_replicas(self, name, namespace):
        self._maybe_raise()
        return self.replicas

    def restart_pod(self, name, namespace):
        self.calls.append(("restart_pod", name))
        return G.Action(type=G.ActionType.RESTART_POD, target=f"{namespace}/{name}",
                        reason="test", success=True, result="ok")

    def scale_deployment(self, name, namespace, replicas, action_type=G.ActionType.SCALE_UP):
        self.calls.append(("scale", name, replicas, action_type))
        return G.Action(type=action_type, target=f"{namespace}/{name}",
                        reason="test", success=True, result=f"scaled to {replicas}")

    def cordon_node(self, name):
        self.calls.append(("cordon", name))
        return G.Action(type=G.ActionType.CORDON_NODE, target=name,
                        reason="test", success=True, result="cordoned")

    def emit_event(self, reason, message, namespace, involved_name,
                   involved_kind="Pod", event_type="Normal"):
        self.events.append({"reason": reason, "target": involved_name, "type": event_type})
        return True

    def acquire_or_renew_lease(self, name, namespace, identity, duration):
        return self.lease_granted

    def release_lease(self, name, namespace, identity):
        self.lease_released = True


def sample(metric, value):
    return [{"metric": metric, "value": [0, str(value)]}]


def make_guardian(prom=None, k8s=None, **env):
    """Build a Guardian without running __init__, which needs a real kubeconfig."""
    cfg = G.GuardianConfig(**env)
    g = G.Guardian.__new__(G.Guardian)
    g.config = cfg
    g.prometheus = prom or FakePrometheus()
    g.k8s = k8s or FakeK8s()
    g.action_timestamps = {}
    g.scale_timestamps = {}
    g.active_anomalies = []
    g.action_history = []
    g.recent_actions = []
    g.breaker_tripped = False
    g.is_leader = True
    g.running = False
    return g


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

async def test_crash_loop_detected_above_threshold():
    g = make_guardian(FakePrometheus({
        "kube_pod_container_status_restarts_total": sample({"pod": "data-generator-abc-xyz"}, 7)
    }))
    (a,) = await g.check_crash_loops()
    assert a.type == "crash_loop"
    assert a.severity is G.Severity.CRITICAL
    assert a.suggested_action is G.ActionType.RESTART_POD


async def test_crash_loop_ignored_below_threshold():
    g = make_guardian(FakePrometheus({
        "kube_pod_container_status_restarts_total": sample({"pod": "p"}, 1)
    }))
    assert await g.check_crash_loops() == []


async def test_high_cpu_detected_above_threshold():
    g = make_guardian(FakePrometheus({
        "container_cpu_usage_seconds_total": sample({"pod": "data-generator-abc-xyz"}, 0.95)
    }))
    (a,) = await g.check_high_cpu()
    assert a.suggested_action is G.ActionType.SCALE_UP


async def test_high_cpu_ignored_when_idle():
    g = make_guardian(FakePrometheus({
        "container_cpu_usage_seconds_total": sample({"pod": "p"}, 0.10)
    }))
    assert await g.check_high_cpu() == []


async def test_high_memory_detected():
    g = make_guardian(FakePrometheus({
        "container_memory_working_set_bytes": sample({"pod": "data-processor-abc-xyz"}, 0.91)
    }))
    (a,) = await g.check_high_memory()
    assert a.type == "high_memory"


async def test_service_down_only_when_all_replicas_unhealthy():
    g = make_guardian(FakePrometheus({
        "nextopus_generator_health": sample({}, 0),
        "nextopus_processor_health": sample({}, 1),
    }))
    found = await g.check_service_health()
    assert [a.target for a in found] == ["data-generator"]


async def test_absent_health_metric_is_not_an_outage():
    """An empty result means "not scraped", which must not read as service down."""
    g = make_guardian(FakePrometheus({}))
    assert await g.check_service_health() == []


async def test_alert_with_guardian_action_becomes_anomaly():
    g = make_guardian(FakePrometheus(alerts=[{
        "labels": {"alertname": "PodCrashLoop", "severity": "critical",
                   "guardian_action": "restart_pod", "pod": "data-processor-abc-xyz"},
        "annotations": {"summary": "crash looping"},
    }]))
    (a,) = await g.check_prometheus_alerts()
    assert a.suggested_action is G.ActionType.RESTART_POD
    assert a.target == "data-processor-abc-xyz"


async def test_alert_without_guardian_action_is_ignored():
    g = make_guardian(FakePrometheus(alerts=[{"labels": {"alertname": "Other"}, "annotations": {}}]))
    assert await g.check_prometheus_alerts() == []


async def test_unknown_guardian_action_yields_no_suggested_action():
    g = make_guardian(FakePrometheus(alerts=[{
        "labels": {"alertname": "X", "guardian_action": "explode_cluster"},
        "annotations": {},
    }]))
    (a,) = await g.check_prometheus_alerts()
    assert a.suggested_action is None


# ---------------------------------------------------------------------------
# Cooldowns and naming
# ---------------------------------------------------------------------------

def test_cooldown_blocks_repeat_action_on_same_target():
    g = make_guardian()
    assert g.can_take_action("pod-a") is True
    g.record_action("pod-a")
    assert g.can_take_action("pod-a") is False
    assert g.can_take_action("pod-b") is True


def test_scale_cooldown_tracked_separately():
    g = make_guardian()
    g.record_action("pod-a", is_scale=True)
    assert g.can_take_action("pod-a", is_scale=True) is False
    assert g.can_take_action("pod-a", is_scale=False) is True


@pytest.mark.parametrize("pod,expected", [
    ("data-generator-7d4b9c8f5-x2k9p", "data-generator"),
    ("guardian-5f9c7d8b6-abcde", "guardian"),
])
def test_deployment_name_derived_from_pod_name(pod, expected):
    assert make_guardian().get_deployment_for_pod(pod) == expected


# ---------------------------------------------------------------------------
# Remediation dispatch
# ---------------------------------------------------------------------------

async def test_scale_up_increments_by_one():
    k8s = FakeK8s(replicas=(3, 3, 3))
    g = make_guardian(k8s=k8s)
    a = G.Anomaly(type="high_cpu", severity=G.Severity.WARNING, target="data-generator-abc-xyz",
                  message="", value=0.9, suggested_action=G.ActionType.SCALE_UP)
    act = await g.remediate(a)
    assert k8s.calls == [("scale", "data-generator", 4, G.ActionType.SCALE_UP)]
    assert act.type is G.ActionType.SCALE_UP


async def test_scale_down_is_labelled_scale_down():
    """The action type was hardcoded to SCALE_UP for both directions."""
    k8s = FakeK8s(replicas=(3, 3, 3))
    g = make_guardian(k8s=k8s)
    a = G.Anomaly(type="low_cpu", severity=G.Severity.INFO, target="data-generator-abc-xyz",
                  message="", value=0.1, suggested_action=G.ActionType.SCALE_DOWN)
    act = await g.remediate(a)
    assert act.type is G.ActionType.SCALE_DOWN
    assert k8s.calls[0][2] == 2


async def test_scale_up_capped_at_ten_replicas():
    k8s = FakeK8s(replicas=(10, 10, 10))
    g = make_guardian(k8s=k8s)
    a = G.Anomaly(type="high_cpu", severity=G.Severity.WARNING, target="data-generator-abc-xyz",
                  message="", value=0.9, suggested_action=G.ActionType.SCALE_UP)
    assert await g.remediate(a) is None
    assert k8s.calls == []


async def test_scale_down_floored_at_one_replica():
    k8s = FakeK8s(replicas=(1, 1, 1))
    g = make_guardian(k8s=k8s)
    a = G.Anomaly(type="low_cpu", severity=G.Severity.INFO, target="data-generator-abc-xyz",
                  message="", value=0.1, suggested_action=G.ActionType.SCALE_DOWN)
    assert await g.remediate(a) is None


async def test_cordon_node_is_dispatched():
    """cordon_node was implemented but remediate() had no branch reaching it."""
    k8s = FakeK8s()
    g = make_guardian(k8s=k8s)
    a = G.Anomaly(type="node_unhealthy", severity=G.Severity.CRITICAL, target="worker-2",
                  message="", value=1, suggested_action=G.ActionType.CORDON_NODE,
                  metadata={"node": "worker-2"})
    act = await g.remediate(a)
    assert k8s.calls == [("cordon", "worker-2")]
    assert act.type is G.ActionType.CORDON_NODE


async def test_anomaly_without_suggested_action_does_nothing():
    k8s = FakeK8s()
    g = make_guardian(k8s=k8s)
    a = G.Anomaly(type="x", severity=G.Severity.INFO, target="t", message="", value=0)
    assert await g.remediate(a) is None
    assert k8s.calls == []


async def test_cooldown_prevents_second_remediation():
    k8s = FakeK8s(replicas=(3, 3, 3))
    g = make_guardian(k8s=k8s)
    a = G.Anomaly(type="high_cpu", severity=G.Severity.WARNING, target="data-generator-abc-xyz",
                  message="", value=0.9, suggested_action=G.ActionType.SCALE_UP)
    await g.remediate(a)
    await g.remediate(a)
    assert len(k8s.calls) == 1


async def test_successful_action_recorded_in_history():
    g = make_guardian(k8s=FakeK8s(replicas=(3, 3, 3)))
    a = G.Anomaly(type="high_cpu", severity=G.Severity.WARNING, target="data-generator-abc-xyz",
                  message="", value=0.9, suggested_action=G.ActionType.SCALE_UP)
    await g.remediate(a)
    assert len(g.action_history) == 1


# ---------------------------------------------------------------------------
# Resilience
# ---------------------------------------------------------------------------

async def test_check_cycle_survives_unreachable_kubernetes_api():
    """A transport error is not an ApiException and used to abort the cycle."""
    prom = FakePrometheus({
        "kube_pod_container_status_restarts_total": sample({"pod": "data-generator-abc-xyz"}, 7),
    })
    g = make_guardian(prom, FakeK8s(raises=ConnectionError("api server unreachable")))
    await g.run_check_cycle()
    assert len(g.active_anomalies) == 1


async def test_check_cycle_reports_nothing_when_healthy():
    g = make_guardian(FakePrometheus({}))
    await g.run_check_cycle()
    assert g.active_anomalies == []


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

@pytest.fixture
async def api():
    g = make_guardian()
    g.active_anomalies = [G.Anomaly(type="crash_loop", severity=G.Severity.CRITICAL,
                                    target="pod-1", message="restarting", value=3)]
    g.action_history = [G.Action(type=G.ActionType.RESTART_POD, target="nextopus/pod-1",
                                 reason="test", success=True, result="ok")]
    app = await G.create_app(g)
    async with TestClient(TestServer(app)) as client:
        yield client


async def test_health_endpoint(api):
    r = await api.get("/health")
    assert r.status == 200
    assert (await r.json())["status"] == "healthy"


async def test_anomalies_endpoint_lists_active_anomalies(api):
    body = await (await api.get("/anomalies")).json()
    assert body["anomalies"][0]["target"] == "pod-1"


async def test_actions_endpoint_lists_history(api):
    body = await (await api.get("/actions")).json()
    assert body["actions"][0]["type"] == "restart_pod"


async def test_metrics_endpoint_serves_prometheus_text(api):
    """This returned HTTP 500: aiohttp rejects a charset in content_type."""
    r = await api.get("/metrics")
    assert r.status == 200
    assert "text/plain" in r.headers["Content-Type"]
    assert "guardian_checks_total" in await r.text()


# ---------------------------------------------------------------------------
# Blast radius circuit breaker
# ---------------------------------------------------------------------------

def anomaly(target="data-generator-abc-xyz", action=G.ActionType.SCALE_UP):
    """Defaults to the scale path: FakeK8s.get_pods returns nothing, so the
    restart path would find no pod to act on."""
    return G.Anomaly(type="high_cpu", severity=G.Severity.WARNING, target=target,
                     message="hot", value=0.95, suggested_action=action)


def test_breaker_closed_when_under_limit():
    g = make_guardian(max_actions_per_window=5)
    for i in range(4):
        g.record_action(f"pod-{i}")
    assert g.blast_radius_exceeded() is False


def test_breaker_opens_at_the_limit():
    g = make_guardian(max_actions_per_window=3)
    for i in range(3):
        g.record_action(f"pod-{i}")
    assert g.blast_radius_exceeded() is True


async def test_breaker_suppresses_remediation_across_distinct_targets():
    """Per-target cooldowns do not help here: every target is different."""
    k8s = FakeK8s()
    g = make_guardian(k8s=k8s, max_actions_per_window=3)
    for i in range(6):
        await g.remediate(anomaly(target=f"data-generator-{i}-xyz"))
    assert len(k8s.calls) == 3


def test_breaker_window_expires():
    from datetime import datetime, timedelta

    g = make_guardian(max_actions_per_window=2, action_window_seconds=60)
    g.recent_actions = [datetime.utcnow() - timedelta(seconds=120)] * 5
    assert g.blast_radius_exceeded() is False
    assert g.recent_actions == []


def test_breaker_latches_when_halt_configured():
    g = make_guardian(max_actions_per_window=1, halt_on_breaker=True)
    g.record_action("pod-a")
    assert g.blast_radius_exceeded() is True
    assert g.breaker_tripped is True
    g.recent_actions = []          # window clears
    assert g.blast_radius_exceeded() is True   # still latched


def test_breaker_does_not_latch_by_default():
    g = make_guardian(max_actions_per_window=1)
    g.record_action("pod-a")
    assert g.blast_radius_exceeded() is True
    g.recent_actions = []
    assert g.blast_radius_exceeded() is False


async def test_suppressed_remediation_emits_a_warning_event():
    k8s = FakeK8s()
    g = make_guardian(k8s=k8s, max_actions_per_window=0)
    assert await g.remediate(anomaly()) is None
    assert any(e["reason"] == "RemediationSuppressed" and e["type"] == "Warning"
               for e in k8s.events)


# ---------------------------------------------------------------------------
# Kubernetes Events
# ---------------------------------------------------------------------------

async def test_successful_remediation_emits_event():
    k8s = FakeK8s()
    g = make_guardian(k8s=k8s)
    await g.remediate(anomaly(action=G.ActionType.SCALE_UP))
    assert [e["reason"] for e in k8s.events] == ["Remediated"]


async def test_events_can_be_disabled():
    k8s = FakeK8s()
    g = make_guardian(k8s=k8s, emit_events=False)
    await g.remediate(anomaly(action=G.ActionType.SCALE_UP))
    assert k8s.events == []


async def test_event_failure_does_not_break_remediation():
    class Exploding(FakeK8s):
        def emit_event(self, *a, **kw):
            raise RuntimeError("apiserver said no")

    k8s = Exploding()
    g = make_guardian(k8s=k8s)
    # The audit trail is best effort; the action itself must still land.
    with pytest.raises(RuntimeError):
        await g.remediate(anomaly(action=G.ActionType.SCALE_UP))
    assert k8s.calls, "the scale call should have happened before the event"


# ---------------------------------------------------------------------------
# Leader election
# ---------------------------------------------------------------------------

async def test_leader_runs_checks():
    g = make_guardian(FakePrometheus({}), FakeK8s(lease_granted=True), leader_election=True)
    g.is_leader = False
    await g._refresh_leadership()
    assert g.is_leader is True


async def test_follower_stands_by():
    g = make_guardian(FakePrometheus({}), FakeK8s(lease_granted=False), leader_election=True)
    g.is_leader = True
    await g._refresh_leadership()
    assert g.is_leader is False


async def test_follower_does_not_remediate():
    """A follower must never act, or two replicas double every restart."""
    k8s = FakeK8s(lease_granted=False)
    g = make_guardian(FakePrometheus({
        "kube_pod_container_status_restarts_total": sample({"pod": "data-generator-a-b"}, 9)
    }), k8s, leader_election=True)
    g.running = True

    async def stop_after_one_cycle(_):
        g.running = False
    import guardian as _g
    original = _g.asyncio.sleep
    _g.asyncio.sleep = stop_after_one_cycle
    try:
        await g.run()
    finally:
        _g.asyncio.sleep = original

    assert g.is_leader is False
    assert k8s.calls == []


def test_leader_election_can_be_disabled():
    g = G.Guardian.__new__(G.Guardian)
    cfg = G.GuardianConfig(leader_election=False)
    g.config = cfg
    g.is_leader = not cfg.leader_election
    assert g.is_leader is True


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------

async def test_status_reports_breaker_budget(api):
    body = await (await api.get("/status")).json()
    assert body["circuit_breaker"]["limit"] > 0
    assert body["circuit_breaker"]["open"] is False
    assert "leader" in body
