"""
NextOpus Guardian - Autonomous Self-Healing Controller

This script monitors the Kubernetes cluster via Prometheus metrics and
automatically takes corrective actions when anomalies are detected.

Actions include:
- Restarting unhealthy pods
- Scaling deployments up/down based on resource usage
- Cordoning/uncordoning nodes
- Sending notifications

The Guardian is the "immune system" of the NextOpus platform.
"""

import asyncio
import logging
import os
import signal
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
from kubernetes import client, config
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from aiohttp import web

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("guardian")


# ==============================================================================
# Configuration
# ==============================================================================

@dataclass
class GuardianConfig:
    prometheus_url: str = os.getenv("PROMETHEUS_URL", "http://prometheus-kube-prometheus-prometheus.observability:9090")
    check_interval: int = int(os.getenv("CHECK_INTERVAL", "30"))
    namespace: str = os.getenv("WATCH_NAMESPACE", "nextopus")
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"
    port: int = int(os.getenv("PORT", "8080"))

    # Thresholds
    cpu_scale_up_threshold: float = float(os.getenv("CPU_SCALE_UP_THRESHOLD", "0.8"))
    memory_scale_up_threshold: float = float(os.getenv("MEMORY_SCALE_UP_THRESHOLD", "0.85"))
    restart_count_threshold: int = int(os.getenv("RESTART_COUNT_THRESHOLD", "3"))

    # Cooldown periods (seconds)
    action_cooldown: int = int(os.getenv("ACTION_COOLDOWN", "300"))
    scale_cooldown: int = int(os.getenv("SCALE_COOLDOWN", "600"))

    # Predictive remediation. Thresholds only fire once a pod is already in
    # trouble; predict_linear extrapolates the current trend so the Guardian can
    # act before the OOM kill rather than restarting after it.
    predictive: bool = os.getenv("PREDICTIVE", "true").lower() == "true"
    predict_horizon_seconds: int = int(os.getenv("PREDICT_HORIZON_SECONDS", "1800"))
    predict_lookback: str = os.getenv("PREDICT_LOOKBACK", "30m")
    # Only act on a forecast this confident it will breach, to avoid chasing
    # noise. 0.95 means predicted to reach 95% of the limit.
    predict_memory_threshold: float = float(os.getenv("PREDICT_MEMORY_THRESHOLD", "0.95"))

    # Blast radius. Per-target cooldowns do not help during a cluster-wide
    # incident, where every target is distinct and the Guardian would happily
    # restart everything at once. This caps total actions across all targets.
    max_actions_per_window: int = int(os.getenv("MAX_ACTIONS_PER_WINDOW", "10"))
    action_window_seconds: int = int(os.getenv("ACTION_WINDOW_SECONDS", "600"))
    # Trips the breaker permanently until a human restarts the Guardian.
    halt_on_breaker: bool = os.getenv("HALT_ON_BREAKER", "false").lower() == "true"

    # Kubernetes Events give operators an audit trail in `kubectl describe`
    # rather than only a log line inside a pod that may get deleted.
    emit_events: bool = os.getenv("EMIT_EVENTS", "true").lower() == "true"

    # Leader election. More than one replica remediating the same anomaly
    # means double restarts and double scale-ups.
    leader_election: bool = os.getenv("LEADER_ELECTION", "true").lower() == "true"
    lease_name: str = os.getenv("LEASE_NAME", "guardian-leader")
    lease_namespace: str = os.getenv("LEASE_NAMESPACE", "nextopus-system")
    lease_duration: int = int(os.getenv("LEASE_DURATION", "30"))
    identity: str = os.getenv("HOSTNAME", "guardian-local")


# ==============================================================================
# Metrics
# ==============================================================================

CHECKS_TOTAL = Counter(
    'guardian_checks_total',
    'Total health checks performed'
)

ANOMALIES_DETECTED = Counter(
    'guardian_anomalies_detected_total',
    'Total anomalies detected',
    ['type', 'severity']
)

PREDICTED_BREACHES = Counter(
    'guardian_predicted_breaches_total',
    'Resource limit breaches forecast before they happened',
    ['resource']
)

PREDICTED_SECONDS_TO_BREACH = Gauge(
    'guardian_predicted_seconds_to_breach',
    'Forecast seconds until a pod reaches its limit',
    ['pod', 'resource']
)

ACTIONS_TAKEN = Counter(
    'guardian_actions_taken_total',
    'Total remediation actions taken',
    ['action', 'target']
)

ACTIONS_FAILED = Counter(
    'guardian_actions_failed_total',
    'Total failed remediation actions',
    ['action', 'target']
)

HEALTH_STATUS = Gauge(
    'guardian_health',
    'Guardian health status (1 = healthy, 0 = unhealthy)'
)

CHECK_DURATION = Histogram(
    'guardian_check_duration_seconds',
    'Duration of health check cycles',
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0]
)

ACTIONS_SUPPRESSED = Counter(
    'guardian_actions_suppressed_total',
    'Remediations not taken, by reason',
    ['reason']
)

BREAKER_OPEN = Gauge(
    'guardian_circuit_breaker_open',
    'Blast-radius breaker state (1 = open, actions suppressed)'
)

IS_LEADER = Gauge(
    'guardian_is_leader',
    'Whether this replica currently holds the leader lease'
)

EVENTS_EMITTED = Counter(
    'guardian_events_emitted_total',
    'Kubernetes Events written for remediation decisions',
    ['reason']
)


# ==============================================================================
# Data Models
# ==============================================================================

class ActionType(Enum):
    RESTART_POD = "restart_pod"
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    CORDON_NODE = "cordon_node"
    ROLLBACK_CANARY = "rollback_canary"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Anomaly:
    type: str
    severity: Severity
    target: str
    message: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    suggested_action: Optional[ActionType] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Action:
    type: ActionType
    target: str
    reason: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    success: bool = False
    result: str = ""


# ==============================================================================
# Prometheus Client
# ==============================================================================

class PrometheusClient:
    """Client for querying Prometheus metrics."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.client = httpx.AsyncClient(timeout=10.0)

    async def query(self, promql: str) -> List[Dict[str, Any]]:
        """Execute a PromQL instant query."""
        try:
            response = await self.client.get(
                f"{self.url}/api/v1/query",
                params={"query": promql}
            )
            response.raise_for_status()
            data = response.json()

            if data["status"] == "success":
                return data["data"]["result"]
            else:
                logger.error(f"Prometheus query failed: {data}")
                return []
        except Exception as e:
            logger.error(f"Prometheus query error: {e}")
            return []

    async def get_alerts(self) -> List[Dict[str, Any]]:
        """Get active alerts from Prometheus."""
        try:
            response = await self.client.get(f"{self.url}/api/v1/alerts")
            response.raise_for_status()
            data = response.json()

            if data["status"] == "success":
                return data["data"]["alerts"]
            return []
        except Exception as e:
            logger.error(f"Error fetching alerts: {e}")
            return []

    async def close(self):
        await self.client.aclose()


# ==============================================================================
# Kubernetes Controller
# ==============================================================================

class KubernetesController:
    """Controller for Kubernetes operations."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

        # Try in-cluster config first, then local kubeconfig
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            config.load_kube_config()
            logger.info("Loaded local kubeconfig")

        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.coordination_v1 = client.CoordinationV1Api()
        self.custom_objects = client.CustomObjectsApi()

    def restart_pod(self, name: str, namespace: str) -> Action:
        """Delete a pod to trigger restart."""
        action = Action(
            type=ActionType.RESTART_POD,
            target=f"{namespace}/{name}",
            reason="Anomaly detected - restarting pod"
        )

        if self.dry_run:
            action.success = True
            action.result = "DRY RUN - would delete pod"
            logger.info(f"[DRY RUN] Would restart pod {namespace}/{name}")
            return action

        try:
            self.core_v1.delete_namespaced_pod(
                name=name,
                namespace=namespace,
                grace_period_seconds=30
            )
            action.success = True
            action.result = f"Pod {name} deleted for restart"
            logger.info(f"Restarted pod {namespace}/{name}")
            ACTIONS_TAKEN.labels(action="restart_pod", target=namespace).inc()
        except Exception as e:
            reason = getattr(e, "reason", e)
            action.result = f"Failed to restart pod: {reason}"
            logger.error(f"Failed to restart pod {namespace}/{name}: {e}")
            ACTIONS_FAILED.labels(action="restart_pod", target=namespace).inc()

        return action

    def scale_deployment(
        self,
        name: str,
        namespace: str,
        replicas: int,
        action_type: ActionType = ActionType.SCALE_UP,
    ) -> Action:
        """Scale a deployment to specified replicas."""
        action = Action(
            type=action_type,
            target=f"{namespace}/{name}",
            reason=f"Scaling to {replicas} replicas"
        )

        if self.dry_run:
            action.success = True
            action.result = f"DRY RUN - would scale to {replicas}"
            logger.info(f"[DRY RUN] Would scale {namespace}/{name} to {replicas}")
            return action

        try:
            # Get current deployment
            deployment = self.apps_v1.read_namespaced_deployment(name, namespace)
            current_replicas = deployment.spec.replicas

            # Apply scale
            deployment.spec.replicas = replicas
            self.apps_v1.patch_namespaced_deployment(name, namespace, deployment)

            action.success = True
            action.result = f"Scaled from {current_replicas} to {replicas}"
            logger.info(f"Scaled {namespace}/{name} from {current_replicas} to {replicas}")
            ACTIONS_TAKEN.labels(action="scale", target=namespace).inc()
        except Exception as e:
            reason = getattr(e, "reason", e)
            action.result = f"Failed to scale: {reason}"
            logger.error(f"Failed to scale {namespace}/{name}: {e}")
            ACTIONS_FAILED.labels(action="scale", target=namespace).inc()

        return action

    def get_deployment_replicas(self, name: str, namespace: str) -> tuple:
        """Get current and desired replicas for a deployment."""
        try:
            deployment = self.apps_v1.read_namespaced_deployment(name, namespace)
            return (
                deployment.status.ready_replicas or 0,
                deployment.spec.replicas,
                deployment.status.replicas or 0
            )
        except Exception as e:
            logger.error(f"Failed to get deployment info: {e}")
            return (0, 0, 0)

    def cordon_node(self, name: str) -> Action:
        """Mark a node as unschedulable."""
        action = Action(
            type=ActionType.CORDON_NODE,
            target=name,
            reason="Cordoning unhealthy node"
        )

        if self.dry_run:
            action.success = True
            action.result = "DRY RUN - would cordon node"
            return action

        try:
            body = {"spec": {"unschedulable": True}}
            self.core_v1.patch_node(name, body)
            action.success = True
            action.result = f"Node {name} cordoned"
            logger.info(f"Cordoned node {name}")
            ACTIONS_TAKEN.labels(action="cordon", target="node").inc()
        except Exception as e:
            reason = getattr(e, "reason", e)
            action.result = f"Failed to cordon: {reason}"
            logger.error(f"Failed to cordon {name}: {e}")
            ACTIONS_FAILED.labels(action="cordon", target="node").inc()

        return action

    def emit_event(
        self,
        reason: str,
        message: str,
        namespace: str,
        involved_name: str,
        involved_kind: str = "Pod",
        event_type: str = "Normal",
    ) -> bool:
        """Write a Kubernetes Event so the decision shows up in kubectl.

        The ClusterRole already grants events create/patch. Without this the
        only record of a remediation is a log line inside a pod that may be
        deleted moments later.
        """
        now = datetime.now(timezone.utc)
        body = client.CoreV1Event(
            metadata=client.V1ObjectMeta(
                generate_name=f"guardian-{reason.lower()}-",
                namespace=namespace,
            ),
            involved_object=client.V1ObjectReference(
                kind=involved_kind,
                name=involved_name,
                namespace=namespace,
            ),
            reason=reason,
            message=message,
            type=event_type,
            source=client.V1EventSource(component="nextopus-guardian"),
            first_timestamp=now,
            last_timestamp=now,
            count=1,
        )
        try:
            self.core_v1.create_namespaced_event(namespace=namespace, body=body)
            EVENTS_EMITTED.labels(reason=reason).inc()
            return True
        except Exception as e:
            # An audit trail failing must never stop the remediation itself.
            logger.warning(f"Could not emit event {reason} for {namespace}/{involved_name}: {e}")
            return False

    # -- Leader election ------------------------------------------------------

    def acquire_or_renew_lease(self, name: str, namespace: str, identity: str,
                               duration: int) -> bool:
        """Try to hold the leader Lease. Returns True if this replica leads.

        Standard Kubernetes lease semantics: take it if it does not exist, renew
        it if we already hold it, steal it only once it has visibly expired.
        """
        now = datetime.now(timezone.utc)
        try:
            lease = self.coordination_v1.read_namespaced_lease(name, namespace)
        except Exception as e:
            if getattr(e, "status", None) != 404:
                logger.error(f"Lease read failed: {e}")
                return False
            lease = None

        if lease is None:
            body = client.V1Lease(
                metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                spec=client.V1LeaseSpec(
                    holder_identity=identity,
                    lease_duration_seconds=duration,
                    acquire_time=now,
                    renew_time=now,
                ),
            )
            try:
                self.coordination_v1.create_namespaced_lease(namespace, body)
                logger.info(f"Acquired leader lease {namespace}/{name} as {identity}")
                return True
            except Exception as e:
                logger.info(f"Lost the race to create the lease: {e}")
                return False

        spec = lease.spec
        holder = spec.holder_identity
        renewed = spec.renew_time
        ttl = spec.lease_duration_seconds or duration

        if holder == identity:
            spec.renew_time = now
            try:
                self.coordination_v1.replace_namespaced_lease(name, namespace, lease)
                return True
            except Exception as e:
                logger.warning(f"Lease renewal failed: {e}")
                return False

        # Someone else holds it. Only take over once it has actually expired.
        if renewed is not None:
            age = (now - renewed).total_seconds()
            if age < ttl:
                return False
            logger.info(f"Lease held by {holder} expired {age:.0f}s ago, taking over")

        spec.holder_identity = identity
        spec.acquire_time = now
        spec.renew_time = now
        spec.lease_duration_seconds = duration
        try:
            self.coordination_v1.replace_namespaced_lease(name, namespace, lease)
            logger.info(f"Took over leader lease {namespace}/{name} as {identity}")
            return True
        except Exception as e:
            logger.info(f"Lease takeover failed, another replica won: {e}")
            return False

    def release_lease(self, name: str, namespace: str, identity: str) -> None:
        """Give up leadership on shutdown so a peer takes over immediately."""
        try:
            lease = self.coordination_v1.read_namespaced_lease(name, namespace)
            if lease.spec.holder_identity != identity:
                return
            lease.spec.holder_identity = ""
            lease.spec.renew_time = None
            self.coordination_v1.replace_namespaced_lease(name, namespace, lease)
            logger.info("Released leader lease")
        except Exception as e:
            logger.debug(f"Lease release skipped: {e}")

    def rollback_canary(self, name: str, namespace: str) -> Action:
        """Send all traffic back to the stable subset.

        A rollback is a weight change on the VirtualService, not a redeploy, so
        it takes effect as fast as Istio can push config rather than as fast as
        pods can restart.
        """
        action = Action(
            type=ActionType.ROLLBACK_CANARY,
            target=f"{namespace}/{name}",
            reason="Canary error rate above gate",
        )

        if self.dry_run:
            action.success = True
            action.result = "DRY RUN - would shift traffic back to stable"
            logger.info(f"[DRY RUN] Would roll back canary {namespace}/{name}")
            return action

        try:
            vs = self.custom_objects.get_namespaced_custom_object(
                group="networking.istio.io", version="v1beta1",
                namespace=namespace, plural="virtualservices", name=name,
            )

            changed = False
            for route in vs.get("spec", {}).get("http", []):
                destinations = route.get("route", [])
                # Skip the header-pinned rule: that one is for deliberate
                # testing and should keep working during a rollback.
                if len(destinations) < 2:
                    continue
                for dest in destinations:
                    subset = dest.get("destination", {}).get("subset")
                    if subset == "stable" and dest.get("weight") != 100:
                        dest["weight"] = 100
                        changed = True
                    elif subset == "canary" and dest.get("weight") != 0:
                        dest["weight"] = 0
                        changed = True

            if not changed:
                action.success = True
                action.result = "Already fully on stable, nothing to roll back"
                return action

            self.custom_objects.patch_namespaced_custom_object(
                group="networking.istio.io", version="v1beta1",
                namespace=namespace, plural="virtualservices", name=name, body=vs,
            )
            action.success = True
            action.result = "Traffic shifted back to stable (canary weight 0)"
            logger.info(f"Rolled back canary {namespace}/{name}")
            ACTIONS_TAKEN.labels(action="rollback_canary", target=namespace).inc()
        except Exception as e:
            reason = getattr(e, "reason", e)
            action.result = f"Failed to roll back canary: {reason}"
            logger.error(f"Canary rollback failed for {namespace}/{name}: {e}")
            ACTIONS_FAILED.labels(action="rollback_canary", target=namespace).inc()

        return action

    def get_pods(self, namespace: str, label_selector: str = "") -> List:
        """Get pods in namespace."""
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector
            )
            return pods.items
        except Exception as e:
            logger.error(f"Failed to list pods: {e}")
            return []


# ==============================================================================
# Guardian - Main Controller
# ==============================================================================

class Guardian:
    """The autonomous self-healing controller."""

    def __init__(self, config: GuardianConfig):
        self.config = config
        self.prometheus = PrometheusClient(config.prometheus_url)
        self.k8s = KubernetesController(dry_run=config.dry_run)

        # Track action cooldowns
        self.action_timestamps: Dict[str, datetime] = {}
        self.scale_timestamps: Dict[str, datetime] = {}

        # Track detected anomalies
        self.active_anomalies: List[Anomaly] = []
        self.action_history: List[Action] = []

        # Timestamps of recent actions across every target, for blast radius.
        self.recent_actions: List[datetime] = []
        self.breaker_tripped = False

        self.is_leader = not config.leader_election

        self.running = False

    def can_take_action(self, target: str, is_scale: bool = False) -> bool:
        """Check if an action can be taken (cooldown check)."""
        timestamps = self.scale_timestamps if is_scale else self.action_timestamps
        cooldown = self.config.scale_cooldown if is_scale else self.config.action_cooldown

        last_action = timestamps.get(target)
        if last_action:
            elapsed = (datetime.utcnow() - last_action).total_seconds()
            if elapsed < cooldown:
                logger.debug(f"Action on {target} in cooldown ({elapsed:.0f}s / {cooldown}s)")
                return False
        return True

    def record_action(self, target: str, is_scale: bool = False):
        """Record that an action was taken (for cooldown tracking)."""
        timestamps = self.scale_timestamps if is_scale else self.action_timestamps
        timestamps[target] = datetime.utcnow()
        self.recent_actions.append(datetime.utcnow())

    def _prune_action_window(self) -> int:
        """Drop actions older than the window and return what is left."""
        cutoff = datetime.utcnow() - timedelta(seconds=self.config.action_window_seconds)
        self.recent_actions = [t for t in self.recent_actions if t >= cutoff]
        return len(self.recent_actions)

    def blast_radius_exceeded(self) -> bool:
        """Cap total actions across all targets, not just per target.

        Per-target cooldowns do nothing during a cluster-wide incident, where
        every target is different. Without this the Guardian would restart
        everything at once and turn a partial outage into a full one.
        """
        if self.breaker_tripped:
            return True

        count = self._prune_action_window()
        if count < self.config.max_actions_per_window:
            BREAKER_OPEN.set(0)
            return False

        logger.error(
            f"Blast radius exceeded: {count} actions in the last "
            f"{self.config.action_window_seconds}s (limit {self.config.max_actions_per_window}). "
            f"Suppressing further remediation."
        )
        BREAKER_OPEN.set(1)
        if self.config.halt_on_breaker:
            self.breaker_tripped = True
            logger.error("HALT_ON_BREAKER set: staying latched until restart")
        return True

    async def check_crash_loops(self) -> List[Anomaly]:
        """Check for pods in CrashLoopBackOff."""
        anomalies = []

        query = f'''
            increase(kube_pod_container_status_restarts_total{{namespace="{self.config.namespace}"}}[1h])
        '''
        results = await self.prometheus.query(query)

        for result in results:
            restarts = float(result["value"][1])
            if restarts >= self.config.restart_count_threshold:
                pod = result["metric"].get("pod", "unknown")
                anomalies.append(Anomaly(
                    type="crash_loop",
                    severity=Severity.CRITICAL,
                    target=pod,
                    message=f"Pod has restarted {restarts:.0f} times in the last hour",
                    value=restarts,
                    suggested_action=ActionType.RESTART_POD,
                    metadata=result["metric"]
                ))

        return anomalies

    async def check_high_cpu(self) -> List[Anomaly]:
        """Check for high CPU usage."""
        anomalies = []

        query = f'''
            sum by (pod) (rate(container_cpu_usage_seconds_total{{namespace="{self.config.namespace}"}}[5m]))
            / sum by (pod) (kube_pod_container_resource_limits{{namespace="{self.config.namespace}", resource="cpu"}})
        '''
        results = await self.prometheus.query(query)

        for result in results:
            cpu_ratio = float(result["value"][1])
            pod = result["metric"].get("pod", "unknown")

            if cpu_ratio >= self.config.cpu_scale_up_threshold:
                anomalies.append(Anomaly(
                    type="high_cpu",
                    severity=Severity.WARNING,
                    target=pod,
                    message=f"CPU usage at {cpu_ratio*100:.1f}%",
                    value=cpu_ratio,
                    suggested_action=ActionType.SCALE_UP,
                    metadata=result["metric"]
                ))

        return anomalies

    async def check_high_memory(self) -> List[Anomaly]:
        """Check for high memory usage."""
        anomalies = []

        query = f'''
            sum by (pod) (container_memory_working_set_bytes{{namespace="{self.config.namespace}"}})
            / sum by (pod) (kube_pod_container_resource_limits{{namespace="{self.config.namespace}", resource="memory"}})
        '''
        results = await self.prometheus.query(query)

        for result in results:
            mem_ratio = float(result["value"][1])
            pod = result["metric"].get("pod", "unknown")

            if mem_ratio >= self.config.memory_scale_up_threshold:
                anomalies.append(Anomaly(
                    type="high_memory",
                    severity=Severity.WARNING,
                    target=pod,
                    message=f"Memory usage at {mem_ratio*100:.1f}%",
                    value=mem_ratio,
                    suggested_action=ActionType.SCALE_UP,
                    metadata=result["metric"]
                ))

        return anomalies

    async def check_predicted_memory_exhaustion(self) -> List[Anomaly]:
        """Forecast pods heading for an OOM kill and scale before it lands.

        A threshold check fires at 85% and by then the pod may be seconds from
        being killed. predict_linear extrapolates the working-set trend over the
        lookback window, so a slow leak is caught while there is still time to
        add a replica.

        Only a rising trend counts. A pod sitting flat at 84% is not going
        anywhere and does not need action.
        """
        if not self.config.predictive:
            return []

        anomalies = []
        horizon = self.config.predict_horizon_seconds
        lookback = self.config.predict_lookback
        ns = self.config.namespace

        # Predicted usage as a fraction of the limit, horizon seconds from now.
        query = f'''
            (
              predict_linear(
                container_memory_working_set_bytes{{namespace="{ns}", container!=""}}[{lookback}],
                {horizon}
              )
              / on(pod) group_left()
              sum by (pod) (
                kube_pod_container_resource_limits{{namespace="{ns}", resource="memory"}}
              )
            )
            and
            deriv(container_memory_working_set_bytes{{namespace="{ns}", container!=""}}[{lookback}]) > 0
        '''
        results = await self.prometheus.query(query)

        for result in results:
            try:
                predicted_ratio = float(result["value"][1])
            except (ValueError, KeyError, IndexError):
                continue

            pod = result["metric"].get("pod", "unknown")

            if predicted_ratio < self.config.predict_memory_threshold:
                continue

            # Rough time-to-breach for the operator, derived from the same trend.
            seconds_to_breach = horizon
            if predicted_ratio > 0:
                seconds_to_breach = int(horizon / predicted_ratio)
            PREDICTED_SECONDS_TO_BREACH.labels(pod=pod, resource="memory").set(seconds_to_breach)
            PREDICTED_BREACHES.labels(resource="memory").inc()

            anomalies.append(Anomaly(
                type="predicted_memory_exhaustion",
                severity=Severity.WARNING,
                target=pod,
                message=(
                    f"Memory trending to {predicted_ratio * 100:.0f}% of limit "
                    f"within {horizon // 60}m; acting before the OOM kill"
                ),
                value=predicted_ratio,
                suggested_action=ActionType.SCALE_UP,
                metadata={**result["metric"], "seconds_to_breach": str(seconds_to_breach)},
            ))

        return anomalies

    async def check_service_health(self) -> List[Anomaly]:
        """Check if services are healthy."""
        anomalies = []

        # Check generator health
        results = await self.prometheus.query("nextopus_generator_health")
        total_healthy = sum(float(r["value"][1]) for r in results)
        if total_healthy == 0 and len(results) > 0:
            anomalies.append(Anomaly(
                type="service_down",
                severity=Severity.CRITICAL,
                target="data-generator",
                message="All Data Generator instances are unhealthy",
                value=0,
                suggested_action=ActionType.RESTART_POD
            ))

        # Check processor health
        results = await self.prometheus.query("nextopus_processor_health")
        total_healthy = sum(float(r["value"][1]) for r in results)
        if total_healthy == 0 and len(results) > 0:
            anomalies.append(Anomaly(
                type="service_down",
                severity=Severity.CRITICAL,
                target="data-processor",
                message="All Data Processor instances are unhealthy",
                value=0,
                suggested_action=ActionType.RESTART_POD
            ))

        return anomalies

    async def check_prometheus_alerts(self) -> List[Anomaly]:
        """Check for active Prometheus alerts with guardian_action label."""
        anomalies = []
        alerts = await self.prometheus.get_alerts()

        for alert in alerts:
            labels = alert.get("labels", {})
            if labels.get("guardian_action"):
                action_str = labels["guardian_action"]
                try:
                    action = ActionType(action_str)
                except ValueError:
                    action = None

                anomalies.append(Anomaly(
                    type=f"alert_{alert['labels'].get('alertname', 'unknown')}",
                    severity=Severity.WARNING if labels.get("severity") == "warning" else Severity.CRITICAL,
                    target=labels.get("pod", labels.get("service", "unknown")),
                    message=alert.get("annotations", {}).get("summary", "Alert triggered"),
                    value=1,
                    suggested_action=action,
                    metadata=labels
                ))

        return anomalies

    def get_deployment_for_pod(self, pod_name: str) -> Optional[str]:
        """Extract deployment name from pod name."""
        # Pod names are typically: deployment-name-replicaset-hash-pod-hash
        parts = pod_name.rsplit("-", 2)
        if len(parts) >= 2:
            return parts[0]
        return None

    async def remediate(self, anomaly: Anomaly) -> Optional[Action]:
        """Take remediation action for an anomaly."""
        if not anomaly.suggested_action:
            logger.debug(f"No suggested action for anomaly: {anomaly.type}")
            return None

        target = anomaly.target
        is_scale = anomaly.suggested_action in [ActionType.SCALE_UP, ActionType.SCALE_DOWN]

        if self.blast_radius_exceeded():
            ACTIONS_SUPPRESSED.labels(reason="blast_radius").inc()
            self._emit(
                "RemediationSuppressed",
                f"Blast radius limit reached; not acting on {anomaly.type} for {target}",
                target, event_type="Warning",
            )
            return None

        if not self.can_take_action(target, is_scale):
            logger.info(f"Skipping action on {target} - in cooldown")
            ACTIONS_SUPPRESSED.labels(reason="cooldown").inc()
            return None

        action = None

        if anomaly.suggested_action == ActionType.RESTART_POD:
            pods = self.k8s.get_pods(self.config.namespace)
            for pod in pods:
                if target in pod.metadata.name:
                    action = self.k8s.restart_pod(pod.metadata.name, self.config.namespace)
                    break

        elif anomaly.suggested_action == ActionType.SCALE_UP:
            deployment = self.get_deployment_for_pod(target)
            if deployment:
                _, desired, _ = self.k8s.get_deployment_replicas(deployment, self.config.namespace)
                # Scale up by 1, max 10
                new_replicas = min(desired + 1, 10)
                if new_replicas > desired:
                    action = self.k8s.scale_deployment(
                        deployment, self.config.namespace, new_replicas, ActionType.SCALE_UP
                    )

        elif anomaly.suggested_action == ActionType.ROLLBACK_CANARY:
            # The alert names the VirtualService via its service label.
            vs_name = anomaly.metadata.get("virtualservice") or f"{target}-canary"
            action = self.k8s.rollback_canary(vs_name, self.config.namespace)

        elif anomaly.suggested_action == ActionType.CORDON_NODE:
            node = anomaly.metadata.get("node") or target
            action = self.k8s.cordon_node(node)

        elif anomaly.suggested_action == ActionType.SCALE_DOWN:
            deployment = self.get_deployment_for_pod(target)
            if deployment:
                _, desired, _ = self.k8s.get_deployment_replicas(deployment, self.config.namespace)
                # Scale down by 1, min 1
                new_replicas = max(desired - 1, 1)
                if new_replicas < desired:
                    action = self.k8s.scale_deployment(
                        deployment, self.config.namespace, new_replicas, ActionType.SCALE_DOWN
                    )

        if action and action.success:
            self.record_action(target, is_scale)
            self.action_history.append(action)
            # Keep only last 100 actions
            self.action_history = self.action_history[-100:]
            self._emit(
                "Remediated",
                f"{action.type.value} on {action.target}: {action.result} "
                f"(triggered by {anomaly.type}: {anomaly.message})",
                target,
            )
        elif action:
            ACTIONS_SUPPRESSED.labels(reason="action_failed").inc()
            self._emit(
                "RemediationFailed",
                f"{action.type.value} on {action.target} failed: {action.result}",
                target, event_type="Warning",
            )

        return action

    def _emit(self, reason: str, message: str, target: str,
              event_type: str = "Normal") -> None:
        """Write a Kubernetes Event, if enabled. Never raises."""
        if not self.config.emit_events:
            return
        # Node-scoped actions have no namespace; attribute them to the pod
        # namespace we watch so they stay discoverable.
        self.k8s.emit_event(
            reason=reason,
            message=message,
            namespace=self.config.namespace,
            involved_name=target,
            event_type=event_type,
        )

    async def run_check_cycle(self):
        """Run a full health check cycle."""
        import time
        start = time.time()
        CHECKS_TOTAL.inc()

        logger.info("Starting health check cycle...")

        # Collect anomalies from all checks
        all_anomalies = []

        checks = [
            self.check_crash_loops(),
            self.check_high_cpu(),
            self.check_high_memory(),
            self.check_predicted_memory_exhaustion(),
            self.check_service_health(),
            self.check_prometheus_alerts(),
        ]

        results = await asyncio.gather(*checks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Check failed: {result}")
            else:
                all_anomalies.extend(result)

        # Update active anomalies
        self.active_anomalies = all_anomalies

        # Record metrics
        for anomaly in all_anomalies:
            ANOMALIES_DETECTED.labels(
                type=anomaly.type,
                severity=anomaly.severity.value
            ).inc()

        if all_anomalies:
            logger.warning(f"Detected {len(all_anomalies)} anomalies")

            # Take remediation actions
            for anomaly in all_anomalies:
                logger.info(f"Anomaly: {anomaly.type} on {anomaly.target}: {anomaly.message}")
                try:
                    action = await self.remediate(anomaly)
                except Exception as e:
                    logger.error(f"Remediation failed for {anomaly.target}: {e}")
                    continue
                if action:
                    logger.info(f"Action taken: {action.type.value} on {action.target}: {action.result}")
        else:
            logger.info("No anomalies detected")

        duration = time.time() - start
        CHECK_DURATION.observe(duration)
        logger.info(f"Check cycle completed in {duration:.2f}s")

    async def run(self):
        """Main run loop."""
        self.running = True
        HEALTH_STATUS.set(1)

        logger.info(f"Guardian starting - watching namespace: {self.config.namespace}")
        logger.info(f"Prometheus URL: {self.config.prometheus_url}")
        logger.info(f"Check interval: {self.config.check_interval}s")
        logger.info(f"Dry run: {self.config.dry_run}")

        while self.running:
            try:
                if self.config.leader_election:
                    await self._refresh_leadership()

                if self.is_leader:
                    await self.run_check_cycle()
                else:
                    logger.debug("Not leader, standing by")
            except Exception as e:
                logger.exception(f"Check cycle failed: {e}")
                HEALTH_STATUS.set(0)

            await asyncio.sleep(self.config.check_interval)

        HEALTH_STATUS.set(0)
        if self.config.leader_election and self.is_leader:
            await asyncio.to_thread(
                self.k8s.release_lease,
                self.config.lease_name, self.config.lease_namespace, self.config.identity,
            )
        await self.prometheus.close()

    async def _refresh_leadership(self) -> None:
        """Take or renew the lease. Only the leader remediates."""
        was_leader = self.is_leader
        self.is_leader = await asyncio.to_thread(
            self.k8s.acquire_or_renew_lease,
            self.config.lease_name,
            self.config.lease_namespace,
            self.config.identity,
            self.config.lease_duration,
        )
        IS_LEADER.set(1 if self.is_leader else 0)
        if self.is_leader != was_leader:
            logger.info("Became leader" if self.is_leader else "Lost leadership, standing by")

    def stop(self):
        """Stop the guardian."""
        logger.info("Guardian stopping...")
        self.running = False


# ==============================================================================
# HTTP API
# ==============================================================================

async def create_app(guardian: Guardian) -> web.Application:
    """Create the aiohttp web application."""
    app = web.Application()

    async def health(request):
        return web.json_response({
            "status": "healthy",
            "running": guardian.running,
            "namespace": guardian.config.namespace,
            "leader": guardian.is_leader,
            "identity": guardian.config.identity,
        })

    async def status(request):
        """Operational state: leadership and remaining blast-radius budget."""
        used = guardian._prune_action_window()
        limit = guardian.config.max_actions_per_window
        return web.json_response({
            "leader": guardian.is_leader,
            "identity": guardian.config.identity,
            "leader_election": guardian.config.leader_election,
            "dry_run": guardian.config.dry_run,
            "circuit_breaker": {
                "open": guardian.breaker_tripped or used >= limit,
                "latched": guardian.breaker_tripped,
                "actions_in_window": used,
                "limit": limit,
                "window_seconds": guardian.config.action_window_seconds,
                "remaining": max(0, limit - used),
            },
            "active_anomalies": len(guardian.active_anomalies),
            "actions_recorded": len(guardian.action_history),
        })

    async def metrics(request):
        # CONTENT_TYPE_LATEST carries a charset, which aiohttp refuses in the
        # content_type argument; set it as a raw header instead.
        return web.Response(
            body=generate_latest(),
            headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    async def anomalies(request):
        return web.json_response({
            "anomalies": [
                {
                    "type": a.type,
                    "severity": a.severity.value,
                    "target": a.target,
                    "message": a.message,
                    "value": a.value,
                    "timestamp": a.timestamp.isoformat()
                }
                for a in guardian.active_anomalies
            ]
        })

    async def actions(request):
        return web.json_response({
            "actions": [
                {
                    "type": a.type.value,
                    "target": a.target,
                    "reason": a.reason,
                    "success": a.success,
                    "result": a.result,
                    "timestamp": a.timestamp.isoformat()
                }
                for a in guardian.action_history[-20:]
            ]
        })

    app.router.add_get("/health", health)
    app.router.add_get("/status", status)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/anomalies", anomalies)
    app.router.add_get("/actions", actions)

    return app


# ==============================================================================
# Main
# ==============================================================================

async def main():
    config = GuardianConfig()
    guardian = Guardian(config)

    # Create web app
    app = await create_app(guardian)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.port)

    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, guardian.stop)

    # Start
    await site.start()
    logger.info(f"Guardian API listening on port {config.port}")

    await guardian.run()

    # Cleanup
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
