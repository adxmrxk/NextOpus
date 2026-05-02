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
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict

import httpx
from kubernetes import client, config
from kubernetes.client.rest import ApiException
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
    cpu_scale_down_threshold: float = float(os.getenv("CPU_SCALE_DOWN_THRESHOLD", "0.3"))
    memory_scale_up_threshold: float = float(os.getenv("MEMORY_SCALE_UP_THRESHOLD", "0.85"))
    restart_count_threshold: int = int(os.getenv("RESTART_COUNT_THRESHOLD", "3"))

    # Cooldown periods (seconds)
    action_cooldown: int = int(os.getenv("ACTION_COOLDOWN", "300"))
    scale_cooldown: int = int(os.getenv("SCALE_COOLDOWN", "600"))


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


# ==============================================================================
# Data Models
# ==============================================================================

class ActionType(Enum):
    RESTART_POD = "restart_pod"
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    CORDON_NODE = "cordon_node"
    UNCORDON_NODE = "uncordon_node"
    DELETE_POD = "delete_pod"


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

    async def query_range(
        self,
        promql: str,
        start: datetime,
        end: datetime,
        step: str = "1m"
    ) -> List[Dict[str, Any]]:
        """Execute a PromQL range query."""
        try:
            response = await self.client.get(
                f"{self.url}/api/v1/query_range",
                params={
                    "query": promql,
                    "start": start.isoformat() + "Z",
                    "end": end.isoformat() + "Z",
                    "step": step
                }
            )
            response.raise_for_status()
            data = response.json()

            if data["status"] == "success":
                return data["data"]["result"]
            else:
                logger.error(f"Prometheus range query failed: {data}")
                return []
        except Exception as e:
            logger.error(f"Prometheus range query error: {e}")
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
        except ApiException as e:
            action.result = f"Failed to restart pod: {e.reason}"
            logger.error(f"Failed to restart pod {namespace}/{name}: {e}")
            ACTIONS_FAILED.labels(action="restart_pod", target=namespace).inc()

        return action

    def scale_deployment(self, name: str, namespace: str, replicas: int) -> Action:
        """Scale a deployment to specified replicas."""
        action = Action(
            type=ActionType.SCALE_UP if replicas > 0 else ActionType.SCALE_DOWN,
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
        except ApiException as e:
            action.result = f"Failed to scale: {e.reason}"
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
        except ApiException as e:
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
        except ApiException as e:
            action.result = f"Failed to cordon: {e.reason}"
            logger.error(f"Failed to cordon {name}: {e}")
            ACTIONS_FAILED.labels(action="cordon", target="node").inc()

        return action

    def get_pods(self, namespace: str, label_selector: str = "") -> List:
        """Get pods in namespace."""
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector
            )
            return pods.items
        except ApiException as e:
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

        if not self.can_take_action(target, is_scale):
            logger.info(f"Skipping action on {target} - in cooldown")
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
                ready, desired, _ = self.k8s.get_deployment_replicas(deployment, self.config.namespace)
                # Scale up by 1, max 10
                new_replicas = min(desired + 1, 10)
                if new_replicas > desired:
                    action = self.k8s.scale_deployment(deployment, self.config.namespace, new_replicas)

        elif anomaly.suggested_action == ActionType.SCALE_DOWN:
            deployment = self.get_deployment_for_pod(target)
            if deployment:
                ready, desired, _ = self.k8s.get_deployment_replicas(deployment, self.config.namespace)
                # Scale down by 1, min 1
                new_replicas = max(desired - 1, 1)
                if new_replicas < desired:
                    action = self.k8s.scale_deployment(deployment, self.config.namespace, new_replicas)

        if action and action.success:
            self.record_action(target, is_scale)
            self.action_history.append(action)
            # Keep only last 100 actions
            self.action_history = self.action_history[-100:]

        return action

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
                action = await self.remediate(anomaly)
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
                await self.run_check_cycle()
            except Exception as e:
                logger.error(f"Check cycle failed: {e}", exc_info=True)
                HEALTH_STATUS.set(0)

            await asyncio.sleep(self.config.check_interval)

        HEALTH_STATUS.set(0)
        await self.prometheus.close()

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
            "namespace": guardian.config.namespace
        })

    async def metrics(request):
        return web.Response(
            body=generate_latest(),
            content_type=CONTENT_TYPE_LATEST
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
