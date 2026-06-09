# NextOpus

**Self-healing Kubernetes platform on Oracle Cloud Always Free Tier.**

NextOpus is a production-style cloud-native platform that provisions itself end-to-end on Oracle's always-free ARM instances: Terraform creates the infrastructure, Ansible installs and hardens a K3s cluster across four nodes, ArgoCD takes over the cluster via GitOps, and an autonomous "Guardian" controller monitors the platform through Prometheus and automatically remediates problems before a human ever sees them.

---

## Table of Contents

- [Project Overview](#project-overview)
- [The Guardian](#the-guardian)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [GitOps with ArgoCD](#gitops-with-argocd)
- [Configuration](#configuration)

---

## Project Overview

### What It Is

A reference implementation of a fully automated, self-healing Kubernetes platform that runs on infrastructure with zero monthly cost. Four ARM compute instances on Oracle Cloud Always Free Tier are provisioned via Terraform, configured into a K3s cluster via Ansible, and managed declaratively through ArgoCD. The platform layer includes service mesh (Istio), policy enforcement (Kyverno), secret management (Vault), distributed tracing (Jaeger), and observability (Prometheus + Grafana). Two demo applications and an autonomous remediation controller round it out.

### The Problem It Solves

Running production-grade Kubernetes is expensive in two ways: cloud bills and operator toil. A typical EKS or GKE cluster runs hundreds of dollars per month even when idle, and even a healthy cluster requires constant manual intervention when pods crash, nodes go unhealthy, or load spikes occur outside business hours.

NextOpus tackles both problems at once:

- **Zero infrastructure cost.** Oracle Cloud Always Free Tier provides 4 ARM cores and 24 GB of RAM at no charge, indefinitely. NextOpus runs entirely within those limits.
- **Zero operator toil for common failures.** The Guardian controller watches Prometheus, detects anomalies (crash loops, CPU pressure, memory pressure, service-down conditions, alerts with a `guardian_action` label), and takes the appropriate corrective action: restart the pod, scale the deployment, cordon a node. It honors cooldowns to avoid thrashing, supports a dry-run mode, and surfaces its decisions through Prometheus metrics and an HTTP API.

### What's On The Platform

- **K3s** (lightweight Kubernetes) across 1 server and 3 agents
- **ArgoCD** managing all platform and app workloads via the app-of-apps pattern
- **Istio** service mesh for mTLS and traffic management
- **Kyverno** for cluster-wide policy enforcement
- **Vault** for secret management
- **NGINX Ingress** with rate limiting
- **Prometheus** for metrics, **Jaeger** for tracing
- **MetalLB** for LoadBalancer services on bare metal
- **Guardian** autonomous self-healing controller
- **Data Generator** (Go) and **Data Processor** (Python/FastAPI) demo apps

---

## The Guardian

The Guardian is the autonomous self-healing controller that gives NextOpus its name. It runs as a single deployment, watches Prometheus, and takes Kubernetes-level action when anomalies are detected.

### What It Detects

| Check                  | Trigger Condition                                                 | Default Action |
|------------------------|-------------------------------------------------------------------|----------------|
| **Crash loops**        | `>= 3` container restarts in the last hour                        | Restart pod    |
| **High CPU**           | Pod CPU usage at or above 80% of limit (5-minute average)         | Scale up deployment by 1, max 10 replicas |
| **High memory**        | Pod memory usage at or above 85% of limit                         | Scale up deployment |
| **Service down**      | All replicas of `data-generator` or `data-processor` unhealthy    | Restart pods   |
| **Custom alerts**      | Any Prometheus alert with a `guardian_action` label               | Configurable action |

### Safety Features

- **Cooldowns.** Per-target cooldowns prevent action thrashing. Default 5 minutes for restarts, 10 minutes for scale operations.
- **Dry-run mode.** Set `DRY_RUN=true` to log decisions without executing them. Useful for tuning thresholds before going hot.
- **Scale bounds.** Scale-up capped at 10 replicas, scale-down floored at 1 replica.
- **Action history.** Last 100 actions retained in memory and surfaced at `GET /actions`.
- **Metrics on every decision.** `guardian_anomalies_detected_total`, `guardian_actions_taken_total`, `guardian_actions_failed_total` exposed for alerting.

### Inspecting the Guardian

```bash
kubectl port-forward -n nextopus svc/guardian 8080:8080

curl localhost:8080/health         # Liveness + namespace info
curl localhost:8080/anomalies      # Currently active anomalies
curl localhost:8080/actions        # Last 20 actions taken
curl localhost:8080/metrics        # Prometheus metrics
```

---

## How It Works

NextOpus boots in three deterministic phases:

```
                    ┌─────────────────────────────────┐
   Phase 1          │  Terraform                       │
   Infrastructure   │  • OCI VCN + subnets + NSGs      │
                    │  • 4 ARM instances (1 + 3)       │
                    │  • Auto-generates Ansible        │
                    │    inventory file                 │
                    └─────────────────┬───────────────┘
                                      ▼
                    ┌─────────────────────────────────┐
   Phase 2          │  Ansible                         │
   Cluster Setup    │  • CIS-style host hardening      │
                    │  • K3s server on control plane   │
                    │  • K3s agents on 3 workers       │
                    │  • Fetches kubeconfig to local   │
                    └─────────────────┬───────────────┘
                                      ▼
                    ┌─────────────────────────────────┐
   Phase 3          │  ArgoCD                          │
   GitOps Takeover  │  • Self-install ArgoCD           │
                    │  • Bootstrap root Application    │
                    │  • Root App syncs all platform   │
                    │    and workload Applications     │
                    │  • Cluster now manages itself    │
                    └─────────────────┬───────────────┘
                                      ▼
                    ┌─────────────────────────────────┐
   Steady State     │  Guardian + Workloads            │
                    │  • Demo apps generate + ingest   │
                    │    events                         │
                    │  • Guardian watches Prometheus   │
                    │  • Guardian takes corrective     │
                    │    action when anomalies appear  │
                    └─────────────────────────────────┘
```

The interesting property of this design is that after Phase 3, **the cluster manages itself**. Pushing a change to the Kubernetes manifests in this repo triggers ArgoCD to roll it out. No `kubectl apply` from a developer machine. No CI/CD pipeline with cluster credentials. Just `git push`.

---

## Architecture

```
                       ┌──────────────────────────────────────────┐
                       │           ORACLE CLOUD INFRA             │
                       │   Always Free Tier:                       │
                       │   1x VM.Standard.A1.Flex (control plane) │
                       │   3x VM.Standard.A1.Flex (workers)        │
                       │   24 GB RAM total, 4 ARM cores total      │
                       └────────────────────┬─────────────────────┘
                                            ▼
        ┌──────────────────────────────────────────────────────────┐
        │                  K3S CLUSTER LAYER                        │
        │   K3s server  +  3 K3s agents  +  MetalLB                │
        └────────────────────────┬─────────────────────────────────┘
                                 ▼
        ┌──────────────────────────────────────────────────────────┐
        │                  PLATFORM LAYER                           │
        │   ArgoCD        Istio service mesh                        │
        │   Kyverno       NGINX Ingress (rate limited)              │
        │   Vault         Prometheus + Jaeger                       │
        └────────────────────────┬─────────────────────────────────┘
                                 ▼
        ┌──────────────────────────────────────────────────────────┐
        │                  WORKLOAD LAYER                           │
        │  ┌──────────────────┐  ┌──────────────────┐              │
        │  │  Data Generator  │─▶│  Data Processor  │              │
        │  │  Go              │  │  Python/FastAPI  │              │
        │  │  100 evt/s default│  │  In-memory store │              │
        │  └──────────────────┘  └──────────────────┘              │
        └──────────────────────────────────────────────────────────┘
                                 ▼
        ┌──────────────────────────────────────────────────────────┐
        │                  CONTROL LAYER                            │
        │   Guardian (Python + aiohttp)                             │
        │   Polls Prometheus every 30s                              │
        │   Takes Kubernetes-level action via official client       │
        └──────────────────────────────────────────────────────────┘
```

---

## Tech Stack

### Infrastructure

| Component       | Purpose |
|-----------------|---------|
| **Oracle Cloud Always Free Tier** | 4 ARM cores + 24 GB RAM at zero cost, indefinitely. The platform target. |
| **Terraform**   | Provisions VCN, subnets, security lists, NSGs, and the four compute instances. Auto-generates the Ansible inventory file. |
| **Ansible**     | Three roles: `common` (base configuration), `hardening` (security baseline), and `k3s-server` / `k3s-agent` for cluster install. |
| **K3s**         | Lightweight Kubernetes distribution. Smaller resource footprint than vanilla K8s, perfect for ARM. |
| **MetalLB**     | LoadBalancer-type services on bare-metal Kubernetes. |

### Platform

| Component       | Purpose |
|-----------------|---------|
| **ArgoCD**      | GitOps controller. Implements the app-of-apps pattern so a single root Application manages every other Application. |
| **Istio**       | Service mesh for mTLS, traffic shaping, and per-service telemetry. |
| **Kyverno**     | Policy engine. Enforces image provenance, resource limits, security context, etc. |
| **NGINX Ingress** | Cluster ingress with annotation-driven rate limiting. |
| **Vault**       | Secret management. |
| **Prometheus**  | Metrics scraping and alerting. The source of truth for the Guardian. |
| **Jaeger**      | Distributed tracing. |

### Workloads

| Component              | Language | Purpose |
|------------------------|----------|---------|
| **Data Generator**     | Go       | Generates synthetic events (metrics, logs, traces, alerts, audit, heartbeat) at configurable rates. Batches and POSTs to the processor. |
| **Data Processor**     | Python (FastAPI) | Ingests events, indexes them by type and source, supports query and aggregation, evicts old events on a configurable retention window. |
| **Guardian**           | Python (aiohttp + asyncio) | Autonomous controller. Async PromQL queries, official Kubernetes Python client for remediation. |

### Tooling

| Tool             | Purpose |
|------------------|---------|
| **GitHub Actions** | Lint Ansible, validate Terraform, run CI for the apps, drive GitOps updates. |
| **yamllint**     | YAML hygiene across the entire repo. |

---

## Project Structure

```
nextopus/
│
├── terraform/                        OCI infrastructure provisioning
│   ├── main.tf                       Root: composes network, security, compute modules
│   ├── providers.tf                  OCI provider config
│   ├── variables.tf                  Inputs (tenancy OCID, compartment, SSH key, etc.)
│   ├── outputs.tf                    Instance IPs, kubeconfig hint
│   ├── terraform.tfvars.example      Template tfvars
│   └── modules/
│       ├── network/                  VCN, subnets, route tables, gateways
│       ├── security/                 Security lists, NSGs, K3s firewall rules
│       └── compute/                  ARM instances + cloud-init + SSH key handling
│
├── ansible/                          K3s cluster install + hardening
│   ├── ansible.cfg                   Defaults, inventory location
│   ├── group_vars/all.yml            Cluster-wide vars (versions, kubeconfig path)
│   ├── playbooks/
│   │   ├── site.yml                  Full bootstrap: common + hardening + k3s
│   │   └── k3s-install.yml           K3s-only re-run path
│   ├── requirements.yml              Collection dependencies
│   └── roles/
│       ├── common/                   Base packages, sysctl, kernel modules
│       ├── hardening/                CIS-style host hardening, sshd_config, ufw
│       ├── k3s-server/               Control plane install + kubeconfig fetch + MetalLB
│       └── k3s-agent/                Worker join
│
├── kubernetes/                       Everything ArgoCD syncs
│   ├── argocd/
│   │   ├── install.yaml              Bootstrap ArgoCD itself
│   │   ├── app-of-apps.yaml          Root Application + AppProject
│   │   └── applications/             One Application per platform component
│   │       ├── namespaces.yaml
│   │       ├── platform.yaml
│   │       ├── observability.yaml
│   │       ├── nginx-ingress.yaml
│   │       ├── data-generator.yaml
│   │       ├── data-processor.yaml
│   │       └── guardian.yaml
│   │
│   ├── platform/                     Platform-tier manifests
│   │   ├── istio/install.yaml
│   │   ├── jaeger/install.yaml
│   │   ├── kyverno/install.yaml
│   │   ├── vault/install.yaml
│   │   └── nginx-ingress/            Install + ingress rules + rate-limit test
│   │
│   ├── observability/                Prometheus + alert rules
│   │   ├── prometheus.yaml
│   │   └── alerts.yaml               Rules with `guardian_action` labels
│   │
│   ├── apps/                         Workload deployments
│   │   ├── data-generator/deployment.yaml
│   │   ├── data-processor/deployment.yaml
│   │   └── guardian/deployment.yaml
│   │
│   └── base/namespace.yaml
│
├── services/                         Demo application source
│   ├── data-generator/               Go service
│   │   ├── main.go                   Generator, batcher, HTTP API, Prometheus metrics
│   │   ├── Dockerfile
│   │   └── go.mod
│   └── data-processor/               Python service
│       ├── main.py                   FastAPI: /ingest, /events, /aggregations, /stats
│       ├── Dockerfile
│       └── requirements.txt
│
├── scripts/
│   └── guardian/                     Self-healing controller source
│       ├── guardian.py               Anomaly detection + remediation + HTTP API
│       ├── Dockerfile
│       └── requirements.txt
│
├── .github/workflows/
│   ├── ci.yaml                       Build + test demo services
│   ├── ansible-lint.yaml             Ansible lint on PR
│   ├── terraform.yaml                terraform fmt/validate/plan
│   └── gitops-update.yaml            Push image-tag updates to Kubernetes manifests
│
├── .yamllint.yaml                    YAML lint config
├── .gitignore
└── README.md
```

---

## Getting Started

### Prerequisites

- An Oracle Cloud account (free) with API key set up
- Terraform 1.6+
- Ansible 2.14+
- `kubectl`
- An SSH keypair

### 1. Provision Infrastructure (Terraform)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit tenancy_ocid, user_ocid, fingerprint, private_key_path, region, etc.

terraform init
terraform plan
terraform apply
```

Terraform also writes `ansible/inventory/hosts.yml` and `hosts.ini` so Ansible can connect to the new instances immediately.

### 2. Configure The Cluster (Ansible)

```bash
cd ../ansible
ansible-playbook playbooks/site.yml
```

This applies host hardening, installs the K3s server on the control plane, joins the three workers, installs MetalLB, and fetches the kubeconfig locally.

```bash
export KUBECONFIG=~/.kube/nextopus.yaml
kubectl get nodes
```

### 3. Hand The Cluster To ArgoCD

```bash
kubectl apply -f kubernetes/argocd/install.yaml
kubectl apply -f kubernetes/argocd/app-of-apps.yaml
```

ArgoCD installs itself, then the root `nextopus-apps` Application discovers every `Application` under `kubernetes/argocd/applications/` and syncs them. From this point forward, every change pushed to the repo is rolled out automatically.

### 4. Tear Down

```bash
cd terraform && terraform destroy
```

---

## GitOps with ArgoCD

NextOpus uses the **app-of-apps pattern**. A single root ArgoCD `Application` watches the `kubernetes/argocd/applications/` directory and creates one Application per platform or workload component. Each child Application points at its own subdirectory of manifests.

The sync policy is aggressive by design:

```yaml
syncPolicy:
  automated:
    prune: true        # Resources removed from git are removed from the cluster
    selfHeal: true     # Drift in the cluster is reverted to match git
  retry:
    limit: 5
    backoff: { duration: 5s, factor: 2, maxDuration: 3m }
```

This means **the repository is the source of truth**. If you `kubectl edit` something out of band, ArgoCD will revert it within seconds. The intended workflow is `git push`, then watch.

The `.github/workflows/gitops-update.yaml` action closes the loop on image tags: when CI builds a new image, the workflow rewrites the relevant deployment YAML with the new tag, commits it, and pushes. ArgoCD picks it up.

---

## Configuration

### Data Generator

| Variable                | Description                                | Default |
|-------------------------|--------------------------------------------|---------|
| `PORT`                  | API port                                    | `8080`  |
| `METRICS_PORT`          | Prometheus scrape port                      | `9090`  |
| `DATA_RATE`             | Events per second                           | `100`   |
| `BATCH_SIZE`            | Events per outbound batch                   | `50`    |
| `FLUSH_INTERVAL_MS`     | Flush partial batches after this many ms    | `1000`  |
| `PROCESSOR_ENDPOINT`    | Where to POST batches                       | `http://data-processor:8080/ingest` |

### Data Processor

| Variable           | Description                                | Default |
|--------------------|--------------------------------------------|---------|
| `PORT`             | API port                                    | `8080`  |
| `METRICS_PORT`     | Prometheus scrape port                      | `9090`  |
| `MAX_EVENTS`       | In-memory event cap                         | `100000`|
| `RETENTION_HOURS`  | Drop events older than this many hours      | `24`    |
| `LOG_LEVEL`        | DEBUG / INFO / WARN / ERROR                 | `INFO`  |

### Guardian

| Variable                       | Description                                 | Default |
|--------------------------------|---------------------------------------------|---------|
| `PROMETHEUS_URL`               | Prometheus base URL                         | `http://prometheus-kube-prometheus-prometheus.observability:9090` |
| `WATCH_NAMESPACE`              | Namespace the Guardian acts on              | `nextopus` |
| `CHECK_INTERVAL`               | Seconds between check cycles                | `30`    |
| `DRY_RUN`                      | Log decisions without acting                | `false` |
| `CPU_SCALE_UP_THRESHOLD`       | Fraction of CPU limit                       | `0.8`   |
| `CPU_SCALE_DOWN_THRESHOLD`     | Fraction of CPU limit                       | `0.3`   |
| `MEMORY_SCALE_UP_THRESHOLD`    | Fraction of memory limit                    | `0.85`  |
| `RESTART_COUNT_THRESHOLD`      | Restarts/hour before treating as crash loop | `3`     |
| `ACTION_COOLDOWN`              | Seconds between actions per target          | `300`   |
| `SCALE_COOLDOWN`               | Seconds between scale actions per target    | `600`   |
