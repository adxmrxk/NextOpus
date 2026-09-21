# NextOpus

**Self-healing Kubernetes platform that runs at zero cost.**

Terraform provisions four ARM instances on Oracle Cloud Always Free Tier, Ansible turns them into a hardened K3s cluster, and ArgoCD takes over via GitOps. An autonomous controller called the Guardian watches Prometheus and remediates failures before anyone gets paged.

Sized to fit the free tier exactly: 4 OCPUs, 24 GB RAM, 200 GB storage. $0/month, indefinitely.

## Architecture

```
Terraform ──> 4x ARM instances (1 control plane + 3 workers)
Ansible   ──> hardening + K3s + MetalLB, writes kubeconfig locally
ArgoCD    ──> app-of-apps syncs everything below from this repo
              │
              ├─ Platform   Istio · Kyverno · Vault · NGINX Ingress
              ├─ Observing  Prometheus · Grafana · Alertmanager · Jaeger
              └─ Workloads  data-generator (Go) ──> data-processor (Python)
                            Guardian (watches Prometheus, acts on Kubernetes)
```

After ArgoCD is bootstrapped the cluster manages itself. Push to `main` and it rolls out; drift is reverted.

## The Guardian

A Python controller that queries Prometheus over PromQL and acts through the Kubernetes API.

| Detects | Trigger | Action |
|---|---|---|
| Crash loop | ≥3 restarts in an hour | Restart pod |
| High CPU | ≥80% of limit, 5m average | Scale up, max 10 |
| High memory | ≥85% of limit | Scale up |
| Service down | All replicas unhealthy | Restart pods |
| **Predicted OOM** | `predict_linear` says a rising pod reaches its limit within 30m | Scale up *before* the kill |
| Failing canary | v2 subset over 5% 5xx | Shift traffic back to stable |
| Custom alert | Any alert with a `guardian_action` label | Per the label |

Prediction is the difference between restarting after a crash and adding a
replica while there is still time. A flat pod near its limit is ignored: the
query gates on `deriv() > 0`, so only a rising trend counts.

**Safety.** It does destructive things, so:

- **Leader election.** Two replicas, but only the holder of the `guardian-leader` Lease acts. Without this a second replica doubles every restart.
- **Blast radius cap.** Total actions across *all* targets, default 10 per 10 minutes. Per-target cooldowns do nothing in a cluster-wide incident where every target differs. `HALT_ON_BREAKER=true` latches until a human intervenes.
- **Per-target cooldowns.** 5 minutes for restarts, 10 for scaling.
- **Kubernetes Events.** Every decision lands in `kubectl describe pod`, not just a log inside a pod that gets deleted.
- **Dry run.** `DRY_RUN=true` logs decisions without executing them.

```bash
kubectl port-forward -n nextopus-system svc/guardian 8080:8080
curl localhost:8080/status      # leadership + remaining action budget
curl localhost:8080/anomalies   # what it currently sees
curl localhost:8080/actions     # what it has done
```

## Supply Chain

CI signs every image with **cosign keyless** — identity comes from the
workflow's OIDC token and the signature goes to the public Rekor log, so there
is no private key to leak or rotate. Images ship with an SBOM and build
provenance, and Trivy scans them.

Kyverno then verifies that signature at admission, pinned to the exact workflow
and branch allowed to publish:

```yaml
keyless:
  subject: ".../.github/workflows/ci.yaml@refs/heads/main"
  issuer:  "https://token.actions.githubusercontent.com"
```

A signature from any other workflow, repo or fork does not satisfy it. It runs
in `Audit` until a signed build has rolled out; one field flips it to `Enforce`,
after which an unsigned or tampered image is rejected outright.

## Progressive Delivery

`data-processor` ships behind an Istio traffic split. The canary takes 0% by
default and is reachable on demand with `x-nextopus-canary: true`, so a new
version can be exercised before any real traffic moves onto it. Raise the
weight to roll out.

If the canary's own error rate passes 5%, an alert labelled
`guardian_action: rollback_canary` fires and the Guardian sets the weight back
to zero. Rollback is a config push rather than a redeploy, so it lands as fast
as Istio can distribute it. The rate is measured on the **canary subset only** —
a canary taking 10% of traffic can be failing completely while the service
average still looks fine.

## Quick start

### Locally (free, no account, ~10 minutes)

```bash
k3d cluster create nextopus --agents 2 \
  --image rancher/k3s:v1.31.2-k3s1 \
  --k3s-arg "--disable=traefik@server:*" \
  -p "8081:80@loadbalancer"

kubectl create namespace observability
kubectl create secret generic grafana-admin -n observability \
  --from-literal=admin-user=admin \
  --from-literal=admin-password="$(openssl rand -base64 24)"

kubectl apply -f kubernetes/base/namespace.yaml
kubectl apply -f kubernetes/platform/kyverno/install.yaml
kubectl apply -f kubernetes/platform/nginx-ingress/install.yaml
kubectl apply -f kubernetes/observability/
kubectl apply -f kubernetes/apps/data-processor/ -f kubernetes/apps/data-generator/ -f kubernetes/apps/guardian/
```

`servicelb` stays enabled locally because MetalLB is installed by Ansible, not by anything here.

### On Oracle Cloud

```bash
cd terraform && cp terraform.tfvars.example terraform.tfvars   # 4 OCIDs + region
terraform init && terraform apply                              # expect capacity retries

cd ../ansible && ansible-playbook playbooks/site.yml           # run from WSL, not Windows
export KUBECONFIG=$(pwd)/kubeconfig

kubectl apply -f ../kubernetes/argocd/install.yaml
kubectl apply -f ../kubernetes/argocd/app-of-apps.yaml
```

Set `metallb_ip_range` in `ansible/group_vars/all.yml` to a free range in your subnet first. Free ARM capacity is scarce; `terraform apply` may need retrying for days. Tear down with `terraform destroy`.

Hostnames are not real DNS — point `*.nextopus.local` at the ingress IP in your hosts file.

## Verification

```bash
cd services/data-generator && go test ./...     # 16
cd services/data-processor && pytest            # 28
cd scripts/guardian        && pytest            # 43
```

**Self-healing is proved, not asserted.** A chaos job deletes a real pod and watches the Kubernetes API until the deployment recovers, ignoring anything the Guardian says about itself:

```bash
kubectl apply -f kubernetes/chaos/
kubectl -n nextopus logs job/chaos-self-healing-test
```

It lives outside every ArgoCD path deliberately — the guardian app syncs with `selfHeal`, so a Job stored there would kill a pod on every sync.

**Free-tier budget.** `free-tier-budget.yaml` tracks requests against the real
ceilings (4 OCPU, 24 GB, 200 GB). On Always Free you are not billed for
overrunning, the request just fails, so the failure mode is a deploy that
silently will not schedule weeks after someone bumped a replica count. It also
catches the Guardian scaling up into a full cluster, where the remediation
cannot possibly work.

**Vault** is bootstrapped by a Job that initialises, unseals, enables
Kubernetes auth and writes a scoped policy. The unseal key and root token go
into `secret/vault-keys` rather than the pod logs. Auth is bound to named
service accounts, not a namespace wildcard.

**SLOs** in `kubernetes/observability/slo.yaml` measure the "removes toil" claim: availability and latency SLIs, multi-window burn-rate alerts so blips stay quiet, and alerts on the healer itself being broken, thrashing, leaderless, or wedged.

## Configuration

**Data Generator** — `DATA_RATE` (50/s), `BATCH_SIZE` (25), `FLUSH_INTERVAL_MS` (2000), `PROCESSOR_ENDPOINT`

**Data Processor** — `MAX_EVENTS` (50000), `RETENTION_HOURS` (12), `LOG_LEVEL`

**Guardian**

| Variable | Default |
|---|---|
| `PROMETHEUS_URL` | `http://prometheus-kube-prometheus-prometheus.observability:9090` |
| `WATCH_NAMESPACE` / `CHECK_INTERVAL` | `nextopus` / `30` |
| `DRY_RUN` / `HALT_ON_BREAKER` | `false` / `false` |
| `CPU_SCALE_UP_THRESHOLD` / `MEMORY_SCALE_UP_THRESHOLD` | `0.8` / `0.85` |
| `RESTART_COUNT_THRESHOLD` | `3` |
| `ACTION_COOLDOWN` / `SCALE_COOLDOWN` | `300` / `600` |
| `MAX_ACTIONS_PER_WINDOW` / `ACTION_WINDOW_SECONDS` | `10` / `600` |
| `LEADER_ELECTION` / `EMIT_EVENTS` | `true` / `true` |

**Tracing.** Both demo services are OpenTelemetry-instrumented and off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The generator's POST carries a W3C `traceparent`, so one batch is a single trace across both services:

```
data-generator  flush_batch → HTTP POST
  data-processor  POST /ingest → count_events, store_events
```

## Layout

```
terraform/     OCI VCN, subnets, NSGs, 4 ARM instances; writes the Ansible inventory
ansible/       common + hardening + k3s-server/agent roles, MetalLB
kubernetes/
  argocd/      bootstrap + app-of-apps, one Application per component
  platform/    istio, kyverno, vault, nginx-ingress
  observability/  prometheus stack, alerts, SLOs
  apps/        data-generator, data-processor, guardian
  chaos/       self-healing verification (not ArgoCD-synced, run manually)
services/      Go generator, Python FastAPI processor
scripts/guardian/  the controller
```

## Stack

Terraform · Ansible · K3s · ArgoCD · Istio · Kyverno · Vault · MetalLB · NGINX Ingress · Prometheus · Grafana · Jaeger · OpenTelemetry · Go · Python · FastAPI · GitHub Actions
