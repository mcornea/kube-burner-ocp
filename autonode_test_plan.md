# ROSA HCP AutoNode PerfScale Test Plan

## Purpose

The goal of this testing is to validate the performance, scalability and stability of the **AutoNode** feature (powered by Karpenter) on ROSA Hosted Control Planes (HCP). This plan focuses on verifying the efficiency of node provisioning, the impact of scale on the Karpenter controller, and the interaction between Karpenter and the Hosted Control Plane's Vertical Pod Autoscaler (VPA).

### Goals

* **G1 — Provisioning Latency:** Measure AutoNode provisioning latency (pending pod → node Ready).
* **G2 — Termination Latency:** Measure node termination and consolidation latency.
* **G3 — Pod Scheduling Latency:** Measure end-to-end pod scheduling latency via kube-burner.
* **G4 — Single-HCP Scale:** Validate single-HCP scale at 50, 500, 1,000, and 5,000 cores.
* **G5 — Multi-HCP Scale:** Validate 60 concurrent AutoNode-enabled HCPs each running 50-core workloads.
* **G6 — AWS API Throttling Detection:** Detect AWS API throttling under scale.
* **G7 — API Server Memory:** Validate kube-apiserver memory stays below 5Gi.
* **G8 — Consolidation Efficiency:** Confirm consolidation efficiency after churn.
* **G9 — Stability:** Detect pod flapping and over-provisioning.
* **G10 — VPA Interaction:** Characterize VPA/Karpenter interaction.

### Non-Goals

* Karpenter code-level debugging
* Functional testing of Karpenter features (drift, spot interruption)
* Non-AutoNode scaling (Cluster Autoscaler, MachinePool)
* Multi-region testing (all tests in us-east-1)
* Application-level performance benchmarks
* Cost optimization analysis
* Spot instance testing (On-Demand only unless stated)

### Jira

EPIC — [https://issues.redhat.com/browse/PERFSCALE-3624](https://issues.redhat.com/browse/PERFSCALE-3624)

## Environment

### Infrastructure

* **Platform:** ROSA HCP on AWS, us-east-1
* **Instance Types:**
  * `m5.xlarge` — 4 vCPU
  * `m5.2xlarge` — 8 vCPU
  * `m5.4xlarge` — 16 vCPU
  * `m5.8xlarge` — 32 vCPU

### Software Versions

* OpenShift 4.21

### Karpenter Resources

* `OpenshiftEC2NodeClass` — API group: `karpenter.hypershift.openshift.io/v1beta1`
* `NodePool` — API group: `karpenter.sh/v1`
* `EC2NodeClass` — auto-created from `OpenshiftEC2NodeClass`, API group: `karpenter.k8s.aws`

### Observability Setup

* **Metrics:** Prometheus (in-cluster, with PodMonitors for Karpenter per [HyperShift PR #6206](https://github.com/openshift/hypershift/pull/6206)), Grafana, CloudWatch
* **Karpenter metrics endpoint:** scraped via PodMonitor targeting Karpenter controller in management cluster
* **Tooling:** `kube-burner` for workload orchestration and latency measurement
* **Indexing:** Elasticsearch/OpenSearch for cross-run comparison

## Methodology

### 1. Cluster Creation

1. Create ROSA HCP cluster with AutoNode:
   ```bash
   rosa create cluster \
     --cluster-name=$CLUSTER_NAME \
     --autonode \
     --version=4.21 \
     --region=us-east-1 \
     --sts \
     --hosted-cp
   ```
2. Private Preview provision shard: `9f11dd2b-98c1-11f0-8fe5-0a580a830a08`
3. Enable AutoNode post-creation (if not set at creation):
   ```bash
   rosa edit cluster -c $CLUSTER_ID \
     --autonode=enabled \
     --autonode-iam-role-arn=$ROLE_ARN
   ```
4. Tag subnets and security groups with `karpenter.sh/discovery: $CLUSTER_ID`

### 2. NodePool Configuration

One NodePool per test run with all instance types; Karpenter bin-packing selects the optimal mix.

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: test-pool
spec:
  template:
    spec:
      nodeClassRef:
        group: karpenter.k8s.aws
        kind: EC2NodeClass
        name: default
      requirements:
        - key: node.kubernetes.io/instance-type
          operator: In
          values: ["m5.xlarge", "m5.2xlarge", "m5.4xlarge", "m5.8xlarge"]
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["on-demand"]
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 30s
  limits:
    cpu: "5000"  # varies per test target
```

Key configuration rules:
* Set `limits.cpu` to match test target (e.g., `"5000"` for the largest test)
* `nodeClassRef` must reference `EC2NodeClass` (not `OpenshiftEC2NodeClass`)
* Capacity type: `on-demand` only

### 3. Monitoring Setup

#### Karpenter Prometheus Metrics

Verified from [Karpenter source code](https://github.com/kubernetes-sigs/karpenter/tree/main/pkg/cloudprovider/metrics) and [official docs](https://karpenter.sh/docs/reference/metrics/). Metric names below follow the official docs; some may differ slightly in the ROSA HCP environment (e.g., singular vs plural subsystem prefixes). Confirm exact names by querying Prometheus on a live cluster during the pilot run.

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `karpenter_cloudprovider_duration_seconds` | Histogram | controller, method, provider | AWS API call duration |
| `karpenter_cloudprovider_errors_total` | Counter | controller, method, provider, error | AWS API errors (throttling detection) |
| `karpenter_nodeclaims_created_total` | Counter | reason, nodepool | Node provisioning rate |
| `karpenter_nodeclaims_terminated_total` | Counter | nodepool | Node termination rate |
| `karpenter_nodeclaims_disrupted_total` | Counter | reason, nodepool | Consolidation/disruption counts |
| `operator_status_condition_transition_seconds{kind="NodeClaim"}` | Histogram | type, status, group, kind | NodeClaim phase durations (see Provisioning Phase Breakdown below) |
| `karpenter_nodeclaims_termination_duration_seconds` | Histogram | nodepool | NodeClaim termination duration |
| `karpenter_cluster_state_node_count` | Gauge | — | Current node count |
| `karpenter_pods_startup_duration_seconds` | Summary | — | Pod creation → running |
| `karpenter_scheduler_scheduling_duration_seconds` | Histogram | — | Scheduling simulation duration |
| `karpenter_voluntary_disruption_decisions_total` | Counter | decision, reason, consolidation_type | Disruption decision count |
| `karpenter_nodepools_usage` | Gauge | nodepool, resource_type | Resource usage per NodePool |

#### kube-burner Measurements

Verified from [kube-burner docs](https://kube-burner.github.io/kube-burner/latest/measurements/).

| Measurement | Indexed Document | Key Fields |
|-------------|-----------------|------------|
| `podLatency` | `podLatencyMeasurement` | `schedulingLatency`, `initializedLatency`, `containersReadyLatency`, `podReadyLatency` (all in ms) |
| `podLatency` quantiles | `podLatencyQuantilesMeasurement` | `quantileName` (PodScheduled, Initialized, ContainersReady, Ready), P99/P95/P50/max/avg |
| `nodeLatency` | `nodeLatencyMeasurement` | `nodeReadyLatency` (ms) |
| `nodeLatency` quantiles | `nodeLatencyQuantilesMeasurement` | Same quantile structure as podLatency |

#### AutoNode Latency Calculation

**Authoritative metric:** kube-burner's `nodeReadyLatency` from `nodeLatencyMeasurement` (node object creation → Ready condition). This is an exact per-node end-to-end measurement indexed with P99/P95/P50/max/avg quantiles. Use this for KPI evaluation.

**Diagnostic drill-down:** If `nodeReadyLatency` P99 exceeds the 120s target, use the per-phase Karpenter metrics to identify which phase is the bottleneck. NodeClaim lifecycle durations are tracked by the [operatorpkg status condition framework](https://github.com/awslabs/operatorpkg/blob/main/status/metrics.go) via `operator_status_condition_transition_seconds{kind="NodeClaim"}`.

##### Provisioning Phase Breakdown

The [lifecycle controller](https://github.com/kubernetes-sigs/karpenter/blob/main/pkg/controllers/nodeclaim/lifecycle/controller.go) reconciles three sequential status conditions ([source](https://github.com/kubernetes-sigs/karpenter/blob/main/pkg/apis/v1/nodeclaim_status.go)):

```
NodeClaim created ──→ Launched ──→ Registered ──→ Initialized
```

| Phase | Condition | What Happens |
|-------|-----------|--------------|
| Launch | `Launched` | CloudProvider calls EC2 `RunInstances`, condition set to `True` on success |
| Registration | `Registered` | Node object found in API server, metadata synced, startup taints verified |
| Initialization | `Initialized` | Node Ready, startup taints removed, requested resources registered |

Per-phase P99 query (replace `Launched` with `Registered` or `Initialized` for other phases):

```promql
histogram_quantile(0.99,
  sum(rate(operator_status_condition_transition_seconds_bucket{kind="NodeClaim", type="Launched"}[5m])) by (le)
)
```

> **Note:** Do not sum per-phase P99 values to estimate end-to-end P99 — percentiles of independent distributions are not additive. Use kube-burner `nodeReadyLatency` P99 for the end-to-end number, and the per-phase queries only to isolate which phase contributes most when investigating a missed target.

##### Correlating Node Provisioning with Pod Scheduling Latency

kube-burner's `schedulingLatency` (per-pod) and `nodeReadyLatency` (per-node) measure different things starting from different clocks:

```
Karpenter                 Node object
decides to    ──→    appears in     ──→   Node Ready
provision            API server
|                    |                    |
|__ Launch phase ____|                    |
|   (not visible to                      |
|    kube-burner)    |___ nodeReadyLatency ___|
|                    (kube-burner, per-node)
|
|   Pod created ──→ Pending ──→ ... ──→ Scheduled ──→ Ready
|   |                                   |              |
|   |______ schedulingLatency __________|              |
|   |______ podReadyLatency ___________________________|
|           (kube-burner, per-pod)
```

`nodeReadyLatency` starts when the node object appears in the API server, so it covers Registration + Initialization but **not** the Launch phase (EC2 API call + instance boot). `schedulingLatency` covers everything the pod waits for, including the Launch phase, but is per-pod (multiple pods may wait for the same node).

**Diagnostic workflow when `schedulingLatency` P99 misses target:**

1. Check `nodeReadyLatency` P99 — how much time is Kubernetes-side (node object → Ready)?
2. Check `operator_status_condition_transition_seconds{kind="NodeClaim", type="Launched"}` P99 — how much time is EC2-side (NodeClaim created → node object appears)?
3. The remainder is Karpenter queue/scheduling overhead (`karpenter_scheduler_scheduling_duration_seconds`).

These P99s come from different populations (pods vs nodes vs NodeClaims), so they cannot be subtracted precisely. Compare them side by side to identify which phase dominates:

| Scenario | Launched P99 | nodeReadyLatency P99 | Interpretation |
|----------|-------------|---------------------|----------------|
| EC2-bound | High (e.g., 60s) | Low (e.g., 30s) | Most time in EC2 API + instance boot |
| Init-bound | Low (e.g., 10s) | High (e.g., 90s) | Node boots fast but daemonsets/taints slow to clear |
| Queue-bound | Low | Low | Both fast, but `schedulingLatency` still high → Karpenter queue or kube-scheduler is bottleneck |

### 4. kube-burner Workload Design

* Use pause container pods (`registry.k8s.io/pause:3.9`) with explicit CPU requests
* Default: 500m CPU, 128Mi memory per pod → N cores requires N×2 pods
* Enable both `podLatency` and `nodeLatency` measurements
* Index results to Elasticsearch/OpenSearch for comparison across runs

Example kube-burner job config:

```yaml
---
global:
  gc: false
  measurements:
    - name: podLatency
      thresholds:
        - conditionType: Ready
          metric: P99
          threshold: 60000ms
    - name: nodeLatency

jobs:
  - name: autonode-scale
    jobType: create
    jobIterations: 1
    qps: 50
    burst: 50
    namespacedIterations: false
    namespace: autonode-test
    objects:
      - objectTemplate: pause-deployment.yml
        replicas: 1000  # varies per test
        inputVars:
          cpuRequest: 500m
          memoryRequest: 128Mi
```

Example `pause-deployment.yml` template:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pause-{{.Iteration}}
  namespace: {{.Namespace}}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: pause-{{.Iteration}}
  template:
    metadata:
      labels:
        app: pause-{{.Iteration}}
    spec:
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.9
          resources:
            requests:
              cpu: "{{.cpuRequest}}"
              memory: "{{.memoryRequest}}"
      terminationGracePeriodSeconds: 0
```

## Performance and Scale Benchmarks

### Use Case 1: Single HCP Scale Testing

#### Test Matrix

Each test allows all instance types so Karpenter's bin-packing minimizes node count. Best case = all m5.8xlarge (32c). Worst case = all m5.xlarge (4c).

| Test ID | Instance Types | Cores | Nodes (best→worst) | Pods (@500m) | CPU/Pod |
|---------|---------------|-------|---------------------|--------------|---------|
| SingleHCP-01 | m5.xlarge, m5.2xlarge, m5.4xlarge, m5.8xlarge | 50 | 2–13 | 100 | 500m |
| SingleHCP-02 | m5.xlarge, m5.2xlarge, m5.4xlarge, m5.8xlarge | 500 | 16–125 | 1,000 | 500m |
| SingleHCP-03 | m5.xlarge, m5.2xlarge, m5.4xlarge, m5.8xlarge | 1,000 | 32–250 | 2,000 | 500m |
| SingleHCP-04 | m5.xlarge, m5.2xlarge, m5.4xlarge, m5.8xlarge | 5,000 | 157–1,250 | 10,000 | 500m |

##### Churn Test

| Test ID | Instance Types | Cores | Nodes (best→worst) | Pods (@500m) | CPU/Pod | Churn |
|---------|---------------|-------|---------------------|--------------|---------|-------|
| SingleHCP-05 | m5.xlarge, m5.2xlarge, m5.4xlarge, m5.8xlarge | 500 | 16–125 | 1,000 | 500m | 10%/1h |

Rationale for churn topology:
* **500 cores**: 16–125 nodes depending on instance mix — enough to observe consolidation behavior meaningfully, but not so many that test costs are high.
* **All instance types**: exercises Karpenter's bin-packing during consolidation (repacking pods across different instance sizes after churn), which reflects real-world usage.
* **10% churn over 1 hour**: deletes and recreates ~100 pods per cycle. With mixed instance types, the consolidation outcome depends on the instance mix Karpenter chose, so pass/fail criteria are based on aggregate behavior rather than per-node counts. See Churn Consolidation under Consolidation & Stability Tests for details.

> **Note:** Expected node counts assume all allocatable CPU consumed by test pods. System reservations (kubelet, daemonsets) consume ~5–15% of node CPU. Actual counts will be slightly higher — validate in pilot run SingleHCP-01.

#### Execution Phases Per Test

1. **Baseline (5 min)** — Verify cluster health, idle apiserver memory, zero worker nodes.
2. **Scale-Up** — kube-burner creates all pods; measure provisioning and pod ready latencies.
3. **Steady State (15 min)** — Hold workload; monitor for flapping, over-provisioning, VPA evictions.
4. **Churn (1h, SingleHCP-05 only)** — 10% delete/recreate; wait enough for consolidation between cycles.
5. **Scale-Down / Consolidation** — Delete all workloads; watch `karpenter_cluster_state_node_count` drop to 0.
   * 30 min observation for churn test (SingleHCP-05)
   * 15 min observation for non-churn tests

#### Memory Budget Constraint

At 10,000 pods (5,000-core test), ~30,000 API objects are expected to consume ~2–3Gi of kube-apiserver memory. The threshold is **< 5Gi**. Mitigation if exceeded: increase per-pod CPU request to 1 core (halving pod count).

### Use Case 2: Multi-Cluster Scale (60 HCPs)

#### Test Matrix

| Test ID | HCPs | Cores/HCP | Instance Type | Total Nodes | Churn |
|---------|-------|-----------|---------------|-------------|-------|
| MultiHCP-01 | 60 | 50 | m5.2xlarge | ~390 | No |

#### Configuration

* 100 pods per HCP (50 cores @ 500m each), ~7 nodes per HCP (m5.2xlarge)
* Launch kube-burner on all 60 HCPs concurrently
* Per-HCP: same KPIs as Single HCP tests (provisioning latency, pod ready latency, node count, API server memory)
* Management cluster: Karpenter controller CPU/memory/restarts, etcd size/latency

#### AWS API Throttling Detection

* `karpenter_cloudprovider_errors_total` — any value > 0 with `error` label indicating throttling
* `karpenter_cloudprovider_duration_seconds` — P99 spikes vs single-HCP baseline
* CloudTrail `ThrottlingException` events

#### Optional: Staggered Scale Test

Scale up in batches of 10 HCPs (6 batches) to observe whether later batches exhibit higher latency due to API throttling or resource contention.

## KPI

### AutoNode-Specific Latencies

| KPI | Metric Source | Target |
|-----|---------------|--------|
| Node Provisioning Latency | `nodeReadyLatency` from `nodeLatencyMeasurement` (kube-burner) | P99 < 120s |
| Node Termination Latency | `karpenter_nodeclaims_termination_duration_seconds` | P99 < 60s |
| Scheduling Simulation | `karpenter_scheduler_scheduling_duration_seconds` | P99 < 5s |
| Pod Startup (Karpenter) | `karpenter_pods_startup_duration_seconds` | P99 < 120s |
| Pod Ready (kube-burner) | `podReadyLatency` from `podLatencyMeasurement` (kube-burner) | P99 < 60s |
| Consolidation Convergence | Time from last workload deletion to `karpenter_cluster_state_node_count == 0` | < 10 min (≤1,250 nodes) |

### Control Plane Health

| KPI | Metric Source | Target |
|-----|---------------|--------|
| API Latency (mutating) | `apiserver_request_duration_seconds` | P99 < 1s |
| API Latency (LIST) | `apiserver_request_duration_seconds` | P99 < 5s |
| etcd Latency | `etcd_request_duration_seconds` | P99 < 200ms |
| API Server Memory | `container_memory_working_set_bytes{container="kube-apiserver"}` | < 5Gi |
| etcd DB Size | `etcd_db_total_size_in_bytes` | < 8Gi |

### VPA Interaction

| KPI | Metric Source | Target |
|-----|---------------|--------|
| Control Plane Pod Restarts | `kube_pod_container_status_restarts_total` correlated with VPA update events | < 2 per component per test |
| Karpenter/VPA Correlation | Karpenter node creation within 2 min of VPA evictions | Zero correlated events |

## Consolidation & Stability Tests

These checks are performed during the Steady State and Scale-Down phases of each test.

### Scale-Down Consolidation (all tests)

After all workloads are deleted, Karpenter should terminate every node via its `WhenEmptyOrUnderutilized` policy (`consolidateAfter: 30s`).

* **What to watch:** `karpenter_cluster_state_node_count` should drop monotonically to 0. Any increase after the initial drop indicates oscillation (Karpenter re-provisioning nodes it just terminated).
* **Pass criteria:** node count reaches 0 within 10 min (per the Consolidation Convergence KPI). The observation window (15 min for non-churn, 30 min for SingleHCP-05) exists to catch late oscillation after convergence.

### Over-Provisioning Detection (Steady State, all tests)

During the 15-min steady state hold, the actual node count should match the workload requirement.

* **What to watch:** `karpenter_cluster_state_node_count` compared to the expected range from the test matrix. Since Karpenter picks the instance mix, the actual count will fall somewhere in the best→worst range (e.g., 16–125 for SingleHCP-02). A count above the worst case means Karpenter provisioned more capacity than needed.
* **Pass criteria:** actual node count ≤ worst-case value from the test matrix (cores ÷ 4, the smallest instance). Also check `karpenter_nodeclaims_disrupted_total` — if Karpenter is repeatedly consolidating during steady state, the workload may not be stable.

### Pod Flapping (Steady State, all tests)

* **What to watch:** `kube_pod_container_status_restarts_total` for pods in the `autonode-test` namespace. During steady state, pause pods have no reason to restart.
* **Pass criteria:** zero restarts during the 15-min steady state window. During the churn phase (SingleHCP-05 only), restarts should only come from the 10% of pods being explicitly deleted and recreated — not from pods that weren't part of the churn cycle.

### Churn Consolidation (SingleHCP-05 only)

Each churn cycle deletes ~100 pods (10% of 1,000). With mixed instance types, Karpenter's response depends on how pods were distributed across different-sized nodes. The `WhenEmptyOrUnderutilized` policy with `consolidateAfter: 30s` should trigger consolidation — either terminating empty nodes or repacking underutilized nodes onto fewer/larger instances.

* **What to watch:**
  * `karpenter_cluster_state_node_count` — should never exceed the pre-churn steady-state value during the 1h churn phase.
  * `karpenter_nodepools_usage{resource_type="cpu"}` — should drop after each delete cycle and recover after each recreate cycle, confirming Karpenter tracks the workload rather than holding idle capacity.
  * `karpenter_voluntary_disruption_decisions_total` — rate should be stable, not spiking repeatedly (which would indicate consolidation oscillation).
* **Pass criteria:** (1) peak node count during churn ≤ steady-state node count; (2) no consolidation oscillation (no repeated provision→terminate cycles for the same capacity); (3) CPU usage metric reflects churn cycles.

## Test Execution Order

Priority order:

1. **SingleHCP-01** (50 cores) — Pilot run: validate tooling, metrics collection, memory baseline.
2. **SingleHCP-02** (500 cores) — First meaningful scale test.
3. **SingleHCP-03** (1,000 cores) — Higher scale validation.
4. **SingleHCP-05** (500 cores, churn) — Consolidation and stability test.
5. **SingleHCP-04** (5,000 cores) — Maximum scale push.
6. **MultiHCP-01** (60 HCPs × 50 cores) — Multi-cluster baseline.

## Appendices

### Grafana Dashboard Panels

The following 12 panels should be configured for test observation:

1. **Node Count Over Time** — `karpenter_cluster_state_node_count`
2. **NodeClaim Creation Rate** — `rate(karpenter_nodeclaims_created_total[5m])`
3. **NodeClaim Termination Rate** — `rate(karpenter_nodeclaims_terminated_total[5m])`
4. **AWS API Latency** — `karpenter_cloudprovider_duration_seconds` histogram
5. **AWS API Errors** — `karpenter_cloudprovider_errors_total`
6. **Pod Startup Duration** — `karpenter_pods_startup_duration_seconds` summary
7. **Scheduling Duration** — `karpenter_scheduler_scheduling_duration_seconds` histogram
8. **API Server Memory** — `container_memory_working_set_bytes{container="kube-apiserver"}`
9. **API Server Request Latency** — `apiserver_request_duration_seconds` P99
10. **etcd Latency** — `etcd_request_duration_seconds` P99
11. **Pod Restarts** — `kube_pod_container_status_restarts_total`
12. **Consolidation Actions** — `karpenter_voluntary_disruption_decisions_total`

### Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| AWS EC2 API throttling at 60 HCPs | Increased provisioning latency, failed NodeClaims | Staggered scale-up, monitor `karpenter_cloudprovider_errors_total` |
| API server OOM at 5,000 cores | Control plane instability | Monitor memory, reduce pod count by increasing per-pod CPU request |
| Karpenter controller instability | Node provisioning failures | Monitor controller restarts, resource usage |
| Instance capacity exhaustion in us-east-1 | Unable to provision nodes | Use multiple AZs, consider fallback instance types |
| Test infrastructure costs | Budget overrun | Clean up resources after each test, use consolidation aggressively |

## Reference

* [Karpenter Metrics Reference](https://karpenter.sh/docs/reference/metrics/)
* [kube-burner Measurements](https://kube-burner.github.io/kube-burner/latest/measurements/)
* [HyperShift PR #6206 — Karpenter PodMonitor](https://github.com/openshift/hypershift/pull/6206)
