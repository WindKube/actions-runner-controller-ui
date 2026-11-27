# arc-ui Helm chart

Read-only dashboard for GitHub Actions Runner Controller. One replica, one PVC, a
ClusterRole with `get`/`list`/`watch` and nothing else.

```bash
helm install arc-ui ./chart \
  --namespace arc-systems --create-namespace \
  --set prometheus.url=http://prometheus-operated.monitoring:9090 \
  --set env.ARC_UI_NAMESPACES=arc-runners \
  --set env.ARC_UI_CONTROLLER_NAMESPACE=arc-systems
```

`prometheus.url` is required. This file covers only the parts that will
otherwise bite you.

## What the Prometheus needs

- **ARC metrics.** Enable the `metrics:` block in the
  `gha-runner-scale-set-controller` values, then scrape the controller and every
  listener on their `metrics` port. `podMonitors.arc.enabled=true` renders two
  PodMonitors for that, scraping `rbac.controllerNamespace`, with a `labelmap`
  that turns `actions.github.com/*` pod labels into `actions_github_com_*`. They
  are created there too, unless `podMonitors.arc.namespace` names the namespace
  your Prometheus selects monitors from.
- **cAdvisor.** The kubelet's `/metrics/cadvisor`, for runner pod CPU and memory.
- **kube-state-metrics with the scale set label allowlisted.** It drops pod
  labels unless told otherwise:

  ```text
  --metric-labels-allowlist=pods=[actions.github.com/scale-set-name]
  ```

  In kube-prometheus-stack: `kube-state-metrics.metricLabelsAllowlist`.

## Why a Deployment with an explicit PVC

A StatefulSet is the reflex for "pod with a disk", and it is wrong here:

* `volumeClaimTemplates` are **immutable**. Growing the history volume would mean
  deleting and recreating the StatefulSet.
* The PVCs it generates are **invisible to Helm** — `helm uninstall` leaves them
  behind with no ownership metadata and no way to reason about them.
* The stable network identity a StatefulSet exists to give you is worth exactly
  nothing to one pod behind a ClusterIP.

So: Deployment + a PVC Helm owns. That inherits one trap, which the chart closes.

### RollingUpdate + ReadWriteOnce = permanent deadlock

A Deployment defaults to `RollingUpdate`. With a ReadWriteOnce volume that
deadlocks *forever*, not slowly:

1. the new pod cannot start — the volume is still attached to the old node;
2. the old pod is not terminated — the new one never became Ready.

The rollout sits in `ContainerCreating` with `Multi-Attach error for volume` and
only a manual `kubectl delete pod` clears it. The chart sets `strategy: Recreate`
and **fails the render** if `replicaCount > 1`, because a second replica would
either fail to schedule or (on RWX) corrupt the SQLite file — two writers, one
database.

## The PVC survives uninstall — and what that costs you

The claim carries:

```yaml
annotations:
  helm.sh/resource-policy: keep
```

The job table, which links each GitHub job to the runner pod that ran it, cannot
be re-derived from Prometheus; a fat-fingered `helm uninstall` must not delete
it.

The cost shows up on **reinstall under the same release name**:

```
Error: INSTALLATION FAILED: Unable to continue with install:
PersistentVolumeClaim "arc-ui-data" in namespace "arc-systems" exists and cannot be
imported into the current release: invalid ownership metadata; label
"app.kubernetes.io/managed-by" must be "Helm" ...
```

The kept PVC has no ownership metadata for the *new* release. Three ways out:

**Helm 3.17+ — let Helm adopt it:**

```bash
helm install arc-ui ./chart --namespace arc-systems --take-ownership
```

**Any Helm — adopt it by hand:**

```bash
kubectl -n arc-systems annotate pvc arc-ui-data \
  meta.helm.sh/release-name=arc-ui meta.helm.sh/release-namespace=arc-systems --overwrite
kubectl -n arc-systems label pvc arc-ui-data app.kubernetes.io/managed-by=Helm --overwrite
helm install arc-ui ./chart --namespace arc-systems
```

**Or bind it without adopting it** — leave the PVC unmanaged and point the chart
at it:

```bash
helm install arc-ui ./chart --set persistence.existingClaim=arc-ui-data
```

To actually drop the history, delete the PVC explicitly after uninstalling.

## RBAC

`get`, `list`, `watch` on the four ARC resources, pods and events. Deliberately
withheld: `secrets` (ARC keeps the GitHub App credentials there), `pods/exec`
and `pods/attach` (remote code execution in a runner), `pods/log` (job logs
regularly contain masked-but-recoverable values), every write verb, and nodes
and `metrics.k8s.io`, since usage comes from Prometheus.

Runners live in `ARC_UI_NAMESPACES` (default `arc-runners`); the controller and
the AutoscalingListeners live in `ARC_UI_CONTROLLER_NAMESPACE` (default
`arc-systems`). Bind only the runner namespace and every listener shows as
unknown.

`rbac.clusterWide=false` binds the ClusterRole with RoleBindings into
`rbac.namespaces` and `rbac.controllerNamespace` instead.

## Probes

| Probe | Path | Why |
| --- | --- | --- |
| `startupProbe` | `/livez` | 40 × 3s to open and migrate the history database |
| `livenessProbe` | `/livez` | process-local only |
| `readinessProbe` | `/readyz` | informer sync + store reachable |

**Liveness must never point at `/readyz`.** Readiness folds in API server
reachability, so an apiserver blip or a rolled ClusterRole would restart the pod —
discarding every warm informer cache and forcing a full re-LIST of every runner at
the exact moment the API server is already under strain. That makes recovery
strictly slower, and can turn one blip into a CrashLoopBackOff.

## Security context

`runAsNonRoot`, uid 65532, all capabilities dropped, `seccompProfile:
RuntimeDefault`, and `readOnlyRootFilesystem: true`.

That last one has a consequence people hit at runtime, not at install: **SQLite
needs somewhere to write temporary files** — sorter spills, and the journal
fallback when a rollback cannot be held in memory. The chart mounts a
memory-backed `emptyDir` at `/tmp`. Remove it and the first non-trivial history
query fails with `SQLITE_CANTOPEN`, which reads like data corruption and is not.

`fsGroupChangePolicy: OnRootMismatch` is not cosmetic. The default (`Always`)
makes the kubelet **recursively chown the entire volume on every pod start** —
once the database is measured in gigabytes that is a multi-minute hang before the
container even begins, repeated on every single restart.

## Shutdown arithmetic

```
terminationGracePeriodSeconds (45)
  >= preStopDelay (10)  +  shutdownTimeout (20)  +  slack (5)
```

`preStopDelay` keeps the process serving *after* readiness flips false, so
endpoint controllers and every proxy in front stop routing to the pod before the
listener goes away (it must be at least twice the readiness period, or clients see
502s). `shutdownTimeout` is the drain: in-flight requests, SSE streams, and the
SQLite checkpoint. Undershoot the grace period and the kubelet SIGKILLs the
process mid-checkpoint, leaving a WAL to recover on next start. The chart
validates this and refuses to render an impossible combination.

There is no `preStop` exec hook: the image is distroless and has no shell. The
delay is implemented inside the process, from `ARC_UI_PRESTOP_DELAY`.

## Ingress and SSE

`ingress.enabled=true` renders an Ingress with, by default, the annotations that
keep the live stream unbuffered:

```yaml
nginx.ingress.kubernetes.io/proxy-buffering: "off"
nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
```

The app already sets `X-Accel-Buffering: no` on the stream, but a buffering proxy
that ignores it will hold the response and flush nothing for minutes: the page
loads and then simply never updates, with no error anywhere. The long read timeout
matters just as much — an idle SSE connection looks dead to the default 60s, so
the stream is cut and the browser reconnect-loops. Behind Traefik, HAProxy or an
ALB the equivalent knobs have other names; set them through
`ingress.annotations` and leave `ingress.sseAnnotations=false`.

## ServiceMonitor

Guarded on `serviceMonitor.enabled`, a real values flag — **not** on
`.Capabilities.APIVersions.Has` alone. Capabilities are populated from a live
cluster; under `helm template`, which is what Argo CD and every other GitOps
pipeline runs, the set is empty and a capability-only guard makes the resource
disappear from the manifest silently. Nothing errors, metrics simply never get
scraped, and the reason is invisible in the diff. Set
`serviceMonitor.requireCapability=false` in GitOps so the resource always renders.

## Values worth knowing

| Key | Default | Notes |
| --- | --- | --- |
| `replicaCount` | `1` | anything else fails the render |
| `image.repository` / `image.tag` | `ghcr.io/windkube/actions-runner-controller-ui` / chart appVersion | |
| `prometheus.url` | — | **required**; the render fails without it |
| `prometheus.headers` | `{}` | e.g. `X-Scope-OrgID` for Mimir |
| `prometheus.scaleSetLabel` | `name` | the scale set label on `gha_*` series |
| `prometheus.ksmScaleSetLabel` | `label_actions_github_com_scale_set_name` | needs the kube-state-metrics allowlist above |
| `env.*` | see values.yaml | `ARC_UI_*` passthrough; `null` drops it, `""` is emitted |
| `extraEnv` | `[]` | for `valueFrom`: the Sentry DSN, Prometheus token or password |
| `persistence.size` | `1Gi` | the job table only; time series live in Prometheus |
| `persistence.existingClaim` | `""` | bind a kept PVC without adopting it |
| `rbac.clusterWide` | `true` | required when `ARC_UI_NAMESPACES` is empty |
| `gracePeriod.*` | `10` / `20` | rendered into the env vars *and* validated |
| `serviceMonitor.enabled` | `false` | scrapes arc-ui's own `/metrics` |
| `podMonitors.arc.enabled` | `false` | scrapes the ARC controller and listeners |
| `ingress.enabled` | `false` | |
