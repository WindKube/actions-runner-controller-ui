# arc-ui Helm chart

Read-only dashboard for GitHub Actions Runner Controller. One replica, one PVC, a
ClusterRole with `get`/`list`/`watch` and nothing else.

```bash
helm install arc-ui ./chart \
  --namespace arc-systems --create-namespace \
  --set env.ARC_UI_NAMESPACES=arc-runners \
  --set env.ARC_UI_CONTROLLER_NAMESPACE=arc-systems
```

`values.yaml` is commented in full; this file covers only the parts that will
otherwise bite you.

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

The dashboard's value is its history. Months of samples cannot be re-derived from
anywhere, because nothing else keeps them; a fat-fingered `helm uninstall` must
not delete that.

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

`get`, `list`, `watch`. That is the whole verb set. Deliberately withheld:
`secrets` (ARC keeps the GitHub App credentials there), `pods/exec` and
`pods/attach` (remote code execution in a runner), `pods/log` (job logs regularly
contain masked-but-recoverable values), and every write verb.

Two things about scope:

**Both namespaces.** Runners live in `ARC_UI_NAMESPACES` (default `arc-runners`);
the controller and the **AutoscalingListener pods** live in
`ARC_UI_CONTROLLER_NAMESPACE` (default `arc-systems`). The listener carries queue
depth and listener health. Bind only the runner namespace and the fleet renders
perfectly while the listener panel stays blank forever.

**`metrics.k8s.io` resources are `pods` and `nodes`.** Not `podmetrics`, not
`nodemetrics`, not `pods/metrics`. The Go kinds are `PodMetrics`/`NodeMetrics`,
which is where the wrong guess comes from — and the wrong name is accepted
silently by the apiserver, matches nothing, and yields a 403 on every scrape that
is **indistinguishable from metrics-server not being installed**. If usage columns
are empty, check this rule before you check metrics-server:

```bash
kubectl auth can-i list pods.metrics.k8s.io \
  --as=system:serviceaccount:arc-systems:arc-ui
```

Set `rbac.clusterWide=false` for RoleBindings into named namespaces instead of a
ClusterRoleBinding. Tighter, but nodes and node metrics are cluster-scoped and
will be denied — the dashboard reports them as unavailable sources rather than
failing, which is the intended degradation.

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
  >= preStopDelay (8)  +  shutdownTimeout (20)  +  slack (5)
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
| `env.*` | see values.yaml | every `ARC_UI_*` variable; `null` drops it, `""` is emitted |
| `extraEnv` | `[]` | for `valueFrom` (put the Sentry DSN here) |
| `persistence.size` | `4Gi` | resizable if the StorageClass allows expansion |
| `persistence.existingClaim` | `""` | bind a kept PVC without adopting it |
| `rbac.clusterWide` | `true` | required when `ARC_UI_NAMESPACES` is empty |
| `gracePeriod.*` | `8` / `20` | rendered into the env vars *and* validated |
| `serviceMonitor.enabled` | `false` | |
| `ingress.enabled` | `false` | |
