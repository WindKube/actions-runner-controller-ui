#!/usr/bin/env bash
# Installs ARC and arc-ui (with the bundled VictoriaMetrics store) into the
# current kube context, normally a throwaway kind cluster, and checks that the
# app serves, stays read-only, and that cAdvisor is scraped via nodes/metrics.
set -euo pipefail

cd "$(dirname "$0")/../.."
ns=arc-systems
image=arc-ui:e2e
arc_version="$(cat internal/arcapi/v1alpha1/arc-version)"
cluster="${KIND_CLUSTER:-kind}"

step() { printf '\n== %s\n' "$*"; }

pids=()
cleanup() {
  local rc=$?
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if (( rc != 0 )); then
    kubectl get pods -A -o wide || true
    kubectl get autoscalingrunnersets -A -o yaml || true
    kubectl -n "$ns" logs deploy/arc-gha-rs-controller --tail=60 || true
    kubectl -n "$ns" logs deploy/arc-ui --tail=100 || true
    kubectl -n "$ns" describe pods || true
  fi
}
trap cleanup EXIT

forward() {
  kubectl -n "$ns" port-forward "svc/$1" "$2:$3" >/dev/null &
  pids+=($!)
  for _ in $(seq 30); do curl -fs -o /dev/null "http://127.0.0.1:$2$4" && return 0; sleep 1; done
  echo "port-forward to $1 never answered" >&2
  return 1
}

step "build and load $image"
docker build -t "$image" .
kind load docker-image "$image" --name "$cluster"

step "install ARC $arc_version"
helm install arc oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set-controller \
  --version "$arc_version" -n "$ns" --create-namespace --wait --timeout 5m \
  --set metrics.controllerManagerAddr=:8080 \
  --set metrics.listenerAddr=:8080 \
  --set metrics.listenerEndpoint=/metrics

helm install e2e-set oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --version "$arc_version" -n arc-runners --create-namespace \
  --set githubConfigUrl=https://github.com/example/e2e \
  --set githubConfigSecret.github_token=not-a-token \
  --set maxRunners=2 \
  --set controllerServiceAccount.namespace="$ns" \
  --set controllerServiceAccount.name=arc-gha-rs-controller
kubectl -n arc-runners get autoscalingrunnersets

# The dummy token means ARC never starts a runner, so this stands in for one:
# a pod carrying the labels ARC puts on runner pods.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: e2e-set-runner-stand-in
  namespace: arc-runners
  labels:
    actions-ephemeral-runner: "True"
    actions.github.com/scale-set-name: e2e-set
    actions.github.com/scale-set-namespace: arc-runners
spec:
  containers:
    - name: runner
      image: registry.k8s.io/pause:3.10
      resources:
        requests: {cpu: 10m, memory: 16Mi}
EOF

step "install arc-ui with the bundled store and kube-state-metrics"
helm repo add vm https://victoriametrics.github.io/helm-charts --force-update
helm repo add grafana https://grafana.github.io/helm-charts --force-update
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
helm dependency build chart/
helm install arc-ui chart/ -n "$ns" --wait --timeout 5m \
  --set image.repository=arc-ui --set image.tag=e2e --set image.pullPolicy=Never \
  --set victoria-metrics-single.enabled=true \
  --set kubeStateMetrics.enabled=true \
  --set podMonitors.arc.enabled=false \
  --set env.ARC_UI_NAMESPACES=arc-runners \
  --set env.ARC_UI_CONTROLLER_NAMESPACE="$ns"

step "the app is read-only"
sa="system:serviceaccount:$ns:arc-ui"
for verb in create update patch delete; do
  for res in pods autoscalingrunnersets.actions.github.com ephemeralrunners.actions.github.com; do
    [[ "$(kubectl auth can-i "$verb" "$res" -n arc-runners --as "$sa" || true)" == no ]] \
      || { echo "arc-ui can $verb $res" >&2; exit 1; }
  done
done
denied() {
  [[ "$(kubectl auth can-i "$@" -n arc-runners --as "$sa" || true)" == no ]] \
    || { echo "arc-ui can $*" >&2; exit 1; }
}
denied get secrets
denied get nodes
denied create pods --subresource=exec
denied create pods --subresource=attach
denied get pods --subresource=log
[[ "$(kubectl auth can-i list autoscalingrunnersets.actions.github.com -n arc-runners --as "$sa")" == yes ]]

step "the scraper has nodes/metrics and not nodes/proxy"
vm_sa="system:serviceaccount:$ns:arc-ui-vm"
[[ "$(kubectl auth can-i get nodes --subresource=metrics --as "$vm_sa")" == yes ]]
[[ "$(kubectl auth can-i get nodes --subresource=proxy --as "$vm_sa" || true)" == no ]]

step "the app serves"
forward arc-ui 18080 8080 /livez
curl -fsS http://127.0.0.1:18080/readyz
for path in / /runnersets/e2e-set /workflows /jobs /alerts; do
  curl -fsS -o /dev/null -w "$path %{http_code}\n" "http://127.0.0.1:18080$path"
done
curl -fsS http://127.0.0.1:18080/ | grep -q e2e-set || { echo "overview does not list e2e-set" >&2; exit 1; }
curl -fsS http://127.0.0.1:18080/metrics | grep -q '^arc_ui_sse_clients'

step "the store scrapes cAdvisor, the ARC controller and kube-state-metrics"
forward arc-ui-vm 18428 8428 /health
query() {
  curl -fsS "http://127.0.0.1:18428/api/v1/query" --data-urlencode "query=$1"
}
eventually() {
  local what=$1 q=$2
  for i in $(seq 40); do
    query "$q" | grep -q '"value":\[[0-9.]*,"1"\]' && { echo "$what"; return 0; }
    [[ $i == 40 ]] && { echo "never saw: $what" >&2; query "$q" >&2 || true; exit 1; }
    sleep 5
  done
}
for job in kubelet-cadvisor arc kube-state-metrics; do
  eventually "$job up" "max(up{job=\"$job\"})"
done
eventually "kube_pod_labels carries the ARC labels" \
  'max(kube_pod_labels{namespace="arc-runners",pod="e2e-set-runner-stand-in",label_actions_ephemeral_runner="True",label_actions_github_com_scale_set_name="e2e-set",label_actions_github_com_scale_set_namespace="arc-runners"})'

step "ok"
