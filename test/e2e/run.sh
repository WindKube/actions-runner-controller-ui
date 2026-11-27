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
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if [[ -n "${failed:-}" ]]; then
    kubectl get pods -A -o wide || true
    kubectl get autoscalingrunnersets -A -o yaml || true
    kubectl -n "$ns" logs deploy/arc-gha-rs-controller --tail=60 || true
    kubectl -n "$ns" logs deploy/arc-ui --tail=100 || true
    kubectl -n "$ns" describe pods || true
  fi
}
trap 'failed=1' ERR
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

step "install arc-ui with the bundled store"
helm repo add vm https://victoriametrics.github.io/helm-charts --force-update
helm repo add grafana https://grafana.github.io/helm-charts --force-update
helm dependency build chart/
helm install arc-ui chart/ -n "$ns" --wait --timeout 5m \
  --set image.repository=arc-ui --set image.tag=e2e --set image.pullPolicy=Never \
  --set victoria-metrics-single.enabled=true \
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

step "the store scrapes cAdvisor and the ARC controller"
forward arc-ui-vm 18428 8428 /health
up() {
  curl -fsS "http://127.0.0.1:18428/api/v1/query" --data-urlencode "query=max(up{job=\"$1\"})" \
    | grep -q '"value":\[[0-9.]*,"1"\]'
}
for job in kubelet-cadvisor arc; do
  for i in $(seq 40); do
    up "$job" && { echo "$job up"; break; }
    [[ $i == 40 ]] && { echo "$job never came up in the store" >&2; exit 1; }
    sleep 5
  done
done

step "ok"
