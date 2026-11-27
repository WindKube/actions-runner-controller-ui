{{- define "arc-ui.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "arc-ui.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "arc-ui.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "arc-ui.labels" -}}
helm.sh/chart: {{ include "arc-ui.chart" . }}
{{ include "arc-ui.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: arc-ui
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Nothing version-dependent may go here: Deployment.spec.selector is immutable,
and a changing selector fails every upgrade.
*/}}
{{- define "arc-ui.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arc-ui.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "arc-ui.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "arc-ui.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "arc-ui.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- printf "%s-data" (include "arc-ui.fullname" .) }}
{{- end }}
{{- end }}

{{- define "arc-ui.vmURL" -}}
{{- $vm := index .Values "victoria-metrics-single" }}
{{- if $vm.enabled }}
{{- $svc := $vm.server.service | default dict }}
{{- $port := dig "portOverrides" "http" "port" "" $svc | default $svc.servicePort | default 8428 }}
{{- printf "http://%s.%s.svc:%v" $vm.server.fullnameOverride .Release.Namespace $port }}
{{- end }}
{{- end }}

{{/* The store scrapes unless the collector writes into it, which would count everything twice. */}}
{{- define "arc-ui.vmScrapes" -}}
{{- $vm := index .Values "victoria-metrics-single" }}
{{- $url := .Values.collection.remoteWrite.url }}
{{- $host := printf "^https?://%s(\\.%s(\\.svc(\\.[^:/]+)?)?)?([:/]|$)" (regexQuoteMeta $vm.server.fullnameOverride) (regexQuoteMeta .Release.Namespace) }}
{{- $intoVM := or (not $url) (regexMatch $host $url) }}
{{- if and $vm.enabled (not (and .Values.collector.enabled $intoVM)) }}true{{ end }}
{{- end }}

{{/* An explicit prometheus.url wins; otherwise the bundled store, if on. */}}
{{- define "arc-ui.prometheusURL" -}}
{{- .Values.prometheus.url | default (include "arc-ui.vmURL" .) }}
{{- end }}

{{- define "arc-ui.remoteWriteURL" -}}
{{- with .Values.collection.remoteWrite.url }}
{{- . }}
{{- else }}
{{- with include "arc-ui.vmURL" . }}{{ . }}/api/v1/write{{ end }}
{{- end }}
{{- end }}

{{/* Runner namespaces, sorted and comma-joined. Unset is the app's default, arc-runners; "" means all. */}}
{{- define "arc-ui.runnerNamespaces" -}}
{{- $raw := "arc-runners" }}
{{- if and (hasKey .Values.env "ARC_UI_NAMESPACES") (not (kindIs "invalid" .Values.env.ARC_UI_NAMESPACES)) }}
{{- $raw = .Values.env.ARC_UI_NAMESPACES }}
{{- end }}
{{- include "arc-ui.sortedList" $raw }}
{{- end }}

{{/* A comma string or a list, trimmed, sorted and comma-joined. */}}
{{- define "arc-ui.sortedList" -}}
{{- $in := . }}
{{- if kindIs "slice" $in }}{{ $in = join "," $in }}{{ end }}
{{- $out := list }}
{{- range splitList "," ($in | default "") }}
{{- with trim . }}{{ $out = append $out . }}{{ end }}
{{- end }}
{{- $out | sortAlpha | join "," }}
{{- end }}

{{/* host:port of the bundled kube-state-metrics, when it is on. */}}
{{- define "arc-ui.ksmTarget" -}}
{{- with .Values.kubeStateMetrics }}
{{- if .enabled }}
{{- printf "%s.%s.svc:%v" .fullnameOverride $.Release.Namespace (.service.port | default 8080) }}
{{- end }}
{{- end }}
{{- end }}

{{/* Runner namespaces as an anchored-by-Prometheus regex; empty means all. */}}
{{- define "arc-ui.runnerNamespaceRegex" -}}
{{- $ns := list }}
{{- range splitList "," (include "arc-ui.runnerNamespaces" .) }}
{{- with trim . }}{{ $ns = append $ns (regexQuoteMeta .) }}{{ end }}
{{- end }}
{{- if $ns }}{{ join "|" $ns }}{{ else }}.+{{ end }}
{{- end }}

{{- define "arc-ui.controllerNamespace" -}}
{{- .Values.env.ARC_UI_CONTROLLER_NAMESPACE | default "arc-systems" }}
{{- end }}

{{/* A map as k=v,k=v, sorted so the rendered pod spec is stable. */}}
{{- define "arc-ui.headers" -}}
{{- $pairs := list }}
{{- range $k := keys . | sortAlpha }}
{{- $pairs = append $pairs (printf "%s=%s" $k (get $ $k)) }}
{{- end }}
{{- join "," $pairs }}
{{- end }}

{{- define "arc-ui.validate" -}}
{{- if ne (int .Values.replicaCount) 1 -}}
{{- fail (printf "arc-ui: replicaCount=%v is not supported; the app owns one SQLite file on one ReadWriteOnce volume, so it runs exactly one replica" .Values.replicaCount) -}}
{{- end -}}
{{- if not (include "arc-ui.prometheusURL" .) -}}
{{- fail "arc-ui: prometheus.url is required unless victoria-metrics-single.enabled; every chart and usage figure is a PromQL query" -}}
{{- end -}}
{{- if and .Values.collector.enabled (not (include "arc-ui.remoteWriteURL" .)) -}}
{{- fail "arc-ui: collector.enabled needs collection.remoteWrite.url or victoria-metrics-single.enabled to send samples to" -}}
{{- end -}}
{{- with .Values.kubeStateMetrics -}}
{{- if .enabled -}}
{{- $ksm := include "arc-ui.sortedList" .namespaces -}}
{{- $app := include "arc-ui.runnerNamespaces" $ -}}
{{- if ne $ksm $app -}}
{{- fail (printf "arc-ui: kubeStateMetrics.namespaces (%q) must list the same namespaces as env.ARC_UI_NAMESPACES (%q), or the runner pods have no kube_pod_labels" $ksm $app) -}}
{{- end -}}
{{- if and (not $ksm) (not .rbac.useClusterRole) -}}
{{- fail "arc-ui: kubeStateMetrics.namespaces is empty (every namespace), which needs kubeStateMetrics.rbac.useClusterRole=true" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if and .Values.persistence.enabled (not .Values.persistence.existingClaim) -}}
{{- if not .Values.persistence.size -}}
{{- fail "arc-ui: persistence.enabled is true but persistence.size is empty" -}}
{{- end -}}
{{- end -}}
{{- if lt (int .Values.gracePeriod.preStopDelaySeconds) 0 -}}
{{- fail (printf "arc-ui: gracePeriod.preStopDelaySeconds=%v must not be negative" .Values.gracePeriod.preStopDelaySeconds) -}}
{{- end -}}
{{- if le (int .Values.gracePeriod.shutdownTimeoutSeconds) 0 -}}
{{- fail (printf "arc-ui: gracePeriod.shutdownTimeoutSeconds=%v must be positive" .Values.gracePeriod.shutdownTimeoutSeconds) -}}
{{- end -}}
{{- $needed := add (int .Values.gracePeriod.preStopDelaySeconds) (int .Values.gracePeriod.shutdownTimeoutSeconds) 5 -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) $needed -}}
{{- fail (printf "arc-ui: terminationGracePeriodSeconds=%v is shorter than preStopDelay(%v) + shutdownTimeout(%v) + 5s slack = %vs; the process would be SIGKILLed mid-shutdown" .Values.terminationGracePeriodSeconds .Values.gracePeriod.preStopDelaySeconds .Values.gracePeriod.shutdownTimeoutSeconds $needed) -}}
{{- end -}}
{{- end -}}
