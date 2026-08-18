{{/*
Expand the name of the chart.
*/}}
{{- define "arc-ui.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name, capped at 63 chars for the DNS label limit.
*/}}
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

{{/*
Chart name and version, as a label value (no "+" allowed).
*/}}
{{- define "arc-ui.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels. These go on every object and MAY change between releases —
app.kubernetes.io/version in particular changes on every upgrade.
*/}}
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
Selector labels.

NOTHING VERSION-DEPENDENT MAY EVER GO IN HERE. Deployment.spec.selector is
immutable after creation: add app.kubernetes.io/version (or the chart version,
or an image tag) and the next `helm upgrade` fails with

    field is immutable

and the only fix is deleting the Deployment — which, with the Recreate strategy
and a single PVC, means downtime. Keep this to identity, not to state.
*/}}
{{- define "arc-ui.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arc-ui.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
The ServiceAccount name to use.
*/}}
{{- define "arc-ui.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "arc-ui.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The PVC name. Explicit, not a volumeClaimTemplate: see the comment in pvc.yaml.
*/}}
{{- define "arc-ui.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- printf "%s-data" (include "arc-ui.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Guard rails, evaluated once from deployment.yaml. `fail` aborts the render, so
these are caught by `helm template` in CI — not at 3am by a Deployment that can
never roll.
*/}}
{{- define "arc-ui.validate" -}}
{{- if gt (int .Values.replicaCount) 1 -}}
{{- fail (printf "arc-ui: replicaCount=%v is not supported. This is a single-writer app: it owns one SQLite file on one ReadWriteOnce PVC, and a second replica would either fail to schedule (the volume is already attached) or corrupt the database (if the volume is RWX). Scale the cluster, not the dashboard." .Values.replicaCount) -}}
{{- end -}}
{{- if and .Values.persistence.enabled (not .Values.persistence.existingClaim) -}}
{{- if not .Values.persistence.size -}}
{{- fail "arc-ui: persistence.enabled is true but persistence.size is empty" -}}
{{- end -}}
{{- end -}}
{{/*
terminationGracePeriodSeconds must outlast the whole shutdown sequence:
  preStopDelay (readiness flipped, still serving)  ARC_UI_PRESTOP_DELAY
+ in-flight request drain + store close            ARC_UI_SHUTDOWN_TIMEOUT
+ slack for a slow SQLite checkpoint
Undershoot it and the kubelet SIGKILLs mid-checkpoint, which is exactly how a
WAL ends up needing recovery on the next start.
*/}}
{{- $needed := add (int .Values.gracePeriod.preStopDelaySeconds) (int .Values.gracePeriod.shutdownTimeoutSeconds) 5 -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) $needed -}}
{{- fail (printf "arc-ui: terminationGracePeriodSeconds=%v is shorter than preStopDelay(%v) + shutdownTimeout(%v) + 5s slack = %vs; the process would be SIGKILLed mid-shutdown" .Values.terminationGracePeriodSeconds .Values.gracePeriod.preStopDelaySeconds .Values.gracePeriod.shutdownTimeoutSeconds $needed) -}}
{{- end -}}
{{- end -}}
