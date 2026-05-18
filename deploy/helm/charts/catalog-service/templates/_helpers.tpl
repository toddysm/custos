{{- define "svc.name" -}}{{ .Chart.Name }}{{- end -}}
{{- define "svc.fullname" -}}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "svc.labels" -}}
app.kubernetes.io/name: {{ include "svc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .Chart.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}
{{- define "svc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "svc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "svc.daprAnnotations" -}}
{{- if .Values.dapr.enabled }}
dapr.io/enabled: "true"
dapr.io/app-id: {{ include "svc.name" . | quote }}
dapr.io/app-port: {{ .Values.dapr.appPort | quote }}
dapr.io/app-protocol: {{ .Values.dapr.appProtocol | quote }}
dapr.io/log-level: {{ .Values.dapr.logLevel | quote }}
{{- end }}
{{- end -}}
{{/*
Compose container image reference. Order of precedence:
  1. .Values.image.repository (full repo path) — explicit per-subchart override.
  2. <.Values.global.imageRegistry>/<chart name> — default umbrella-provided registry.
Tag precedence: .Values.image.tag, then .Values.global.imageTag, then "dev".
*/}}
{{- define "svc.image" -}}
{{- $global := .Values.global | default dict -}}
{{- $registry := $global.imageRegistry | default "ghcr.io/toddysm/custos" -}}
{{- $repo := default (printf "%s/%s" $registry .Chart.Name) .Values.image.repository -}}
{{- $tag := default ($global.imageTag | default "dev") .Values.image.tag -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- define "svc.imagePullPolicy" -}}
{{- $global := .Values.global | default dict -}}
{{- default ($global.imagePullPolicy | default "IfNotPresent") .Values.image.pullPolicy -}}
{{- end -}}
{{- define "svc.replicaCount" -}}
{{- $global := .Values.global | default dict -}}
{{- default ($global.replicaCount | default 1) .Values.replicaCount -}}
{{- end -}}
{{- define "svc.resources" -}}
{{- $global := .Values.global | default dict -}}
{{- $local := .Values.resources | default dict -}}
{{- if $local }}
{{- toYaml $local -}}
{{- else if $global.resources -}}
{{- toYaml $global.resources -}}
{{- else -}}
{}
{{- end -}}
{{- end -}}
