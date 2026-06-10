{{- define "svc.name" -}}{{ .Chart.Name }}{{- end -}}
{{- define "svc.fullname" -}}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "svc.labels" -}}
app.kubernetes.io/name: {{ include "svc.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/component: {{ .Chart.Name | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{- end -}}
{{- define "svc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "svc.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}
{{- define "svc.daprAnnotations" -}}
{{- if .Values.dapr.enabled }}
dapr.io/enabled: "true"
dapr.io/app-id: {{ .Values.dapr.appId | default (include "svc.name" .) | quote }}
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
{{- /*
Resolve whether HA-gated autoscaling is on for this service. Per-service
`.Values.autoscaling.enabled` (true/false) overrides; when null it inherits
`.Values.global.profile == "ha"`. Returns the string "true" or "false" so
callers compare with `eq (include "svc.autoscalingEnabled" .) "true"`.
*/ -}}
{{- define "svc.autoscalingEnabled" -}}
{{- $as := .Values.autoscaling | default dict -}}
{{- $global := .Values.global | default dict -}}
{{- if not (kindIs "invalid" $as.enabled) -}}
{{- $as.enabled -}}
{{- else -}}
{{- eq ($global.profile | default "eval") "ha" -}}
{{- end -}}
{{- end -}}
{{- /*
Resolve whether a PodDisruptionBudget should render. Same inheritance rule as
autoscaling: per-service `.Values.podDisruptionBudget.enabled` overrides, else
inherits the HA profile. Returns "true"/"false".
*/ -}}
{{- define "svc.pdbEnabled" -}}
{{- $pdb := .Values.podDisruptionBudget | default dict -}}
{{- $global := .Values.global | default dict -}}
{{- if not (kindIs "invalid" $pdb.enabled) -}}
{{- $pdb.enabled -}}
{{- else -}}
{{- eq ($global.profile | default "eval") "ha" -}}
{{- end -}}
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
