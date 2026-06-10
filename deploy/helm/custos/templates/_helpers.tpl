{{/*
Common helpers for the Custos umbrella chart.
*/}}

{{- define "custos.namespace" -}}
{{- .Release.Namespace -}}
{{- end -}}

{{- define "custos.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
custos.io/topology: {{ .Values.topology | quote }}
custos.io/profile: {{ .Values.profile | quote }}
{{- end -}}

{{- define "custos.imageRegistry" -}}
{{- default "ghcr.io/toddysm/custos" .Values.global.imageRegistry -}}
{{- end -}}
