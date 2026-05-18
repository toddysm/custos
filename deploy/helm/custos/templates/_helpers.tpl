{{/*
Common helpers for the Custos umbrella chart.
*/}}

{{- define "custos.namespace" -}}
{{- .Release.Namespace -}}
{{- end -}}

{{- define "custos.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
custos.io/topology: {{ .Values.topology | quote }}
custos.io/profile: {{ .Values.profile | quote }}
{{- end -}}

{{- define "custos.imageRegistry" -}}
{{- default "ghcr.io/toddysm/custos" .Values.global.imageRegistry -}}
{{- end -}}
