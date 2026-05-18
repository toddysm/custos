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
