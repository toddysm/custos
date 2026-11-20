{{/*
Opt-in environment projection. Managed keys deliberately replace legacy profile
env entries; unrelated operator entries survive. Secret values never enter Helm.
*/}}
{{- define "custos.receiverEnv" -}}
{{- $r := .Values.global.receiverRuntime -}}
{{- $service := .Chart.Name -}}
{{- $env := dict -}}
{{- range .Values.env -}}{{- $_ := set $env .name . -}}{{- end -}}
{{- $plain := dict "ENVIRONMENT" "production" "HOST" "0.0.0.0" "PORT" "8080"
  "CUSTOS_HTTP_DRAIN_SECONDS" "15" "DAPR_HTTP_HOST" "127.0.0.1" "DAPR_HTTP_PORT" "3500" -}}
{{- $invoke := "http://127.0.0.1:3500/v1.0/invoke" -}}
{{- $auth := printf "%s/custos-auth/method" $invoke -}}
{{- if has $service (list "api-gateway" "trigger-service" "workflow-service" "activity-runtime-manager") -}}
{{- $_ := set $plain "CUSTOS_EXECUTION_IDENTITY_ENABLED" "true" -}}
{{- $_ := unset $env "CUSTOS_EXECUTION_BINDINGS" -}}
{{- $_ := unset $env "CUSTOS_EXECUTION_BINDINGS_FILE" -}}
{{- $_ := unset $env "CUSTOS_EXECUTION_BINDINGS_SHA256" -}}
{{- if $r.executionBindingsBundle -}}
{{- $_ := set $plain "CUSTOS_EXECUTION_BINDINGS_FILE" "/custos/execution/bindings.json" -}}
{{- $_ := set $plain "CUSTOS_EXECUTION_BINDINGS_SHA256" $r.executionBindingsBundle.sha256 -}}
{{- else -}}
{{- $_ := set $plain "CUSTOS_EXECUTION_BINDINGS" (toJson $r.executionBindings) -}}
{{- end -}}
{{- $_ := set $plain "CUSTOS_EXECUTION_SECRET_STORE" "kubernetes" -}}
{{- $_ := set $plain "CUSTOS_EXECUTION_AUTH_APP_ID" "custos-auth" -}}
{{- end -}}
{{- $stores := dict
  "api-gateway" (list "CUSTOS_GATEWAY_METADATA_STORE")
  "auth-service" (list "CUSTOS_AUTH_STORE_DSN" "CUSTOS_AUTH_METADATA_STORE_DSN")
  "catalog-service" (list "CAT_DEFINITION_STORE" "CAT_CATALOG_STORE" "CAT_METADATA_STORE")
  "connector-service" (list "CONN_CATALOG_STORE" "CONN_METADATA_STORE")
  "workflow-service" (list "WF_METADATA_STORE")
  "trigger-service" (list "TRIGGER_METADATA_STORE")
  "activity-runtime-manager" (list "ARM_METADATA_STORE")
  "observability-audit-service" (list "CUSTOS_OBS_METADATA_STORE_DSN") -}}
{{- range (index $stores $service) -}}
{{- $_ := set $env . (dict "name" . "valueFrom" (dict "secretKeyRef" (dict "name" $r.databaseSecret "key" "uri"))) -}}
{{- end -}}
{{- $config := dict -}}
{{- if eq $service "auth-service" -}}
{{- $config = dict "CUSTOS_AUTH_EXECUTION_CONTEXT_ENABLED" "true"
  "CUSTOS_AUTH_EXECUTION_CALLER_NAMESPACE" .Release.Namespace
  "CUSTOS_AUTH_CALL_CONTEXT_KEY_REF" $r.authSigningKeyRef
  "CUSTOS_AUTH_CALL_CONTEXT_SECRET_STORE" "kubernetes"
  "CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION" "0" -}}
{{- else if eq $service "trigger-service" -}}
{{- $config = dict "TRIGGER_AUTHZ_ENDPOINT" $auth
  "TRIGGER_RECEIVER_CONFIGURATION_ENABLED" "true" "TRIGGER_SOURCE_RECEIVERS_ENABLED" "true"
  "TRIGGER_WEBHOOK_RECEIVER_ENABLED" (toString $r.admissionEnabled)
  "TRIGGER_RECEIVER_WORKERS_ENABLED" (toString $r.admissionEnabled)
  "TRIGGER_DURABLE_DISPATCH_ENABLED" "true" "TRIGGER_DEPLOYMENT_ID" $r.deploymentId
  "TRIGGER_RECEIVER_DRAIN_SECONDS" (toString $r.drainSeconds)
  "TRIGGER_TELEMETRY_ENDPOINT" $r.telemetryEndpoint
  "TRIGGER_WORKFLOW_NAMESPACE" .Release.Namespace "TRIGGER_WORKFLOW_APP_ID" "workflow-service"
  "TRIGGER_GATEWAY_NAMESPACE" .Release.Namespace "TRIGGER_GATEWAY_APP_ID" "api-gateway"
  "TRIGGER_CONNECTOR_APP_ID" "connector-service" "TRIGGER_PUBSUB_COMPONENT" "custos-pubsub"
  "TRIGGER_CONNECTOR_EVENTS_TOPIC" "custos.connector.events"
  "TRIGGER_NORMALIZED_TOPIC" "custos.triggers.normalized"
  "TRIGGER_WORKFLOW_EVENTS_TOPIC" "custos.workflow.events" -}}
{{- else if eq $service "connector-service" -}}
{{- $config = dict "CONN_AUTHZ_ENDPOINT" $auth "CONN_DURABLE_PULL_ENABLED" "true"
  "CONN_PULL_WORKER_ENABLED" (toString $r.admissionEnabled)
  "CONN_RECEIVER_DRAIN_SECONDS" (toString $r.drainSeconds)
  "CONN_TRIGGER_NAMESPACE" .Release.Namespace "CONN_WORKFLOW_NAMESPACE" .Release.Namespace
  "CONN_RUNTIME_NAMESPACE" .Release.Namespace "CONN_DAPR_HTTP_ENDPOINT" "http://127.0.0.1:3500" -}}
{{- $_ := set $config "OTEL_EXPORTER_OTLP_ENDPOINT" $r.telemetryEndpoint -}}
{{- $_ := set $config "OTEL_EXPORTER_OTLP_PROTOCOL" "http/protobuf" -}}
{{- else if eq $service "workflow-service" -}}
{{- $config = dict "WF_AUTHZ_ENDPOINT" $auth "WF_REQUIRE_CALL_CONTEXT" "1"
  "WF_DAPR_WORKFLOW_COMPONENT" "dapr"
  "WF_DAPR_HTTP_ENDPOINT" "http://127.0.0.1:3500"
  "WF_PUBLISH_PUBSUB" "custos-pubsub" "WF_PUBLISH_TOPIC" "custos.workflow.events"
  "WF_TRIGGER_DELIVERY_ENABLED" "true" "WF_TRIGGER_NAMESPACE" .Release.Namespace
  "WF_ARM_ENDPOINT" "activity-runtime-manager" "WF_TS_ENDPOINT" "trigger-service"
  "WF_CONNECTOR_ENDPOINT" "connector-service" "WF_CATALOG_ENDPOINT" "catalog-service" -}}
{{- else if eq $service "activity-runtime-manager" -}}
{{- $config = dict "ARM_AUTHZ_ENDPOINT" $auth "ARM_RUNTIME_ENABLED" "true"
  "ARM_WORKFLOW_NAMESPACE" .Release.Namespace "ARM_ARTIFACT_STORE" "csi"
  "ARM_CATALOG_ENDPOINT" (printf "%s/catalog-service/method" $invoke)
  "ARM_CONNECTOR_ENDPOINT" (printf "%s/connector-service/method" $invoke)
  "ARM_CREDENTIAL_PROXY_URL" (printf "https://%s.%s.svc:8080" (include "svc.fullname" .) .Release.Namespace)
  "ARM_SIDECAR_CA_FILE" "/custos/tls/ca.crt" "CUSTOS_CSI_PVC_MOUNT" "/custos/artifacts"
  "ARM_TLS_CERT_FILE" "/custos/tls/tls.crt" "ARM_TLS_KEY_FILE" "/custos/tls/tls.key"
  "ARM_METRICS_PORT" "8090"
  "ARM_SANDBOX_NAMESPACE" $r.sandboxNamespace "ARM_STAGING_ROOT" "/var/run/custos/attempts"
  "ARM_SIDECAR_IMAGE" $r.sidecarImage "ARM_IO_BRIDGE_IMAGE" $r.ioBridgeImage -}}
{{- else if eq $service "catalog-service" -}}
{{- $config = dict "CAT_AUTHZ_ENDPOINT" $auth
  "CAT_CONNECTOR_ENDPOINT" (printf "%s/connector-service/method" $invoke) -}}
{{- else if eq $service "observability-audit-service" -}}
{{- $config = dict "CUSTOS_OBS_AUTHZ_JWKS_URL" (printf "%s/.well-known/jwks.json" $auth) -}}
{{- end -}}
{{- range $name, $value := mergeOverwrite $plain $config -}}
{{- $_ := set $env $name (dict "name" $name "value" (toString $value)) -}}
{{- end -}}
{{- $_ := set $env "APP_API_TOKEN" (dict "name" "APP_API_TOKEN" "valueFrom"
  (dict "secretKeyRef" (dict "name" (index $r.appTokenSecrets $service) "key" "token"))) -}}
{{- $_ := set $env "DAPR_API_TOKEN" (dict "name" "DAPR_API_TOKEN" "valueFrom"
  (dict "secretKeyRef" (dict "name" $r.apiTokenSecret "key" "token"))) -}}
{{- range $name := keys $env | sortAlpha }}
- {{ toYaml (index $env $name) | nindent 2 | trim }}
{{- end }}
{{- end -}}

{{- define "custos.receiverAnnotations" -}}
{{- $r := (.Values.global | default dict).receiverRuntime | default dict -}}
{{- if $r.enabled }}
dapr.io/app-token-secret: {{ index $r.appTokenSecrets .Chart.Name | quote }}
dapr.io/api-token-secret: {{ $r.apiTokenSecret | quote }}
{{- if and $r.executionBindingsBundle (has .Chart.Name (list "api-gateway" "trigger-service" "workflow-service" "activity-runtime-manager")) }}
dapr.io/config: {{ index $r.executionBindingsBundle.daprConfigurations .Chart.Name | quote }}
checksum/execution-bindings: {{ $r.executionBindingsBundle.sha256 | quote }}
checksum/execution-bundle: {{ $r.executionBindingsBundle.bundleSha256 | quote }}
{{- else }}
dapr.io/config: {{ printf "%s-receivers-%s" .Release.Name .Chart.Name | quote }}
{{- end }}
dapr.io/graceful-shutdown-seconds: "5"
{{- if has .Chart.Name (list "trigger-service" "connector-service" "workflow-service" "activity-runtime-manager") }}
dapr.io/block-shutdown-duration: {{ printf "%ds" (add (int $r.drainSeconds) 60) | quote }}
dapr.io/enable-app-health-check: "false"
{{- end }}
checksum/receiver-config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum | quote }}
{{- end }}
{{- end -}}

{{/* One read-only projected directory; no subPath, hot reload or API access. */}}
{{- define "custos.executionVolume" -}}
- name: execution-bindings
  configMap:
    name: {{ .Values.global.receiverRuntime.executionBindingsBundle.configMapName | quote }}
    defaultMode: 0444
    items:
      - key: bindings.json
        path: bindings.json
{{- end -}}

{{- define "custos.executionMount" -}}
- name: execution-bindings
  mountPath: /custos/execution
  readOnly: true
{{- end -}}
