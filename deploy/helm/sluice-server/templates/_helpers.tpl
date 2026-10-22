{{- define "sluice.name" -}}{{ .Chart.Name }}{{- end -}}
{{- define "sluice.fullname" -}}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "sluice.labels" -}}
app.kubernetes.io/name: {{ include "sluice.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "sluice.selector" -}}
app.kubernetes.io/name: {{ include "sluice.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "sluice.secretName" -}}{{ default (include "sluice.fullname" .) .Values.auth.existingSecret }}{{- end -}}
