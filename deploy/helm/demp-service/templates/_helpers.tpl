{{- define "demp.name" -}}
{{- default .Release.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "demp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "demp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "demp.labels" -}}
{{ include "demp.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
app.kubernetes.io/part-of: demp
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
