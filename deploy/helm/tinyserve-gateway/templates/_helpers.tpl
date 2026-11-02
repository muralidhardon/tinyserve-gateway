{{- define "tsg.labels" -}}
app.kubernetes.io/name: tinyserve-gateway
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{- define "tsg.selector" -}}
app.kubernetes.io/name: tinyserve-gateway
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "tsg.tinyserve" -}}{{ .Release.Name }}-tinyserve{{- end }}
{{- define "tsg.gateway" -}}{{ .Release.Name }}-gateway{{- end }}
