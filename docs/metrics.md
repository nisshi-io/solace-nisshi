# Metrics

Metrics are exported over OTLP/HTTP when `--otlp-endpoint-url` (or
`OTEL_EXPORTER_OTLP_ENDPOINT`) is set. `v1/metrics` is appended to the URL's
path, so `https://collector:4318` and `https://gateway/otlp` export to
`https://collector:4318/v1/metrics` and `https://gateway/otlp/v1/metrics`.
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is not read. Collector credentials go
in `OTEL_EXPORTER_OTLP_HEADERS`.

The exported resource honours the standard OpenTelemetry environment
variables. Every `key=value` pair in `OTEL_RESOURCE_ATTRIBUTES` is attached
as given; values are not percent-decoded, and entries without a `=` are
ignored with a warning. `service.name` is resolved as `OTEL_SERVICE_NAME`,
then `service.name` in `OTEL_RESOURCE_ATTRIBUTES`, then the default for the
subcommand (`nisshi-broker`, or `nisshi-proxy` / `nisshi-generator` for
`proxy` and `generator`). Unset, empty or whitespace-only values fall through
to the next source. The endpoint, `service.name` and attribute names are
logged at `info` on startup. Resource attributes are sent with every export,
so use an `https` endpoint when they carry anything sensitive.

```shell
OTEL_EXPORTER_OTLP_ENDPOINT=https://collector:4318/ \
OTEL_SERVICE_NAME=kafka-broker \
OTEL_RESOURCE_ATTRIBUTES=service.version=0.7.0,deployment.environment.name=staging \
nisshi broker
```

Note for existing deployments: before 0.7 `service.name` was always
`nisshi-broker` and these variables were ignored. If `OTEL_SERVICE_NAME` or
`OTEL_RESOURCE_ATTRIBUTES` is already set in the broker's environment, for
example injected pod-wide for another agent, its series move to that name on
upgrade and dashboards or alerts keyed on `nisshi-broker` go quiet. Unset the
inherited variable or set `OTEL_SERVICE_NAME=nisshi-broker`. Rolling back
reverts to `nisshi-broker` regardless of these variables. On shutdown the
broker now flushes pending metrics before exiting.
