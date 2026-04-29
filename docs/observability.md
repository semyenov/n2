# Observability

N2 services export Effect traces, metrics, and logs through OTLP when an
OpenTelemetry endpoint is configured. The local Docker Compose stack routes
that telemetry to Grafana, Tempo, Prometheus, and Loki.

## Local Docker Compose

Start the profile and request providers with the local observability stack:

```bash
docker compose up -d --build
```

The Compose stack enables observability for both services with:

```bash
OBSERVABILITY_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
DEPLOYMENT_ENVIRONMENT=local
```

The collector receives OTLP on:

- `4317` for gRPC
- `4318` for HTTP

The UI endpoints are:

- Grafana: <http://localhost:3333>
- Prometheus: <http://localhost:9091>
- Tempo: <http://localhost:3200>
- Loki: <http://localhost:3100>
- ClickHouse UI: <http://localhost:3488>
- ClickHouse built-in SQL playground: <http://localhost:8123/play>

For ClickHouse UI and the built-in playground, use the local Compose
credentials `n2` / `n2`.

If any of those ports are already taken, set `GRAFANA_PORT`,
`PROMETHEUS_PORT`, `TEMPO_PORT`, `LOKI_PORT`, `CLICKHOUSE_UI_PORT`, or
`CLICKHOUSE_HTTP_PORT` before starting Compose. For example:

```bash
GRAFANA_PORT=3334 PROMETHEUS_PORT=9092 docker compose up -d --build
```

Run the service smoke test to generate traces, metrics, and logs:

```bash
bun run test:services
```

Run the configurable service stress test when you want a denser local telemetry
sample for the Grafana dashboard:

```bash
bun run stress:services
STRESS_ITERATIONS=100 STRESS_CONCURRENCY=10 bun run stress:services
STRESS_SERVICES=profile-provider bun run stress:services
```

## Grafana

Open <http://localhost:3333>. Anonymous local admin access is enabled in
Compose for development.

Open **Dashboards** -> **N2 Services** -> **N2 Services** for the provisioned
service dashboard. Use the `Service` variable to switch between
`profile-provider` and `request-provider`. Run `bun run test:services` after
the stack is healthy to generate sample command metrics, traces, and logs for
both services.

Use **Explore** with these datasources:

- `Tempo` for traces. Search by service name, then inspect spans such as
  `POST /rpc/profile-provider`, `POST /rpc/request-provider`, and `GET /health`.
- `Prometheus` for metrics. Start with the metric browser and filter for the
  `n2_` namespace.
- `Loki` for logs. Filter by service labels such as `service_name`.

Grafana datasources are provisioned from
`observability/grafana/provisioning/datasources/datasources.yaml`.
Grafana dashboards are provisioned from
`observability/grafana/provisioning/dashboards/dashboards.yaml` and loaded from
`observability/grafana/dashboards/`.

## Pipeline

Services send OTLP HTTP to `otel-collector:4318`.

- Traces go from the collector to Tempo.
- Metrics are exposed by the collector on `:8889` and scraped by Prometheus.
- Logs go from the collector to Loki through Loki's OTLP endpoint.

For low-level troubleshooting:

```bash
docker compose ps
docker compose logs -f otel-collector
docker compose logs -f grafana tempo prometheus loki
```

## Runtime Configuration

Services opt in by passing `observability` to `makeServerEntrypoint` or
`makeClusterEntrypoint`. The runtime layer is disabled unless
`OTEL_EXPORTER_OTLP_ENDPOINT` is present or `OBSERVABILITY_ENABLED=true`.

Useful environment variables:

- `OTEL_EXPORTER_OTLP_ENDPOINT` - OTLP HTTP base URL, for example `http://localhost:4318`
- `OBSERVABILITY_ENABLED` - set `true` to use the default local endpoint when no endpoint is set
- `OTEL_SERVICE_NAME` - optional override for the service name configured in code
- `OTEL_RESOURCE_ATTRIBUTES` - comma-separated resource attributes, for example `service.namespace=n2`
- `DEPLOYMENT_ENVIRONMENT` - resource environment attribute, default `local`

If the collector is not configured, services keep using the normal console
logger and do not require an observability backend.
