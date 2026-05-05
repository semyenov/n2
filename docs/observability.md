# Observability

N2 services export Effect traces, metrics, and logs through OTLP when an
OpenTelemetry endpoint is configured. The local Docker Compose stack routes
that telemetry to Grafana, Tempo, Prometheus, and Loki.

## Local Docker Compose

Start the profile, request, and PII providers with the local observability stack:

```bash
docker compose up -d --build
```

The Compose stack enables observability for the provider services with:

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
- Postgres UI: <http://localhost:8081>
- MinIO API: <http://localhost:9002>
- MinIO console: <http://localhost:9001>

For Postgres UI, use system `PostgreSQL`, server `postgres`, username `n2`,
password `n2`, and database `n2`.

For MinIO, use the local Compose credentials `minioadmin` / `minioadmin`.
Provider services use MinIO for source asset payloads and encrypted PII
payloads.

If any of those ports are already taken, set `GRAFANA_PORT`,
`PROMETHEUS_PORT`, `TEMPO_PORT`, `LOKI_PORT`, `POSTGRES_UI_PORT`,
`MINIO_API_PORT`, or `MINIO_CONSOLE_PORT` before starting Compose. For example:

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
STRESS_SERVICES=pii-provider bun run stress:services
```

In Postgres UI, inspect operational read models such as
`profile_provider_profiles_read`, `request_provider_requests_read`, and
`pii_provider_records`.

Tables named `*_aggregate_snapshots` are entity recovery checkpoints. Business
snapshot rows live in `profile_provider_snapshots_read` and
`request_provider_snapshots_read`.

ClickHouse is optional analytics infrastructure. Start it only when you need
the ClickHouse projection tables or SQL playground:

```bash
docker compose --profile analytics up -d clickhouse clickhouse-ui
```

Optional ClickHouse endpoints:

- ClickHouse UI: <http://localhost:3488>
- ClickHouse built-in SQL playground: <http://localhost:8123/play>

For ClickHouse UI and the built-in playground, use the local Compose
credentials `n2` / `n2`. If those ports are already taken, set
`CLICKHOUSE_UI_PORT` or `CLICKHOUSE_HTTP_PORT`.

## Grafana

Open <http://localhost:3333>. Anonymous local admin access is enabled in
Compose for development.

Open **Dashboards** -> **N2 Services** -> **N2 Services** for the provisioned
service dashboard. Use the `Service` variable to switch between
`profile-provider`, `request-provider`, and `pii-provider`. Run
`bun run test:services` after the stack is healthy to generate sample command
metrics, traces, and logs for all three services.

Use **Explore** with these datasources:

- `Tempo` for traces. Search by service name, then inspect spans such as
  `POST /rpc/profile-provider`, `POST /rpc/request-provider`,
  `POST /rpc/pii-provider`, and `GET /health`.
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
docker compose logs -f grafana tempo prometheus loki postgres-ui minio minio-init
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
