import { FetchHttpClient } from "@effect/platform"
import * as OtlpLogger from "@effect/opentelemetry/OtlpLogger"
import * as OtlpMetrics from "@effect/opentelemetry/OtlpMetrics"
import * as OtlpSerialization from "@effect/opentelemetry/OtlpSerialization"
import * as OtlpTracer from "@effect/opentelemetry/OtlpTracer"
import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Tracer from "effect/Tracer"

export interface ObservabilityOptions {
  readonly serviceName: string
  readonly serviceVersion?: string
  readonly attributes?: Record<string, unknown>
  readonly defaultEnvironment?: string
}

export class ObservabilityTracer extends Context.Tag("@semyenov/n2/ObservabilityTracer")<
  ObservabilityTracer,
  Option.Option<Tracer.Tracer>
>() {}

export const ObservabilityDisabled: Layer.Layer<ObservabilityTracer> = Layer.succeed(
  ObservabilityTracer,
  Option.none()
)

const parseResourceAttributes = (input: string): Record<string, string> => {
  const attributes: Record<string, string> = {}

  for (const item of input.split(",")) {
    const [rawKey, ...rawValue] = item.split("=")
    const key = rawKey?.trim()
    const value = rawValue.join("=").trim()
    if (key !== undefined && key.length > 0 && value.length > 0) {
      attributes[key] = value
    }
  }

  return attributes
}

const signalUrl = (baseUrl: string, path: string) => `${baseUrl.replace(/\/+$/, "")}${path}`

export const makeObservabilityLayer = (options: ObservabilityOptions): Layer.Layer<ObservabilityTracer> =>
  Layer.unwrapEffect(
    Effect.gen(function* () {
      const enabledConfig = yield* Config.option(Config.boolean("OBSERVABILITY_ENABLED"))
      const endpointConfig = yield* Config.option(Config.string("OTEL_EXPORTER_OTLP_ENDPOINT"))
      const configuredServiceName = yield* Config.string("OTEL_SERVICE_NAME").pipe(
        Config.withDefault(options.serviceName)
      )
      const environment = yield* Config.string("DEPLOYMENT_ENVIRONMENT").pipe(
        Config.withDefault(options.defaultEnvironment ?? "local")
      )
      const rawResourceAttributes = yield* Config.option(Config.string("OTEL_RESOURCE_ATTRIBUTES"))

      const enabled = Option.match(enabledConfig, {
        onNone: () => Option.isSome(endpointConfig),
        onSome: Boolean
      })

      if (!enabled) {
        return ObservabilityDisabled
      }

      const endpoint = Option.getOrElse(endpointConfig, () => "http://localhost:4318")
      const resourceAttributes = Option.match(rawResourceAttributes, {
        onNone: () => ({}),
        onSome: parseResourceAttributes
      })

      const resource = {
        serviceName: configuredServiceName,
        ...(options.serviceVersion === undefined ? {} : { serviceVersion: options.serviceVersion }),
        attributes: {
          "deployment.environment": environment,
          ...options.attributes,
          ...resourceAttributes
        }
      }

      const TracerLayer = Layer.unwrapScoped(
        Effect.map(
          OtlpTracer.make({
            url: signalUrl(endpoint, "/v1/traces"),
            resource
          }),
          (tracer) =>
            Layer.mergeAll(
              Layer.succeed(ObservabilityTracer, Option.some(tracer)),
              Layer.setTracer(tracer)
            )
        )
      )

      return Layer.mergeAll(
        TracerLayer,
        OtlpLogger.layer({
          url: signalUrl(endpoint, "/v1/logs"),
          resource
        }),
        OtlpMetrics.layer({
          url: signalUrl(endpoint, "/v1/metrics"),
          resource
        })
      ).pipe(
        Layer.provide(Layer.merge(FetchHttpClient.layer, OtlpSerialization.layerProtobuf))
      )
    }).pipe(Effect.orDie)
  )
