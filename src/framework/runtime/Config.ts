/**
 * @since 1.0.0
 * @module Config
 *
 * Application configuration with Schema validation.
 * Reads from environment variables, validates at startup.
 */
import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

// ---------------------------------------------------------------------------
// Config schemas
// ---------------------------------------------------------------------------

export class DatabaseConfig extends Schema.Class<DatabaseConfig>("DatabaseConfig")({
  url: Schema.String,
  poolSize: Schema.optionalWith(Schema.Number, { default: () => 10 })
}) { }

export class KafkaConfig extends Schema.Class<KafkaConfig>("KafkaConfig")({
  brokers: Schema.Array(Schema.String),
  clientId: Schema.optionalWith(Schema.String, { default: () => "n2" })
}) { }

export class HttpConfig extends Schema.Class<HttpConfig>("HttpConfig")({
  port: Schema.optionalWith(Schema.Number, { default: () => 3000 }),
  host: Schema.optionalWith(Schema.String, { default: () => "0.0.0.0" })
}) { }

export class ClusterConfig extends Schema.Class<ClusterConfig>("ClusterConfig")({
  shardsPerGroup: Schema.optionalWith(Schema.Number, { default: () => 300 }),
  entityMaxIdleMinutes: Schema.optionalWith(Schema.Number, { default: () => 10 })
}) { }

export class N2Config extends Schema.Class<N2Config>("N2Config")({
  db: Schema.optionalWith(DatabaseConfig, { default: () => new DatabaseConfig({ url: "postgres://localhost:5432/n2" }) }),
  kafka: Schema.optionalWith(KafkaConfig, { default: () => new KafkaConfig({ brokers: ["localhost:9092"] }) }),
  http: Schema.optionalWith(HttpConfig, { default: () => new HttpConfig({}) }),
  cluster: Schema.optionalWith(ClusterConfig, { default: () => new ClusterConfig({}) })
}) { }

// ---------------------------------------------------------------------------
// Service tag
// ---------------------------------------------------------------------------

export class N2ConfigService extends Context.Tag("n2/Config")<
  N2ConfigService,
  N2Config
>() { }

// ---------------------------------------------------------------------------
// Layers
// ---------------------------------------------------------------------------

/**
 * Reads config from environment variables:
 * - N2_DB_URL, N2_DB_POOL_SIZE
 * - N2_KAFKA_BROKERS (comma-separated), N2_KAFKA_CLIENT_ID
 * - N2_HTTP_PORT, N2_HTTP_HOST
 * - N2_CLUSTER_SHARDS, N2_CLUSTER_IDLE_MINUTES
 *
 * @since 1.0.0
 * @category layers
 */
export const layerFromEnv: Layer.Layer<N2ConfigService, import("effect/ConfigError").ConfigError> = Layer.effect(
  N2ConfigService,
  Effect.gen(function* () {
    const dbUrl = yield* Config.string("N2_DB_URL").pipe(
      Config.withDefault("postgres://localhost:5432/n2")
    )
    const dbPool = yield* Config.number("N2_DB_POOL_SIZE").pipe(
      Config.withDefault(10)
    )
    const kafkaBrokers = yield* Config.string("N2_KAFKA_BROKERS").pipe(
      Config.withDefault("localhost:9092"),
      Config.map((s) => s.split(",").map((b) => b.trim()))
    )
    const kafkaClientId = yield* Config.string("N2_KAFKA_CLIENT_ID").pipe(
      Config.withDefault("n2")
    )
    const httpPort = yield* Config.number("N2_HTTP_PORT").pipe(
      Config.withDefault(3000)
    )
    const httpHost = yield* Config.string("N2_HTTP_HOST").pipe(
      Config.withDefault("0.0.0.0")
    )

    return new N2Config({
      db: new DatabaseConfig({ url: dbUrl, poolSize: dbPool }),
      kafka: new KafkaConfig({ brokers: kafkaBrokers, clientId: kafkaClientId }),
      http: new HttpConfig({ port: httpPort, host: httpHost }),
      cluster: new ClusterConfig({})
    })
  })
)

/**
 * Default config for development/testing.
 *
 * @since 1.0.0
 * @category layers
 */
export const layerDefault: Layer.Layer<N2ConfigService> = Layer.succeed(
  N2ConfigService,
  new N2Config({})
)
