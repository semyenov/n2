/**
 * @since 1.0.0
 * @module Health
 *
 * Health check endpoints: /health (liveness) and /ready (readiness).
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Layer from "effect/Layer"
import * as HttpRouter from "@effect/platform/HttpRouter"
import * as HttpServerResponse from "@effect/platform/HttpServerResponse"

/**
 * @since 1.0.0
 * @category schemas
 */
export class HealthResponse extends Schema.Class<HealthResponse>("HealthResponse")({
  status: Schema.Literal("ok", "degraded", "unavailable"),
  checks: Schema.optional(Schema.Record({ key: Schema.String, value: Schema.String }))
}) {}

/**
 * @since 1.0.0
 * @category models
 */
export interface ReadinessCheck {
  readonly name: string
  readonly check: Effect.Effect<void, unknown>
}

/**
 * Creates health check routes.
 *
 * - `GET /health` -- liveness (always 200 if process is up)
 * - `GET /ready` -- readiness (runs all checks, 200 if all pass, 503 if any fail)
 *
 * @since 1.0.0
 * @category constructors
 */
export const routes = (checks: ReadonlyArray<ReadinessCheck> = []) =>
  HttpRouter.empty.pipe(
    HttpRouter.get("/health",
      HttpServerResponse.json({ status: "ok" })
    ),
    HttpRouter.get("/ready",
      Effect.gen(function*() {
        const results: Record<string, string> = {}
        let allOk = true

        for (const { name, check } of checks) {
          const result = yield* check.pipe(
            Effect.timeout("5 seconds"),
            Effect.map(() => "ok" as const),
            Effect.catchAll(() => Effect.succeed("fail" as const))
          )
          results[name] = result
          if (result !== "ok") allOk = false
        }

        if (allOk) {
          return yield* HttpServerResponse.json({ status: "ok", checks: results })
        } else {
          return yield* HttpServerResponse.json(
            { status: "degraded", checks: results },
            { status: 503 }
          )
        }
      })
    )
  )
