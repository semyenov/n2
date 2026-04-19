import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ClickhouseClient from "@effect/sql-clickhouse/ClickhouseClient"

export const ProfileProviderClickhouseLayer = Layer.unwrapEffect(
  Effect.gen(function* () {
    const url = yield* Config.string("CLICKHOUSE_URL")
    const database = yield* Config.string("CLICKHOUSE_DATABASE").pipe(Config.withDefault("default"))

    return ClickhouseClient.layer({
      url,
      database
    })
  })
)
