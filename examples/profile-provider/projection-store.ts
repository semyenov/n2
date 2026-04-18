import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type { SqlError } from "@effect/sql/SqlError"
import type { ProfileEvent } from "./contracts.js"
import type { ProfileProviderEventMessage } from "./workflows.js"

export class ProfileProviderProjectionStore extends Context.Tag("ProfileProviderProjectionStore")<
  ProfileProviderProjectionStore,
  {
    readonly project: (
      event: ProfileEvent,
      message: ProfileProviderEventMessage
    ) => Effect.Effect<void, SqlError>
  }
>() {}
