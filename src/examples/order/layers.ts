/**
 * Composition root. Uses @effect native layers directly.
 */
import * as Layer from "effect/Layer"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { Identity } from "@effect/experimental/EventLog"

export const InfrastructureLayer = Layer.mergeAll(
  ExpEventJournal.layerMemory,
  Layer.succeed(Identity, Identity.makeRandom())
)
