/**
 * Composition root. Uses @effect native layers directly.
 */
import * as Layer from "effect/Layer"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { Identity } from "@effect/experimental/EventLog"
import { WorkflowEngine } from "@effect/workflow"
import { OrderFulfillmentHandlers } from "./workflows.js"

// Shared workflow layer — exported so entity.ts can import the same reference.
// Effect deduplicates layers by identity, so this is built once and shared
// between the route handlers and InfrastructureLayer.
export const WorkflowLayer = Layer.provideMerge(
  OrderFulfillmentHandlers,
  WorkflowEngine.layerMemory
)

export const InfrastructureLayer = Layer.mergeAll(
  ExpEventJournal.layerMemory,
  Layer.succeed(Identity, Identity.makeRandom()),
  WorkflowLayer
)
