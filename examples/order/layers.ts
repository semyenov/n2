/**
 * Infrastructure layer composition.
 *
 * WorkflowLayer is exported as a named constant so entity.ts can import
 * the same object reference. Effect deduplicates layers by identity —
 * the same reference is built once regardless of how many places provide it.
 */
import * as Layer from "effect/Layer"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { Identity } from "@effect/experimental/EventLog"
import { WorkflowEngine } from "@effect/workflow"
import { OrderFulfillmentHandlers } from "./workflows.js"

// WorkflowEngine + registered workflow handlers.
// Swap WorkflowEngine.layerMemory → WorkflowEngine.layerSql for durable persistence.
export const WorkflowLayer = Layer.provideMerge(
  OrderFulfillmentHandlers,
  WorkflowEngine.layerMemory
)

// Full infrastructure stack.
// Swap ExpEventJournal.layerMemory → ExpEventJournal.layerSql for persistence.
export const InfrastructureLayer = Layer.mergeAll(
  ExpEventJournal.layerMemory,
  Layer.succeed(Identity, Identity.makeRandom()),
  WorkflowLayer
)
