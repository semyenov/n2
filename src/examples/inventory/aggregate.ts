/**
 * Inventory aggregate: decide + evolve.
 * Commands are Schema.TaggedRequest.
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as AggregateDefinition from "../../framework/domain/AggregateDefinition.js"
import {
  type InventoryState,
  type InventoryCommand,
  type InventoryEvent,
  InventoryEvent as InventoryEventSchema,
  InventoryState as InventoryStateSchema,
  InventoryErrors as InventoryErrorsSchema,
  initialInventoryState,
  StockReserved,
  StockReleased,
  InsufficientStock,
  ReserveStock,
  ReleaseStock
} from "./contracts.js"

export const evolve = (state: InventoryState, event: InventoryEvent): InventoryState => {
  switch (event._tag) {
    case "StockReserved":
      return new InventoryStateSchema({
        ...state,
        available: state.available - event.quantity,
        reserved: state.reserved + event.quantity
      })
    case "StockReleased": {
      const releaseFromReserved = Math.min(event.quantity, state.reserved)
      return new InventoryStateSchema({
        ...state,
        available: state.available + event.quantity,
        reserved: state.reserved - releaseFromReserved
      })
    }
  }
}

export const decide = (
  state: InventoryState,
  command: InventoryCommand
): Effect.Effect<ReadonlyArray<InventoryEvent>, InsufficientStock> =>
  Effect.gen(function*() {
    switch (command._tag) {
      case "ReserveStock": {
        if (state.available < command.quantity) {
          return yield* new InsufficientStock({
            sku: command.sku,
            requested: command.quantity,
            available: state.available
          })
        }
        return [new StockReserved({ sku: command.sku, quantity: command.quantity, orderId: command.orderId })]
      }
      case "ReleaseStock": {
        return [new StockReleased({ sku: command.sku, quantity: command.quantity, orderId: command.orderId })]
      }
    }
  })

export const handleCommand = (
  state: InventoryState,
  command: InventoryCommand
): Effect.Effect<
  { readonly events: ReadonlyArray<InventoryEvent>; readonly state: InventoryState },
  InsufficientStock
> =>
  Effect.gen(function*() {
    const events = yield* decide(state, command)
    let newState = state
    for (const event of events) {
      newState = evolve(newState, event)
    }
    return { events, state: newState }
  })

/** @deprecated Use handleCommand + decide + evolve directly */
export const InventoryAggregate = AggregateDefinition.define({
  name: "Inventory" as const,
  initialState: initialInventoryState,
  decide,
  evolve,
  schemas: {
    state: InventoryStateSchema,
    command: Schema.Union(ReserveStock, ReleaseStock),
    event: InventoryEventSchema,
    error: InventoryErrorsSchema
  }
})
