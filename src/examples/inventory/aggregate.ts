/**
 * Inventory aggregate.
 */
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as N2 from "../../framework/helpers/index.js"
import * as C from "./contracts.js"

export const Inventory = N2.define<C.InventoryEvent, C.InventoryCommand>()({
  initialState: C.initialInventoryState,
  commands: C.InventoryCommands.constructors,
  evolve: {
    StockReserved: (state, event) => ({
      ...state,
      available: state.available - event.quantity,
      reserved: state.reserved + event.quantity
    }),
    StockReleased: (state, event) => {
      const releaseFromReserved = Math.min(event.quantity, state.reserved)
      return {
        ...state,
        available: state.available + event.quantity,
        reserved: state.reserved - releaseFromReserved
      }
    }
  },
  decide: {
    ReserveStock: (state, command) =>
      Effect.gen(function* () {
        if (state.available < command.quantity) {
          return yield* new C.InsufficientStock({
            sku: command.sku,
            requested: command.quantity,
            available: state.available
          })
        }

        const now = yield* DateTime.now
        return [
          new C.StockReserved({
            sku: command.sku,
            occurredAt: now,
            quantity: command.quantity,
            orderId: command.orderId
          })
        ]
      }),
    ReleaseStock: (_state, command) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now
        return [
          new C.StockReleased({
            sku: command.sku,
            occurredAt: now,
            quantity: command.quantity,
            orderId: command.orderId
          })
        ]
      })
  }
})

export const {
  evolve,
  decide,
  handle: handleCommand,
  run,
  initialState: initialInventoryState
} = Inventory;
