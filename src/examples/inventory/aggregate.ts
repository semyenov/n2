/**
 * Inventory aggregate.
 */
import * as Effect from "effect/Effect"
import * as N2 from "../../framework/helpers/index.js"
import * as C from "./contracts.js"

const Inventory = N2.Aggregate.define<C.InventoryEvent, C.InventoryCommand>()(
  C.initialInventoryState,
  {
    evolve: {
      StockReserved: (state, e) => ({
        ...state,
        available: state.available - e.quantity,
        reserved: state.reserved + e.quantity
      }),
      StockReleased: (state, e) => {
        const releaseFromReserved = Math.min(e.quantity, state.reserved)
        return {
          ...state,
          available: state.available + e.quantity,
          reserved: state.reserved - releaseFromReserved
        }
      }
    },
    decide: {
      ReserveStock: (state, cmd) =>
        Effect.gen(function*() {
          if (state.available < cmd.quantity) {
            return yield* new C.InsufficientStock({
              sku: cmd.sku, requested: cmd.quantity, available: state.available
            })
          }
          return [new C.StockReserved({ sku: cmd.sku, quantity: cmd.quantity, orderId: cmd.orderId })]
        }),
      ReleaseStock: (_state, cmd) =>
        Effect.succeed([new C.StockReleased({ sku: cmd.sku, quantity: cmd.quantity, orderId: cmd.orderId })])
    }
  }
)

export const { evolve, decide, handleCommand, initialState: initialInventoryState } = Inventory
