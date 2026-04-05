/**
 * Inventory aggregate behavior tests.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { Entity, ShardingConfig } from "@effect/cluster"
import { InventoryEntity, StockResult, InsufficientStock } from "./contracts.js"
import { InventoryEntityLayer } from "./entity.js"
import { layerMemory as EventLogMemory } from "../../framework/runtime/EventJournalEventLog.js"
import { layerMemory as SnapshotStoreMemory } from "../../framework/runtime/SnapshotStore.js"
import * as InMemoryKafkaPublisher from "../../framework/testing/InMemoryKafkaPublisher.js"
import { IdGeneratorUuid } from "../../framework/runtime/IdGenerator.js"
import { N2ClockLive } from "../../framework/runtime/Clock.js"

const InfraLayer = Layer.mergeAll(
  EventLogMemory,
  SnapshotStoreMemory,
  InMemoryKafkaPublisher.layerSimple,
  IdGeneratorUuid,
  N2ClockLive
)

const EntityLayer = Layer.provide(InventoryEntityLayer, InfraLayer)
const ShardingConfigLive = ShardingConfig.layer({})

const runCluster = <A>(effect: Effect.Effect<A, unknown, unknown>): Promise<A> =>
  // @ts-expect-error -- test erases R
  effect.pipe(Effect.scoped, Effect.provide(ShardingConfigLive), Effect.runPromise)

test("inventory: reserve and release stock", () =>
  runCluster(
    Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(InventoryEntity, EntityLayer)
      const client = yield* makeClient("SKU-001")

      // Initial state has 0 available, so reserving should fail
      const err = yield* client.ReserveStock({
        sku: "SKU-001",
        quantity: 5,
        orderId: "order-1"
      }).pipe(Effect.flip)
      expect(err).toBeInstanceOf(InsufficientStock)

      // Release stock (adds to available)
      const releaseResult = yield* client.ReleaseStock({
        sku: "SKU-001",
        quantity: 10,
        orderId: "restock"
      })
      expect(releaseResult).toBeInstanceOf(StockResult)
      expect(releaseResult.available).toBe(10)

      // Now reserve should succeed
      const reserveResult = yield* client.ReserveStock({
        sku: "SKU-001",
        quantity: 3,
        orderId: "order-2"
      })
      expect(reserveResult.available).toBe(7)
      expect(reserveResult.reserved).toBe(3)
    })
  ))

test("inventory: insufficient stock error", () =>
  runCluster(
    Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(InventoryEntity, EntityLayer)
      const client = yield* makeClient("SKU-002")

      // Release 5 units
      yield* client.ReleaseStock({ sku: "SKU-002", quantity: 5, orderId: "restock" })

      // Try to reserve 10 -- should fail
      const err = yield* client.ReserveStock({
        sku: "SKU-002",
        quantity: 10,
        orderId: "order-3"
      }).pipe(Effect.flip)

      expect(err).toBeInstanceOf(InsufficientStock)
      expect(err.available).toBe(5)
      expect(err.requested).toBe(10)
    })
  ))
