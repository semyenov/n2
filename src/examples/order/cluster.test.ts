/**
 * Cluster mode integration test.
 *
 * Boots OrderEntity with TestRunner (in-memory cluster),
 * sends commands via Entity.makeTestClient, verifies results.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { Entity, TestRunner, ShardingConfig } from "@effect/cluster"
import { OrderEntity, CommandResult, OrderError } from "./contracts.js"
import { OrderEntityLayer } from "./entity.js"
import { InfrastructureLayer } from "./layers.js"

/**
 * Entity behavior layer: registers entity handlers.
 * Infrastructure layer: provides EventLog, SnapshotStore, KafkaPublisher, etc.
 * These are passed to makeTestClient which handles TestRunner/Sharding internally.
 */
const EntityBehaviorWithInfra = Layer.provide(OrderEntityLayer, InfrastructureLayer)
const ShardingConfigLayer = ShardingConfig.layer({})

const runCluster = <A>(effect: Effect.Effect<A, unknown, unknown>): Promise<A> =>
  // @ts-expect-error -- test erases R
  effect.pipe(Effect.scoped, Effect.provide(ShardingConfigLayer), Effect.runPromise)

test("cluster mode: create and submit order", () =>
  runCluster(
    Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(
        OrderEntity,
        EntityBehaviorWithInfra
      )

      const client = yield* makeClient("order-c1")
      const createResult = yield* client.CreateOrder!({
        orderId: "order-c1",
        customerId: "cust-1"
      })
      expect(createResult).toBeInstanceOf(CommandResult)
      expect(Number(createResult.revision)).toBe(1)

      const addResult = yield* client.AddItem!({
        orderId: "order-c1",
        sku: "SKU-A",
        quantity: 2,
        price: 15
      })
      expect(Number(addResult.revision)).toBe(2)

      const submitResult = yield* client.SubmitOrder!({ orderId: "order-c1" })
      expect(Number(submitResult.revision)).toBe(3)
    })
  ))

test("cluster mode: error on submit with no items", () =>
  runCluster(
    Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(
        OrderEntity,
        EntityBehaviorWithInfra
      )
      const client = yield* makeClient("order-c2")
      yield* client.CreateOrder!({ orderId: "order-c2", customerId: "cust-2" })
      const err = yield* client.SubmitOrder!({ orderId: "order-c2" }).pipe(Effect.flip)
      expect(err).toBeInstanceOf(OrderError)
    })
  ))

test("cluster mode: separate entities are independent", () =>
  runCluster(
    Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(
        OrderEntity,
        EntityBehaviorWithInfra
      )

      const c1 = yield* makeClient("order-c3")
      const c2 = yield* makeClient("order-c4")

      yield* c1.CreateOrder!({ orderId: "order-c3", customerId: "cust-a" })
      yield* c2.CreateOrder!({ orderId: "order-c4", customerId: "cust-b" })
      yield* c1.AddItem!({ orderId: "order-c3", sku: "X", quantity: 1, price: 10 })

      const r1 = yield* c1.SubmitOrder!({ orderId: "order-c3" })
      expect(Number(r1.revision)).toBe(3)

      const err = yield* c2.SubmitOrder!({ orderId: "order-c4" }).pipe(Effect.flip)
      expect(err).toBeInstanceOf(OrderError)
    })
  ))
