import { test, expect, afterAll, beforeAll } from "bun:test"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import { makeFetchClient } from "../../framework/helpers/index.js"
import { OrderRpcs } from "./contracts.js"
import { OrderRpcRoute } from "./http.js"
import { InfrastructureLayer } from "./layers.js"

// ---------------------------------------------------------------------------
// Mock server — unit tests (fast, no real server)
// ---------------------------------------------------------------------------

const mockServer = Bun.serve({
  port: 0,
  async fetch(req) {
    if (new URL(req.url).pathname !== "/") {
      return new Response(null, { status: 404, statusText: "Not Found" })
    }

    const { id, method, params } = await req.json() as {
      id: number
      method: string
      params: Record<string, unknown>
    }

    switch (method) {
      case "CreateOrder":
        return Response.json([{ jsonrpc: "2.0", id, result: { orderId: params["orderId"], revision: 1 } }])
      case "AddItem":
        return Response.json([{ jsonrpc: "2.0", id, result: { orderId: params["orderId"], revision: 2 } }])
      case "SubmitOrder":
        return Response.json([{ jsonrpc: "2.0", id, result: { orderId: params["orderId"], revision: 3 } }])
      case "CancelOrder":
        return Response.json([{ jsonrpc: "2.0", id, error: { _tag: "OrderError", message: "Cannot cancel" } }])
      case "FulfillOrder":
        return Response.json([{ jsonrpc: "2.0", id, result: { orderId: params["orderId"], shipped: true } }])
      case "GetOrder":
        return Response.json([{ jsonrpc: "2.0", id, result: {
          status: "draft",
          orderId: { _tag: "Some", value: params["orderId"] },
          customerId: { _tag: "Some", value: "c-1" },
          items: [],
          totalAmount: 0
        }}])
      default:
        return new Response(null, { status: 404, statusText: "Not Found" })
    }
  }
})

const client = makeFetchClient(OrderRpcs, `http://localhost:${mockServer.port}`)

afterAll(() => mockServer.stop())

test("CreateOrder returns CommandResult", async () => {
  const result = await client.CreateOrder({ orderId: "o-1", customerId: "c-1" })
  expect(result.orderId).toBe("o-1")
  expect(result.revision).toBe(1)
})

test("AddItem returns CommandResult", async () => {
  const result = await client.AddItem({ orderId: "o-1", sku: "SKU-1", quantity: 2, price: 9.99 })
  expect(result.orderId).toBe("o-1")
  expect(result.revision).toBe(2)
})

test("SubmitOrder returns CommandResult", async () => {
  const result = await client.SubmitOrder({ orderId: "o-1" })
  expect(result.orderId).toBe("o-1")
  expect(result.revision).toBe(3)
})

test("FulfillOrder returns FulfillmentResult", async () => {
  const result = await client.FulfillOrder({ orderId: "o-1", sku: "SKU-1", quantity: 2 })
  expect(result.orderId).toBe("o-1")
  expect(result.shipped).toBe(true)
})

test("GetOrder returns OrderState", async () => {
  const state = await client.GetOrder({ orderId: "o-1" })
  expect(state.status).toBe("draft")
  expect(state.totalAmount).toBe(0)
})

test("error response is thrown as rejected Promise", async () => {
  await expect(
    client.CancelOrder({ orderId: "o-1", reason: "test" })
  ).rejects.toMatchObject({ _tag: "OrderError", message: "Cannot cancel" })
})

test("non-200 HTTP response is thrown as Error", async () => {
  const badClient = makeFetchClient(OrderRpcs, `http://localhost:${mockServer.port}/no-such-path`)
  await expect(
    badClient.CreateOrder({ orderId: "o-1", customerId: "c-1" })
  ).rejects.toThrow("HTTP 404")
})

// ---------------------------------------------------------------------------
// Real server — integration tests (actual state + workflow engine)
// ---------------------------------------------------------------------------

const INTEGRATION_PORT = 14001

const RealServerLayer = HttpLayerRouter.serve(OrderRpcRoute).pipe(
  Layer.provide(BunHttpServer.layer({ port: INTEGRATION_PORT })),
  Layer.provide(InfrastructureLayer)
)

let serverFiber: Fiber.RuntimeFiber<never, unknown>

beforeAll(async () => {
  serverFiber = Effect.runFork(Effect.never.pipe(Effect.provide(RealServerLayer)))
  // Allow the server to bind and start accepting connections
  await new Promise(r => setTimeout(r, 50))
})

afterAll(async () => {
  await Effect.runPromise(Fiber.interrupt(serverFiber))
})

const realClient = makeFetchClient(OrderRpcs, `http://localhost:${INTEGRATION_PORT}/rpc/orders`)

test("integration: state persists across commands", async () => {
  const orderId = "int-o-1"

  await realClient.CreateOrder({ orderId, customerId: "c-1" })

  const afterCreate = await realClient.GetOrder({ orderId })
  expect(afterCreate.status).toBe("draft")

  await realClient.AddItem({ orderId, sku: "SKU-1", quantity: 2, price: 9.99 })

  const afterAdd = await realClient.GetOrder({ orderId })
  expect(afterAdd.items.length).toBe(1)
  expect(afterAdd.totalAmount).toBe(19.98)

  await realClient.SubmitOrder({ orderId })

  const afterSubmit = await realClient.GetOrder({ orderId })
  expect(afterSubmit.status).toBe("submitted")
})

test("integration: GetOrder fails for unknown orderId", async () => {
  await expect(
    realClient.GetOrder({ orderId: "no-such-order" })
  ).rejects.toMatchObject({ _tag: "OrderNotFound" })
})

test("integration: FulfillOrder runs the workflow end-to-end", async () => {
  const orderId = "int-o-2"
  await realClient.CreateOrder({ orderId, customerId: "c-2" })

  const result = await realClient.FulfillOrder({ orderId, sku: "SKU-1", quantity: 1 })
  expect(result.orderId).toBe(orderId)
  expect(result.shipped).toBe(true)
}, 15_000) // workflow includes a 5s DurableClock.sleep
