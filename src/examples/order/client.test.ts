import { test, expect, afterAll, beforeEach } from "bun:test"
import { makeFetchClient } from "../../framework/helpers/index.js"
import { OrderRpcs } from "./contracts.js"

// ---------------------------------------------------------------------------
// Mock server — unit tests (fast, no real server)
// ---------------------------------------------------------------------------

type MockOrder = {
  readonly orderId: string
  readonly customerId: string
  readonly items: Array<{ readonly sku: string; readonly quantity: number; readonly price: number }>
  readonly totalAmount: number
  readonly status: "draft" | "submitted" | "cancelled"
}

const originalFetch = globalThis.fetch
const mockOrders = new Map<string, MockOrder>()

const mockFetch = Object.assign(async (
  input: Parameters<typeof fetch>[0],
  init?: Parameters<typeof fetch>[1]
) => {
  const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url)
  if (url.pathname !== "/" && url.pathname !== "/rpc/orders") {
    return new Response(null, { status: 404, statusText: "Not Found" })
  }

  const body = typeof init?.body === "string"
    ? JSON.parse(init.body)
    : await new Response(init?.body).json()
  const { id, method, params } = body as {
    readonly id: number
    readonly method: string
    readonly params: Record<string, unknown>
  }

  const orderId = String(params["orderId"])
  const order = mockOrders.get(orderId)

  switch (method) {
    case "CreateOrder":
      mockOrders.set(orderId, {
        orderId,
        customerId: String(params["customerId"]),
        items: [],
        totalAmount: 0,
        status: "draft"
      })
      return Response.json([{ jsonrpc: "2.0", id, result: { orderId, revision: 1 } }])
    case "AddItem": {
      const item = {
        sku: String(params["sku"]),
        quantity: Number(params["quantity"]),
        price: Number(params["price"])
      }
      mockOrders.set(orderId, {
        ...(order ?? { orderId, customerId: "c-1", items: [], totalAmount: 0, status: "draft" as const }),
        items: [...(order?.items ?? []), item],
        totalAmount: (order?.totalAmount ?? 0) + item.price * item.quantity
      })
      return Response.json([{ jsonrpc: "2.0", id, result: { orderId, revision: 2 } }])
    }
    case "SubmitOrder":
      if (order) {
        mockOrders.set(orderId, { ...order, status: "submitted" })
      }
      return Response.json([{ jsonrpc: "2.0", id, result: { orderId, revision: 3 } }])
    case "CancelOrder":
      return Response.json([{ jsonrpc: "2.0", id, error: { _tag: "OrderError", message: "Cannot cancel" } }])
    case "FulfillOrder":
      return Response.json([{ jsonrpc: "2.0", id, result: { orderId, shipped: true } }])
    case "GetOrder":
      if (!order) {
        return Response.json([{ jsonrpc: "2.0", id, error: { _tag: "OrderNotFound", orderId } }])
      }
      return Response.json([{ jsonrpc: "2.0", id, result: {
        status: order.status,
        orderId: { _tag: "Some", value: order.orderId },
        customerId: { _tag: "Some", value: order.customerId },
        items: order.items,
        totalAmount: order.totalAmount
      }}])
    default:
      return new Response(null, { status: 404, statusText: "Not Found" })
  }
}, { preconnect: originalFetch.preconnect })

globalThis.fetch = mockFetch

const client = makeFetchClient(OrderRpcs, "http://localhost")

beforeEach(() => {
  mockOrders.clear()
})

afterAll(() => {
  globalThis.fetch = originalFetch
})

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
  await client.CreateOrder({ orderId: "o-1", customerId: "c-1" })
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
  const badClient = makeFetchClient(OrderRpcs, "http://localhost/no-such-path")
  await expect(
    badClient.CreateOrder({ orderId: "o-1", customerId: "c-1" })
  ).rejects.toThrow("HTTP 404")
})

// ---------------------------------------------------------------------------
// Stateful fetch-backed integration tests
// ---------------------------------------------------------------------------

const realClient = makeFetchClient(OrderRpcs, "http://localhost/rpc/orders")

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
