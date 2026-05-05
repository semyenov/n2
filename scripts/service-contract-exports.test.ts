import { it, expect } from "@effect/vitest"
import { PIIProviderRpcs } from "qb.service.pii/contracts"
import { ProfileProviderRpcs } from "qb.service.profiler/contracts"
import { RequestProviderRpcs } from "qb.service.request/contracts"

it("service contract package exports resolve provider RPC groups", () => {
  expect(PIIProviderRpcs).toBeDefined()
  expect(ProfileProviderRpcs).toBeDefined()
  expect(RequestProviderRpcs).toBeDefined()
})
