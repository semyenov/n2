import { it, expect } from "@effect/vitest"
import * as Root from "@semyenov/n2"
import * as Helpers from "@semyenov/n2/helpers"
import * as Runtime from "@semyenov/n2/runtime"
import * as Domain from "@semyenov/n2/domain"
import * as Testing from "@semyenov/n2/testing"
import * as Http from "@semyenov/n2/adapters/http"
import * as LegacyMain from "@semyenov/n2/src/main"
import * as LegacyHelpers from "@semyenov/n2/framework/helpers"
import * as LegacyHelpersIndex from "@semyenov/n2/framework/helpers/index.js"

it("package exports support current and documented legacy import paths", () => {
  expect(typeof Root.Helpers.define).toBe("function")
  expect(typeof Helpers.define).toBe("function")
  expect(typeof Helpers.makeOutboxJsonService).toBe("function")
  expect(typeof Helpers.makeStandardOutboxWiring).toBe("function")
  expect("makeConfiguredClickhouseLayer" in Helpers).toBe(false)
  expect("makeServerEntrypoint" in Helpers).toBe(false)
  expect("makeClusterEntrypoint" in Helpers).toBe(false)
  expect("makeReplayInfrastructureLayer" in Helpers).toBe(false)
  expect(typeof Helpers.makeEventMessage).toBe("function")
  expect(typeof Runtime.makeConfiguredClickhouseLayer).toBe("function")
  expect(typeof Runtime.makePgSqlLayer).toBe("function")
  expect(typeof Runtime.makeServerEntrypoint).toBe("function")
  expect(typeof Runtime.makeClusterEntrypoint).toBe("function")
  expect(typeof Runtime.makeReplayInfrastructureLayer).toBe("function")
  expect(typeof Domain.BrandedId).toBe("object")
  expect(typeof Testing.DeterministicIdGenerator.make).toBe("function")
  expect(typeof Http.BunHttpServer).toBe("object")
  expect(typeof LegacyMain.Helpers.define).toBe("function")
  expect(typeof LegacyHelpers.define).toBe("function")
  expect(typeof LegacyHelpersIndex.define).toBe("function")
})
