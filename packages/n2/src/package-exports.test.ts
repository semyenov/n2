import { test, expect } from "bun:test"
import * as Root from "n2"
import * as Helpers from "n2/helpers"
import * as Domain from "n2/domain"
import * as Testing from "n2/testing"
import * as Http from "n2/adapters/http"
import * as LegacyMain from "n2/src/main"
import * as LegacyHelpers from "n2/framework/helpers"
import * as LegacyHelpersIndex from "n2/framework/helpers/index.js"

test("package exports support current and documented legacy import paths", () => {
  expect(typeof Root.Helpers.define).toBe("function")
  expect(typeof Helpers.define).toBe("function")
  expect(typeof Helpers.makeOutboxJsonService).toBe("function")
  expect(typeof Domain.BrandedId).toBe("object")
  expect(typeof Testing.DeterministicIdGenerator.make).toBe("function")
  expect(typeof Http.BunHttpServer).toBe("object")
  expect(typeof LegacyMain.Helpers.define).toBe("function")
  expect(typeof LegacyHelpers.define).toBe("function")
  expect(typeof LegacyHelpersIndex.define).toBe("function")
})
