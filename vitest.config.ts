import { coverageConfigDefaults, defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    environment: "node",
    globals: false,
    include: [
      "packages/n2/src/**/*.test.ts",
      "services/*/src/**/*.test.ts",
      "examples/order/**/*.test.ts"
    ],
    testTimeout: 10_000,
    coverage: {
      provider: "v8",
      reporter: ["text", "html", "lcov"],
      skipFull: true,
      include: [
        "packages/n2/src/**/*.ts",
        "services/*/src/**/*.ts",
        "examples/order/**/*.ts"
      ],
      exclude: [
        ...coverageConfigDefaults.exclude,
        "**/migrations/**",
        "**/server.ts",
        "**/cluster.ts",
        "**/migrate.ts",
        "**/clickhouse*.ts",
        "**/projection-store*.ts",
        "scripts/**",
        "examples/order/projector.ts",
        "packages/n2/src/adapters/**",
        "packages/n2/src/framework/helpers/*Entrypoint.ts",
        "packages/n2/src/framework/helpers/FetchClient.ts",
        "packages/n2/src/framework/helpers/Observability.ts",
        "packages/n2/src/framework/helpers/Runtime.ts",
        "packages/n2/src/framework/testing/**",
        "packages/n2/src/main.ts"
      ],
      thresholds: {
        statements: 80,
        lines: 80,
        functions: 75,
        branches: 80,
        "packages/n2/src/framework/helpers/EventDecoder.ts": {
          100: true
        },
        "packages/n2/src/framework/helpers/Replay.ts": {
          statements: 90,
          lines: 90,
          functions: 85,
          branches: 85
        },
        "packages/n2/src/framework/helpers/PublishWorkflow.ts": {
          100: true
        }
      }
    }
  }
})
