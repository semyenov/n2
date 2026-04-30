# AGENTS.md

## Purpose
This file guides coding agents working in `/home/alexander/Projects/n2`.
The repo is a Bun + TypeScript + Effect codebase exploring DDD-style aggregates,
RPC wiring, entity layers, and workflow examples.

## Rule Sources Checked
- Previous `AGENTS.md`
- `package.json`
- `tsconfig.json`
- `.editorconfig`
- Source conventions in `src/framework/**` and `src/examples/**`

Additional agent rule files checked: `.cursorrules` not present, `.cursor/rules/` not present, `.github/copilot-instructions.md` not present.
If those files appear later, merge their guidance into this document.

## Project Layout
- `index.ts`: package entrypoint; re-exports from `./src/main.js`
- `src/main.ts`: public exports for framework modules
- `src/framework/domain/`: branded IDs, revisions, and domain primitives
- `src/framework/helpers/`: definition helpers and entity/RPC adapters
- `src/framework/testing/`: deterministic testing helpers
- `src/adapters/http/`: Bun / HTTP adapter code
- `src/examples/order/`: primary reference implementation
- `src/examples/inventory/`: secondary reference implementation
- `src/**/*.test.ts`: colocated Vitest / `@effect/vitest` files

## Tooling Snapshot
- Package manager/runtime: `bun`
- Language: TypeScript + ESM
- Core libraries: `effect`, `@effect/cluster`, `@effect/rpc`, `@effect/workflow`
- Testing: `vitest` with `@effect/vitest`
- Benchmarks: `mitata`
- No dedicated lint config exists today
- No build script exists in `package.json`

## Install And Validation Commands
- `bun install` - install dependencies
- `bun --version` - verify Bun is available
- `bunx tsc --noEmit` - run strict type checking
- `bun run test` - run the full test suite

## Single-Test Commands
Use the configured Vitest runner; tests are normally filtered by file path or test name.
- `bun run test -- examples/order/aggregate.test.ts` - run one test file
- `bun run test -- packages/n2/src/framework/helpers/Definition.test.ts` - run another single file
- `bun run test -- examples/order/aggregate.test.ts -t "SubmitOrder with no items fails"` - run one named test

Prefer targeted runs while iterating, then finish with `bun run test`.

## Other Useful Commands
- `bun src/examples/order/index.ts` - run the main order example
- `bun src/examples/order/production.ts` - run the HTTP / production wiring example
- `bun src/examples/order/bench.ts` - run benchmarks

## Build / Lint Expectations
There is no formal lint or build pipeline. For meaningful changes, agents should run:

1. `bunx tsc --noEmit`
2. `bun run test`

If a change is very local, a targeted test file is acceptable during iteration,
but full validation is the preferred final check.

## Formatting Rules
- Follow `.editorconfig`
- Use UTF-8, LF endings, and a trailing newline
- Use 2-space indentation
- Trim trailing whitespace except in Markdown
- Keep formatting simple and consistent with nearby files
- Avoid formatter-driven churn when touching a small area

## Import Conventions
- Use ESM imports only
- Keep local TypeScript import specifiers ending in `.js`
- Group external imports before local imports
- Use `import type` for type-only imports when helpful
- Prefer namespace imports for Effect modules, for example:
  - `import * as Effect from "effect/Effect"`
  - `import * as Schema from "effect/Schema"`
  - `import * as Layer from "effect/Layer"`
- Mirror surrounding import style instead of reordering unrelated code

## TypeScript Rules
Compiler settings that matter most:
- `strict: true`
- `moduleResolution: bundler`
- `verbatimModuleSyntax: true`
- `allowImportingTsExtensions: true`
- `noEmit: true`
- `noFallthroughCasesInSwitch: true`
- `noUncheckedIndexedAccess: true`
- `noImplicitOverride: true`

Agent guidance:
- Preserve precise types; avoid widening to loose objects
- Avoid `any` unless a library boundary truly requires it
- `unknown` is acceptable at boundaries, but narrow it quickly
- Prefer schema-derived and domain-specific types over ad hoc shapes
- Use `readonly` in public-facing types where it improves safety
- Use literal types and `as const` when they clarify tags or states
- Keep generics readable, especially in framework helpers

## Naming Conventions
- Use PascalCase for classes, schemas, domain events, commands, errors, and layers with noun names
- Use camelCase for functions, variables, and local helpers
- Prefer domain-first names like `OrderCreated`, `CancelOrder`, `OrderEntityLayer`
- Use suffixes such as `Layer`, `Handlers`, `Rpcs`, `Entity`, or `Workflow` when they match existing patterns
- Keep test names behavior-focused, e.g. `"SubmitOrder with no items fails"`

Avoid vague utility names when a stronger domain term exists.

## Domain Modeling Conventions
- Define events with `Schema.TaggedClass`
- Define commands / request types with `Schema.TaggedRequest`
- Define business failures with `Schema.TaggedError`
- Define state and DTOs with `Schema.Class` or `Schema.Struct`
- Export inferred types where useful
- Keep state transitions in `evolve`
- Keep business decisions in `decide`
- Keep aggregate logic pure or Effect-based without infrastructure leakage

This repo intentionally treats command classes as both domain commands and RPC schemas; do not duplicate transport DTOs unless there is a clear need.

## Effect And Layer Conventions
- Prefer `Effect.gen(function* () { ... })` for multi-step logic
- Compose dependencies through `Layer`, not ad hoc globals
- Keep side effects at the edges and domain logic deterministic where possible
- Prefer `Effect.matchEffect`, `Effect.either`, `Effect.flip`, and related combinators over manual promise control flow
- Use small adapter functions such as `toResult` and `toError` at integration boundaries

The repo contains both `function*()` and `function* ()`; follow nearby style instead of normalizing everything.

## Error Handling Guidelines
- Prefer typed domain errors over throwing raw `Error`
- Keep expected business failures in Effect error channels
- Use `Schema.TaggedError` for domain and workflow failures
- At boundaries, map unknown errors into typed errors with `String(error)` or a similar safe conversion
- Keep failure paths explicit in signatures and handlers

## Testing Guidelines
- Place tests next to the code they cover using `*.test.ts`
- Use `import { it, expect } from "@effect/vitest"` for Effect-heavy tests
- Keep test names behavior-first
- Add or update tests when aggregate rules, workflows, entity wiring, or HTTP/RPC behavior changes
- For Effect-heavy tests, use `Effect.runPromise` or scoped pipelines consistent with the current suite

Examples double as documentation and regression coverage, so keep them runnable.

## Change Strategy For Agents
- Prefer minimal, local edits over broad refactors
- Preserve public exports unless the task explicitly requires API changes
- When changing framework helpers, inspect both order and inventory examples for impact
- Keep example code runnable after edits
- If you change module boundaries, update `src/main.ts` and `index.ts` when needed

## Before Finishing
For meaningful code changes, run `bunx tsc --noEmit` and `bun run test`.
If you cannot run both, say exactly what you ran and what remains.

## Commit Message Style
- Use short, imperative commit subjects
- Keep each commit focused on one logical change
- Follow existing patterns such as `Add mitata for benchmarking`
- Good default shape: `Verb concise-target`
