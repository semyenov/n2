import { readFileSync } from "node:fs"
import { it, expect } from "@effect/vitest"
import { parse } from "yaml"

type EnvVar = {
  readonly name: string
  readonly value?: string
  readonly valueFrom?: unknown
}

type Container = {
  readonly name: string
  readonly ports?: ReadonlyArray<{
    readonly containerPort?: number
    readonly name?: string
    readonly protocol?: string
  }>
  readonly env?: ReadonlyArray<EnvVar>
  readonly livenessProbe?: {
    readonly httpGet?: {
      readonly path?: string
      readonly port?: number
    }
  }
  readonly readinessProbe?: {
    readonly httpGet?: {
      readonly path?: string
      readonly port?: number
    }
  }
}

type DeploymentManifest = {
  readonly kind?: string
  readonly spec?: {
    readonly template?: {
      readonly spec?: {
        readonly containers?: ReadonlyArray<Container>
      }
    }
  }
}

type ServiceManifest = {
  readonly kind?: string
  readonly spec?: {
    readonly ports?: ReadonlyArray<{
      readonly name?: string
      readonly port?: number
      readonly targetPort?: number
      readonly protocol?: string
    }>
  }
}

const readManifest = <A>(path: string): A =>
  parse(readFileSync(path, "utf8")) as A

const serviceSpecs = [
  {
    name: "profile-provider",
    port: 4100,
    deploymentPath: "services/profile-provider/manifests/deployment.yaml",
    servicePath: "services/profile-provider/manifests/service.yaml",
    env: [
      "PORT",
      "DATABASE_URL",
      "OBJECT_STORAGE_ENDPOINT",
      "OBJECT_STORAGE_PORT",
      "OBJECT_STORAGE_USE_SSL",
      "SOURCE_ASSETS_BUCKET",
      "OBJECT_STORAGE_ACCESS_KEY",
      "OBJECT_STORAGE_SECRET_KEY",
    ],
  },
  {
    name: "request-provider",
    port: 4110,
    deploymentPath: "services/request-provider/manifests/deployment.yaml",
    servicePath: "services/request-provider/manifests/service.yaml",
    env: [
      "PORT",
      "DATABASE_URL",
      "OBJECT_STORAGE_ENDPOINT",
      "OBJECT_STORAGE_PORT",
      "OBJECT_STORAGE_USE_SSL",
      "SOURCE_ASSETS_BUCKET",
      "OBJECT_STORAGE_ACCESS_KEY",
      "OBJECT_STORAGE_SECRET_KEY",
    ],
  },
  {
    name: "pii-provider",
    port: 4120,
    deploymentPath: "services/pii-provider/manifests/deployment.yaml",
    servicePath: "services/pii-provider/manifests/service.yaml",
    env: [
      "PORT",
      "DATABASE_URL",
      "PII_MASTER_KEY",
      "OBJECT_STORAGE_ENDPOINT",
      "OBJECT_STORAGE_PORT",
      "OBJECT_STORAGE_USE_SSL",
      "PII_PAYLOADS_BUCKET",
      "PROFILE_PROVIDER_BASE_URL",
      "OBJECT_STORAGE_ACCESS_KEY",
      "OBJECT_STORAGE_SECRET_KEY",
    ],
  },
] as const

for (const spec of serviceSpecs) {
  it(`${spec.name} Kubernetes manifests expose the expected HTTP runtime`, () => {
    const deployment = readManifest<DeploymentManifest>(spec.deploymentPath)
    const service = readManifest<ServiceManifest>(spec.servicePath)
    const container = deployment.spec?.template?.spec?.containers?.find(
      ({ name }) => name === "backend",
    )

    expect(deployment.kind).toBe("Deployment")
    expect(service.kind).toBe("Service")
    expect(container).toBeDefined()
    if (container === undefined) {
      throw new Error(`${spec.name} deployment is missing backend container`)
    }

    expect(container.ports ?? []).toContainEqual(
      expect.objectContaining({
        containerPort: spec.port,
        name: "http",
        protocol: "TCP",
      }),
    )
    expect(container.livenessProbe?.httpGet).toEqual({
      path: "/health",
      port: spec.port,
    })
    expect(container.readinessProbe?.httpGet).toEqual({
      path: "/health",
      port: spec.port,
    })

    const envByName = new Map(
      (container.env ?? []).map((env) => [env.name, env]),
    )
    for (const envName of spec.env) {
      expect(envByName.has(envName)).toBe(true)
    }
    expect(envByName.get("PORT")?.value).toBe(String(spec.port))

    expect(service.spec?.ports ?? []).toContainEqual(
      expect.objectContaining({
        name: "http",
        port: 80,
        targetPort: spec.port,
        protocol: "TCP",
      }),
    )
  })
}
