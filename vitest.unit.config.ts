import { defineConfig } from "vitest/config";

/** Fast tests that need no Docker: used by mutation testing and quick local runs. */
export default defineConfig({
  test: {
    include: [
      "packages/sluice/test/primitives.test.ts",
      "packages/sluice/test/engine.test.ts",
      "packages/sluice/test/simulation.test.ts",
      "packages/sluice/test/metrics.test.ts",
      "packages/sluice/test/core-units.test.ts",
      "packages/sluice/test/target.test.ts",
      "packages/sluice/test/otel.test.ts",
      "packages/sluice/test/intercept.test.ts",
    ],
    testTimeout: 60_000,
  },
});
