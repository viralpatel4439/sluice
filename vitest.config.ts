import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const src = (p: string) => fileURLToPath(new URL(`./packages/sluice/src/${p}`, import.meta.url));

export default defineConfig({
  resolve: {
    alias: [
      { find: /^@viralpatel4439\/sluice\/(redis|remote|prometheus|otel)$/, replacement: src("$1/index.ts") },
      { find: /^@viralpatel4439\/sluice$/, replacement: src("index.ts") },
    ],
  },
  test: {
    include: ["packages/*/test/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 120_000,
    globalSetup: ["packages/sluice/test/redis-setup.ts"],
    coverage: {
      provider: "v8",
      include: ["packages/*/src/**"],
      exclude: ["**/proto-text.ts", "**/main.ts", "**/ui.ts"],
      reporter: ["text-summary", "lcov"],
      thresholds: { lines: 85, branches: 80 },
    },
  },
});
