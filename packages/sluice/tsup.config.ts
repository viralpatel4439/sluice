import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    register: "src/register.ts",
    "redis/index": "src/redis/index.ts",
    "remote/index": "src/remote/index.ts",
    prometheus: "src/prometheus.ts",
    otel: "src/otel.ts",
  },
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "node22",
  splitting: true,
  external: ["ioredis", "@grpc/grpc-js", "protobufjs", "@opentelemetry/api"],
});
