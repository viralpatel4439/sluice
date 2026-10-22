import { defineConfig } from "tsup";

export default defineConfig({
  entry: { index: "src/index.ts", main: "src/main.ts" },
  format: ["esm"],
  dts: false,
  sourcemap: true,
  clean: true,
  target: "node22",
  external: ["@viralpatel4439/sluice", "ioredis", "@grpc/grpc-js", "protobufjs"],
});
