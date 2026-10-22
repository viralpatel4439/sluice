#!/usr/bin/env node
import { startServer } from "./server.js";

const server = await startServer().catch((error) => {
  console.error(`sluice-server failed to start: ${error instanceof Error ? error.message : error}`);
  process.exit(1);
});

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.once(signal, () => {
    server.close().finally(() => process.exit(0));
  });
}
