// The explicit API, for when you want full control (library authors, custom transports).
import { consoleLogger, createEngine } from "@viralpatel4439/sluice";

const TARGET_URL = process.env.TARGET_URL ?? "http://localhost:3000/records";
const ROWS = Number(process.env.ROWS ?? 2000);

let coordination;
if (process.env.SLUICE_SERVER_URL) {
  const { remotePool } = await import("@viralpatel4439/sluice/remote");
  coordination = await remotePool({
    url: process.env.SLUICE_SERVER_URL,
    instanceId: `migrator-${process.pid}`,
    caller: "migrator",
  });
}

const engine = createEngine({ caller: "migrator", coordination, logger: consoleLogger("info") });

const records = engine.target("records", {
  capacity: { type: "auto", start: 10, url: new URL("/_capacity", TARGET_URL).toString() },
  queue: { maxItems: 5_000, overflow: "block" },
  retry: { max: 5, baseMs: 200, maxMs: 10_000 },
  send: (item, signal) =>
    fetch(TARGET_URL, {
      method: "POST",
      signal,
      headers: { "content-type": "application/json", ...item.headers },
      body: JSON.stringify(item.payload),
    }),
});

engine.on("capacity:changed", (e) => console.log(`capacity ${e.previous} → ${e.capacity}`));
process.on("SIGTERM", () =>
  engine.drain({ timeoutMs: 20_000, onUnsent: (u) => console.log(`${u.length} rows to replay`) }),
);

const started = Date.now();
let done = 0;
await Promise.all(
  Array.from({ length: ROWS }, (_, id) =>
    records
      .submit(
        { id, migratedAt: Date.now() },
        { key: `row-${id}`, priority: id % 10 === 0 ? "critical" : "bulk" },
      )
      .then(() => {
        if (++done % 500 === 0) console.log(`${done}/${ROWS} rows`);
      }),
  ),
);
console.log(`migrated ${ROWS} rows in ${Date.now() - started} ms`, records.snapshot().counters);
await engine.close();
