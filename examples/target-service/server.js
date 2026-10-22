// Demo "service B": slow-ish work, a hard concurrency limit, and an honest scoreboard.
// It knows nothing about sluice except that it advertises its capacity in a response header.
import { createServer } from "node:http";
import { Redis } from "ioredis";

const PORT = Number(process.env.PORT ?? 3000);
const TOTAL = Number(process.env.TOTAL_CAPACITY ?? 20); // what the whole service (all replicas) can take
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 50);
const redis = process.env.DEMO_REDIS_URL ? new Redis(process.env.DEMO_REDIS_URL) : undefined;

// Atomically bump the fleet-wide in-flight count and keep its peak.
const ENTER = `local n = redis.call('INCR', KEYS[1]); if n > tonumber(redis.call('GET', KEYS[2]) or '0') then redis.call('SET', KEYS[2], n) end; return n`;
let local = 0;

const server = createServer(async (req, res) => {
  if (req.url === "/_capacity") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ maxConcurrency: TOTAL, inFlight: local, health: "ok" }));
  }
  if (req.url === "/stats") {
    const [inflight, peak, served, rejected] = redis
      ? await redis.mget("demo:inflight", "demo:peak", "demo:served", "demo:rejected")
      : [local, 0, 0, 0];
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(
      JSON.stringify({
        capacity: TOTAL,
        inflight: Number(inflight),
        peak: Number(peak),
        served: Number(served),
        rejected: Number(rejected),
      }),
    );
  }
  if (req.url === "/reset" && req.method === "POST") {
    if (redis) await redis.mset("demo:peak", 0, "demo:served", 0, "demo:rejected", 0);
    return res.writeHead(204).end();
  }
  for await (const _ of req); // read the body
  local++;
  const global = redis ? await redis.eval(ENTER, 2, "demo:inflight", "demo:peak") : local;
  try {
    // Over capacity: behave like a real overloaded service.
    if (global > TOTAL) {
      redis?.incr("demo:rejected");
      res.writeHead(503, { "retry-after": "1", "x-sluice-capacity": String(TOTAL) });
      return res.end("overloaded");
    }
    await new Promise((r) => setTimeout(r, LATENCY_MS * (0.5 + Math.random())));
    redis?.incr("demo:served");
    res.writeHead(200, { "content-type": "application/json", "x-sluice-capacity": String(TOTAL) });
    res.end(JSON.stringify({ ok: true, key: req.headers["idempotency-key"] ?? null }));
  } finally {
    local--;
    await redis?.decr("demo:inflight");
  }
});

server.listen(PORT, () => console.log(`target-service on :${PORT}, total capacity ${TOTAL}`));
process.on("SIGTERM", () => server.close(() => process.exit(0)));
