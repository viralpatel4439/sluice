// Ordinary service code: bursts of POSTs with fetch. Nothing here knows about sluice.
const TARGET = process.env.TARGET_URL ?? "http://target/records";
const BURST = Number(process.env.BURST ?? 200);
const PAUSE_MS = Number(process.env.PAUSE_MS ?? 3000);

async function burst(round) {
  const started = Date.now();
  const results = await Promise.allSettled(
    Array.from({ length: BURST }, (_, i) =>
      fetch(TARGET, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": `orders-${process.pid}-${round}-${i}`,
        },
        body: JSON.stringify({ order: i, round }),
      }),
    ),
  );
  const ok = results.filter((r) => r.status === "fulfilled" && r.value.status === 200).length;
  console.log(`[orders] round ${round}: ${ok}/${BURST} ok in ${Date.now() - started} ms`);
}

for (let round = 1; ; round++) {
  await burst(round);
  await new Promise((r) => setTimeout(r, PAUSE_MS));
}
