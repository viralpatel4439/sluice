// Ordinary service code using axios. Nothing here knows about sluice.
import axios from "axios";

const TARGET = process.env.TARGET_URL ?? "http://target/records";
const BURST = Number(process.env.BURST ?? 150);
const PAUSE_MS = Number(process.env.PAUSE_MS ?? 4000);

for (let round = 1; ; round++) {
  const started = Date.now();
  const results = await Promise.allSettled(
    Array.from({ length: BURST }, (_, i) =>
      axios.post(TARGET, { invoice: i, round }, { validateStatus: () => true }),
    ),
  );
  const ok = results.filter((r) => r.status === "fulfilled" && r.value.status === 200).length;
  console.log(`[billing] round ${round}: ${ok}/${BURST} ok in ${Date.now() - started} ms`);
  await new Promise((r) => setTimeout(r, PAUSE_MS));
}
