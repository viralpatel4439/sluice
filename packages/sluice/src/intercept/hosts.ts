import { isIP } from "node:net";

/** Normalise a URL host into a target name: lowercase host, default ports dropped, ":" kept for others. */
export function targetNameFor(url: URL): string {
  const host = url.hostname
    .toLowerCase()
    .replace(/^\[|\]$/g, "")
    .replace(/\.$/, "");
  const defaultPort = url.protocol === "https:" ? "443" : "80";
  const port = url.port && url.port !== defaultPort ? `_${url.port}` : "";
  return `${host.replace(/:/g, "-")}${port}`;
}

/**
 * Hosts that look internal to the cluster/VPC. Only these get sluice headers
 * and `/_capacity` probes by default; third-party APIs are gated but left untouched.
 */
export function isInternalHost(hostname: string): boolean {
  const h = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (h === "localhost") return true;
  if (!h.includes(".") && isIP(h) === 0) return true; // bare service name, e.g. "inventory"
  if (/\.(svc|local|internal|cluster\.local|consul|lan)$/.test(h)) return true;
  if (isIP(h) === 4) {
    const [a, b] = h.split(".").map(Number) as [number, number];
    return a === 10 || a === 127 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168);
  }
  if (isIP(h) === 6) return h === "::1" || /^f[cd]/.test(h);
  return false;
}

const ALWAYS_EXCLUDED = ["169.254.169.254", "fd00:ec2::254", "metadata.google.internal", "metadata"];

function globToRegExp(glob: string): RegExp {
  const escaped = glob
    .trim()
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*");
  return new RegExp(`^${escaped}$`, "i");
}

/** Builds the predicate for hosts that must never be gated. */
export function exclusionMatcher(
  extra: string[] = [],
  env: NodeJS.ProcessEnv = process.env,
): (url: URL) => boolean {
  const hosts = new Set(ALWAYS_EXCLUDED);
  for (const key of Object.keys(env)) {
    // Telemetry exporters must never queue behind the traffic they report on.
    if (/^OTEL_EXPORTER_.*ENDPOINT$/.test(key) || key === "SLUICE_SERVER_URL" || key === "SLUICE_REDIS_URL") {
      try {
        hosts.add(new URL(env[key]!).hostname.toLowerCase());
      } catch {}
    }
  }
  const patterns = extra.filter(Boolean).map(globToRegExp);
  return (url) => {
    const h = url.hostname.toLowerCase().replace(/^\[|\]$/g, "");
    if (hosts.has(h)) return true;
    if (url.pathname === "/_capacity") return true;
    return patterns.some((p) => p.test(h) || p.test(url.host));
  };
}
