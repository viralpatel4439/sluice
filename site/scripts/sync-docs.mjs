// Builds the site's content from the repository's own docs, so the two never drift apart.
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repo = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const out = join(repo, "site", "src", "content", "docs");
rmSync(out, { recursive: true, force: true });

const write = (rel, title, body, description) => {
  const file = join(out, rel);
  mkdirSync(dirname(file), { recursive: true });
  const fm = [
    `title: ${JSON.stringify(title)}`,
    description ? `description: ${JSON.stringify(description)}` : "",
  ]
    .filter(Boolean)
    .join("\n");
  // Drop the leading H1 (Starlight renders the title) and point repo-relative links at GitHub.
  const text = body
    .replace(/^# .*\n+/, "")
    .replace(
      /\]\((?!https?:|#)([^)]+)\)/g,
      (_, link) =>
        `](https://github.com/viralpatel4439/sluice/blob/main/${link.replace(/^\.\.\//, "").replace(/^\.\//, "")})`,
    );
  writeFileSync(file, `---\n${fm}\n---\n\n${text}`);
};
const read = (p) => readFileSync(join(repo, p), "utf8");

// Split the README into focused pages.
const readme = read("README.md");
const section = (heading) => {
  const start = readme.indexOf(`## ${heading}`);
  if (start === -1) return "";
  const next = readme.indexOf("\n## ", start + 3);
  return readme.slice(start, next === -1 ? undefined : next).replace(/^## .*\n/, "");
};
const intro = readme.slice(0, readme.indexOf("## 30-second start"));
write(
  "index.md",
  "sluice",
  `${intro}\n## Where next\n\n- [Quick start](guides/quick-start/)\n- [Architecture](reference/architecture/)\n`,
  "Distributed capacity engine for Node.js",
);
write(
  "guides/quick-start.md",
  "Quick start",
  [
    "## 30-second start",
    section("30-second start"),
    "## Three ways to use it",
    section("Three ways to use it"),
    "## Coordination modes",
    section("Coordination modes"),
    "## When not to use it",
    section("When not to use it"),
  ].join("\n"),
);
write("guides/configuration.md", "Configuration", section("Configuration (environment)"));
write("guides/delivery-semantics.md", "Delivery semantics", section("Delivery semantics"));
write("guides/observability.md", "Observability", section("Observability"));
write("guides/server.md", "sluice-server", read("packages/server/README.md"));
write("reference/architecture.md", "Architecture", read("docs/architecture.md"));
write("reference/protocol.md", "Coordination protocol", read("docs/protocol.md"));
for (const dir of ["adr", "runbooks"]) {
  for (const f of readdirSync(join(repo, "docs", dir))
    .filter((x) => x.endsWith(".md"))
    .sort()) {
    const body = read(join("docs", dir, f));
    const title = (body.match(/^# (.*)$/m)?.[1] ?? f).replace(/^Runbook: /, "");
    write(join(dir, f), title, body);
  }
}
for (const f of readdirSync(join(repo, "packages", "sluice", "api"))
  .filter((x) => x.endsWith(".api.md"))
  .sort()) {
  const name = f.replace(".api.md", "");
  const entry = name === "index" ? "@viralpatel4439/sluice" : `@viralpatel4439/sluice/${name}`;
  write(
    join("api", `${name}.md`),
    entry,
    `The public API of \`${entry}\`, as locked by API Extractor. Changes to it need review.\n\n${read(join("packages", "sluice", "api", f)).replace(/^## API Report File.*\n/, "")}`,
  );
}
console.log("site content synced");
