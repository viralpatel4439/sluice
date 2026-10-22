// Locks the public API: writes one API report per entry point into packages/sluice/api/.
// `--check` fails when the API changed without the reports being updated.
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Extractor, ExtractorConfig } from "@microsoft/api-extractor";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const check = process.argv.includes("--check");
const entries = {
  index: "dist/index.d.ts",
  redis: "dist/redis/index.d.ts",
  remote: "dist/remote/index.d.ts",
  prometheus: "dist/prometheus.d.ts",
  otel: "dist/otel.d.ts",
};
mkdirSync(join(root, "api"), { recursive: true });
let failed = false;
for (const [name, entry] of Object.entries(entries)) {
  const configFile = join(root, "api", `.${name}.api-extractor.json`);
  writeFileSync(
    configFile,
    JSON.stringify({
      projectFolder: root,
      mainEntryPointFilePath: join(root, entry),
      compiler: {
        overrideTsconfig: {
          compilerOptions: {
            lib: ["ES2023", "DOM"],
            types: ["node"],
            skipLibCheck: true,
            module: "NodeNext",
            moduleResolution: "NodeNext",
          },
        },
      },
      apiReport: {
        enabled: true,
        reportFileName: `${name}.api.md`,
        reportFolder: join(root, "api"),
        reportTempFolder: join(root, "api", ".tmp"),
      },
      docModel: { enabled: false },
      dtsRollup: { enabled: false },
      tsdocMetadata: { enabled: false },
      messages: {
        extractorMessageReporting: {
          default: { logLevel: "none" },
          "ae-missing-release-tag": { logLevel: "none" },
        },
        compilerMessageReporting: { default: { logLevel: "warning" } },
        tsdocMessageReporting: { default: { logLevel: "none" } },
      },
    }),
  );
  const result = Extractor.invoke(ExtractorConfig.loadFileAndPrepare(configFile), {
    localBuild: !check,
    showVerboseMessages: false,
  });
  if (result.apiReportChanged && check) {
    console.error(
      `API of "${name}" changed. Run: npm run api:update, and review packages/sluice/api/${name}.api.md.`,
    );
    failed = true;
  } else if (!result.succeeded && !result.apiReportChanged) {
    console.error(`api-extractor failed for ${name}`);
    failed = true;
  } else console.log(`${name}: ${result.apiReportChanged ? "updated" : "unchanged"}`);
}
process.exit(failed ? 1 : 0);
