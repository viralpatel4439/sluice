import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://viralpatel4439.github.io",
  base: "/sluice",
  integrations: [
    starlight({
      title: "sluice",
      description: "Release bursts to downstream services only as fast as they can take them.",
      social: [{ icon: "github", label: "GitHub", href: "https://github.com/viralpatel4439/sluice" }],
      editLink: { baseUrl: "https://github.com/viralpatel4439/sluice/edit/main/" },
      sidebar: [
        {
          label: "Start here",
          items: ["index", "guides/quick-start", "guides/configuration", "guides/delivery-semantics"],
        },
        {
          label: "Operate",
          items: [
            "guides/server",
            "guides/observability",
            { label: "Runbooks", items: [{ autogenerate: { directory: "runbooks" } }] },
          ],
        },
        {
          label: "Internals",
          items: [
            "reference/architecture",
            "reference/protocol",
            { label: "Decisions (ADRs)", items: [{ autogenerate: { directory: "adr" } }] },
          ],
        },
        { label: "API", items: [{ autogenerate: { directory: "api" } }] },
      ],
    }),
  ],
});
