# syntax=docker/dockerfile:1.7
# Multi-stage build for the sluice monorepo.
#   docker build --target server -t sluice-server .     → the coordination server
#   docker build --target examples -t sluice-examples . → demo apps used by docker-compose

FROM node:22-alpine AS build
WORKDIR /repo
COPY package.json package-lock.json ./
COPY packages/sluice/package.json packages/sluice/
COPY packages/server/package.json packages/server/
COPY examples/target-service/package.json examples/target-service/
COPY examples/orders-fetch/package.json examples/orders-fetch/
COPY examples/billing-axios/package.json examples/billing-axios/
COPY examples/data-migration/package.json examples/data-migration/
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY tsconfig.base.json ./
COPY packages ./packages
RUN npm run build --workspace @viralpatel4439/sluice --workspace @viralpatel4439/sluice-server \
 && npm prune --omit=dev --ignore-scripts --no-audit --no-fund

FROM node:22-alpine AS server
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /repo/node_modules ./node_modules
COPY --from=build --chown=node:node /repo/packages/sluice/package.json ./packages/sluice/package.json
COPY --from=build --chown=node:node /repo/packages/sluice/dist ./packages/sluice/dist
COPY --from=build --chown=node:node /repo/packages/server/package.json ./packages/server/package.json
COPY --from=build --chown=node:node /repo/packages/server/dist ./packages/server/dist
USER node
EXPOSE 7070 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s CMD wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1
ENTRYPOINT ["node", "packages/server/dist/main.js"]

FROM node:22-alpine AS examples
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build --chown=node:node /repo/node_modules ./node_modules
COPY --from=build --chown=node:node /repo/packages/sluice/package.json ./packages/sluice/package.json
COPY --from=build --chown=node:node /repo/packages/sluice/dist ./packages/sluice/dist
COPY --chown=node:node examples ./examples
USER node
