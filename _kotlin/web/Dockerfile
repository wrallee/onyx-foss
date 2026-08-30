ARG BASE_IMAGE_REGISTRY=docker.io
ARG NODE_BUILDER_IMAGE=${BASE_IMAGE_REGISTRY}/library/node:24-trixie-slim
ARG NODE_RUNTIME_IMAGE=${BASE_IMAGE_REGISTRY}/library/node:24-trixie-slim

FROM ${NODE_BUILDER_IMAGE} AS builder
WORKDIR /app

COPY package.json package-lock.json .npmrc tsconfig.json ./
COPY lib/opal/ ./lib/opal/
COPY lib/shared/ ./lib/shared/
RUN npm ci

COPY . .
ENV NEXT_PRIVATE_STANDALONE=true
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM ${NODE_RUNTIME_IMAGE} AS runner
LABEL org.opencontainers.image.licenses="MIT"
LABEL com.onyx.description="Kotlin administration UI built from approved FOSS sources"
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV HOSTNAME=0.0.0.0

COPY --from=builder --chown=node:node /app/public ./public
COPY --from=builder --chown=node:node /app/.next/standalone ./
COPY --from=builder --chown=node:node /app/.next/static ./.next/static

USER node
CMD ["node", "server.js"]
