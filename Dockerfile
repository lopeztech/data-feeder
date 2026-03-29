# ── Build stage ───────────────────────────────────────────────────────────────
FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

# Build arg injected at image build time by CI
ARG VITE_GOOGLE_CLIENT_ID

RUN npm run build

# ── Run stage ─────────────────────────────────────────────────────────────────
FROM node:20-alpine AS run

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist
COPY --from=build /app/dist-server ./dist-server

# Cloud Run expects the container to listen on $PORT (default 8080)
ENV PORT=8080
EXPOSE 8080

CMD ["node", "dist-server/index.mjs"]
