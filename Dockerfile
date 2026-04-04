# ── Build stage ───────────────────────────────────────────────────────────────
FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

# Build args injected at image build time by CI
ARG VITE_GOOGLE_CLIENT_ID
ARG VITE_UPLOAD_API_URL
ARG VITE_COMMIT_SHA
ENV VITE_GOOGLE_CLIENT_ID=$VITE_GOOGLE_CLIENT_ID
ENV VITE_UPLOAD_API_URL=$VITE_UPLOAD_API_URL
ENV VITE_COMMIT_SHA=$VITE_COMMIT_SHA

RUN npm run build

# ── Serve stage ────────────────────────────────────────────────────────────────
FROM nginx:1.27-alpine AS serve

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

# Cloud Run expects the container to listen on $PORT (default 8080)
EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
