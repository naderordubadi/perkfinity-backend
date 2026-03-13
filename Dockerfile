# BUILD STAGE
FROM node:20-bookworm-slim AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY package*.json ./

# Install dependencies (since we have no package-lock.json here yet, we use install)
RUN npm install

# Copy source files
COPY . .

RUN npx prisma generate --schema=./prisma/schema.prisma

# Build TypeScript
RUN npm run build

# PRODUCTION STAGE
FROM node:20-bookworm-slim AS production

WORKDIR /app
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Copy package files and install production dependencies only
COPY package*.json ./
RUN npm install --only=production

# Copy compiled code from builder
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/prisma ./prisma
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma

# Set ownership
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
  CMD wget --no-verbose --tries=1 --spider http://localhost:3001/health || exit 1

CMD ["npm", "start"]
