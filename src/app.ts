import fastify from 'fastify';
import helmet from '@fastify/helmet';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import jwt from '@fastify/jwt';
import { logger } from './config/logger.js';
import { env } from './config/env.js';
import { prismaPlugin } from './plugins/prisma.js';
import { rateLimitErrorHandlerPlugin } from './plugins/rateLimit.js';
import authRoutes from './modules/auth/routes.js';
import qrRoutes from './modules/qr/routes.js';
import campaignRoutes from './modules/campaigns/routes.js';
import redemptionRoutes from './modules/redemptions/routes.js';
import merchantRoutes from './modules/merchants/routes.js';
import analyticsRoutes from './modules/analytics/routes.js';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { jsonSchemaTransform, validatorCompiler, serializerCompiler } from 'fastify-type-provider-zod';
export async function buildApp() {
  const app = fastify({
    logger,
    disableRequestLogging: true,
  });

  app.setValidatorCompiler(validatorCompiler);
  app.setSerializerCompiler(serializerCompiler);
  // 1. Helmet (security headers)
  await app.register(helmet);

  // 2. CORS (restrict to known origins in production)
  await app.register(cors, {
    origin: env.NODE_ENV === 'production'
      ? ['https://dashboard.perkfinity.com', 'https://perkfinity.net', 'https://www.perkfinity.net']
      : true,
    credentials: true,
    allowedHeaders: ['Authorization', 'Content-Type', 'Accept', 'Origin', 'User-Agent', 'Idempotency-Key']
  });

  // 3. Rate limiting (global default)
  await app.register(rateLimit, {
    max: env.RATE_LIMIT_MAX,
    timeWindow: env.RATE_LIMIT_WINDOW_MS,
  });

  await app.register(prismaPlugin);

  // 4. JWT plugin
  await app.register(jwt, {
    secret: env.JWT_SECRET,
  });

  // 6. Swagger API Documentation (Task C10)
  await app.register(swagger, {
    openapi: {
      info: { title: 'Perkfinity API', version: '1.0.0' },
      components: {
        securitySchemes: {
          bearerAuth: { type: 'http', scheme: 'bearer', bearerFormat: 'JWT' }
        }
      },
      security: [{ bearerAuth: [] }]
    },
    transform: jsonSchemaTransform
  });

  if (env.NODE_ENV === 'development') {
    await app.register(swaggerUi, {
      routePrefix: '/docs',
    });
  }

  // 7. All module routes with /api/v1 prefix
  await app.register(rateLimitErrorHandlerPlugin);
  await app.register(authRoutes, { prefix: '/api/v1/auth' });
  await app.register(qrRoutes, { prefix: '/api/v1/qr' });
  await app.register(campaignRoutes, { prefix: '/api/v1/campaigns' });
  await app.register(redemptionRoutes, { prefix: '/api/v1/redemptions' });
  await app.register(merchantRoutes, { prefix: '/api/v1/merchants' });
  await app.register(analyticsRoutes, { prefix: '/api/v1/analytics' });

  // Task A14 - Health Check
  app.get('/health', async (request, reply) => {
    return {
      ok: true,
      status: 'healthy',
      timestamp: new Date().toISOString(),
      version: '1.0.0',
    };
  });

  app.addHook('onRequest', (req, reply, done) => {
    req.log.info({ req: { method: req.method, url: req.url } }, 'incoming request');
    done();
  });

  app.addHook('onResponse', (req, reply, done) => {
    req.log.info({ res: { statusCode: reply.statusCode }, responseTime: reply.elapsedTime }, 'request completed');
    done();
  });

  return app;
}
