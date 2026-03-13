import { FastifyInstance } from 'fastify';
import { RegisterMerchantSchema, LoginMerchantSchema, RefreshTokenSchema, AnonymousUserSchema } from './schemas.js';
import { AuthService } from './service.js';
import { failure, success } from '../../utils/response.js';
import { rateLimitConfig } from '../../plugins/rateLimit.js';

export default async function authRoutes(fastify: FastifyInstance) {
  const authService = new AuthService(fastify.prisma, fastify.jwt);

  fastify.post('/merchant/register', {
    config: { rateLimit: rateLimitConfig.auth },
    schema: { body: RegisterMerchantSchema },
    handler: async (request, reply) => {
      try {
        const result = await authService.registerMerchant(request.body);
        return success(result);
      } catch (err: any) {
        console.error('REGISTER MERCHANT ERROR:', err);

        if (err.code === 'P2002') {
          return reply.code(400).send(failure('VALIDATION_ERROR', 'Email already exists'));
        }

        return reply.code(500).send({
          ok: false,
          error: {
            code: 'INTERNAL_ERROR',
            message: err?.message || 'Internal server error'
          }
        });
      }
    }
  });

  fastify.post('/merchant/login', {
    config: { rateLimit: rateLimitConfig.auth },
    schema: { body: LoginMerchantSchema },
    handler: async (request, reply) => {
      const result = await authService.loginMerchant(request.body);
      if (!result) {
        return reply.code(401).send(failure('AUTH_INVALID_CREDENTIALS', 'Invalid email or password'));
      }
      return success(result);
    }
  });

  fastify.post('/merchant/refresh', {
    config: { rateLimit: rateLimitConfig.auth },
    schema: { body: RefreshTokenSchema },
    handler: async (request: any, reply) => {
      try {
        const result = await authService.refreshAccessToken(request.body.refresh_token);
        return success(result);
      } catch (err) {
        return reply.code(401).send(failure('AUTH_TOKEN_EXPIRED', 'Invalid or expired refresh token'));
      }
    }
  });

  fastify.post('/user/anonymous', {
    config: { rateLimit: rateLimitConfig.auth },
    schema: { body: AnonymousUserSchema },
    handler: async (request, reply) => {
      const result = await authService.getOrCreateAnonymousUser();
      return success(result);
    }
  });
}
