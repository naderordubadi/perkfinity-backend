import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { RedemptionService } from './service.js';
import { failure, success } from '../../utils/response.js';
import { requireMerchantAuth, requireRole } from '../../plugins/auth.js';

export default async function redemptionRoutes(fastify: FastifyInstance) {
  const service = new RedemptionService(fastify.prisma);

  fastify.post('/validate', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager', 'staff')],
    schema: {
      body: z.object({ token: z.string() }),
      security: [{ bearerAuth: [] }]
    },
    handler: async (request: any, reply) => {
      try {
        const result = await service.validateRedemption(request.user.sub, request.user.merchant_id, request.body.token);
        return success(result);
      } catch (err: any) {
        if (err.message === 'TOKEN_NOT_FOUND') return reply.code(404).send(failure('TOKEN_NOT_FOUND', 'Token not found'));
        if (err.message === 'TOKEN_EXPIRED') return reply.code(400).send(failure('TOKEN_EXPIRED', 'Token has expired'));
        if (err.message === 'TOKEN_ALREADY_REDEEMED') return reply.code(400).send(failure('TOKEN_ALREADY_REDEEMED', 'Token already redeemed'));
        if (err.message === 'MERCHANT_MISMATCH') return reply.code(403).send(failure('MERCHANT_MISMATCH', 'Token does not belong to this merchant'));
        throw err;
      }
    }
  });

  fastify.get('/', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: {
      querystring: z.object({
        page: z.coerce.number().min(1).optional(),
        limit: z.coerce.number().min(1).max(100).optional()
      }),
      security: [{ bearerAuth: [] }]
    },
    handler: async (request: any, reply) => {
      const { page, limit } = request.query;
      const result = await service.listRedemptions(request.user.merchant_id, page, limit);
      return success(result);
    }
  });
}
