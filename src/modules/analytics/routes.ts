import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { AnalyticsService } from './service.js';
import { failure, success } from '../../utils/response.js';
import { requireMerchantAuth, requireRole } from '../../plugins/auth.js';

export default async function analyticsRoutes(fastify: FastifyInstance) {
  const service = new AnalyticsService(fastify.prisma);

  fastify.get('/summary', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: {
      querystring: z.object({
        period: z.enum(['7d', '30d']).default('30d')
      }),
      security: [{ bearerAuth: [] }]
    },
    handler: async (request: any, reply) => {
      const result = await service.getAnalyticsSummary(request.user.merchant_id, request.query.period);
      return success(result);
    }
  });

  fastify.get('/events', {
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
      const result = await service.getEventsList(request.user.merchant_id, page, limit);
      return success(result);
    }
  });
}
