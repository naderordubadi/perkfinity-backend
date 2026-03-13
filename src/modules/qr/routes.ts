import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { QrService } from './service.js';
import { failure, success } from '../../utils/response.js';
import { requireMerchantAuth, requireRole } from '../../plugins/auth.js';
import { rateLimitConfig } from '../../plugins/rateLimit.js';

export default async function qrRoutes(fastify: FastifyInstance) {
  const qrService = new QrService(fastify.prisma);

  fastify.get('/resolve/:public_code', {
    config: { rateLimit: rateLimitConfig.public },
    schema: {
      params: z.object({ public_code: z.string() })
    },
    handler: async (request: any, reply) => {
      const result = await qrService.resolveQrCode(request.params.public_code);
      if (!result) {
        return reply.code(404).send(failure('QR_INACTIVE', 'QR code not found or inactive'));
      }
      if (!result.campaign) {
        return reply.code(404).send(failure('NO_ACTIVE_CAMPAIGN', 'No active campaign found for this QR code'));
      }
      return success(result);
    }
  });

  fastify.post('/', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: {
      body: z.object({ location_id: z.string().uuid().optional() }),
      security: [{ bearerAuth: [] }]
    },
    handler: async (request: any, reply) => {
      const user = request.user;
      const result = await qrService.generateQrCode(user.sub, user.merchant_id, request.body.location_id);
      return success(result);
    }
  });
  fastify.get('/merchant', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: {
      security: [{ bearerAuth: [] }]
    },
    handler: async (request: any, reply) => {
      const user = request.user;
      
      // Get the first active QR code or generate a new one
      let qrRecord = await fastify.prisma.qrCode.findFirst({
        where: { merchant_id: user.merchant_id, status: 'active' }
      });

      if (!qrRecord) {
        const result = await qrService.generateQrCode(user.sub, user.merchant_id);
        qrRecord = result.qr;
      }

      // Generate the physical image payload
      const qrDataUrl = await qrService.generateMerchantQrImage(qrRecord.public_code);
      return success({ image: qrDataUrl });
    }
  });
}
