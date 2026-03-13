import { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { CampaignService } from './service.js';
import { CreateCampaignSchema, UpdateCampaignSchema } from './schemas.js';
import { failure, success } from '../../utils/response.js';
import { requireMerchantAuth, requireRole, requireUserAuth } from '../../plugins/auth.js';
import { getIdempotencyResult, setIdempotencyResult } from '../../utils/idempotency.js';
import crypto from 'node:crypto';
import { nowPlusMinutes } from '../../utils/time.js';

export default async function campaignRoutes(fastify: FastifyInstance) {
  const service = new CampaignService(fastify.prisma);

  fastify.post('/', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: { body: CreateCampaignSchema, security: [{ bearerAuth: [] }] },
    handler: async (request: any, reply) => {
      try {
        const { sub, merchant_id } = request.user;
        const merchant = await fastify.prisma.merchant.findUnique({ where: { id: merchant_id } });
        if (!merchant) return reply.code(404).send(failure('MERCHANT_NOT_FOUND', 'Merchant not found'));

        const result = await service.createCampaign(sub, merchant_id, merchant.subscription_tier, request.body);
        return success(result);
      } catch (err: any) {
        if (err.message === 'TIER_LIMIT_REACHED') {
          return reply.code(403).send(failure('TIER_LIMIT_REACHED', 'You have reached the maximum number of active campaigns for your tier'));
        }
        throw err;
      }
    }
  });

  fastify.get('/', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager', 'staff')],
    schema: { security: [{ bearerAuth: [] }] },
    handler: async (request: any, reply) => {
      const result = await service.listCampaigns(request.user.merchant_id);
      return success(result);
    }
  });

  fastify.get('/:id', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager', 'staff')],
    schema: { security: [{ bearerAuth: [] }] },
    handler: async (request: any, reply) => {
      const result = await service.getCampaign(request.user.merchant_id, request.params.id);
      if (!result) return reply.code(404).send(failure('CAMPAIGN_NOT_FOUND', 'Campaign not found'));
      return success(result);
    }
  });

  fastify.patch('/:id', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: { body: UpdateCampaignSchema, security: [{ bearerAuth: [] }] },
    handler: async (request: any, reply) => {
      try {
        const result = await service.updateCampaign(request.user.sub, request.user.merchant_id, request.params.id, request.body);
        return success(result);
      } catch (err: any) {
        if (err.message === 'CAMPAIGN_NOT_FOUND') return reply.code(404).send(failure('CAMPAIGN_NOT_FOUND', 'Campaign not found'));
        throw err;
      }
    }
  });

  fastify.post('/:id/pause', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: { security: [{ bearerAuth: [] }] },
    handler: async (request: any, reply) => {
      try {
        const result = await service.pauseCampaign(request.user.sub, request.user.merchant_id, request.params.id);
        return success(result);
      } catch (err: any) {
        if (err.message === 'CAMPAIGN_NOT_FOUND') return reply.code(404).send(failure('CAMPAIGN_NOT_FOUND', 'Campaign not found'));
        throw err;
      }
    }
  });

  fastify.post('/:id/activate', {
    preHandler: [requireMerchantAuth, requireRole('owner', 'manager')],
    schema: {
      headers: z.object({ 'idempotency-key': z.string().uuid().optional() }).passthrough(),
      body: z.any().optional(),
      security: [{ bearerAuth: [] }]
    },
    handler: async (request: any, reply) => {
      const idempotencyKey = request.headers['idempotency-key'];
      if (idempotencyKey) {
        const cached = getIdempotencyResult(idempotencyKey);
        if (cached) return success(cached);
      }

      try {
        const merchant = await fastify.prisma.merchant.findUnique({ where: { id: request.user.merchant_id } });
        if (!merchant) return reply.code(404).send(failure('MERCHANT_NOT_FOUND', 'Merchant not found'));

        const result = await service.activateCampaign(request.user.sub, request.user.merchant_id, merchant.subscription_tier, request.params.id);
        
        if (idempotencyKey) {
          setIdempotencyResult(idempotencyKey, result, 20 * 60 * 1000);
        }
        
        return success(result);
      } catch (err: any) {
        if (err.message === 'CAMPAIGN_NOT_FOUND') return reply.code(404).send(failure('CAMPAIGN_NOT_FOUND', 'Campaign not found'));
        if (err.message === 'TIER_LIMIT_REACHED') return reply.code(403).send(failure('TIER_LIMIT_REACHED', 'Tier limit reached'));
        throw err;
      }
    }
  });

  fastify.delete('/:id', {
    preHandler: [requireMerchantAuth, requireRole('owner')],
    schema: { security: [{ bearerAuth: [] }] },
    handler: async (request: any, reply) => {
      try {
        const result = await service.deleteCampaign(request.user.sub, request.user.merchant_id, request.params.id);
        return success(result);
      } catch (err: any) {
        if (err.message === 'CAMPAIGN_NOT_FOUND') return reply.code(404).send(failure('CAMPAIGN_NOT_FOUND', 'Campaign not found'));
        throw err;
      }
    }
  });

  // TASK C3 - CONSUMER CLAIM ENDPOINT
  fastify.post('/:id/claim', {
    preHandler: [requireUserAuth],
    schema: {
      headers: z.object({ 'idempotency-key': z.string().uuid().optional() }).passthrough(),
      body: z.any().optional(),
      security: [{ bearerAuth: [] }]
    },
    handler: async (request: any, reply) => {
      const idempotencyKey = request.headers['idempotency-key'];
      const userId = request.user.sub;
      const campaignId = request.params.id;

      if (idempotencyKey) {
        const cached = getIdempotencyResult(idempotencyKey);
        if (cached) return success(cached);
      }

      const now = new Date();

      try {
        const result = await fastify.prisma.$transaction(async (tx) => {
          const campaign = await tx.campaign.findUnique({
            where: { id: campaignId },
            include: { merchant: true }
          });

          if (!campaign || campaign.status !== 'active' || 
             (campaign.start_at && campaign.start_at > now) ||
             (campaign.end_at && campaign.end_at < now)) {
            throw new Error('CAMPAIGN_NOT_ACTIVE');
          }

          if (campaign.merchant.status !== 'active') {
            throw new Error('MERCHANT_NOT_FOUND');
          }

          const existingToken = await tx.redemption.findFirst({
            where: {
              user_id: userId,
              redeemed: false,
              expires_at: { gt: now },
              campaign: { merchant_id: campaign.merchant_id }
            },
            include: {
              campaign: { select: { id: true, title: true, discount_percentage: true, terms: true } }
            }
          });

          if (existingToken) {
            return {
               token: existingToken.token,
               expires_at: existingToken.expires_at,
               campaign: existingToken.campaign,
               merchant: { business_name: campaign.merchant.business_name }
             };
          }

          const token = crypto.randomBytes(32).toString('base64url');
          const expires_at = nowPlusMinutes(campaign.redemption_time_limit_minutes);

          await tx.activation.create({
            data: {
              user_id: userId,
              campaign_id: campaignId,
              idempotency_key: idempotencyKey,
            }
          });

          await tx.redemption.create({
            data: {
              user_id: userId,
              campaign_id: campaignId,
              token,
              expires_at,
            }
          });

          await tx.event.createMany({
            data: [
              { event_name: 'CAMPAIGN_ACTIVATED', user_id: userId, merchant_id: campaign.merchant_id, campaign_id: campaignId },
              { event_name: 'TOKEN_ISSUED', user_id: userId, merchant_id: campaign.merchant_id, campaign_id: campaignId }
            ]
          });

          return {
             token,
             expires_at,
             campaign: { id: campaign.id, title: campaign.title, discount_percentage: campaign.discount_percentage, terms: campaign.terms },
             merchant: { business_name: campaign.merchant.business_name }
          };
        });

        if (idempotencyKey) {
          setIdempotencyResult(idempotencyKey, result, 20 * 60 * 1000);
        }

        return success(result);
      } catch (err: any) {
        if (err.message === 'CAMPAIGN_NOT_ACTIVE') return reply.code(400).send(failure('CAMPAIGN_NOT_ACTIVE', 'Campaign is not active'));
        if (err.message === 'MERCHANT_NOT_FOUND') return reply.code(404).send(failure('MERCHANT_NOT_FOUND', 'Merchant is not active'));
        if (err.code === 'P2002') return reply.code(409).send(failure('VALIDATION_ERROR', 'Idempotency key already used'));
        throw err;
      }
    }
  });

}
