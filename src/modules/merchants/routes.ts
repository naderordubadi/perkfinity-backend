import { FastifyInstance } from 'fastify';
import { SignupMerchantSchema } from './schemas.js';
import { AuthService } from '../auth/service.js';
import { success, failure } from '../../utils/response.js';

export default async function merchantRoutes(fastify: FastifyInstance) {
  const authService = new AuthService(fastify.prisma, fastify.jwt);

  fastify.post('/signup', {
    schema: { body: SignupMerchantSchema },
    handler: async (request, reply) => {
      const data = request.body as any;

      try {
        // 1. Create merchant and owner user via existing service
        const authResult = await authService.registerMerchant({
          business_name: data.name,
          email: data.email,
          password: data.password
        });

        const merchantId = authResult.merchant.id;

        // 2. Create the location
        await fastify.prisma.merchantLocation.create({
          data: {
            merchant_id: merchantId,
            location_name: "Main Location",
            address: data.address,
            city: data.city,
            postal_code: data.zip,
            country: "US"
          }
        });

        // 3. Create the initial perk campaign
        await fastify.prisma.campaign.create({
          data: {
            merchant_id: merchantId,
            title: data.perk || 'Welcome Perk',
            discount_percentage: 10,
            status: 'active',
            start_at: new Date(),
            end_at: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000),
          }
        });

        // 4. Update the merchant's tier and optional fields
        await fastify.prisma.merchant.update({
          where: { id: merchantId },
          data: { 
            subscription_tier: data.tier 
          }
        });

        return success(authResult);
      } catch (err: any) {
        if (err.code === 'P2002') {
          return reply.code(400).send(failure('VALIDATION_ERROR', 'A merchant with this email already exists.'));
        }
        request.log.error(err);
        return reply.code(500).send(failure('INTERNAL_ERROR', 'Failed to sign up merchant'));
      }
    }
  });
}
