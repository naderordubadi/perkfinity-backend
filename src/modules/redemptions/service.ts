import { PrismaClient } from '@prisma/client';
import { logAudit } from '../audit/service.js';

export class RedemptionService {
  constructor(private prisma: PrismaClient) {}

  async validateRedemption(merchant_user_id: string, merchant_id: string, token: string) {
    // 1. Single atomic UPDATE
    const result: any[] = await this.prisma.$queryRaw`
      UPDATE "Redemption" r
      SET
        redeemed = TRUE,
        redeemed_at = NOW(),
        redeemed_by_merchant_user_id = ${merchant_user_id}
      FROM "Campaign" c
      WHERE r.token = ${token}
        AND r.redeemed = FALSE
        AND r.expires_at > NOW()
        AND r.campaign_id = c.id
        AND c.merchant_id = ${merchant_id}
      RETURNING r.id, r.user_id, r.campaign_id, r.expires_at,
                r.redeemed_at, c.title, c.discount_percentage
    `;

    if (result.length > 0) {
      const redeemedToken = result[0];
      
      await logAudit(this.prisma, {
        actor_type: 'merchant_user',
        actor_id: merchant_user_id,
        merchant_id,
        action: 'REDEMPTION_VALIDATED',
        target_type: 'redemption',
        target_id: redeemedToken.id,
      });

      await this.prisma.event.create({
        data: {
          event_name: 'REDEMPTION_VALIDATED_SUCCESS',
          merchant_id,
          user_id: redeemedToken.user_id,
          campaign_id: redeemedToken.campaign_id,
        }
      });

      return redeemedToken;
    }

    // 2. If 0 rows returned, check why exactly
    const existing = await this.prisma.redemption.findUnique({
      where: { token },
      include: { campaign: true }
    });

    if (!existing) {
      await this.logFailureEvent(merchant_id, 'TOKEN_NOT_FOUND', { token });
      throw new Error('TOKEN_NOT_FOUND');
    }

    if (existing.campaign.merchant_id !== merchant_id) {
      await this.logFailureEvent(merchant_id, 'MERCHANT_MISMATCH', { token, actual_merchant: existing.campaign.merchant_id });
      throw new Error('MERCHANT_MISMATCH');
    }

    if (existing.redeemed) {
      await this.logFailureEvent(merchant_id, 'TOKEN_ALREADY_REDEEMED', { token });
      throw new Error('TOKEN_ALREADY_REDEEMED');
    }

    if (new Date() > existing.expires_at) {
      await this.logFailureEvent(merchant_id, 'TOKEN_EXPIRED', { token });
      throw new Error('TOKEN_EXPIRED');
    }

    // Fallback if none of the above matches somehow
    throw new Error('INTERNAL_ERROR');
  }

  private async logFailureEvent(merchant_id: string, reason: string, metadata: any) {
    await this.prisma.event.create({
      data: {
        event_name: 'REDEMPTION_VALIDATED_FAIL',
        merchant_id,
        metadata: { reason, ...metadata },
      }
    });
  }

  async listRedemptions(merchant_id: string, page: number = 1, limit: number = 20) {
    const offset = (page - 1) * limit;

    const [redemptions, total] = await Promise.all([
      this.prisma.redemption.findMany({
        where: { campaign: { merchant_id } },
        orderBy: { issued_at: 'desc' },
        skip: offset,
        take: limit,
        include: {
          campaign: { select: { title: true, discount_percentage: true } }
        }
      }),
      this.prisma.redemption.count({
        where: { campaign: { merchant_id } }
      })
    ]);

    return { redemptions, total, page, limit };
  }
}
