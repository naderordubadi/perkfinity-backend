import { PrismaClient } from '@prisma/client';

export const TIER_LIMITS: Record<string, number> = {
  free: 1,
  growth: 5,
  premium: Infinity,
} as const;

export async function countActiveCampaigns(prisma: PrismaClient, merchantId: string): Promise<number> {
  return await prisma.campaign.count({
    where: {
      merchant_id: merchantId,
      status: 'active',
    },
  });
}

export function assertTierAllowsNewCampaign(tier: string, activeCount: number): void {
  const limit = TIER_LIMITS[tier] ?? TIER_LIMITS['free'];
  if (activeCount >= limit) {
    throw new Error('TIER_LIMIT_REACHED');
  }
}

export function getTierInfo(tier: string) {
  const t = tier.toLowerCase();
  const limit = TIER_LIMITS[t] ?? TIER_LIMITS['free'];
  return {
    name: t.charAt(0).toUpperCase() + t.slice(1),
    maxCampaigns: limit,
    features: [
      `${limit === Infinity ? 'Unlimited' : limit} Active Campaigns`,
      'Basic Analytics',
      'QR Code Support',
    ],
  };
}
