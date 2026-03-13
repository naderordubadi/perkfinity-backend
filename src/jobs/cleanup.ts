import { PrismaClient } from '@prisma/client';
import { logAudit } from '../modules/audit/service.js';

export async function cleanupExpiredRedemptions(prisma: PrismaClient) {
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  const result = await prisma.redemption.deleteMany({
    where: {
      expires_at: { lt: thirtyDaysAgo }
    }
  });

  console.log(`🧹 Cleaned up ${result.count} expired redemptions older than 30 days.`);
}

export async function detectSuspiciousActivity(prisma: PrismaClient) {
  const oneHourAgo = new Date();
  oneHourAgo.setHours(oneHourAgo.getHours() - 1);

  // 1. Alert if > 50 failed validations in 1 hour for same merchant
  const failedValidations = await prisma.event.groupBy({
    by: ['merchant_id'],
    where: {
      event_name: 'REDEMPTION_VALIDATED_FAIL',
      created_at: { gte: oneHourAgo }
    },
    _count: { _all: true },
    having: {
      merchant_id: {
        _count: { gt: 50 }
      }
    }
  });

  for (const group of failedValidations) {
    if (group.merchant_id) {
       await logAudit(prisma, {
         actor_type: 'system',
         merchant_id: group.merchant_id,
         action: 'SUSPICIOUS_ACTIVITY_DETECTED',
         metadata: { reason: 'High failed validation rate', count: group._count._all }
       });
       console.warn(`🚨 Suspicious Activity: Merchant ${group.merchant_id} had ${group._count._all} failed validations in the last hour.`);
    }
  }

  // 2. Alert if > 20 activations from same user in 1 hour
  const highActivations = await prisma.event.groupBy({
    by: ['user_id'],
    where: {
      event_name: 'CAMPAIGN_ACTIVATED',
      created_at: { gte: oneHourAgo }
    },
    _count: { _all: true },
    having: {
      user_id: {
        _count: { gt: 20 }
      }
    }
  });

  for (const group of highActivations) {
    if (group.user_id) {
       await logAudit(prisma, {
         actor_type: 'system',
         action: 'SUSPICIOUS_ACTIVITY_DETECTED',
         metadata: { reason: 'High user activation rate', user_id: group.user_id, count: group._count._all }
       });
       console.warn(`🚨 Suspicious Activity: User ${group.user_id} activated ${group._count._all} times in the last hour.`);
    }
  }
}
