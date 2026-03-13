import { PrismaClient } from '@prisma/client';

export interface AuditData {
  actor_type: 'merchant_user' | 'admin' | 'system';
  actor_id?: string;
  merchant_id?: string;
  action: string;
  target_type?: string;
  target_id?: string;
  metadata?: any;
}

export async function logAudit(prisma: PrismaClient, data: AuditData) {
  return await prisma.auditLog.create({
    data: {
      actor_type: data.actor_type,
      actor_id: data.actor_id,
      merchant_id: data.merchant_id,
      action: data.action,
      target_type: data.target_type,
      target_id: data.target_id,
      metadata: data.metadata || {},
    },
  });
}
