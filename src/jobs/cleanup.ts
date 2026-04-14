import { PrismaClient } from '@prisma/client';
import { neon } from '@neondatabase/serverless';
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

/**
 * purgeCancelledMerchants
 *
 * Runs nightly. Finds merchants whose accounts have been fully cancelled
 * (account_blocked = true, cancelled_at IS NOT NULL) for 6+ months and
 * wipes their PII — same fields as the manual "Permanently Delete" path.
 *
 * Timer resets to NULL when a merchant reactivates, so reactivation before
 * 6 months fully prevents this job from touching the account.
 */
export async function purgeCancelledMerchants() {
  if (!process.env.DATABASE_URL) {
    console.warn('[PurgeCancelledMerchants] DATABASE_URL not set — skipping.');
    return;
  }

  const sql = neon(process.env.DATABASE_URL);

  // Find merchants cancelled 6+ months ago whose PII has not yet been wiped
  const merchants = await sql`
    SELECT m.id, m.business_name, mu.id as merchant_user_id
    FROM "Merchant" m
    LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
    WHERE m.account_blocked = true
      AND m.cancelled_at IS NOT NULL
      AND m.cancelled_at < NOW() - INTERVAL '6 months'
      AND m.business_name IS NOT NULL
  `;

  if (merchants.length === 0) {
    console.log('[PurgeCancelledMerchants] No accounts eligible for purge today.');
    return;
  }

  for (const merchant of merchants) {
    try {
      // Wipe MerchantUser PII
      if (merchant.merchant_user_id) {
        await sql`
          UPDATE "MerchantUser"
          SET email = ${'deleted_' + merchant.merchant_user_id + '@deleted.invalid'},
              password_hash = NULL
          WHERE id = ${merchant.merchant_user_id}
        `;
      }

      // Wipe Merchant PII
      await sql`
        UPDATE "Merchant"
        SET business_name = NULL,
            contact_name = NULL,
            phone = NULL,
            website = NULL,
            logo_url = NULL
        WHERE id = ${merchant.id}
      `;

      // Wipe MerchantLocation PII
      await sql`
        UPDATE "MerchantLocation"
        SET address = NULL,
            suite = NULL,
            city = NULL,
            state = NULL,
            postal_code = NULL
        WHERE merchant_id = ${merchant.id}
      `;

      console.log(`[PurgeCancelledMerchants] ✅ PII wiped for merchant ${merchant.id} (was: ${merchant.business_name})`);
    } catch (err: any) {
      console.error(`[PurgeCancelledMerchants] ❌ Failed to purge merchant ${merchant.id}:`, err.message);
    }
  }

  console.log(`[PurgeCancelledMerchants] Purge complete. ${merchants.length} account(s) processed.`);
}


/**
 * sendPaymentFailureReminders
 *
 * Runs nightly. For each merchant blocked due to an auto-upgrade payment failure,
 * sends escalating reminder emails at Day 3, Day 7, and Day 10.
 * Uses payment_failure_reminder_count to track which reminders have already been sent.
 * Stops automatically if the merchant reactivates (payment_failed_at is cleared to NULL).
 */
// eslint-disable-next-line @typescript-eslint/no-var-requires
const SibApiV3Sdk = require('sib-api-v3-sdk');

export async function sendPaymentFailureReminders() {
  if (!process.env.DATABASE_URL) {
    console.warn('[PaymentReminders] DATABASE_URL not set - skipping.');
    return;
  }
  const BREVO_KEY = process.env.BREVO_API_KEY;
  if (!BREVO_KEY) {
    console.warn('[PaymentReminders] BREVO_API_KEY not set - skipping.');
    return;
  }

  const sql = neon(process.env.DATABASE_URL);

  const merchants = await sql`
    SELECT m.id, m.business_name, m.payment_failed_at, m.payment_failure_reminder_count,
           EXTRACT(DAY FROM NOW() - m.payment_failed_at)::int AS days_since_failure,
           mu.email
    FROM "Merchant" m
    LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
    WHERE m.billing_status = 'payment_failed'
      AND m.account_blocked = true
      AND m.payment_failed_at IS NOT NULL
      AND m.payment_failure_reminder_count < 3
  `;

  if (merchants.length === 0) {
    console.log('[PaymentReminders] No merchants pending reminders today.');
    return;
  }

  const brevoClient = SibApiV3Sdk.ApiClient.instance;
  brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
  const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();

  for (const merchant of merchants) {
    if (!merchant.email) continue;

    const days = merchant.days_since_failure || 0;
    const count = merchant.payment_failure_reminder_count || 0;
    const bizName = merchant.business_name ? ` ${merchant.business_name}` : '';

    let newCount: number | null = null;
    let subject = '';
    let urgencyColor = '#f59e0b';
    let urgencyLabel = '';
    let urgencyMessage = '';

    if (days >= 3 && count === 0) {
      newCount = 1;
      subject = 'Reminder: Your Perkfinity Account Is Still Paused (Day 3)';
      urgencyLabel = '3-Day Reminder';
      urgencyColor = '#f59e0b';
      urgencyMessage = "It's been 3 days since your payment failed. Your account is still paused and members cannot redeem perks. Please update your payment method to restore access.";
    } else if (days >= 7 && count === 1) {
      newCount = 2;
      subject = 'Urgent: Your Perkfinity Account Has Been Paused for 7 Days';
      urgencyLabel = '7-Day Urgent Notice';
      urgencyColor = '#ef4444';
      urgencyMessage = 'Your account has been paused for 7 days. Your members are unable to redeem perks and your campaigns remain frozen. Please update your payment method immediately.';
    } else if (days >= 10 && count === 2) {
      newCount = 3;
      subject = 'Final Notice: Your Perkfinity Account - Payment Still Required (Day 10)';
      urgencyLabel = 'Final Notice - Day 10';
      urgencyColor = '#dc2626';
      urgencyMessage = 'This is your final reminder. Your Perkfinity account has been paused for 10 days due to a failed payment. If payment is not resolved, your account will remain permanently paused. Please act now.';
    }

    if (newCount === null) continue;

    try {
      const emailObj = new SibApiV3Sdk.SendSmtpEmail();
      emailObj.sender = { name: 'Perkfinity Support', email: 'support@perkfinity.net' };
      emailObj.to = [{ email: merchant.email }];
      emailObj.subject = subject;
      emailObj.htmlContent = `
        <div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;background:#ffffff;border-radius:16px;overflow:hidden;border:1px solid #eee;">
          <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;">
            <div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div>
          </div>
          <div style="padding:28px 24px;">
            <div style="font-size:20px;font-weight:700;color:${urgencyColor};margin-bottom:16px;">${urgencyLabel}</div>
            <p style="font-size:15px;color:#555;line-height:1.6;margin-bottom:20px;">
              Hi${bizName},<br><br>${urgencyMessage}
            </p>
            <div style="background:#fef2f2;border:1.5px solid #fecaca;border-radius:10px;padding:16px 20px;margin-bottom:20px;">
              <div style="font-size:13px;font-weight:700;color:#dc2626;margin-bottom:8px;">Account Status:</div>
              <ul style="margin:0;padding-left:18px;font-size:13px;color:#991b1b;line-height:2;">
                <li>Members cannot redeem perks by scanning your QR code</li>
                <li>All campaigns and promotions are frozen</li>
                <li>Your member data is fully preserved and ready to restore</li>
              </ul>
            </div>
            <div style="text-align:center;margin-bottom:24px;">
              <a href="https://perkfinity.net/dashboard.html" style="display:inline-block;background:#5b3fa5;color:#fff;font-weight:700;text-decoration:none;padding:14px 32px;border-radius:10px;font-size:15px;">Update Payment &amp; Restore Access</a>
            </div>
            <p style="font-size:13px;color:#aaa;text-align:center;">Questions? Reply to this email - we are happy to help.</p>
          </div>
        </div>
      `;

      await emailApi.sendTransacEmail(emailObj);

      await sql`
        UPDATE "Merchant"
        SET payment_failure_reminder_count = ${newCount},
            updated_at = NOW()
        WHERE id = ${merchant.id}
      `;

      console.log(`[PaymentReminders] Day-${days} reminder (count->${newCount}) sent to ${merchant.email} for merchant ${merchant.id}`);
    } catch (err: any) {
      console.error(`[PaymentReminders] Failed for merchant ${merchant.id}:`, err.message);
    }
  }

  console.log(`[PaymentReminders] Run complete. ${merchants.length} merchant(s) checked.`);
}
