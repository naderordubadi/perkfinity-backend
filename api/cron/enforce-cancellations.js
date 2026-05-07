/**
 * Perkfinity Cancellation Enforcement Cron
 * Runs nightly at 9:00 AM UTC (1:00 AM PST) via Vercel Cron.
 *
 * PURPOSE: Belt-and-suspenders fallback for when Stripe's
 * `customer.subscription.deleted` webhook fails to deliver.
 *
 * If a merchant has billing_status = 'pending_cancellation' and their
 * next_billing_date has passed, their subscription period is over.
 * Stripe already cancelled them — we just never received the webhook.
 * This cron enforces the block so they can't stay active indefinitely.
 *
 * Actions taken per affected merchant:
 *   1. billing_status  → 'cancelled'
 *   2. account_blocked → true
 *   3. cancelled_at    → NOW()
 *   4. stripe_subscription_id → NULL (period is over, sub is gone)
 *   5. Active campaigns → 'expired'
 *   6. Pending redemptions → 'expired'
 */

const { neon } = require('@neondatabase/serverless');

module.exports = async (req, res) => {
  // ── Security: only Vercel Cron or manual trigger with secret ──
  const cronSecret = process.env.CRON_SECRET;
  const authHeader = req.headers.authorization;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }

  const DATABASE_URL = process.env.DATABASE_URL;
  if (!DATABASE_URL) {
    return res.status(500).json({ success: false, error: 'DATABASE_URL not configured' });
  }
  const sql = neon(DATABASE_URL);

  try {
    // ── 1. Find all merchants past their cancellation deadline ────
    // Criteria:
    //   - billing_status = 'pending_cancellation'  (merchant has cancelled)
    //   - next_billing_date IS NOT NULL             (we have a known end date)
    //   - next_billing_date < NOW()                 (that date has passed)
    //   - account_blocked = false                   (not yet enforced)
    const overduemerchants = await sql`
      SELECT id, business_name, next_billing_date, stripe_subscription_id
      FROM "Merchant"
      WHERE billing_status = 'pending_cancellation'
        AND next_billing_date IS NOT NULL
        AND next_billing_date < NOW()
        AND account_blocked = false
    `;

    if (overduemerchants.length === 0) {
      console.log('✅ Cancellation enforcer: no overdue pending-cancellation merchants found.');
      return res.status(200).json({
        success: true,
        message: 'No overdue cancellations to enforce.',
        stats: { merchants_blocked: 0, campaigns_expired: 0, redemptions_expired: 0 }
      });
    }

    console.log(`⚠️ Cancellation enforcer: found ${overduemerchants.length} overdue merchant(s) — enforcing block now.`);

    let totalCampaignsExpired = 0;
    let totalRedemptionsExpired = 0;
    const blockedMerchants = [];

    for (const merchant of overduemerchants) {
      // ── 2. Block the merchant ─────────────────────────────────
      await sql`
        UPDATE "Merchant"
        SET billing_status       = 'cancelled',
            account_blocked      = true,
            cancelled_at         = NOW(),
            stripe_subscription_id = NULL,
            updated_at           = NOW()
        WHERE id = ${merchant.id}
      `;

      // ── 3. Expire their active campaigns ──────────────────────
      const expiredCampaigns = await sql`
        UPDATE "Campaign"
        SET status = 'expired', updated_at = NOW()
        WHERE merchant_id = ${merchant.id}
          AND status = 'active'
        RETURNING id
      `;

      // ── 4. Expire pending redemptions ─────────────────────────
      let expiredRedemptions = { length: 0 };
      if (expiredCampaigns.length > 0) {
        const campaignIds = expiredCampaigns.map(c => c.id);
        expiredRedemptions = await sql`
          UPDATE "Redemption"
          SET status = 'expired'
          WHERE campaign_id = ANY(${campaignIds})
            AND status = 'created'
            AND redeemed = false
          RETURNING id
        `;
      }

      totalCampaignsExpired += expiredCampaigns.length;
      totalRedemptionsExpired += expiredRedemptions.length;
      blockedMerchants.push({
        id: merchant.id,
        name: merchant.business_name || '[Unnamed]',
        billing_ended: merchant.next_billing_date,
        campaigns_expired: expiredCampaigns.length,
        redemptions_expired: expiredRedemptions.length,
      });

      console.log(
        `🚫 Blocked: ${merchant.business_name || merchant.id} ` +
        `(billing ended ${merchant.next_billing_date}) — ` +
        `${expiredCampaigns.length} campaign(s), ${expiredRedemptions.length} redemption(s) expired.`
      );
    }

    const summary = {
      merchants_blocked: blockedMerchants.length,
      campaigns_expired: totalCampaignsExpired,
      redemptions_expired: totalRedemptionsExpired,
      details: blockedMerchants,
    };

    console.log(
      `✅ Cancellation enforcer done: ${blockedMerchants.length} merchant(s) blocked, ` +
      `${totalCampaignsExpired} campaign(s) expired, ${totalRedemptionsExpired} redemption(s) expired.`
    );

    return res.status(200).json({
      success: true,
      message: `Enforced ${blockedMerchants.length} cancellation(s). ${totalCampaignsExpired} campaign(s) and ${totalRedemptionsExpired} redemption(s) expired.`,
      stats: summary,
    });

  } catch (err) {
    console.error('Cancellation enforcement cron error:', err);
    return res.status(500).json({ success: false, error: err.message || 'Internal server error' });
  }
};
