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

    // ── 5. 6-Month Auto-Deletion: full PII wipe for long-cancelled merchants ─
    // Merchants cancelled for 6+ months who never reactivated are permanently
    // deleted: Stripe customer removed, all PII wiped from DB, campaigns expired.
    const Stripe = require('stripe');
    const stripeClient = process.env.STRIPE_SECRET_KEY ? Stripe(process.env.STRIPE_SECRET_KEY) : null;

    const stalemerchants = await sql`
      SELECT m.id, m.business_name, m.cancelled_at, m.stripe_customer_id,
             mu.id AS user_id
      FROM "Merchant" m
      LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
      WHERE m.billing_status = 'cancelled'
        AND m.account_blocked = true
        AND m.cancelled_at IS NOT NULL
        AND m.cancelled_at < NOW() - INTERVAL '6 months'
        AND m.business_name != '[Deleted]'
    `;

    let autoDeleted = 0;
    if (stalemerchants.length > 0) {
      console.log(`🗑️ Auto-deletion: found ${stalemerchants.length} merchant(s) cancelled 6+ months ago — wiping PII.`);

      for (const staleMerchant of stalemerchants) {
        // ── Step 1: Delete Stripe customer (removes saved card permanently) ──
        if (staleMerchant.stripe_customer_id && stripeClient) {
          try {
            await stripeClient.customers.delete(staleMerchant.stripe_customer_id);
            console.log(`💳 Stripe customer deleted: ${staleMerchant.stripe_customer_id}`);
          } catch (stripeErr) {
            // Non-fatal: Stripe customer may already be deleted, continue
            console.error(`[Auto-Delete] Stripe cleanup failed for ${staleMerchant.id}:`, stripeErr.message);
          }
        }

        // ── Step 2: Wipe MerchantUser PII (email + password) ──
        if (staleMerchant.user_id) {
          const deletedEmail = 'deleted_' + staleMerchant.user_id + '@deleted.invalid';
          await sql`
            UPDATE "MerchantUser"
            SET email = ${deletedEmail}, password_hash = 'DELETED'
            WHERE id = ${staleMerchant.user_id}
          `;
        }

        // ── Step 3: Wipe Merchant PII ──
        await sql`
          UPDATE "Merchant"
          SET business_name  = '[Deleted]',
              contact_name   = NULL,
              phone          = NULL,
              website        = NULL,
              logo_url       = NULL,
              status         = 'cancelled',
              billing_status = 'deleted',
              account_blocked = true,
              updated_at     = NOW()
          WHERE id = ${staleMerchant.id}
        `;

        // ── Step 4: Wipe MerchantLocation PII ──
        await sql`
          UPDATE "MerchantLocation"
          SET address = NULL, suite = NULL, city = NULL, state = NULL, postal_code = NULL
          WHERE merchant_id = ${staleMerchant.id}
        `;

        // ── Step 5: Expire active campaigns ──
        const expiredAutoC = await sql`
          UPDATE "Campaign" SET status = 'expired', updated_at = NOW()
          WHERE merchant_id = ${staleMerchant.id} AND status = 'active'
          RETURNING id
        `;

        // ── Step 6: Expire pending redemptions ──
        if (expiredAutoC.length > 0) {
          const cids = expiredAutoC.map(c => c.id);
          await sql`
            UPDATE "Redemption" SET status = 'expired'
            WHERE campaign_id = ANY(${cids})
              AND status = 'created' AND redeemed = false
          `;
        }

        console.log(`🗑️ Auto-deleted: ${staleMerchant.business_name || staleMerchant.id} (cancelled ${staleMerchant.cancelled_at})`);
        autoDeleted++;
      }
    }

    // ── 6. 6-Month Cleanup: declined Online/Hybrid applications ──────
    // Applicants declined 6+ months ago have their PII fully wiped per privacy policy.
    // Stripe customer was already removed at decline time, so no Stripe step needed here.
    const declinedApps = await sql`
      SELECT m.id, m.business_name, m.declined_at,
             mu.id AS user_id
      FROM "Merchant" m
      LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
      WHERE m.application_status = 'declined'
        AND m.declined_at IS NOT NULL
        AND m.declined_at < NOW() - INTERVAL '6 months'
        AND m.business_name != '[Deleted]'
    `;

    let declinedWiped = 0;
    if (declinedApps.length > 0) {
      console.log(`🗑️ Declined-app cleanup: found ${declinedApps.length} application(s) declined 6+ months ago — wiping PII.`);

      for (const app of declinedApps) {
        // Wipe MerchantUser PII
        if (app.user_id) {
          const deletedEmail = 'deleted_' + app.user_id + '@deleted.invalid';
          await sql`
            UPDATE "MerchantUser"
            SET email = ${deletedEmail}, password_hash = 'DELETED'
            WHERE id = ${app.user_id}
          `;
        }

        // Wipe Merchant PII
        await sql`
          UPDATE "Merchant"
          SET business_name   = '[Deleted]',
              contact_name    = NULL,
              phone           = NULL,
              website         = NULL,
              logo_url        = NULL,
              application_status = 'deleted',
              billing_status  = 'deleted',
              updated_at      = NOW()
          WHERE id = ${app.id}
        `;

        // Wipe MerchantLocation PII
        await sql`
          UPDATE "MerchantLocation"
          SET address = NULL, suite = NULL, city = NULL, state = NULL, postal_code = NULL
          WHERE merchant_id = ${app.id}
        `;

        // Expire any lingering campaigns
        const expiredDecC = await sql`
          UPDATE "Campaign" SET status = 'expired', updated_at = NOW()
          WHERE merchant_id = ${app.id} AND status = 'active'
          RETURNING id
        `;
        if (expiredDecC.length > 0) {
          const cids = expiredDecC.map(c => c.id);
          await sql`
            UPDATE "Redemption" SET status = 'expired'
            WHERE campaign_id = ANY(${cids}) AND status = 'created' AND redeemed = false
          `;
        }

        console.log(`🗑️ Declined-app wiped: ${app.business_name || app.id} (declined ${app.declined_at})`);
        declinedWiped++;
      }
    }

    return res.status(200).json({
      success: true,
      message: `Enforced ${blockedMerchants.length} cancellation(s). ${totalCampaignsExpired} campaign(s) and ${totalRedemptionsExpired} redemption(s) expired. ${autoDeleted} merchant(s) auto-deleted (6mo). ${declinedWiped} declined application(s) wiped (6mo).`,
      stats: { ...summary, auto_deleted: autoDeleted, declined_wiped: declinedWiped },
    });

  } catch (err) {
    console.error('Cancellation enforcement cron error:', err);
    return res.status(500).json({ success: false, error: err.message || 'Internal server error' });
  }
};
