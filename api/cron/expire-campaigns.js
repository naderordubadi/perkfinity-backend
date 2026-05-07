/**
 * Perkfinity Campaign Expiration Cron
 * Runs nightly at 8:00 AM UTC (12:00 AM PST / midnight) via Vercel Cron.
 * Sets status = 'expired' on all campaigns where end_at has passed.
 * Also marks corresponding unredeemed Redemptions as 'expired'.
 *
 * 🐛💥 Catch a bug, Crush a bug!
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
    // ── 1. Expire campaigns where end_at has passed ──────────────
    const expiredCampaigns = await sql`
      UPDATE "Campaign"
      SET status = 'expired', updated_at = NOW()
      WHERE status = 'active'
        AND end_at IS NOT NULL
        AND end_at < NOW()
      RETURNING id, title, end_at
    `;

    // ── 2. Expire unredeemed redemptions for those campaigns ─────
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

    // ── 3. Belt-and-suspenders: expire any active campaigns for deleted merchants ──
    // Catches cases where the delete endpoint failed mid-way, or merchants were
    // deleted before campaign expiration was added to the delete flow.
    const orphanedCampaigns = await sql`
      UPDATE "Campaign"
      SET status = 'expired', updated_at = NOW()
      WHERE status = 'active'
        AND merchant_id IN (
          SELECT id FROM "Merchant" WHERE billing_status = 'deleted'
        )
      RETURNING id
    `;
    if (orphanedCampaigns.length > 0) {
      await sql`
        UPDATE "Redemption"
        SET status = 'expired'
        WHERE campaign_id = ANY(${orphanedCampaigns.map(c => c.id)})
          AND status = 'created'
          AND redeemed = false
      `;
      console.log(`🧹 Orphan cleanup (deleted): ${orphanedCampaigns.length} active campaign(s) expired for deleted merchants.`);
    }

    // ── 4. Belt-and-suspenders: expire any active campaigns for blocked/cancelled merchants ──
    // Catches cases where account_blocked was set via direct DB update, admin override,
    // or any code path that didn't properly expire campaigns at the time.
    const blockedOrphanedCampaigns = await sql`
      UPDATE "Campaign"
      SET status = 'expired', updated_at = NOW()
      WHERE status = 'active'
        AND merchant_id IN (
          SELECT id FROM "Merchant"
          WHERE account_blocked = true
            OR billing_status IN ('cancelled')
        )
      RETURNING id
    `;
    if (blockedOrphanedCampaigns.length > 0) {
      await sql`
        UPDATE "Redemption"
        SET status = 'expired'
        WHERE campaign_id = ANY(${blockedOrphanedCampaigns.map(c => c.id)})
          AND status = 'created'
          AND redeemed = false
      `;
      console.log(`🧹 Orphan cleanup (blocked/cancelled): ${blockedOrphanedCampaigns.length} active campaign(s) expired for blocked merchants.`);
    }

    const summary = {
      campaigns_expired: expiredCampaigns.length,
      orphaned_campaigns_expired: orphanedCampaigns.length,
      blocked_campaigns_expired: blockedOrphanedCampaigns.length,
      redemptions_expired: expiredRedemptions.length,
      campaign_details: expiredCampaigns.map(c => ({
        id: c.id,
        title: c.title,
        end_at: c.end_at
      }))
    };

    console.log(`🐛💥 Expire cron: ${expiredCampaigns.length} campaign(s), ${expiredRedemptions.length} redemption(s) expired. Orphans: ${orphanedCampaigns.length + blockedOrphanedCampaigns.length}.`);

    return res.status(200).json({
      success: true,
      message: `Expired ${expiredCampaigns.length} campaign(s) and ${expiredRedemptions.length} redemption(s). Orphaned: ${orphanedCampaigns.length + blockedOrphanedCampaigns.length}.`,
      stats: summary
    });
  } catch (err) {
    console.error('Campaign expiration cron error:', err);
    return res.status(500).json({ success: false, error: err.message || 'Internal server error' });
  }
};
