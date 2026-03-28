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

    const summary = {
      campaigns_expired: expiredCampaigns.length,
      redemptions_expired: expiredRedemptions.length,
      campaign_details: expiredCampaigns.map(c => ({
        id: c.id,
        title: c.title,
        end_at: c.end_at
      }))
    };

    console.log(`🐛💥 Expire cron: ${expiredCampaigns.length} campaign(s), ${expiredRedemptions.length} redemption(s) expired.`);

    return res.status(200).json({
      success: true,
      message: `Expired ${expiredCampaigns.length} campaign(s) and ${expiredRedemptions.length} redemption(s)`,
      stats: summary
    });
  } catch (err) {
    console.error('Campaign expiration cron error:', err);
    return res.status(500).json({ success: false, error: err.message || 'Internal server error' });
  }
};
