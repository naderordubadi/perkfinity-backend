/**
 * Perkfinity Ghost User Cleanup Cron
 * Runs nightly via Vercel Cron.
 * Deletes any consumer accounts that were abandoned during signup
 * (identified by having no city or zip code).
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
    // For local testing: we are removing the 48-hour buffer temporarily.
    // In production, we will add: AND created_at < NOW() - INTERVAL '48 hours'
    
    // First, find the ghost users created more than 48 hours ago
    const ghosts = await sql`
      SELECT id FROM "User"
      WHERE city IS NULL
        AND zip_code IS NULL
        AND created_at < NOW() - INTERVAL '48 hours'
    `;

    let deletedGhosts = { length: 0 };
    if (ghosts.length > 0) {
      const ghostIds = ghosts.map(u => u.id);

      // Cascade delete dependent records
      await sql`DELETE FROM "Redemption" WHERE user_id = ANY(${ghostIds})`;
      await sql`DELETE FROM "Activation" WHERE user_id = ANY(${ghostIds})`;
      await sql`DELETE FROM "Event" WHERE user_id = ANY(${ghostIds})`;
      await sql`DELETE FROM "MerchantMember" WHERE user_id = ANY(${ghostIds})`;

      // Delete the users
      deletedGhosts = await sql`
        DELETE FROM "User"
        WHERE id = ANY(${ghostIds})
        RETURNING id, email, created_at
      `;
    }

    console.log(`🧹 Ghost cleanup: ${deletedGhosts.length} abandoned consumer accounts deleted.`);

    return res.status(200).json({
      success: true,
      message: `Deleted ${deletedGhosts.length} ghost account(s).`,
      deleted_accounts: deletedGhosts.map(u => ({ id: u.id, email: u.email, created_at: u.created_at }))
    });
  } catch (err) {
    console.error('Ghost user cleanup cron error:', err);
    return res.status(500).json({ success: false, error: err.message || 'Internal server error' });
  }
};
