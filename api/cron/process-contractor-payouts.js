const { neon } = require('@neondatabase/serverless');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

module.exports = async (req, res) => {
  try {
    const sql = neon(process.env.DATABASE_URL);

    // Fetch all approved payouts where the contractor has completed Stripe onboarding
    // and has a valid stripe_account_id
    const payouts = await sql`
      SELECT p.*, c.stripe_account_id 
      FROM "ContractorPayout" p
      JOIN "Contractor" c ON c.id = p.contractor_id
      WHERE p.status = 'approved'
        AND c.stripe_onboarding_status = 'complete'
        AND c.stripe_account_id IS NOT NULL
    `;

    const results = { successful: 0, failed: 0, errors: [] };

    for (const p of payouts) {
      try {
        if (p.total_cents <= 0) {
          // If the balance is zero, just mark it paid without a Stripe transfer
          await markPaid(sql, p, 'zero_balance');
          results.successful++;
          continue;
        }

        // Execute Stripe Connect Transfer
        const transfer = await stripe.transfers.create({
          amount: p.total_cents,
          currency: 'usd',
          destination: p.stripe_account_id,
          description: `Perkfinity Payout (Period: ${new Date(p.period_start).toISOString().split('T')[0]} to ${new Date(p.period_end).toISOString().split('T')[0]})`,
          metadata: {
            payout_id: p.id,
            contractor_id: p.contractor_id
          }
        });

        // Mark as processing in DB (webhook will mark as paid later)
        await markProcessing(sql, p, transfer.id);
        results.successful++;
      } catch (err) {
        console.error(`Failed to process payout ${p.id}:`, err);
        results.failed++;
        results.errors.push({ payout_id: p.id, error: err.message });
      }
    }

    if (res) return res.status(200).json({ success: true, results });
    return results;

  } catch (globalErr) {
    console.error('Fatal error in process-contractor-payouts:', globalErr);
    if (res) return res.status(500).json({ success: false, error: globalErr.message });
  }
};

async function markProcessing(sql, p, reference) {
  await sql`
    UPDATE "ContractorPayout"
    SET status = 'processing',
        payment_method = 'stripe_connect',
        payment_reference = ${reference},
        updated_at = NOW()
    WHERE id = ${p.id}
  `;
}

async function markPaid(sql, p, reference) {
  const mpYear = new Date().getFullYear();
  const mpBonusCents = p.milestone_bonus_cents + p.retention_bonus_cents + p.special_bonus_cents;
  
  await sql`
    UPDATE "ContractorPayout"
    SET status = 'paid',
        payment_method = 'stripe_connect',
        payment_reference = ${reference},
        paid_at = NOW(),
        updated_at = NOW()
    WHERE id = ${p.id}
  `;
  
  // Maintain earnings summary
  await sql`
    INSERT INTO "ContractorEarningsSummary" (id, contractor_id, year, commission_ytd_cents, retainer_ytd_cents, bonus_ytd_cents, total_ytd_cents, updated_at)
    VALUES (gen_random_uuid()::text, ${p.contractor_id}, ${mpYear}, ${p.commission_cents}, ${p.retainer_cents}, ${mpBonusCents}, ${p.total_cents}, NOW())
    ON CONFLICT (contractor_id, year) DO UPDATE SET
      commission_ytd_cents = "ContractorEarningsSummary".commission_ytd_cents + ${p.commission_cents},
      retainer_ytd_cents   = "ContractorEarningsSummary".retainer_ytd_cents   + ${p.retainer_cents},
      bonus_ytd_cents      = "ContractorEarningsSummary".bonus_ytd_cents      + ${mpBonusCents},
      total_ytd_cents      = "ContractorEarningsSummary".total_ytd_cents      + ${p.total_cents},
      updated_at           = NOW()
  `;
}
