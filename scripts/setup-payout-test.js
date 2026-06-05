const { neon } = require('@neondatabase/serverless');
require('dotenv').config({ path: __dirname + '/../.env' });

const sql = neon(process.env.DATABASE_URL);

async function run() {
  console.log('--- Setting up Local Stripe & Payout Test ---');

  // 1. Create a fake rep
  const [rep] = await sql`
    INSERT INTO "Contractor" (
      full_name, legal_name, email, referral_code, status, ica_status, stripe_onboarding_status, stripe_account_id
    ) VALUES (
      'Test Rep Stripe', 'Test Rep Stripe LLC', 'testrep@perkfinity.net', 'REF-TESTSTRIPE', 'active', 'signed', 'complete', 'acct_12345'
    )
    ON CONFLICT (email) DO UPDATE SET status = 'active', stripe_onboarding_status = 'complete'
    RETURNING id, full_name, email;
  `;
  console.log('✅ Created/Updated Test Rep:', rep.full_name);

  // 2. Set 25% compensation rule
  await sql`
    INSERT INTO "ContractorCompensationRule" (contractor_id, commission_rate, commission_duration_months)
    VALUES (${rep.id}, 0.25, 12)
    ON CONFLICT (contractor_id) DO UPDATE SET commission_rate = 0.25;
  `;

  // 3. Get an existing merchant
  const [merchant] = await sql`
    SELECT id, business_name FROM "Merchant"
    LIMIT 1
  `;
  if (!merchant) {
    console.error('No merchant found in the database. Please create one first.');
    process.exit(1);
  }
  console.log('✅ Found Test Merchant:', merchant.business_name);

  // 4. Attribute merchant to rep
  await sql`
    INSERT INTO "ContractorMerchantAttribution" (contractor_id, merchant_id, commission_start_date)
    VALUES (${rep.id}, ${merchant.id}, '2026-04-10')
    ON CONFLICT (merchant_id) DO UPDATE SET contractor_id = ${rep.id}, commission_start_date = '2026-04-10';
  `;
  console.log('✅ Attributed Merchant to Rep and backdated commission start to April 10th.');

  // 5. Inject a backdated Invoice (Simulating a successful Stripe Webhook from April)
  await sql`
    INSERT INTO "Invoice" (merchant_id, stripe_invoice_id, amount_cents, currency, status, paid_at, period_start, period_end)
    VALUES (${merchant.id}, 'in_test123', 29900, 'usd', 'paid', '2026-04-15 10:00:00+00', '2026-04-15 10:00:00+00', '2026-05-15 10:00:00+00')
    ON CONFLICT (stripe_invoice_id) DO UPDATE SET paid_at = '2026-04-15 10:00:00+00';
  `;
  console.log('✅ Injected backdated invoice for $299.00 (paid April 15th).');

  console.log('\n--- Test Setup Complete ---');
  console.log('To run the Net-45 Payout Cron Job, run:');
  console.log('node backend/api/cron/generate-contractor-payouts.js --date="2026-06-01"');
  process.exit(0);
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
