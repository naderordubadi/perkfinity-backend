require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');

async function main() {
  const sql = neon(process.env.DATABASE_URL);
  
  // Check breakdown
  const payouts = await sql`SELECT breakdown FROM "ContractorPayout" ORDER BY created_at DESC LIMIT 1`;
  if (payouts.length > 0) {
    console.log('--- Last Payout Breakdown ---');
    console.log(JSON.stringify(payouts[0].breakdown, null, 2));
  } else {
    console.log('No payouts found.');
  }
  
  // Update stripe_onboarding_status
  const updated = await sql`UPDATE "Contractor" SET stripe_onboarding_status = 'complete' WHERE stripe_onboarding_status != 'complete' RETURNING email, full_name`;
  console.log('--- Updated Contractors ---');
  console.log(updated);
}

main().catch(console.error);
