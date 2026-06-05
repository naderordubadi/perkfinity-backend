require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');
const sql = neon(process.env.DATABASE_URL);

async function run() {
  await sql`UPDATE "Contractor" SET stripe_onboarding_status = 'complete' WHERE stripe_account_id = 'acct_12345'`;
  console.log('Fixed Stripe Status');
  process.exit(0);
}
run();
