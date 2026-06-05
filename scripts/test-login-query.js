require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');

async function testQuery() {
  const sql = neon(process.env.DATABASE_URL);
  const email = 'naderordubadi@yahoo.com';
  try {
      const result = await sql`
        SELECT id, full_name, email, referral_code, status, ica_status, stripe_onboarding_status, stripe_account_id, password_hash
        FROM "Contractor"
        WHERE email = ${email.toLowerCase().trim()}
        LIMIT 1
      `;
      console.log('Query result:', result);
  } catch (err) {
      console.error('Query error:', err);
  }
}
testQuery();
