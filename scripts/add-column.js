require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');

async function fixDb() {
  const sql = neon(process.env.DATABASE_URL);
  try {
      await sql`ALTER TABLE "Contractor" ADD COLUMN "stripe_onboarding_status" TEXT NOT NULL DEFAULT 'pending'`;
      console.log('Added stripe_onboarding_status');
  } catch (err) {
      console.error('Add column error:', err);
  }
}
fixDb();
