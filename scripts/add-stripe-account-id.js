require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');

async function fixDb() {
  const sql = neon(process.env.DATABASE_URL);
  try {
      await sql`ALTER TABLE "Contractor" ADD COLUMN "stripe_account_id" TEXT`;
      console.log('Added stripe_account_id');
  } catch (err) {
      console.error('Add column error:', err);
  }
}
fixDb();
