const bcrypt = require('bcryptjs');
const { neon } = require('@neondatabase/serverless');
require('dotenv').config({ path: __dirname + '/../.env' });

async function run() {
  const hash = await bcrypt.hash('Perks@2026', 10);
  const sql = neon(process.env.DATABASE_URL);
  await sql`UPDATE "Contractor" SET password_hash = ${hash} WHERE email = 'testrep@perkfinity.net'`;
  console.log('✅ Password successfully set to Perks@2026 for testrep@perkfinity.net');
  process.exit(0);
}
run();
