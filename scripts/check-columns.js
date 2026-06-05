require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');

async function testQuery() {
  const sql = neon(process.env.DATABASE_URL);
  try {
      const result = await sql`SELECT column_name FROM information_schema.columns WHERE table_name = 'Contractor'`;
      console.log('Columns:', result.map(r => r.column_name).join(', '));
  } catch (err) {
      console.error('Query error:', err);
  }
}
testQuery();
