require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');

async function test() {
  const sql = neon(process.env.DATABASE_URL);
  
  // Find POUF Magic merchant
  const merchants = await sql`SELECT id, business_name FROM "Merchant" WHERE business_name = 'POUF Magic'`;
  if (merchants.length === 0) {
    console.log("No merchant found");
    return;
  }
  
  const m = merchants[0];
  console.log("Merchant:", m);
  
  const invoices = await sql`SELECT id, amount_cents, status, paid_at FROM "Invoice" WHERE merchant_id = ${m.id}`;
  console.log("Invoices:", invoices);
  
}

test().catch(console.error);
