const { neon } = require('@neondatabase/serverless');
require('dotenv').config({ path: '.env' });

async function getQr() {
  const sql = neon(process.env.DATABASE_URL);
  
  try {
    const data = await sql`SELECT public_code FROM "QrCode" WHERE status = 'active' LIMIT 1`;
    if (data.length > 0) {
      console.log("QR:", data[0].public_code);
    } else {
      console.log("No active QR codes found.");
    }
  } catch (err) {
    console.error("Failed:", err);
  }
}
getQr();
