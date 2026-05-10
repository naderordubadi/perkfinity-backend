const jwt = require('jsonwebtoken');
const { neon } = require('@neondatabase/serverless');

const sql = neon(process.env.DATABASE_URL);

async function run() {
  const users = await sql`SELECT id FROM "User" WHERE email = 'ryan.mission.viejo2@gmail.com'`;
  if (users.length === 0) return console.log("User not found");
  
  const token = jwt.sign({ userId: users[0].id, role: 'consumer' }, process.env.JWT_SECRET, { expiresIn: '8h' });
  
  const qrCodes = await sql`SELECT public_code FROM "QrCode" WHERE merchant_id = 'f76634b8-c183-4dd6-a141-77ea98108708' AND status = 'active' LIMIT 1`;
  if (qrCodes.length === 0) return console.log("MV Bakery QR not found");
  
  const publicCode = qrCodes[0].public_code;
  
  console.log(`Fetching from Vercel for code: ${publicCode}`);
  const res = await fetch(`https://perkfinity-backend.vercel.app/api/v1/qr/resolve/${publicCode}`, {
    headers: { "Authorization": `Bearer ${token}` }
  });
  
  const data = await res.json();
  console.log("Vercel returned campaigns:", JSON.stringify(data.data?.campaigns, null, 2));
}

run().catch(console.error);
