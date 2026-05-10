const jwt = require('jsonwebtoken');
const { neon } = require('@neondatabase/serverless');

const sql = neon(process.env.DATABASE_URL);

async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

async function run() {
  console.log("Waiting 10 seconds for Vercel deployment to finish...");
  await wait(10000); 

  const email = `test.mvbakery.${Date.now()}@perkfinity.internal`;
  
  const qrCodes = await sql`SELECT public_code FROM "QrCode" WHERE merchant_id = 'f76634b8-c183-4dd6-a141-77ea98108708' AND status = 'active' LIMIT 1`;
  const qrCode = qrCodes[0].public_code;
  
  console.log(`Signing up with email: ${email} and QR: ${qrCode}...`);
  
  const res = await fetch("https://perkfinity-backend.vercel.app/api/v1/consumers/signup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password: "Password123", qrCode })
  });
  
  const data = await res.json();
  if (!data.success) {
    return console.error("Signup failed:", data);
  }
  
  const userId = data.data.user.id;
  console.log(`Success! User ID: ${userId}. Verifying DB entries...`);
  
  await wait(2000); // Wait for the async SQL inserts to finish in neon

  const members = await sql`SELECT * FROM "MerchantMember" WHERE user_id = ${userId}`;
  const redemptions = await sql`SELECT status, campaign_id FROM "Redemption" WHERE user_id = ${userId}`;
  
  console.log(`Memberships found: ${members.length}`);
  console.log(`Redemptions found: ${redemptions.length}`);
  if (members.length > 0 && redemptions.length > 0) {
    console.log("🔥 FIX IS WORKING PERFECTLY! 🔥");
  } else {
    console.log("❌ Something is still wrong. Redemptions were not created.");
  }
}

run().catch(console.error);
