const { neon } = require('@neondatabase/serverless');


const sql = neon(process.env.DATABASE_URL);

async function main() {
  const rs = await sql`
    SELECT id, user_id, campaign_id, status, redeemed, expires_at, issued_at 
    FROM "Redemption" 
    ORDER BY issued_at DESC 
    LIMIT 20
  `;
  console.log("Recent Redemptions:", JSON.stringify(rs, null, 2));
}

main().catch(console.error);
