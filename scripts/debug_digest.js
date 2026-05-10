const { neon } = require('@neondatabase/serverless');

const sql = neon(process.env.DATABASE_URL);

async function run() {
  const email = 'naderordubadi@gmail.com';
  
  const users = await sql`SELECT id FROM "User" WHERE email = ${email}`;
  if (users.length === 0) return console.log("User not found: ", email);
  const userId = users[0].id;
  console.log("User ID:", userId);

  // Get memberships
  const memberships = await sql`
    SELECT m.merchant_id, merch.business_name 
    FROM "Member" m
    JOIN "Merchant" merch ON m.merchant_id = merch.id
    WHERE m.user_id = ${userId}
  `;
  console.log("\n--- Member of ---");
  memberships.forEach(m => console.log(`- ${m.business_name}: ${m.merchant_id}`));

  // Get active campaigns for those merchants
  const merchantIds = memberships.map(m => m.merchant_id);
  let activeCampaigns = [];
  if (merchantIds.length > 0) {
    activeCampaigns = await sql`
      SELECT c.id, c.merchant_id, c.title, merch.business_name 
      FROM "Campaign" c
      JOIN "Merchant" merch ON c.merchant_id = merch.id
      WHERE c.merchant_id = ANY(${merchantIds}) AND c.status = 'active'
    `;
    console.log("\n--- Active Campaigns ---");
    activeCampaigns.forEach(c => console.log(`- ${c.business_name}: [${c.title}]`));
  }

  // Get NotificationQueue entries
  const queue = await sql`
    SELECT q.id, q.campaign_id, q.channel, q.status, q.created_at, q.updated_at, merch.business_name, c.title 
    FROM "NotificationQueue" q
    JOIN "Campaign" c ON q.campaign_id = c.id
    JOIN "Merchant" merch ON c.merchant_id = merch.id
    WHERE q.user_id = ${userId}
    ORDER BY q.created_at DESC
    LIMIT 20
  `;
  console.log(`\n--- Recent Notification Queue Entries (${queue.length}) ---`);
  queue.forEach(q => {
    console.log(`- [${q.status.toUpperCase()}] ${q.business_name} | Channel: ${q.channel} | Created: ${q.created_at} | Updated: ${q.updated_at}`);
  });
}

run().catch(console.error);
