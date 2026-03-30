const { neon } = require('@neondatabase/serverless');

const sql = neon(process.env.DATABASE_URL);

async function run() {
  const email = 'naderordubadi@gmail.com';
  
  const users = await sql`SELECT id FROM "User" WHERE email = ${email}`;
  if (users.length === 0) return console.log("User not found: ", email);
  const userId = users[0].id;

  console.log(`Checking queue for ${email} (UserID: ${userId})`);

  // 1. Check NotificationQueue
  const queue = await sql`
    SELECT id, merchant_id, store_name, title, sent, created_at
    FROM "NotificationQueue"
    WHERE user_id = ${userId}
    ORDER BY created_at DESC
  `;
  
  console.log(`\n--- ALL Queue Entries (${queue.length}) ---`);
  queue.forEach(q => {
    console.log(`- [Sent: ${q.sent}] Store: ${q.store_name} | Title: ${q.title} | Time: ${q.created_at}`);
  });

  // 2. Check Member records
  const members = await sql`
    SELECT m.merchant_id, merch.business_name 
    FROM "MerchantMember" m
    JOIN "Merchant" merch ON m.merchant_id = merch.id
    WHERE m.user_id = ${userId}
  `;
  console.log(`\n--- Member of Stores (${members.length}) ---`);
  members.forEach(m => console.log(`- ${m.business_name} (ID: ${m.merchant_id})`));

  // 3. Are there campaigns for the missing stores that active?
  const allCampaigns = await sql`
    SELECT c.id, c.merchant_id, c.title, merch.business_name, c.status
    FROM "Campaign" c
    JOIN "Merchant" merch ON c.merchant_id = merch.id
    WHERE c.status = 'active'
  `;
  const relevantCampaigns = allCampaigns.filter(c => members.some(m => m.merchant_id === c.merchant_id));
  
  console.log(`\n--- Active Campaigns for User's Subscribed Stores (${relevantCampaigns.length}) ---`);
  relevantCampaigns.forEach(c => console.log(`- ${c.business_name}: [${c.title}]`));

}

run().catch(console.error);
