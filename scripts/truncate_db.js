const { neon } = require('@neondatabase/serverless');
require('dotenv').config();

const sql = neon(process.env.DATABASE_URL);

async function truncateAll() {
  console.log("💣 Force Truncating All Connected Tables with CASCADE...");
  try {
    await sql(`
      TRUNCATE 
          "User", 
          "Merchant", 
          "MerchantLocation", 
          "MerchantUser", 
          "Campaign", 
          "QrCode", 
          "Activation", 
          "Redemption", 
          "Subscription", 
          "Event", 
          "AuditLog", 
          "NotificationQueue", 
          "NotificationHistory", 
          "MerchantMember", 
          "Invoice" 
      CASCADE;
    `);
    console.log("✅ WIPE SUCCESSFUL: The test database is perfectly clean and architecture is intact.");
  } catch (err) {
    console.error("❌ TRUNCATE FAILED:", err);
  }
}

truncateAll();
