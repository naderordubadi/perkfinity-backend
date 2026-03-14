const { neon } = require('@neondatabase/serverless');
require('dotenv').config({ path: '.env' });

async function migrate() {
  const sql = neon(process.env.DATABASE_URL);
  
  try {
    await sql`ALTER TABLE "User" 
      ADD COLUMN IF NOT EXISTS email VARCHAR(255) UNIQUE,
      ADD COLUMN IF NOT EXISTS password_hash VARCHAR(255),
      ADD COLUMN IF NOT EXISTS full_name VARCHAR(255),
      ADD COLUMN IF NOT EXISTS phone_number VARCHAR(50),
      ADD COLUMN IF NOT EXISTS city VARCHAR(100),
      ADD COLUMN IF NOT EXISTS zip_code VARCHAR(20),
      ADD COLUMN IF NOT EXISTS location_sharing_enabled BOOLEAN DEFAULT false,
      ADD COLUMN IF NOT EXISTS push_notifications_enabled BOOLEAN DEFAULT false
    `;
    console.log("Migration successful");
  } catch (err) {
    console.error("Migration failed:", err);
  }
}
migrate();
