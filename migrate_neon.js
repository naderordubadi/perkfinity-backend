const { neon } = require('@neondatabase/serverless');
require('dotenv').config({ path: '.env' });

async function migrate() {
  const sql = neon(process.env.DATABASE_URL);
  
  try {
    console.log("Adding columns to User table...");
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "email" TEXT UNIQUE`;
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "password_hash" TEXT`;
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "full_name" TEXT`;
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "phone_number" TEXT`;
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "city" TEXT`;
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "zip_code" TEXT`;
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "location_sharing_enabled" BOOLEAN DEFAULT false`;
    await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "push_notifications_enabled" BOOLEAN DEFAULT false`;
    console.log("Migration successful!");
  } catch (err) {
    console.error("Migration Failed:", err);
  }
}
migrate();
