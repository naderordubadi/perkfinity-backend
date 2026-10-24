/**
 * legacy-migrations.js
 * 
 * Archived one-time schema and backfill migrations previously located in api/index.js.
 * These migrations have already completed in production and are preserved here
 * for development / fresh database initialization.
 */

const { neon } = require('@neondatabase/serverless');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

const envPath = path.resolve(__dirname, '../.env.production');
const env = dotenv.parse(fs.readFileSync(envPath));
const sql = neon(env.DATABASE_URL);

async function runLegacyMigrations() {
  console.log('Running legacy migrations...');

  // 1. Campaign campaign_type
  await sql`ALTER TABLE "Campaign" ADD COLUMN IF NOT EXISTS campaign_type TEXT DEFAULT 'perk'`;
  await sql`UPDATE "Campaign" SET campaign_type='announcement' WHERE discount_percentage = -1 AND campaign_type IS DISTINCT FROM 'announcement'`;

  // 2. User social logins
  await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS apple_sub TEXT UNIQUE`;
  await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS google_sub TEXT UNIQUE`;

  // 3. Invoice revenue_type
  await sql`ALTER TABLE "Invoice" ADD COLUMN IF NOT EXISTS "revenue_type" TEXT DEFAULT 'platform'`;
  await sql`UPDATE "Invoice" SET "revenue_type" = 'sponsorship' WHERE "amount_cents" IN (4999, 9999, 12999)`;
  await sql`UPDATE "Invoice" SET "revenue_type" = 'pouf' WHERE "merchant_id" IN (SELECT "id" FROM "Merchant" WHERE "billing_cycle" = 'lifetime') OR "stripe_invoice_id" LIKE 'cs_%'`;

  // 4. AnnouncementLog table
  await sql`
    CREATE TABLE IF NOT EXISTS "AnnouncementLog" (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      subject TEXT NOT NULL,
      sender TEXT NOT NULL,
      audience_type TEXT,
      filters JSONB,
      recipient_count INTEGER DEFAULT 0,
      external_count INTEGER DEFAULT 0,
      has_attachments BOOLEAN DEFAULT false,
      status TEXT DEFAULT 'sent',
      html_body TEXT,
      scheduled_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `;
  await sql`ALTER TABLE "AnnouncementLog" ADD COLUMN IF NOT EXISTS html_body TEXT`;
  await sql`ALTER TABLE "AnnouncementLog" ADD COLUMN IF NOT EXISTS merchant_id TEXT`;
  await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS spotlight_sent_at TIMESTAMPTZ`;

  console.log('✅ Legacy migrations finished.');
}

if (require.main === module) {
  runLegacyMigrations().catch(console.error);
}

module.exports = { runLegacyMigrations };
