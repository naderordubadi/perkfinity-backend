/**
 * migrate-task11-prod.js
 * Applies Task 11 (Merchant Outbound Traffic Telemetry & ROI Analytics) 
 * database migration to the LIVE PRODUCTION Neon database (.env.production).
 * 
 * Rules:
 * - Strictly quoted table & column names ("Merchant", "SystemSetting", "MerchantOutboundClick")
 * - Seed feature flags to 'false' (disabled by default)
 * - Run pre-flight schema verification after applying
 */

const path = require('path');
const dotenv = require('dotenv');
const { neon } = require('@neondatabase/serverless');

dotenv.config({ path: path.resolve(__dirname, '../.env.production') });

if (!process.env.DATABASE_URL) {
  console.error('❌ ERROR: DATABASE_URL not found in .env.production!');
  process.exit(1);
}

const sql = neon(process.env.DATABASE_URL);

async function runMigration() {
  console.log('================================================================');
  console.log('🚀 TASK 11 PRODUCTION DATABASE MIGRATION');
  console.log('🔗 Target DB: PRODUCTION Database (ep-weathered-block-akv57uj9)');
  console.log('================================================================\n');

  try {
    // 1. Create SystemSetting table
    console.log('1️⃣ Creating "SystemSetting" table if not exists...');
    await sql`
      CREATE TABLE IF NOT EXISTS "SystemSetting" (
        "key" TEXT PRIMARY KEY,
        "value" TEXT NOT NULL,
        "description" TEXT,
        "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
      );
    `;
    console.log('   ✅ "SystemSetting" table ready.');

    // 2. Seed initial feature flags (both OFF)
    console.log('2️⃣ Seeding initial feature toggles (disabled by default)...');
    await sql`
      INSERT INTO "SystemSetting" ("key", "value", "description", "created_at", "updated_at")
      VALUES
        ('roi_feature_reps_enabled', 'false', 'Global toggle to enable/disable Traffic & ROI visibility on Sales Rep Portal', NOW(), NOW()),
        ('roi_feature_merchants_enabled', 'false', 'Global toggle to enable/disable Traffic & ROI visibility on Merchant Dashboard', NOW(), NOW())
      ON CONFLICT ("key") DO NOTHING;
    `;
    console.log('   ✅ Initial feature flags seeded.');

    // 3. Create MerchantOutboundClick table
    console.log('3️⃣ Creating "MerchantOutboundClick" table if not exists...');
    await sql`
      CREATE TABLE IF NOT EXISTS "MerchantOutboundClick" (
        "id" TEXT PRIMARY KEY DEFAULT (gen_random_uuid())::text,
        "merchant_id" TEXT NOT NULL,
        "user_id" TEXT,
        "platform" TEXT NOT NULL,
        "source" TEXT NOT NULL,
        "action_type" TEXT NOT NULL,
        "target_url" TEXT,
        "ip_hash" TEXT,
        "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
      );
    `;
    console.log('   ✅ "MerchantOutboundClick" table ready.');

    // Foreign key constraint
    console.log('   Checking foreign key constraint...');
    const existingFK = await sql`
      SELECT conname FROM pg_constraint 
      WHERE conname = 'MerchantOutboundClick_merchant_id_fkey';
    `;
    if (existingFK.length === 0) {
      await sql`
        ALTER TABLE "MerchantOutboundClick" 
        ADD CONSTRAINT "MerchantOutboundClick_merchant_id_fkey" 
        FOREIGN KEY ("merchant_id") REFERENCES "Merchant"("id") ON DELETE CASCADE;
      `;
      console.log('   ✅ Foreign key constraint added.');
    } else {
      console.log('   ℹ️ Foreign key constraint already exists.');
    }

    // Indexes
    console.log('   Creating composite indexes on "MerchantOutboundClick"...');
    await sql`
      CREATE INDEX IF NOT EXISTS "idx_outbound_merchant_created" 
      ON "MerchantOutboundClick" ("merchant_id", "created_at");
    `;
    await sql`
      CREATE INDEX IF NOT EXISTS "idx_outbound_platform_action" 
      ON "MerchantOutboundClick" ("platform", "action_type");
    `;
    console.log('   ✅ Composite indexes ready.');

    // 4. Add monthly_roi_email_enabled column to Merchant
    console.log('4️⃣ Adding "monthly_roi_email_enabled" column to "Merchant"...');
    await sql`
      ALTER TABLE "Merchant" 
      ADD COLUMN IF NOT EXISTS "monthly_roi_email_enabled" BOOLEAN NOT NULL DEFAULT false;
    `;
    console.log('   ✅ Column "monthly_roi_email_enabled" ready.');

    // 5. Pre-flight schema verification
    console.log('\n================================================================');
    console.log('🔍 PRE-FLIGHT SCHEMA VERIFICATION (PRODUCTION DB)');
    console.log('================================================================\n');

    const tables = await sql`
      SELECT table_name, column_name, data_type, is_nullable, column_default
      FROM information_schema.columns 
      WHERE table_name IN ('MerchantOutboundClick', 'SystemSetting')
      ORDER BY table_name, ordinal_position;
    `;
    console.log(`Verified ${tables.length} columns across MerchantOutboundClick and SystemSetting:`);
    for (const col of tables) {
      console.log(`  - [${col.table_name}] ${col.column_name} (${col.data_type}, nullable: ${col.is_nullable}, default: ${col.column_default})`);
    }

    const merchantCol = await sql`
      SELECT column_name, data_type, column_default 
      FROM information_schema.columns 
      WHERE table_name = 'Merchant' AND column_name = 'monthly_roi_email_enabled';
    `;
    console.log('\nVerified Merchant column:');
    console.log(JSON.stringify(merchantCol, null, 2));

    const settings = await sql`SELECT * FROM "SystemSetting" ORDER BY key;`;
    console.log('\nVerified SystemSettings rows:');
    console.log(JSON.stringify(settings, null, 2));

    // Probe SELECTs to confirm runtime queries will succeed
    console.log('\nExecuting probe SELECTs...');
    const probeSettings = await sql`
      SELECT key, value FROM "SystemSetting"
      WHERE key IN ('roi_feature_merchants_enabled', 'roi_feature_reps_enabled');
    `;
    console.log('  Probe SystemSetting:', probeSettings);

    const probeClicks = await sql`
      SELECT COUNT(*)::int as count FROM "MerchantOutboundClick";
    `;
    console.log('  Probe MerchantOutboundClick count:', probeClicks[0].count);

    const probeMerchant = await sql`
      SELECT id, business_name, monthly_roi_email_enabled FROM "Merchant" LIMIT 1;
    `;
    console.log('  Probe Merchant query:', probeMerchant[0]);

    console.log('\n🎉 ALL PRODUCTION DATABASE MIGRATIONS AND PRE-FLIGHT PROBES CLEARED 100%!');

  } catch (err) {
    console.error('❌ MIGRATION FAILED:', err);
    process.exit(1);
  }
}

runMigration();
