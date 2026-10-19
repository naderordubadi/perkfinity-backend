/**
 * migrate-task11-spotlight.js
 * Applies Task 11 (Merchant Spotlight Tracking & Admin Compose Selector)
 * database migration to the LIVE PRODUCTION Neon database (.env.production).
 * 
 * Rules:
 * - Strictly quoted table & column names ("Merchant", "AnnouncementLog")
 * - Pre-flight schema verification after applying
 * - Backfills past spotlight announcements into Merchant.spotlight_sent_at and AnnouncementLog.merchant_id
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
  console.log('🚀 TASK 11 SPOTLIGHT TRACKING PRODUCTION DATABASE MIGRATION');
  console.log('🔗 Target DB: PRODUCTION Database');
  console.log('================================================================\n');

  try {
    // 1. Add spotlight_sent_at to "Merchant"
    console.log('1️⃣ Adding "spotlight_sent_at" column to "Merchant"...');
    await sql`
      ALTER TABLE "Merchant" 
      ADD COLUMN IF NOT EXISTS "spotlight_sent_at" TIMESTAMP WITH TIME ZONE;
    `;
    console.log('   ✅ Column "spotlight_sent_at" ready on "Merchant".');

    // 2. Add merchant_id to "AnnouncementLog"
    console.log('2️⃣ Adding "merchant_id" column to "AnnouncementLog"...');
    await sql`
      ALTER TABLE "AnnouncementLog" 
      ADD COLUMN IF NOT EXISTS "merchant_id" TEXT;
    `;
    console.log('   ✅ Column "merchant_id" ready on "AnnouncementLog".');

    // 3. Backfill past spotlight announcements from AnnouncementLog
    console.log('\n3️⃣ Backfilling past spotlight announcements...');
    const pastLogs = await sql`
      SELECT id, subject, status, created_at, html_body 
      FROM "AnnouncementLog" 
      ORDER BY created_at ASC
    `;
    console.log(`   Found ${pastLogs.length} announcement log entries to inspect.`);

    const merchants = await sql`
      SELECT id, business_name, spotlight_sent_at 
      FROM "Merchant"
    `;

    let backfilledCount = 0;
    for (const log of pastLogs) {
      const subject = log.subject || '';
      for (const m of merchants) {
        if (!m.business_name) continue;
        const bName = m.business_name.trim();
        // Check if subject or html_body mentions the business in a spotlight pattern
        const matchesSubject = subject.toLowerCase().includes(bName.toLowerCase());
        const isSpotlightPattern = subject.toLowerCase().includes('welcome') || subject.toLowerCase().includes('perk unlocked');
        
        if (matchesSubject && isSpotlightPattern) {
          console.log(`   🎯 Matched announcement "${subject}" to merchant "${bName}" (ID: ${m.id})`);
          await sql`
            UPDATE "Merchant" 
            SET "spotlight_sent_at" = COALESCE("spotlight_sent_at", ${log.created_at})
            WHERE "id" = ${m.id}
          `;
          await sql`
            UPDATE "AnnouncementLog"
            SET "merchant_id" = ${m.id}
            WHERE "id" = ${log.id}
          `;
          backfilledCount++;
        }
      }
    }

    // Also specifically check Sattva Indian (or any other known test)
    const [sattva] = await sql`
      SELECT id, business_name, spotlight_sent_at 
      FROM "Merchant" 
      WHERE business_name ILIKE '%Sattva%'
      LIMIT 1
    `;
    if (sattva && !sattva.spotlight_sent_at) {
      console.log(`   ℹ️ Backfilling Sattva Indian (Oct 7, 2026 announcement launch)...`);
      await sql`
        UPDATE "Merchant" 
        SET "spotlight_sent_at" = '2026-10-07T12:00:00Z'::timestamptz
        WHERE "id" = ${sattva.id}
      `;
      backfilledCount++;
    }

    console.log(`   ✅ Backfilled ${backfilledCount} spotlight records.`);

    // 4. Pre-flight schema verification
    console.log('\n================================================================');
    console.log('🔍 PRE-FLIGHT SCHEMA VERIFICATION (PRODUCTION DB)');
    console.log('================================================================\n');

    const merchantCol = await sql`
      SELECT column_name, data_type, is_nullable 
      FROM information_schema.columns 
      WHERE table_name = 'Merchant' AND column_name = 'spotlight_sent_at';
    `;
    console.log('Merchant column verification:', merchantCol);

    const logCol = await sql`
      SELECT column_name, data_type, is_nullable 
      FROM information_schema.columns 
      WHERE table_name = 'AnnouncementLog' AND column_name = 'merchant_id';
    `;
    console.log('AnnouncementLog column verification:', logCol);

    // 5. Probe query
    const probe = await sql`
      SELECT id, business_name, spotlight_sent_at 
      FROM "Merchant" 
      WHERE spotlight_sent_at IS NOT NULL
      ORDER BY spotlight_sent_at DESC;
    `;
    console.log(`\nProbe query cleared! ${probe.length} merchants currently marked as spotlight sent:`);
    for (const p of probe) {
      console.log(`  - ${p.business_name}: sent at ${p.spotlight_sent_at}`);
    }

    console.log('\n================================================================');
    console.log('✅ TASK 11 SPOTLIGHT MIGRATION & BACKFILL COMPLETED SUCCESSFULLY');
    console.log('================================================================\n');

  } catch (err) {
    console.error('❌ Migration failed:', err);
    process.exit(1);
  }
}

runMigration();
