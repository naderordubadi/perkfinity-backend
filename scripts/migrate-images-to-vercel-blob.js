/**
 * migrate-images-to-vercel-blob.js
 * 
 * Migrates all Base64 images in PostgreSQL ("Merchant" table) to Vercel Blob CDN.
 * Updates "Merchant" table columns (cover_photo_url, promo_banner_url, logo_url)
 * to point directly to permanent public Vercel Blob URLs.
 * Reclaims PostgreSQL TOAST storage via VACUUM FULL.
 */

const { neon } = require('@neondatabase/serverless');
const { put } = require('@vercel/blob');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

// Load production environment variables
const envPath = path.resolve(__dirname, '../.env.production');
const env = dotenv.parse(fs.readFileSync(envPath));
const sql = neon(env.DATABASE_URL);
const token = env.BLOB_READ_WRITE_TOKEN;

if (!token) {
  console.error('❌ BLOB_READ_WRITE_TOKEN missing from backend/.env.production');
  process.exit(1);
}

function parseDataUri(val) {
  if (!val || typeof val !== 'string') return null;
  const trimmed = val.trim();
  const match = trimmed.match(/^data:([^;]+);base64,(.*)$/s);
  if (!match) return null;
  const mimeType = match[1].toLowerCase();
  const buffer = Buffer.from(match[2], 'base64');
  let ext = 'jpg';
  if (mimeType.includes('png')) ext = 'png';
  else if (mimeType.includes('webp')) ext = 'webp';
  else if (mimeType.includes('svg')) ext = 'svg';
  else if (mimeType.includes('gif')) ext = 'gif';
  return { mimeType, buffer, ext };
}

async function runMigration() {
  console.log('🚀 Starting Vercel Blob Image Migration...\n');

  const merchants = await sql`
    SELECT id, business_name, cover_photo_url, promo_banner_url, logo_url
    FROM "Merchant"
    ORDER BY business_name ASC
  `;

  console.log(`Found ${merchants.length} total merchants in database.\n`);

  let totalUploaded = 0;
  let totalBytes = 0;

  for (let i = 0; i < merchants.length; i++) {
    const m = merchants[i];
    console.log(`[${i + 1}/${merchants.length}] Processing "${m.business_name}" (${m.id})...`);

    let newCover = null;
    let newBanner = null;
    let newLogo = null;

    // 1. Cover Photo
    const coverParsed = parseDataUri(m.cover_photo_url);
    if (coverParsed) {
      const pathname = `merchants/${m.id}/cover.${coverParsed.ext}`;
      const blob = await put(pathname, coverParsed.buffer, {
        access: 'public',
        contentType: coverParsed.mimeType,
        token,
        addRandomSuffix: true
      });
      newCover = blob.url;
      totalUploaded++;
      totalBytes += coverParsed.buffer.length;
      console.log(`  📸 Uploaded cover: ${blob.url} (${(coverParsed.buffer.length / 1024).toFixed(1)} KB)`);
    }

    // 2. Promo Banner
    const bannerParsed = parseDataUri(m.promo_banner_url);
    if (bannerParsed) {
      const pathname = `merchants/${m.id}/banner.${bannerParsed.ext}`;
      const blob = await put(pathname, bannerParsed.buffer, {
        access: 'public',
        contentType: bannerParsed.mimeType,
        token,
        addRandomSuffix: true
      });
      newBanner = blob.url;
      totalUploaded++;
      totalBytes += bannerParsed.buffer.length;
      console.log(`  🎨 Uploaded banner: ${blob.url} (${(bannerParsed.buffer.length / 1024).toFixed(1)} KB)`);
    }

    // 3. Logo
    const logoParsed = parseDataUri(m.logo_url);
    if (logoParsed) {
      const pathname = `merchants/${m.id}/logo.${logoParsed.ext}`;
      const blob = await put(pathname, logoParsed.buffer, {
        access: 'public',
        contentType: logoParsed.mimeType,
        token,
        addRandomSuffix: true
      });
      newLogo = blob.url;
      totalUploaded++;
      totalBytes += logoParsed.buffer.length;
      console.log(`  🏷️  Uploaded logo: ${blob.url} (${(logoParsed.buffer.length / 1024).toFixed(1)} KB)`);
    }

    // Update Merchant record in PostgreSQL if any image was migrated
    if (newCover || newBanner || newLogo) {
      await sql`
        UPDATE "Merchant"
        SET
          cover_photo_url = COALESCE(${newCover}, cover_photo_url),
          promo_banner_url = COALESCE(${newBanner}, promo_banner_url),
          logo_url = COALESCE(${newLogo}, logo_url)
        WHERE id = ${m.id}
      `;
      console.log(`  💾 Updated database record for "${m.business_name}".`);
    } else {
      console.log(`  ⏭️  No Base64 images found (already URLs or null).`);
    }
  }

  console.log('\n======================================================');
  console.log(`🎉 MIGRATION COMPLETE!`);
  console.log(`Total images uploaded to Vercel Blob: ${totalUploaded}`);
  console.log(`Total payload size migrated: ${(totalBytes / (1024 * 1024)).toFixed(2)} MB`);
  console.log('======================================================\n');

  // Verify database state: count remaining base64 images
  const remaining = await sql`
    SELECT count(*) as count
    FROM "Merchant"
    WHERE cover_photo_url LIKE 'data:%'
       OR promo_banner_url LIKE 'data:%'
       OR logo_url LIKE 'data:%'
  `;
  console.log(`Remaining Base64 images in database: ${remaining[0].count} (Expected: 0)`);

  // Reclaim TOAST disk space
  console.log('\n🧹 Reclaiming PostgreSQL storage with VACUUM FULL "Merchant"...');
  try {
    await sql`VACUUM FULL "Merchant"`;
    console.log('✅ VACUUM FULL completed successfully.');
  } catch (vacErr) {
    console.warn('⚠️ VACUUM notice:', vacErr.message);
  }

  // Check new table size
  const [sizeRes] = await sql`
    SELECT 
      pg_size_pretty(pg_total_relation_size('public."Merchant"')) AS total_size,
      pg_size_pretty(pg_relation_size('public."Merchant"')) AS table_size
  `;
  console.log(`\n📊 New "Merchant" table total size: ${sizeRes.total_size} (was 11 MB)!`);
}

runMigration().catch(err => {
  console.error('❌ Migration failed:', err);
  process.exit(1);
});
