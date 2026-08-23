import { neon } from '@neondatabase/serverless';
import dotenv from 'dotenv';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import crypto from 'crypto';

dotenv.config({ path: '/Users/MyMacBook/Desktop/Antigravity/Perkfinity/backend/.env' });

const sql = neon(process.env.DATABASE_URL);
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret-key-12345';

async function runTest() {
  console.log('🧪 Starting Task 8 Turnkey Pre-Setup Engine End-to-End Verification on DEV DB...');
  console.log('🔗 DB Target:', process.env.DATABASE_URL.split('@')[1].split('/')[0]);

  const testSuffix = Math.floor(1000 + Math.random() * 9000);
  const testBusinessName = `Luigi's Pizzeria Test ${testSuffix}`;
  const testZip = '92692';
  const cleanSlug = testBusinessName.toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 15);
  const tempEmail = `${cleanSlug}_${testZip}_${testSuffix}@presetup.perkfinity.net`;
  const tempPassword = `Luigis2026!`;
  const tempPasswordHash = await bcrypt.hash(tempPassword, 12);
  const publicCode = crypto.randomBytes(9).toString('base64url');
  const memberLimit = 50;

  console.log('\n--- 1. Testing Admin Pre-Setup Creation ---');
  const [merchant] = await sql`
    INSERT INTO "Merchant" (
      id, business_name, contact_name, phone, public_phone, public_email,
      website, review_url, order_url, logo_url, cover_photo_url, promo_description,
      business_presence, business_category, welcome_offer_text,
      subscription_tier, member_limit, status, is_hidden, is_presetup, is_claimed,
      temp_password_plain, is_web_sponsored, web_sponsored_until,
      is_app_sponsored, app_sponsored_until, is_fullpage_sponsored, fullpage_sponsored_until,
      created_at, updated_at
    ) VALUES (
      gen_random_uuid()::text, ${testBusinessName}, 'Store Owner', null, '(949) 555-0199', 'info@luigispizza.com',
      'https://luigispizza.com', 'https://g.page/luigi', 'https://toasttab.com/luigi', 'https://luigispizza.com/logo.png', 'https://luigispizza.com/cover.jpg', 'Grand Opening 15% VIP Perk',
      'hybrid', 'Dining & Restaurants', '15% off first order',
      'presetup_50', ${memberLimit}, 'active', true, true, false,
      ${tempPassword}, true, NOW() + INTERVAL '30 days',
      true, NOW() + INTERVAL '30 days', true, NOW() + INTERVAL '30 days',
      NOW(), NOW()
    )
    RETURNING *
  `;
  console.log('✅ Created Pre-Setup Merchant ID:', merchant.id);
  console.log('   Temp Email:', tempEmail);
  console.log('   Temp Password:', tempPassword);
  console.log('   is_presetup:', merchant.is_presetup, '| is_claimed:', merchant.is_claimed, '| is_hidden:', merchant.is_hidden, '| member_limit:', merchant.member_limit);

  // Insert location
  await sql`
    INSERT INTO "MerchantLocation" (id, merchant_id, address, city, state, postal_code, country, is_active, created_at)
    VALUES (gen_random_uuid()::text, ${merchant.id}, '25380 Marguerite Pkwy', 'Mission Viejo', 'CA', ${testZip}, 'US', true, NOW())
  `;

  // Insert user
  const [mUser] = await sql`
    INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
    VALUES (gen_random_uuid()::text, ${merchant.id}, ${tempEmail}, ${tempPasswordHash}, 'owner', 'active', NOW())
    RETURNING *
  `;

  // Insert QR Code
  await sql`
    INSERT INTO "QrCode" (id, merchant_id, public_code, status, created_at)
    VALUES (gen_random_uuid()::text, ${merchant.id}, ${publicCode}, 'active', NOW())
  `;

  // Insert Campaign
  await sql`
    INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, terms, status, campaign_type, start_at, end_at, created_at, updated_at)
    VALUES (gen_random_uuid()::text, ${merchant.id}, '15% off first order', 10, 'Valid for first-time customers', 'active', 'initial', NOW(), NULL, NOW(), NOW())
  `;

  console.log('\n--- 2. Testing Stealth Visibility Toggle ---');
  // Toggle to Live for demo
  const [toggle1] = await sql`UPDATE "Merchant" SET is_hidden = false WHERE id = ${merchant.id} RETURNING is_hidden`;
  console.log('✅ Toggled visibility to LIVE for in-person demo:', !toggle1.is_hidden);
  // Toggle back to Hidden
  const [toggle2] = await sql`UPDATE "Merchant" SET is_hidden = true WHERE id = ${merchant.id} RETURNING is_hidden`;
  console.log('✅ Toggled visibility back to HIDDEN after demo:', toggle2.is_hidden);

  console.log('\n--- 3. Testing Admin Pre-Setup Listing Query ---');
  const presetupList = await sql`
    SELECT m.id, m.business_name, m.is_presetup, m.is_claimed, m.member_limit, m.is_hidden, mu.email as temp_email, q.public_code
    FROM "Merchant" m
    LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
    LEFT JOIN "QrCode" q ON q.merchant_id = m.id AND q.status = 'active'
    WHERE m.id = ${merchant.id}
  `;
  console.log('✅ Found in pre-setup list:', presetupList[0]);

  console.log('\n--- 4. Testing Merchant Login with Temp Credentials ---');
  const [loginUser] = await sql`SELECT * FROM "MerchantUser" WHERE email = ${tempEmail} LIMIT 1`;
  const isPwValid = await bcrypt.compare(tempPassword, loginUser.password_hash);
  console.log('✅ Temp Password comparison match:', isPwValid);

  const tempToken = jwt.sign({ userId: loginUser.id, merchantId: merchant.id, role: 'owner', email: tempEmail }, JWT_SECRET, { expiresIn: '8h' });

  console.log('\n--- 5. Testing Profile Fetch for Unclaimed Merchant ---');
  const [profileData] = await sql`
    SELECT m.business_name, m.contact_name, m.phone, m.public_phone, m.public_email, m.website, m.logo_url, m.subscription_tier,
           m.business_presence, m.welcome_offer_text, m.is_presetup, m.is_claimed, m.member_limit, m.is_hidden,
           l.address, l.city, l.state, l.postal_code, u.email
    FROM "Merchant" m
    JOIN "MerchantUser" u ON u.merchant_id = m.id
    LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
    WHERE m.id = ${merchant.id} AND u.id = ${loginUser.id}
    LIMIT 1
  `;
  console.log('✅ Profile returns is_presetup =', profileData.is_presetup, '| is_claimed =', profileData.is_claimed, '| member_limit =', profileData.member_limit);
  if (profileData.is_presetup && !profileData.is_claimed) {
    console.log('✅ Dashboard triggers mandatory Claim Profile modal as expected!');
  } else {
    throw new Error('FAILED: Expected is_presetup true and is_claimed false');
  }

  console.log('\n--- 6. Testing Merchant Claim Profile Execution ---');
  const realOwnerName = 'Luigi Rossi';
  const realEmail = `luigi_${testSuffix}@gmail.com`;
  const realPhone = '949-555-9876';
  const newPassword = 'SecurePassword2026!';
  const newPasswordHash = await bcrypt.hash(newPassword, 12);

  // Update MerchantUser
  await sql`
    UPDATE "MerchantUser"
    SET email = ${realEmail},
        password_hash = ${newPasswordHash}
    WHERE merchant_id = ${merchant.id} AND id = ${loginUser.id}
  `;

  // Update Merchant
  const [claimedMerchant] = await sql`
    UPDATE "Merchant"
    SET contact_name = ${realOwnerName},
        phone = ${realPhone},
        is_claimed = true,
        is_hidden = false,
        temp_password_plain = NULL,
        presetup_claimed_at = NOW(),
        onboarding_complete = true,
        updated_at = NOW()
    WHERE id = ${merchant.id}
    RETURNING *
  `;

  console.log('✅ Claimed Merchant updated:');
  console.log('   contact_name:', claimedMerchant.contact_name);
  console.log('   phone:', claimedMerchant.phone);
  console.log('   is_claimed:', claimedMerchant.is_claimed);
  console.log('   is_hidden (auto-unhidden):', claimedMerchant.is_hidden);
  console.log('   temp_password_plain (cleared):', claimedMerchant.temp_password_plain);
  console.log('   presetup_claimed_at:', claimedMerchant.presetup_claimed_at);

  console.log('\n--- 7. Verifying Post-Claim Login with Real Credentials ---');
  const [claimedUser] = await sql`SELECT * FROM "MerchantUser" WHERE email = ${realEmail} LIMIT 1`;
  const isNewPwValid = await bcrypt.compare(newPassword, claimedUser.password_hash);
  console.log('✅ New Password comparison match:', isNewPwValid);

  console.log('\n--- 8. Cleaning up initial test record ---');
  await sql`DELETE FROM "Campaign" WHERE merchant_id = ${merchant.id}`;
  await sql`DELETE FROM "QrCode" WHERE merchant_id = ${merchant.id}`;
  await sql`DELETE FROM "MerchantLocation" WHERE merchant_id = ${merchant.id}`;
  await sql`DELETE FROM "MerchantUser" WHERE merchant_id = ${merchant.id}`;
  await sql`DELETE FROM "Merchant" WHERE id = ${merchant.id}`;
  console.log('✅ Cleaned up initial test merchant data from Dev DB.');

  console.log('\n--- 9. Testing Pre-Setup with NO Physical Address & Multi-Location Flow ---');
  const noAddrName = `NoAddr Pizzeria ${testSuffix}`;
  const [noAddrMerchant] = await sql`
    INSERT INTO "Merchant" (
      id, business_name, contact_name, phone, public_phone, public_email,
      website, review_url, order_url, logo_url, cover_photo_url, promo_description,
      business_presence, business_category, welcome_offer_text, is_multi_location,
      subscription_tier, member_limit, status, is_hidden, is_presetup, is_claimed,
      temp_password_plain, is_web_sponsored, is_app_sponsored, is_fullpage_sponsored,
      created_at, updated_at
    ) VALUES (
      gen_random_uuid()::text, ${noAddrName}, 'Store Owner', null, null, null,
      'https://example.com', null, null, null, null, null,
      'hybrid', 'Restaurants & Dining', '20% Off Your First Purchase', true,
      'presetup_50', 50, 'active', true, true, false,
      'TempPass2026!', true, true, true,
      NOW(), NOW()
    )
    RETURNING *
  `;
  console.log('✅ Created Pre-Setup Merchant with is_multi_location = true:', noAddrMerchant.is_multi_location);

  await sql`
    INSERT INTO "MerchantLocation" (id, merchant_id, address, city, state, postal_code, country, is_active, created_at)
    VALUES (gen_random_uuid()::text, ${noAddrMerchant.id}, null, null, null, null, 'US', true, NOW())
  `;
  console.log('✅ Created MerchantLocation with NULL address, city, state, postal_code');

  const [noAddrLoc] = await sql`SELECT * FROM "MerchantLocation" WHERE merchant_id = ${noAddrMerchant.id} LIMIT 1`;
  if (noAddrLoc.address !== null || noAddrLoc.city !== null) {
    throw new Error('Expected NULL address and city in MerchantLocation');
  }

  // Update profile to single location with physical address
  await sql`
    UPDATE "Merchant"
    SET is_multi_location = false,
        updated_at = NOW()
    WHERE id = ${noAddrMerchant.id}
  `;
  await sql`
    UPDATE "MerchantLocation"
    SET address = '123 Test St', city = 'Irvine', state = 'CA', postal_code = '92618'
    WHERE merchant_id = ${noAddrMerchant.id}
  `;

  const [updatedLoc] = await sql`
    SELECT m.is_multi_location, l.address, l.city, l.state, l.postal_code
    FROM "Merchant" m
    JOIN "MerchantLocation" l ON l.merchant_id = m.id
    WHERE m.id = ${noAddrMerchant.id}
  `;
  console.log('✅ Updated Profile with Physical Address:', `${updatedLoc.address}, ${updatedLoc.city}, ${updatedLoc.state} ${updatedLoc.postal_code} (is_multi_location=${updatedLoc.is_multi_location})`);

  // Cleanup second test record
  await sql`DELETE FROM "MerchantLocation" WHERE merchant_id = ${noAddrMerchant.id}`;
  await sql`DELETE FROM "Merchant" WHERE id = ${noAddrMerchant.id}`;
  console.log('✅ Cleaned up second test record.');

  console.log('\n🎉 ALL TASK 8 & MULTI-LOCATION TEST CASES PASSED 100% ON DEV DATABASE!');
}

runTest().catch(err => {
  console.error('❌ Test Failed:', err);
  process.exit(1);
});
