#!/usr/bin/env node
/**
 * Piece 9 — Online Promo Billing Trigger Test
 * ============================================
 * 1. Lowers billing_starts_at_member_count to 2 for promobrand2@test.com
 * 2. Clears any existing MerchantMember rows (clean slate)
 * 3. Fetches the merchant's QR public code
 * 4. Signs up 2 brand-new test consumers with that QR code in the body
 *    → The 2nd consumer join should fire the Stripe subscription trigger
 * 5. Polls the DB and prints final state for verification
 *
 * Run: node scripts/test-piece9.js [BACKEND_URL]
 * Default BACKEND_URL: http://localhost:3001
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { neon } = require('@neondatabase/serverless');

const BACKEND = process.argv[2] || 'http://localhost:3001';
const MERCHANT_EMAIL = 'promobrand2@test.com';
const THRESHOLD = 2;
const TEST_USERS = [
  { email: `piece9-test-user-A-${Date.now()}@testdomain.com`, password: 'TestPass123!' },
  { email: `piece9-test-user-B-${Date.now() + 1}@testdomain.com`, password: 'TestPass123!' },
];

async function main() {
  const sql = neon(process.env.DATABASE_URL);

  // ── Step 1: Find merchant ─────────────────────────────────────────
  console.log('\n[1] Looking up Promo Brand Two...');
  const [merchant] = await sql`
    SELECT m.id, m.business_name, m.billing_starts_at_member_count, m.billing_status,
           m.stripe_subscription_id, m.subscription_tier,
           m.stripe_customer_id, m.stripe_payment_method_id
    FROM "Merchant" m
    JOIN "MerchantUser" mu ON mu.merchant_id = m.id
    WHERE mu.email = ${MERCHANT_EMAIL}
    LIMIT 1
  `;
  if (!merchant) { console.error('ERROR: Merchant not found for', MERCHANT_EMAIL); process.exit(1); }

  console.log(`   ✅ Found: "${merchant.business_name}" (id=${merchant.id})`);
  console.log(`   Current billing_status: ${merchant.billing_status}`);
  console.log(`   Current threshold: ${merchant.billing_starts_at_member_count}`);
  console.log(`   subscription_tier: ${merchant.subscription_tier}`);
  console.log(`   stripe_customer_id: ${merchant.stripe_customer_id}`);
  console.log(`   stripe_payment_method_id: ${merchant.stripe_payment_method_id}`);

  // ── Step 2: Reset existing members ───────────────────────────────
  console.log('\n[2] Clearing existing MerchantMember rows...');
  const deleted = await sql`
    DELETE FROM "MerchantMember" WHERE merchant_id = ${merchant.id}
    RETURNING id
  `;
  console.log(`   Deleted ${deleted.length} existing member(s)`);

  // ── Step 3: Lower threshold to 2 ─────────────────────────────────
  console.log(`\n[3] Setting billing_starts_at_member_count = ${THRESHOLD}...`);
  await sql`
    UPDATE "Merchant"
    SET billing_starts_at_member_count = ${THRESHOLD},
        billing_status = 'trial',
        stripe_subscription_id = NULL,
        subscription_started_at = NULL,
        next_billing_date = NULL,
        updated_at = NOW()
    WHERE id = ${merchant.id}
  `;
  console.log(`   ✅ Threshold set to ${THRESHOLD}, billing_status reset to 'trial'`);

  // ── Step 4: Get QR public code ────────────────────────────────────
  console.log('\n[4] Fetching QR public code...');
  const [qr] = await sql`
    SELECT public_code FROM "QrCode"
    WHERE merchant_id = ${merchant.id} AND status = 'active'
    LIMIT 1
  `;
  if (!qr) { console.error('ERROR: No active QR code found for this merchant'); process.exit(1); }
  const qrCode = qr.public_code;
  console.log(`   ✅ QR public code: ${qrCode}`);
  console.log(`   Join URL: https://perkfinity.net/join/${qrCode}`);

  // ── Step 5: Sign up 2 test consumers ─────────────────────────────
  for (let i = 0; i < TEST_USERS.length; i++) {
    const u = TEST_USERS[i];
    const label = `Member ${i + 1}/${THRESHOLD}`;
    console.log(`\n[5.${i + 1}] Signing up ${label}: ${u.email}`);

    const resp = await fetch(`${BACKEND}/api/v1/consumers/signup`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: u.email, password: u.password, qrCode }),
    });
    const body = await resp.json();

    if (!body.success) {
      console.error(`   ❌ Signup failed: ${JSON.stringify(body)}`);
      process.exit(1);
    }
    console.log(`   ✅ Signed up — user id: ${body.data?.user?.id}`);

    // Short pause so backend logs are flushed
    await new Promise(r => setTimeout(r, 1500));

    // Verify member count after each join
    const [cnt] = await sql`SELECT COUNT(*)::int as n FROM "MerchantMember" WHERE merchant_id = ${merchant.id}`;
    console.log(`   Member count in DB now: ${cnt.n}`);

    if (i === TEST_USERS.length - 1) {
      // After final join — check the trigger fired
      console.log('\n[6] Checking billing trigger result...');
      await new Promise(r => setTimeout(r, 2000)); // give backend time to finish async work
      const [updated] = await sql`
        SELECT billing_status, stripe_subscription_id, billing_starts_at_member_count,
               subscription_started_at, next_billing_date
        FROM "Merchant" WHERE id = ${merchant.id}
      `;
      console.log('\n   ── RESULT ──────────────────────────────────────────');
      console.log(`   billing_status:               ${updated.billing_status}`);
      console.log(`   stripe_subscription_id:       ${updated.stripe_subscription_id}`);
      console.log(`   billing_starts_at_member_count: ${updated.billing_starts_at_member_count}`);
      console.log(`   subscription_started_at:      ${updated.subscription_started_at}`);
      console.log(`   next_billing_date:            ${updated.next_billing_date}`);
      console.log('   ─────────────────────────────────────────────────────\n');

      if (updated.billing_status === 'active' && updated.stripe_subscription_id) {
        console.log('   🎉 PIECE 9 PASS — billing trigger fired correctly!');
        console.log(`   Stripe subscription: ${updated.stripe_subscription_id}`);
      } else {
        console.log('   ❌ PIECE 9 FAIL — billing did not trigger. Check backend logs for errors.');
        console.log('      Possible causes:');
        console.log('      - STRIPE_ONLINE_GROWTH_PRICE_ID not set in .env');
        console.log('      - stripe_customer_id or stripe_payment_method_id missing for merchant');
        console.log('      - Stripe API error (check backend console)');
      }
    }
  }
}

main().catch(err => {
  console.error('\n❌ Script error:', err.message);
  process.exit(1);
});
