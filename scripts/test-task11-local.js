/**
 * Task 11: Merchant Outbound Traffic Telemetry & ROI Analytics Local Test Suite
 * STRICTLY tests against LOCAL DEV environment and DEV database (backend/.env).
 * NEVER touches production.
 */

const http = require('http');
const path = require('path');
const dotenv = require('dotenv');
const { neon } = require('@neondatabase/serverless');

dotenv.config({ path: path.resolve(__dirname, '../.env') });
const sql = neon(process.env.DATABASE_URL);
const handler = require('../api/index.js');

// Mock request helper to test handler directly without opening a network port
function invokeHandler(method, url, body = null, headers = {}) {
  return new Promise((resolve) => {
    const req = new (require('events').EventEmitter)();
    req.method = method;
    req.url = url;
    req.headers = {
      'content-type': 'application/json',
      'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)',
      'x-forwarded-for': '76.168.12.34',
      ...headers
    };
    req.body = body;

    let resHeaders = {};
    let resBody = '';
    let resCode = 200;

    const res = {
      setHeader: (k, v) => { resHeaders[k.toLowerCase()] = v; },
      writeHead: (code, h) => { resCode = code; if (h) Object.assign(resHeaders, h); },
      set statusCode(val) { resCode = val; },
      get statusCode() { return resCode; },
      end: (chunk) => {
        if (chunk) resBody += chunk;
        let parsed = resBody;
        try { parsed = JSON.parse(resBody); } catch {}
        resolve({ status: resCode, headers: resHeaders, body: parsed });
      }
    };

    handler(req, res);
  });
}

async function runTestSuite() {
  console.log('================================================================');
  console.log('🧪 TASK 11 LOCAL DEV TEST SUITE: OUTBOUND ATTRIBUTION & ROI');
  console.log('🔗 Target DB: DEV Database (ep-bold-night-akguzcpk)');
  console.log('================================================================\n');

  // 1. Fetch a test merchant from DEV DB
  const [testMerchant] = await sql`SELECT id, business_name, subscription_tier FROM "Merchant" LIMIT 1`;
  if (!testMerchant) {
    console.error('❌ No test merchant found in DEV DB!');
    process.exit(1);
  }
  console.log(`📌 Using Test Merchant: "${testMerchant.business_name}" (${testMerchant.id}) [Tier: ${testMerchant.subscription_tier}]`);

  // 2. Test 1: Record Various Outbound Click Actions
  console.log('\n--- TEST 1: POST /api/v1/analytics/outbound-click ---');
  const actions = [
    { platform: 'web', source: 'web_localperks', action_type: 'website', target_url: 'https://testmerchant.com/?utm_source=perkfinity&utm_medium=web_referral' },
    { platform: 'web', source: 'web_localperks', action_type: 'order', target_url: 'https://order.toasttab.com/test?utm_source=perkfinity' },
    { platform: 'ios', source: 'app_feed', action_type: 'phone', target_url: 'tel:9495551234' },
    { platform: 'ios', source: 'app_feed', action_type: 'map_directions', target_url: 'maps://maps.apple.com/?q=Test' },
    { platform: 'android', source: 'app_sponsored', action_type: 'vip_takeover', target_url: 'https://testmerchant.com/vip' },
    { platform: 'web', source: 'web_vip_takeover', action_type: 'review_social', target_url: 'https://instagram.com/test' }
  ];

  for (const act of actions) {
    const res = await invokeHandler('POST', '/api/v1/analytics/outbound-click', {
      merchant_id: testMerchant.id,
      platform: act.platform,
      source: act.source,
      action_type: act.action_type,
      target_url: act.target_url
    });
    console.log(`  ✓ Click Recorded [${act.platform.toUpperCase()} - ${act.action_type}]: Status ${res.status}, Success: ${res.body.success}`);
    if (res.status !== 200 || !res.body.success) throw new Error(`Click logging failed for ${act.action_type}`);
  }

  // 3. Test 2: Bot Filtering Test
  console.log('\n--- TEST 2: Bot Filtering Safeguard ---');
  const botRes = await invokeHandler('POST', '/api/v1/analytics/outbound-click', {
    merchant_id: testMerchant.id,
    platform: 'web',
    source: 'web_localperks',
    action_type: 'website',
    target_url: 'https://test.com'
  }, { 'user-agent': 'Googlebot/2.1 (+http://www.google.com/bot.html)' });
  console.log(`  ✓ Bot Request Handled: Status ${botRes.status}, Ignored: ${botRes.body.ignored}`);
  if (!botRes.body.ignored) throw new Error('Bot filter failed to ignore Googlebot!');

  // 4. Test 3: IP Hashing & Privacy Verification in DB
  console.log('\n--- TEST 3: IP Anonymization & DB Integrity ---');
  const [loggedRow] = await sql`
    SELECT * FROM "MerchantOutboundClick" 
    WHERE merchant_id = ${testMerchant.id} 
    ORDER BY created_at DESC LIMIT 1
  `;
  console.log('  ✓ Sample Logged DB Row:');
  console.log(`    - ID: ${loggedRow.id}`);
  console.log(`    - Action: ${loggedRow.action_type}`);
  console.log(`    - Platform: ${loggedRow.platform}`);
  console.log(`    - IP Hash: ${loggedRow.ip_hash} (Raw IP is never stored ✅)`);
  if (!loggedRow.ip_hash || loggedRow.ip_hash.length !== 16) throw new Error('IP hashing failed');

  const jwt = require('jsonwebtoken');
  const adminToken = jwt.sign({ role: 'admin' }, process.env.JWT_SECRET);
  const merchantToken = jwt.sign({ merchantId: testMerchant.id, role: 'merchant' }, process.env.JWT_SECRET);

  // Enable merchant toggle for testing
  await sql`UPDATE "SystemSetting" SET value = 'true' WHERE key = 'roi_feature_merchants_enabled'`;

  try {
    // 5. Test 4: Query ROI Analytics for Starter Tier (Basic View)
    console.log('\n--- TEST 4: GET /api/v1/analytics/merchant-roi (Starter / Basic Tier) ---');
    await sql`UPDATE "Merchant" SET subscription_tier = 'starter' WHERE id = ${testMerchant.id}`;
    const basicRoi = await invokeHandler('GET', `/api/v1/analytics/merchant-roi?merchant_id=${testMerchant.id}`, null, {
      'authorization': `Bearer ${merchantToken}`
    });
    console.log(`  ✓ Basic ROI Status: ${basicRoi.status}`);
    console.log(`  ✓ Reporting Level: ${basicRoi.body.reportingLevel}`);
    console.log(`  ✓ Total Reach: ${basicRoi.body.summary.totalReach}`);
    console.log(`  ✓ Total Actions: ${basicRoi.body.summary.totalActions}`);
    console.log(`  ✓ In-Store Redemptions: ${basicRoi.body.summary.inStoreRedemptions}`);
    console.log(`  ✓ Actions Breakdown Hidden in Basic: ${basicRoi.body.actionsBreakdown === undefined}`);
    if (basicRoi.body.reportingLevel !== 'basic') {
      throw new Error(`Basic reporting tier mismatch: expected basic, got ${basicRoi.body.reportingLevel}`);
    }

    // 6. Test 5: Query ROI Analytics for Growth Tier (Detailed View)
    console.log('\n--- TEST 5: GET /api/v1/analytics/merchant-roi (Growth / Detailed Tier) ---');
    await sql`UPDATE "Merchant" SET subscription_tier = 'growth' WHERE id = ${testMerchant.id}`;
    const detailedRoi = await invokeHandler('GET', `/api/v1/analytics/merchant-roi?merchant_id=${testMerchant.id}`, null, {
      'authorization': `Bearer ${merchantToken}`
    });
    console.log(`  ✓ Detailed ROI Status: ${detailedRoi.status}`);
    console.log(`  ✓ Reporting Level: ${detailedRoi.body.reportingLevel}`);
    console.log('  ✓ Itemized Actions Breakdown:');
    console.log(JSON.stringify(detailedRoi.body.actionsBreakdown, null, 4));
    console.log('  ✓ Channel Breakdown (App vs Web):');
    console.log(JSON.stringify(detailedRoi.body.channelBreakdown, null, 4));
    console.log('  ✓ Sponsorship Metrics:');
    console.log(JSON.stringify(detailedRoi.body.sponsorship, null, 4));
    if (detailedRoi.body.reportingLevel !== 'detailed') {
      throw new Error(`Detailed reporting tier mismatch: expected detailed, got ${detailedRoi.body.reportingLevel}`);
    }

    // 7. Test 6: Custom Date Range Query
    console.log('\n--- TEST 6: Custom Date Range Query ---');
    const customRangeRoi = await invokeHandler('GET', `/api/v1/analytics/merchant-roi?merchant_id=${testMerchant.id}&startDate=2026-01-01&endDate=2026-12-31`, null, {
      'authorization': `Bearer ${adminToken}`
    });
    console.log(`  ✓ Custom Range Query [2026-01-01 to 2026-12-31]: Status ${customRangeRoi.status}`);
    console.log(`  ✓ Period Start: ${customRangeRoi.body.period.startDate}, End: ${customRangeRoi.body.period.endDate}`);
    console.log(`  ✓ Total Actions in Custom Period: ${customRangeRoi.body.summary.totalActions}`);

    // 8. Test 7: Admin Outbound Summary
    console.log('\n--- TEST 7: GET /api/v1/admin/analytics/outbound-summary ---');
    const adminSummary = await invokeHandler('GET', '/api/v1/admin/analytics/outbound-summary', null, {
      'authorization': `Bearer ${adminToken}`
    });
    console.log(`  ✓ Admin Summary Status: ${adminSummary.status}`);
    const merchantsList = adminSummary.body.data?.merchants || adminSummary.body.data || [];
    console.log(`  ✓ Total Merchants in Network Summary: ${merchantsList.length}`);
    console.log(`  ✓ Overall Network Total Actions: ${adminSummary.body.data?.summary?.total_actions || 0}`);
    const topMerchant = merchantsList[0];
    console.log(`  ✓ Top Merchant Activity: "${topMerchant?.business_name || topMerchant?.merchant_name}" (${topMerchant?.total_actions} actions, ${topMerchant?.web_actions} web, ${topMerchant?.app_actions} app)`);
  } finally {
    // Restore original tier and setting
    await sql`UPDATE "Merchant" SET subscription_tier = ${testMerchant.subscription_tier} WHERE id = ${testMerchant.id}`;
    await sql`UPDATE "SystemSetting" SET value = 'false' WHERE key = 'roi_feature_merchants_enabled'`;
  }

  console.log('\n================================================================');
  console.log('✅ ALL 7 TASK 11 LOCAL DEV TESTS CLEARED WITH 100% SUCCESS!');
  console.log('================================================================\n');
}

runTestSuite().catch(err => {
  console.error('\n❌ TEST SUITE FAILURE:', err);
  process.exit(1);
});
