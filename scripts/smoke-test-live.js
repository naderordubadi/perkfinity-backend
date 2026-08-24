/**
 * smoke-test-live.js
 * Automated smoke test for Perkfinity Live Production Endpoints.
 * Run after every deployment: node backend/scripts/smoke-test-live.js
 */

const https = require('https');

const BASE_URL = 'https://perkfinity-backend.vercel.app';

function getJson(path) {
  return new Promise((resolve, reject) => {
    const url = `${BASE_URL}${path}`;
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          resolve({ status: res.statusCode, data: json });
        } catch (e) {
          resolve({ status: res.statusCode, raw: data });
        }
      });
    }).on('error', reject);
  });
}

async function run() {
  console.log(`🔍 Testing live production backend: ${BASE_URL}...`);
  let passed = true;

  try {
    // 1. Health check
    console.log('Testing GET /health...');
    const healthRes = await getJson('/health');
    if (healthRes.status === 200) {
      console.log('✅ PASSED: Backend is healthy.');
    } else {
      console.error(`❌ FAILED: Health returned status ${healthRes.status}`);
      passed = false;
    }

    // 2. Check sponsored merchants endpoint (web)
    console.log('Testing GET /api/v1/merchants/sponsored?platform=web...');
    const res = await getJson('/api/v1/merchants/sponsored?platform=web');
    const items = res.data && (Array.isArray(res.data) ? res.data : res.data.data);

    if (res.status !== 200) {
      console.error(`❌ FAILED: Expected 200, got ${res.status}.`);
      passed = false;
    } else if (!Array.isArray(items)) {
      console.error('❌ FAILED: Response data is not an array:', res.data);
      passed = false;
    } else {
      console.log(`✅ PASSED: ${items.length} web sponsors returned.`);
    }

    // 3. Check app sponsored merchants endpoint
    console.log('Testing GET /api/v1/merchants/sponsored?platform=app...');
    const appRes = await getJson('/api/v1/merchants/sponsored?platform=app');
    const appItems = appRes.data && (Array.isArray(appRes.data) ? appRes.data : appRes.data.data);

    if (appRes.status !== 200) {
      console.error(`❌ FAILED: Expected 200, got ${appRes.status}.`);
      passed = false;
    } else if (!Array.isArray(appItems)) {
      console.error('❌ FAILED: Response data is not an array:', appRes.data);
      passed = false;
    } else {
      console.log(`✅ PASSED: ${appItems.length} app sponsors returned.`);
    }

    // 4. Check merchant search endpoint
    console.log('Testing GET /api/v1/merchants/search?zip=92691...');
    const merchRes = await getJson('/api/v1/merchants/search?zip=92691');
    const merchItems = merchRes.data && (Array.isArray(merchRes.data) ? merchRes.data : merchRes.data.data || merchRes.data.merchants);
    if (merchRes.status !== 200) {
      console.error(`❌ FAILED: Expected 200, got ${merchRes.status}.`);
      passed = false;
    } else if (!Array.isArray(merchItems)) {
      console.error('❌ FAILED: Response data is not an array:', merchRes.data);
      passed = false;
    } else {
      console.log(`✅ PASSED: ${merchItems.length} merchants found for zip 92691.`);
    }

  } catch (err) {
    console.error('❌ Network error during smoke test:', err.message);
    passed = false;
  }

  if (!passed) {
    console.error('\n🚨 PRODUCTION SMOKE TEST FAILED! Review errors above.');
    process.exit(1);
  } else {
    console.log('\n🎉 ALL PRODUCTION SMOKE TESTS PASSED!');
    process.exit(0);
  }
}

run();
