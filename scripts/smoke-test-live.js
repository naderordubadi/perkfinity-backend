/**
 * smoke-test-live.js
 * Automated smoke test for Perkfinity Live Production Endpoints.
 * Validates HTTP Status, JSON payload structures, non-empty data, and CORS headers.
 * Run after every deployment: node backend/scripts/smoke-test-live.js
 */

const https = require('https');

const BASE_URL = 'https://perkfinity-backend.vercel.app';

function requestUrl(path, options = {}) {
  return new Promise((resolve, reject) => {
    const url = `${BASE_URL}${path}`;
    const reqOptions = {
      headers: options.headers || {}
    };

    https.get(url, reqOptions, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        let json = null;
        try {
          json = JSON.parse(data);
        } catch (e) {}
        resolve({
          status: res.statusCode,
          headers: res.headers,
          data: json,
          raw: data
        });
      });
    }).on('error', reject);
  });
}

async function run() {
  console.log(`🔍 Testing live production backend: ${BASE_URL}...\n`);
  let passed = true;

  const testOrigins = [
    'https://perkfinity.net',
    'https://www.perkfinity.net',
    'capacitor://perkfinity.net'
  ];

  try {
    // 1. Health check
    console.log('Testing GET /health...');
    const healthRes = await requestUrl('/health');
    if (healthRes.status === 200) {
      console.log('  ✅ PASSED: Backend is healthy.');
    } else {
      console.error(`  ❌ FAILED: Health returned status ${healthRes.status}`);
      passed = false;
    }

    // 2. Check sponsored merchants endpoint (web)
    console.log('\nTesting GET /api/v1/merchants/sponsored?platform=web...');
    const res = await requestUrl('/api/v1/merchants/sponsored?platform=web', {
      headers: { 'Origin': 'https://perkfinity.net' }
    });
    const items = res.data && (Array.isArray(res.data) ? res.data : res.data.data);
    const corsOrigin = res.headers['access-control-allow-origin'];

    if (res.status !== 200) {
      console.error(`  ❌ FAILED: Expected 200, got ${res.status}.`);
      passed = false;
    } else if (!Array.isArray(items)) {
      console.error('  ❌ FAILED: Response data is not an array:', res.data);
      passed = false;
    } else if (!corsOrigin || (corsOrigin !== 'https://perkfinity.net' && corsOrigin !== '*')) {
      console.error(`  ❌ FAILED: Missing or incorrect CORS origin: ${corsOrigin}`);
      passed = false;
    } else {
      console.log(`  ✅ PASSED: ${items.length} web sponsors returned (CORS: ${corsOrigin}).`);
    }

    // 3. Check app sponsored merchants endpoint
    console.log('\nTesting GET /api/v1/merchants/sponsored?platform=app...');
    const appRes = await requestUrl('/api/v1/merchants/sponsored?platform=app', {
      headers: { 'Origin': 'capacitor://perkfinity.net' }
    });
    const appItems = appRes.data && (Array.isArray(appRes.data) ? appRes.data : appRes.data.data);
    const appCors = appRes.headers['access-control-allow-origin'];

    if (appRes.status !== 200) {
      console.error(`  ❌ FAILED: Expected 200, got ${appRes.status}.`);
      passed = false;
    } else if (!Array.isArray(appItems)) {
      console.error('  ❌ FAILED: Response data is not an array:', appRes.data);
      passed = false;
    } else if (!appCors || (appCors !== 'capacitor://perkfinity.net' && appCors !== '*')) {
      console.error(`  ❌ FAILED: Missing or incorrect CORS origin: ${appCors}`);
      passed = false;
    } else {
      console.log(`  ✅ PASSED: ${appItems.length} app sponsors returned (CORS: ${appCors}).`);
    }

    // 4. Check merchant search endpoint
    console.log('\nTesting GET /api/v1/merchants/search?zip=92691...');
    const merchRes = await requestUrl('/api/v1/merchants/search?zip=92691', {
      headers: { 'Origin': 'https://perkfinity.net' }
    });
    const merchItems = merchRes.data && (Array.isArray(merchRes.data) ? merchRes.data : merchRes.data.data || merchRes.data.merchants);
    const merchCors = merchRes.headers['access-control-allow-origin'];

    if (merchRes.status !== 200) {
      console.error(`  ❌ FAILED: Expected 200, got ${merchRes.status}.`);
      passed = false;
    } else if (!Array.isArray(merchItems)) {
      console.error('  ❌ FAILED: Response data is not an array:', merchRes.data);
      passed = false;
    } else if (!merchCors || (merchCors !== 'https://perkfinity.net' && merchCors !== '*')) {
      console.error(`  ❌ FAILED: Missing or incorrect CORS origin: ${merchCors}`);
      passed = false;
    } else {
      console.log(`  ✅ PASSED: ${merchItems.length} merchants found for zip 92691 (CORS: ${merchCors}).`);
    }

    // 5. Check online merchants endpoint across origins
    for (const origin of testOrigins) {
      console.log(`\nTesting GET /api/v1/public/online-merchants [Origin: ${origin}]...`);
      const onlineRes = await requestUrl('/api/v1/public/online-merchants', {
        headers: { 'Origin': origin }
      });
      const onlineItems = onlineRes.data && (Array.isArray(onlineRes.data) ? onlineRes.data : onlineRes.data.data);
      const onlineCors = onlineRes.headers['access-control-allow-origin'];

      if (onlineRes.status !== 200) {
        console.error(`  ❌ FAILED: Expected 200, got ${onlineRes.status}.`);
        passed = false;
      } else if (!Array.isArray(onlineItems) || onlineItems.length === 0) {
        console.error('  ❌ FAILED: Response data is empty or not an array:', onlineRes.data);
        passed = false;
      } else if (!onlineCors || (onlineCors !== origin && onlineCors !== '*')) {
        console.error(`  ❌ FAILED: Missing or incorrect CORS origin header. Expected ${origin}, got: ${onlineCors}`);
        passed = false;
      } else {
        console.log(`  ✅ PASSED: ${onlineItems.length} online merchants returned (CORS: ${onlineCors}).`);
      }
    }

    // 6. Check local merchants endpoint across origins
    for (const origin of testOrigins) {
      console.log(`\nTesting GET /api/v1/public/local-merchants [Origin: ${origin}]...`);
      const localRes = await requestUrl('/api/v1/public/local-merchants', {
        headers: { 'Origin': origin }
      });
      const localItems = localRes.data && (Array.isArray(localRes.data) ? localRes.data : localRes.data.data);
      const localCors = localRes.headers['access-control-allow-origin'];

      if (localRes.status !== 200) {
        console.error(`  ❌ FAILED: Expected 200, got ${localRes.status}.`);
        passed = false;
      } else if (!Array.isArray(localItems) || localItems.length === 0) {
        console.error('  ❌ FAILED: Response data is empty or not an array:', localRes.data);
        passed = false;
      } else if (!localCors || (localCors !== origin && localCors !== '*')) {
        console.error(`  ❌ FAILED: Missing or incorrect CORS origin header. Expected ${origin}, got: ${localCors}`);
        passed = false;
      } else {
        console.log(`  ✅ PASSED: ${localItems.length} local merchants returned (CORS: ${localCors}).`);
      }
    }

    // 7. Check consumers campaigns feed
    console.log('\nTesting GET /api/v1/consumers/campaigns...');
    const camRes = await requestUrl('/api/v1/consumers/campaigns', {
      headers: { 'Origin': 'https://perkfinity.net' }
    });
    const camItems = camRes.data && (Array.isArray(camRes.data) ? camRes.data : camRes.data.data);
    const camCors = camRes.headers['access-control-allow-origin'];

    if (camRes.status !== 200) {
      console.error(`  ❌ FAILED: Expected 200, got ${camRes.status}.`);
      passed = false;
    } else if (!Array.isArray(camItems)) {
      console.error('  ❌ FAILED: Response data is not an array:', camRes.data);
      passed = false;
    } else if (!camCors || (camCors !== 'https://perkfinity.net' && camCors !== '*')) {
      console.error(`  ❌ FAILED: Missing or incorrect CORS origin: ${camCors}`);
      passed = false;
    } else {
      console.log(`  ✅ PASSED: ${camItems.length} consumer campaign items returned (CORS: ${camCors}).`);
    }

  } catch (err) {
    console.error('❌ Network error during smoke test:', err.message);
    passed = false;
  }

  if (!passed) {
    console.error('\n🚨 PRODUCTION SMOKE TEST FAILED! Review errors above.');
    process.exit(1);
  } else {
    console.log('\n🎉 ALL PRODUCTION SMOKE TESTS PASSED (Data & CORS Verified)!');
    process.exit(0);
  }
}

run();
