/**
 * Local test script for Stripe integration.
 * Tests: DB migration, Setup Intent creation, Checkout Session creation.
 * 
 * Usage: node scripts/test-stripe-local.js
 */

// Env vars are passed via command line
const http = require('http');

// Load our handler
const handler = require('../api/index.js');

// Set env vars (must be passed via command line)
if (!process.env.STRIPE_SECRET_KEY) {
  console.error('ERROR: Set STRIPE_SECRET_KEY env var');
  process.exit(1);
}
if (!process.env.STRIPE_TIER1_PRICE_ID) {
  console.error('ERROR: Set STRIPE_TIER1_PRICE_ID env var');
  process.exit(1);
}

async function makeRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 3099,
      path,
      method,
      headers: { 'Content-Type': 'application/json' }
    };
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function runTests() {
  // Start a small server
  const server = http.createServer((req, res) => {
    // Parse body for POST requests
    if (req.method === 'POST' || req.method === 'PUT') {
      let body = '';
      req.on('data', chunk => body += chunk);
      req.on('end', () => {
        try { req.body = JSON.parse(body); } catch { req.body = {}; }
        handler(req, res);
      });
    } else {
      req.body = {};
      handler(req, res);
    }
  });

  await new Promise(resolve => server.listen(3099, resolve));
  console.log('Test server started on port 3099\n');

  try {
    // Test 1: Health Check
    console.log('═══ Test 1: Health Check ═══');
    const health = await makeRequest('GET', '/health');
    console.log(`Status: ${health.status} | DB: ${health.body?.db || 'unknown'}`);
    console.log(health.body.ok ? '✅ Health check passed' : '❌ Health check failed');

    // Test 2: Run Stripe DB Migration
    console.log('\n═══ Test 2: Stripe DB Migration ═══');
    const migrate = await makeRequest('GET', '/api/v1/migrate-stripe');
    console.log(`Status: ${migrate.status} | Message: ${migrate.body?.message || 'unknown'}`);
    console.log(migrate.body.success ? '✅ Migration passed' : '❌ Migration failed');

    // Test 3: Create Setup Intent (need a real merchant ID)
    console.log('\n═══ Test 3: Fetch a merchant for testing ═══');
    const merchants = await makeRequest('GET', '/api/v1/admin/merchants');
    const testMerchant = merchants.body?.data?.merchants?.[0];
    if (!testMerchant) {
      console.log('⚠️ No merchants found — skipping Setup Intent and Checkout tests');
    } else {
      console.log(`Using merchant: ${testMerchant.business_name} (${testMerchant.id})`);

      console.log('\n═══ Test 4: Create Setup Intent ═══');
      const setupIntent = await makeRequest('POST', '/api/v1/stripe/create-setup-intent', {
        merchant_id: testMerchant.id
      });
      console.log(`Status: ${setupIntent.status}`);
      if (setupIntent.body.success) {
        console.log(`✅ Setup Intent created! Client secret starts with: ${setupIntent.body.data.client_secret.substring(0, 20)}...`);
        console.log(`   Stripe Customer: ${setupIntent.body.data.customer_id}`);
      } else {
        console.log(`❌ Failed: ${setupIntent.body.error}`);
      }

      console.log('\n═══ Test 5: Create Checkout Session ═══');
      const checkout = await makeRequest('POST', '/api/v1/stripe/create-checkout-session', {
        merchant_id: testMerchant.id
      });
      console.log(`Status: ${checkout.status}`);
      if (checkout.body.success) {
        console.log(`✅ Checkout Session created!`);
        console.log(`   URL: ${checkout.body.data.checkout_url}`);
        console.log(`   Session: ${checkout.body.data.session_id}`);
      } else {
        console.log(`❌ Failed: ${checkout.body.error}`);
      }

      // Test 6: Get billing info
      console.log('\n═══ Test 6: Get Billing Info (unauthenticated — should fail with 401) ═══');
      const billing = await makeRequest('GET', `/api/v1/merchants/${testMerchant.id}/billing`);
      console.log(`Status: ${billing.status} — ${billing.status === 401 ? '✅ Correctly requires auth' : '❌ Unexpected response'}`);
    }

    console.log('\n════════════════════════════════════════════');
    console.log('  ALL LOCAL TESTS COMPLETE');
    console.log('════════════════════════════════════════════\n');

  } catch (err) {
    console.error('Test error:', err);
  } finally {
    server.close();
    process.exit(0);
  }
}

runTests();
