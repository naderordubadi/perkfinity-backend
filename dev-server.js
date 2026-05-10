/**
 * Local development server wrapper for api/index.js
 * Wraps the Vercel serverless handler in a standard Node HTTP server.
 * Run with: npm run dev
 */

require('dotenv').config();
const http = require('http');
const handler = require('./api/index.js');

const PORT = process.env.PORT || 3001;

// Vercel auto-parses req.body — we must do it manually for local Node HTTP.
function parseBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', chunk => { data += chunk; });
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch {
        resolve({});
      }
    });
    req.on('error', () => resolve({}));
  });
}

const server = http.createServer(async (req, res) => {
  const start = Date.now();

  // ── Stripe webhook: route to dedicated handler, skip body pre-read.
  // The webhook handler reads the raw stream itself (needed for signature verification).
  if (req.url === '/api/webhooks/stripe' || req.url.startsWith('/api/webhooks/stripe?')) {
    console.log(`\n→ ${req.method} ${req.url} [STRIPE WEBHOOK]`);
    // Add Express-like shims that the webhook handler uses
    res.status = (code) => { res.statusCode = code; return res; };
    res.json   = (data) => { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(data)); };
    try {
      const webhookHandler = require('./api/webhooks/stripe.js');
      await webhookHandler(req, res);
    } catch (err) {
      console.error('Stripe webhook handler error:', err);
      res.statusCode = 500;
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
    req.body = await parseBody(req);
  } else {
    req.body = {};
  }

  // Log request
  console.log(`\n→ ${req.method} ${req.url}`);
  if (req.body && Object.keys(req.body).length) console.log('  body:', JSON.stringify(req.body));

  // Intercept res.end to log response
  const origEnd = res.end.bind(res);
  res.end = function(chunk, ...args) {
    const ms = Date.now() - start;
    try {
      const body = chunk ? JSON.parse(chunk.toString()) : {};
      if (!body.success) {
        console.log(`← ${res.statusCode} (${ms}ms) ERROR:`, JSON.stringify(body));
      } else {
        console.log(`← ${res.statusCode} (${ms}ms) OK`);
      }
    } catch { console.log(`← ${res.statusCode} (${ms}ms)`); }
    return origEnd(chunk, ...args);
  };

  handler(req, res);
});

server.listen(PORT, () => {
  console.log(`\n✅ Perkfinity local backend running at http://localhost:${PORT}`);
  console.log(`   DB: ${process.env.DATABASE_URL ? process.env.DATABASE_URL.split('@')[1]?.split('/')[0] || 'SET' : '⚠️  MISSING'}`);
  console.log(`   Stripe webhooks → http://localhost:${PORT}/api/webhooks/stripe`);
  console.log(`   Health: http://localhost:${PORT}/health\n`);
});

server.on('error', (err) => {
  console.error('❌ Server error:', err.message);
  process.exit(1);
});
