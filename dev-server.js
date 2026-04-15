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
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
    req.body = await parseBody(req);
  } else {
    req.body = {};
  }
  handler(req, res);
});

server.listen(PORT, () => {
  console.log(`\n✅ Perkfinity local backend running at http://localhost:${PORT}`);
  console.log(`   DB: ${process.env.DATABASE_URL ? process.env.DATABASE_URL.split('@')[1]?.split('/')[0] || 'SET' : '⚠️  MISSING'}`);
  console.log(`   Health: http://localhost:${PORT}/health\n`);
});

server.on('error', (err) => {
  console.error('❌ Server error:', err.message);
  process.exit(1);
});
