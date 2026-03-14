// Minimal test — ZERO external dependencies
// If this works, the issue is with @prisma/client or env vars
// If this fails too, the Vercel project settings are wrong

module.exports = function handler(req, res) {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.statusCode = 200;
  res.end(JSON.stringify({
    ok: true,
    message: 'Vercel function is running',
    url: req.url,
    method: req.method,
    env_check: {
      DATABASE_URL: process.env.DATABASE_URL ? 'SET' : 'MISSING',
      JWT_SECRET: process.env.JWT_SECRET ? 'SET' : 'MISSING',
      NODE_ENV: process.env.NODE_ENV || 'not set',
    }
  }));
};
