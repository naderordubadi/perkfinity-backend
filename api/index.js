/**
 * Perkfinity Backend — Vercel Serverless + Neon
 * Uses @neondatabase/serverless: HTTP-based, no TCP, no build step, works everywhere.
 */

const { neon } = require('@neondatabase/serverless');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

const ALLOWED_ORIGINS = [
  'https://perkfinity.net',
  'https://www.perkfinity.net',
  'null', // Allows local file:// based HTML testing
];

function setCors(req, res) {
  const origin = req.headers.origin;
  const isAllowed = ALLOWED_ORIGINS.includes(origin) || (origin && origin.startsWith('http://localhost:'));
  res.setHeader('Access-Control-Allow-Origin', isAllowed ? origin : '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, Idempotency-Key');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
}

function send(res, status, data) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(data));
}

module.exports = async function handler(req, res) {
  setCors(req, res);
  if (req.method === 'OPTIONS') { res.statusCode = 204; return res.end(); }

  const url = (req.url || '/').split('?')[0];
  const method = req.method;

  try {
    const DATABASE_URL = process.env.DATABASE_URL;
    if (!DATABASE_URL) {
      return send(res, 500, { success: false, error: 'DATABASE_URL is not set in Vercel environment variables' });
    }
    const sql = neon(DATABASE_URL);

    // ── Health check ──────────────────────────────────────────────
    if (method === 'GET' && (url === '/' || url === '/health' || url.endsWith('/health'))) {
      await sql`SELECT 1`;
      return send(res, 200, { ok: true, status: 'healthy', db: 'connected', timestamp: new Date().toISOString() });
    }

    // ── POST /api/v1/merchants/signup ─────────────────────────────
    if (method === 'POST' && url.endsWith('/merchants/signup')) {
      const data = req.body || {};
      if (!data.name || !data.email || !data.password) {
        return send(res, 400, { success: false, error: 'Missing required fields: name, email, password' });
      }

      const email = data.email.toLowerCase();

      // Check duplicate
      const existing = await sql`SELECT id FROM "MerchantUser" WHERE email = ${email} LIMIT 1`;
      if (existing.length > 0) {
        return send(res, 400, { success: false, error: 'A merchant with this email already exists.' });
      }

      const password_hash = await bcrypt.hash(data.password, 12);
      const now = new Date();
      const oneYear = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);

      // Insert merchant
      const [merchant] = await sql`
        INSERT INTO "Merchant" (id, business_name, subscription_tier, status, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${data.name}, ${data.tier || 'trial'}, 'active', ${now}, ${now})
        RETURNING id, business_name, subscription_tier
      `;

      // Insert owner user
      const [merchantUser] = await sql`
        INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${email}, ${password_hash}, 'owner', 'active', ${now})
        RETURNING id, merchant_id, email, role, status, created_at
      `;

      // Insert location
      const address = `${data.address || ''}${data.suite ? ', ' + data.suite : ''}`.trim();
      await sql`
        INSERT INTO "MerchantLocation" (id, merchant_id, address, city, state, postal_code, country, is_active, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${address}, ${data.city || ''}, ${data.state || ''}, ${data.zip || ''}, 'US', true, ${now})
      `;

      // Insert welcome campaign
      await sql`
        INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, status, start_at, end_at, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.perk || 'Welcome Perk'}, 10, 'active', ${now}, ${oneYear}, ${now}, ${now})
      `;

      // Insert QR code
      const public_code = crypto.randomBytes(9).toString('base64url');
      await sql`
        INSERT INTO "QrCode" (id, merchant_id, public_code, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${public_code}, 'active', ${now})
      `;

      const JWT_SECRET = process.env.JWT_SECRET;
      if (!JWT_SECRET) return send(res, 500, { success: false, error: 'JWT_SECRET not configured' });

      const accessToken = jwt.sign(
        { userId: merchantUser.id, merchantId: merchant.id, role: merchantUser.role },
        JWT_SECRET,
        { expiresIn: '8h' }
      );

      return send(res, 201, {
        success: true,
        data: {
          merchant,
          merchantUser,
          accessToken,
          qr_public_code: public_code,
          qr_url: `https://app.perkfinity.net/qr/${public_code}`,
        }
      });
    }

    // ── POST /api/v1/auth/login ────────────────────────────────────
    if (method === 'POST' && (url.endsWith('/auth/login') || url.endsWith('/merchants/login'))) {
      const data = req.body || {};
      if (!data.email || !data.password) {
        return send(res, 400, { success: false, error: 'email and password are required' });
      }

      const [user] = await sql`
        SELECT u.*, m.business_name, m.subscription_tier, m.status as merchant_status
        FROM "MerchantUser" u
        JOIN "Merchant" m ON m.id = u.merchant_id
        WHERE u.email = ${data.email.toLowerCase()}
        LIMIT 1
      `;

      if (!user || !(await bcrypt.compare(data.password, user.password_hash))) {
        return send(res, 401, { success: false, error: 'Invalid email or password' });
      }

      const JWT_SECRET = process.env.JWT_SECRET;
      const accessToken = jwt.sign(
        { userId: user.id, merchantId: user.merchant_id, role: user.role },
        JWT_SECRET,
        { expiresIn: '8h' }
      );

      const { password_hash: _pw, ...safeUser } = user;
      return send(res, 200, { success: true, data: { merchantUser: safeUser, accessToken } });
    }

    // ── GET /api/v1/migrate-users (TEMPORARY DB PRE-FLIGHT) ───────
    if (url === '/api/v1/migrate-users' && method === 'GET') {
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "email" TEXT UNIQUE`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "password_hash" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "full_name" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "phone_number" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "city" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "zip_code" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "location_sharing_enabled" BOOLEAN DEFAULT false`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "push_notifications_enabled" BOOLEAN DEFAULT false`;
      return send(res, 200, { success: true, message: "User table fully migrated!" });
    }

    // ── GET /api/v1/qr/resolve/:code ──────────────────────────────
    const qrMatch = url.match(/\/api\/v1\/qr\/resolve\/([a-zA-Z0-9_-]+)/);
    if (method === 'GET' && qrMatch) {
      const public_code = qrMatch[1];
      const [qrCode] = await sql`SELECT * FROM "QrCode" WHERE public_code = ${public_code} AND status = 'active' LIMIT 1`;
      if (!qrCode) return send(res, 404, { success: false, error: 'QR code not found or inactive' });
      
      const [merchant] = await sql`SELECT id, business_name FROM "Merchant" WHERE id = ${qrCode.merchant_id} LIMIT 1`;
      const [location] = await sql`SELECT address, city, state, postal_code FROM "MerchantLocation" WHERE merchant_id = ${qrCode.merchant_id} AND is_active = true LIMIT 1`;
      const [campaign] = await sql`SELECT * FROM "Campaign" WHERE merchant_id = ${qrCode.merchant_id} AND status = 'active' ORDER BY created_at DESC LIMIT 1`;
      
      return send(res, 200, { success: true, data: { qrCode, merchant, location, campaign } });
    }

    // ── POST /api/v1/consumers/signup ─────────────────────────────
    if (method === 'POST' && url.endsWith('/consumers/signup')) {
      const data = req.body || {};
      if (!data.email || !data.password) return send(res, 400, { success: false, error: 'Missing email or password' });
      
      const existing = await sql`SELECT id FROM "User" WHERE email = ${data.email.toLowerCase()} LIMIT 1`;
      if (existing.length > 0) return send(res, 400, { success: false, error: 'User already exists' });
      
      const hash = await bcrypt.hash(data.password, 12);
      const [user] = await sql`
        INSERT INTO "User" (id, email, password_hash, created_at, last_active)
        VALUES (gen_random_uuid()::text, ${data.email.toLowerCase()}, ${hash}, NOW(), NOW())
        RETURNING id, email
      `;
      
      const JWT_SECRET = process.env.JWT_SECRET;
      const token = jwt.sign({ userId: user.id, role: 'consumer' }, JWT_SECRET, { expiresIn: '30d' });
      return send(res, 201, { success: true, data: { user, accessToken: token } });
    }

    // ── POST /api/v1/consumers/login ──────────────────────────────
    if (method === 'POST' && url.endsWith('/consumers/login')) {
      const data = req.body || {};
      if (!data.email || !data.password) return send(res, 400, { success: false, error: 'Missing credentials' });
      
      const [user] = await sql`SELECT * FROM "User" WHERE email = ${data.email.toLowerCase()} LIMIT 1`;
      if (!user || !(await bcrypt.compare(data.password, user.password_hash))) {
        return send(res, 401, { success: false, error: 'Invalid credentials' });
      }
      
      await sql`UPDATE "User" SET last_active = NOW() WHERE id = ${user.id}`;
      const JWT_SECRET = process.env.JWT_SECRET;
      const token = jwt.sign({ userId: user.id, role: 'consumer' }, JWT_SECRET, { expiresIn: '30d' });
      const { password_hash: _pw, ...safeUser } = user;
      return send(res, 200, { success: true, data: { user: safeUser, accessToken: token } });
    }

    // ── PUT /api/v1/consumers/profile ─────────────────────────────
    if (method === 'PUT' && url.endsWith('/consumers/profile')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); } 
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      
      const data = req.body || {};
      const [user] = await sql`
        UPDATE "User"
        SET full_name = COALESCE(${data.full_name}, full_name),
            phone_number = COALESCE(${data.phone_number}, phone_number),
            city = COALESCE(${data.city}, city),
            zip_code = COALESCE(${data.zip_code}, zip_code),
            location_sharing_enabled = COALESCE(${data.location_sharing_enabled}, location_sharing_enabled),
            push_notifications_enabled = COALESCE(${data.push_notifications_enabled}, push_notifications_enabled),
            last_active = NOW()
        WHERE id = ${payload.userId}
        RETURNING id, email, full_name, phone_number, city, zip_code, location_sharing_enabled, push_notifications_enabled
      `;
      return send(res, 200, { success: true, data: { user } });
    }

    // ── POST /api/v1/campaigns/:id/activate ───────────────────────
    const activateMatch = url.match(/\/api\/v1\/campaigns\/([a-zA-Z0-9_-]+)\/activate/);
    if (method === 'POST' && activateMatch) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); } 
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      
      const campaignId = activateMatch[1];
      const code = crypto.randomBytes(3).toString('hex').toUpperCase(); // 6 chars
      
      const [redemption] = await sql`
        INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed)
        VALUES (gen_random_uuid()::text, ${payload.userId}, ${campaignId}, ${code}, NOW(), NOW() + INTERVAL '5 minutes', false)
        RETURNING *
      `;
      return send(res, 201, { success: true, data: { activation: redemption } });
    }

    return send(res, 404, { success: false, error: `No route: ${method} ${url}` });

  } catch (err) {
    console.error('[perkfinity]', err.message);
    return send(res, 500, {
      success: false,
      error: err.message,
      _env: {
        DATABASE_URL: process.env.DATABASE_URL ? 'SET' : 'MISSING',
        JWT_SECRET: process.env.JWT_SECRET ? 'SET' : 'MISSING',
      }
    });
  }
};
