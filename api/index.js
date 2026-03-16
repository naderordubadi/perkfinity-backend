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
      return send(res, 200, { ok: true, status: 'healthy', db: 'connected', version: 'test-2026', timestamp: new Date().toISOString() });
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
        INSERT INTO "Merchant" (id, business_name, contact_name, phone, website, subscription_tier, status, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${data.name}, ${data.contactName || ''}, ${data.phone || ''}, ${data.website || ''}, ${data.tier || 'trial'}, 'active', ${now}, ${now})
        RETURNING id, business_name, subscription_tier
      `;

      // Insert owner user
      const [merchantUser] = await sql`
        INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${email}, ${password_hash}, 'owner', 'active', ${now})
        RETURNING id, merchant_id, email, role, status, created_at
      `;

      // Insert location
      const address = data.address || '';
      const suite = data.suite || '';
      await sql`
        INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${address}, ${suite}, ${data.city || ''}, ${data.state || ''}, ${data.zip || ''}, 'US', true, ${now})
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
          qr_url: `https://perkfinity-app.vercel.app/qr/${public_code}`,
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
        SELECT u.*, m.business_name, m.subscription_tier, m.status as merchant_status, m.logo_url
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
      
      const [merchant] = await sql`SELECT id, business_name, logo_url FROM "Merchant" WHERE id = ${qrCode.merchant_id} LIMIT 1`;
      const [location] = await sql`SELECT address, city, state, postal_code FROM "MerchantLocation" WHERE merchant_id = ${qrCode.merchant_id} AND is_active = true LIMIT 1`;
      const [campaign] = await sql`SELECT * FROM "Campaign" WHERE merchant_id = ${qrCode.merchant_id} AND status = 'active' ORDER BY created_at DESC LIMIT 1`;
      
      return send(res, 200, { success: true, data: { qrCode, merchant, location, campaign } });
    }

    // ── GET /api/v1/merchants/:id/profile ─────────────────────────
    const getProfileMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/profile/);
    if (method === 'GET' && getProfileMatch) {
      const merchantId = getProfileMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); } 
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const [merchantData] = await sql`
        SELECT m.business_name, m.contact_name, m.phone, m.website, l.address, l.suite, l.city, l.state, l.postal_code, u.email
        FROM "Merchant" m
        JOIN "MerchantUser" u ON u.merchant_id = m.id
        LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
        WHERE m.id = ${merchantId} AND u.id = ${payload.userId}
        LIMIT 1
      `;
      
      if (!merchantData) return send(res, 404, { success: false, error: 'Profile not found' });
      
      // Fetch associated active QR code
      const [qrData] = await sql`
        SELECT public_code FROM "QrCode"
        WHERE merchant_id = ${merchantId} AND status = 'active'
        LIMIT 1
      `;
      
      // Fetch associated welcome (or active) campaign perk
      const [campaignData] = await sql`
        SELECT title FROM "Campaign"
        WHERE merchant_id = ${merchantId} AND status = 'active'
        ORDER BY created_at ASC
        LIMIT 1
      `;

      merchantData.qr_public_code = qrData ? qrData.public_code : null;
      merchantData.qr_url = qrData ? `https://perkfinity-app.vercel.app/qr/${qrData.public_code}` : null;
      merchantData.perk = campaignData ? campaignData.title : 'Welcome Perk';

      return send(res, 200, { success: true, data: merchantData });
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
      
      // Auto-join merchant member if qrCode was provided during signup
      if (data.qrCode) {
        try {
          const [qrData] = await sql`SELECT merchant_id FROM "QrCode" WHERE public_code = ${data.qrCode}`;
          if (qrData) {
            await sql`
              INSERT INTO "MerchantMember" (id, merchant_id, user_id, created_at)
              VALUES (gen_random_uuid()::text, ${qrData.merchant_id}, ${user.id}, NOW())
              ON CONFLICT DO NOTHING
            `;
          }
        } catch (e) {
          console.error("Optional auto-enrollment failed during signup", e);
        }
      }
      
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
      
      // Auto-join merchant member if qrCode was provided during login
      if (data.qrCode) {
        try {
          const [qrData] = await sql`SELECT merchant_id FROM "QrCode" WHERE public_code = ${data.qrCode}`;
          if (qrData) {
            await sql`
              INSERT INTO "MerchantMember" (id, merchant_id, user_id, created_at)
              VALUES (gen_random_uuid()::text, ${qrData.merchant_id}, ${user.id}, NOW())
              ON CONFLICT DO NOTHING
            `;
          }
        } catch (e) {
          console.error("Optional auto-enrollment failed during login", e);
        }
      }
      
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

    // ── GET /api/v1/consumers/campaigns ───────────────────────────
    if (method === 'GET' && url.endsWith('/consumers/campaigns')) {
       // Optional: This could be protected, but since it's just available merchants/campaigns, 
       // keeping it public or semi-public is often fine. Here we assume we just return 
       // active merchants and their active campaigns.
       
       const campaigns = await sql`
         SELECT 
           c.id, c.title as discount, c.merchant_id, 
           m.business_name as merchant_name, m.logo_url,
           l.postal_code as zip_code,
           q.public_code as qr_code
         FROM "Campaign" c
         JOIN "Merchant" m ON m.id = c.merchant_id
         LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id
         LEFT JOIN "QrCode" q ON q.merchant_id = m.id AND q.status = 'active'
         WHERE c.status = 'active' AND m.status = 'active'
       `;
       
       return send(res, 200, { success: true, data: campaigns });
    }

    // ── GET /api/v1/consumers/history ─────────────────────────────
    if (method === 'GET' && url.endsWith('/consumers/history')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const history = await sql`
        SELECT
          r.id,
          r.token,
          r.issued_at,
          r.expires_at,
          r.redeemed,
          r.redeemed_at,
          c.title as campaign_title,
          m.business_name as merchant_name
        FROM "Redemption" r
        JOIN "Campaign" c ON c.id = r.campaign_id
        JOIN "Merchant" m ON m.id = c.merchant_id
        WHERE r.user_id = ${payload.userId}
        ORDER BY r.issued_at DESC
      `;

      return send(res, 200, { success: true, data: history });
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
      
      // Auto-join merchant member
      const [campaign] = await sql`SELECT merchant_id FROM "Campaign" WHERE id = ${campaignId}`;
      if (campaign) {
        await sql`
           INSERT INTO "MerchantMember" (id, merchant_id, user_id, created_at)
           VALUES (gen_random_uuid()::text, ${campaign.merchant_id}, ${payload.userId}, NOW())
           ON CONFLICT DO NOTHING
        `;
      }
      
      const [redemption] = await sql`
        INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed)
        VALUES (gen_random_uuid()::text, ${payload.userId}, ${campaignId}, ${code}, NOW(), NOW() + INTERVAL '5 minutes', false)
        RETURNING *
      `;
      return send(res, 201, { success: true, data: { activation: redemption } });
    }

    // ── POST /api/v1/campaigns/redeem ──────────────────────────────
    if (method === 'POST' && url.endsWith('/campaigns/redeem')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); } 
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      
      const data = req.body || {};
      if (!data.token) return send(res, 400, { success: false, error: 'Missing redemption token' });
      
      const [existing] = await sql`SELECT * FROM "Redemption" WHERE token = ${data.token} AND user_id = ${payload.userId}`;
      if (!existing) return send(res, 404, { success: false, error: 'Redemption token not found' });
      if (existing.redeemed) return send(res, 400, { success: false, error: 'Offer already redeemed' });
      if (new Date(existing.expires_at) < new Date()) return send(res, 400, { success: false, error: 'Offer expired' });
      
      // Allow manual consumer redemption (saving merchant_user_id as null because it was a self-serve redemption)
      const [updated] = await sql`
        UPDATE "Redemption"
        SET redeemed = true, redeemed_at = NOW()
        WHERE id = ${existing.id}
        RETURNING *
      `;
      
      return send(res, 200, { success: true, data: { redemption: updated } });
    }

    // ── POST /api/v1/merchants/:id/logo ───────────────────────────
    const logoMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/logo/);
    if (method === 'POST' && logoMatch) {
      const merchantId = logoMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); } 
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });
      
      const data = req.body || {};
      if (!data.logo_url) return send(res, 400, { success: false, error: 'Missing logo_url' });
      
      await sql`UPDATE "Merchant" SET logo_url = ${data.logo_url} WHERE id = ${merchantId}`;
      return send(res, 200, { success: true, message: 'Logo successfully updated' });
    }

    // ── GET /api/v1/merchants/:id/members ─────────────────────────
    const membersMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/members/);
    if (method === 'GET' && membersMatch) {
      const merchantId = membersMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); } 
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });
      
      const membersResult = await sql`
        SELECT 
          u.id as user_id, u.city, u.zip_code, u.full_name,
          COALESCE(
            json_agg(
              json_build_object(
                'id', COALESCE(r.id, c.id),
                'campaign_title', c.title,
                'token', r.token,
                'status', CASE 
                  WHEN r.id IS NULL THEN 'Created'
                  WHEN r.redeemed = true THEN 'Redeemed'
                  WHEN r.expires_at < NOW() THEN 'Expired'
                  ELSE 'Pending'
                END
              )
            ) FILTER (WHERE c.id IS NOT NULL), '[]'
          ) as promotions
        FROM "MerchantMember" mm
        JOIN "User" u ON u.id = mm.user_id
        LEFT JOIN "Campaign" c ON c.merchant_id = mm.merchant_id AND c.status = 'active'
        LEFT JOIN "Redemption" r ON r.campaign_id = c.id AND r.user_id = u.id
        WHERE mm.merchant_id = ${merchantId}
        GROUP BY u.id, u.city, u.zip_code, u.full_name
      `;
      
      return send(res, 200, { success: true, data: membersResult });
    }

    // ── PUT /api/v1/merchants/:id/profile ─────────────────────────
    const profileMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/profile/);
    if (method === 'PUT' && profileMatch) {
      const merchantId = profileMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); } 
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });
      
      const data = req.body || {};
      if (!data.current_password) return send(res, 400, { success: false, error: 'Current password is required to save changes' });
      
      // Update Password & Email (MerchantUser Table)
      const [user] = await sql`SELECT * FROM "MerchantUser" WHERE id = ${payload.userId} LIMIT 1`;
      if (!user || !(await bcrypt.compare(data.current_password, user.password_hash))) {
        return send(res, 401, { success: false, error: 'Incorrect current password' });
      }

      let newEmail = user.email;
      if (data.email && data.email.toLowerCase() !== user.email) {
         newEmail = data.email.toLowerCase();
         // Check if email already used
         const existing = await sql`SELECT id FROM "MerchantUser" WHERE email = ${newEmail} LIMIT 1`;
         if (existing.length > 0) return send(res, 400, { success: false, error: 'Email already in use' });
      }

      let newHash = user.password_hash;
      if (data.new_password && data.new_password.length >= 8) {
         newHash = await bcrypt.hash(data.new_password, 12);
      } else if (data.new_password && data.new_password.length > 0) {
         return send(res, 400, { success: false, error: 'New password must be at least 8 characters' });
      }

      await sql`
        UPDATE "MerchantUser" 
        SET email = ${newEmail}, password_hash = ${newHash} 
        WHERE id = ${payload.userId}
      `;

      // Update Merchant Details
      if (data.business_name || data.contact_name || data.phone || data.website !== undefined) {
         await sql`
           UPDATE "Merchant" 
           SET 
             business_name = COALESCE(${data.business_name}, business_name),
             contact_name = COALESCE(${data.contact_name}, contact_name),
             phone = COALESCE(${data.phone}, phone),
             website = COALESCE(${data.website}, website)
           WHERE id = ${merchantId}
         `;
      }

      // Update Location Details (assuming 1 location for now per merchant, based on onboarding signup logic)
      if (data.address || data.suite !== undefined || data.city || data.state || data.zip) {
         await sql`
           UPDATE "MerchantLocation" 
           SET 
             address = COALESCE(${data.address}, address),
             suite = COALESCE(${data.suite}, suite),
             city = COALESCE(${data.city}, city),
             state = COALESCE(${data.state}, state),
             postal_code = COALESCE(${data.zip}, postal_code)
           WHERE merchant_id = ${merchantId}
        `;
      }

      return send(res, 200, { success: true, message: 'Profile updated successfully', new_business_name: data.business_name });
    }

    if (url === '/api/v1/update-test-profiles-mission-viejo' && method === 'GET') {
      const addresses = ["1", "2", "3", "4", "5"];
      for (const num of addresses) {
        const email = `ryan.mission.viejo${num}@gmail.com`;
        const fullName = `Ryan Testuser ${num}`;
        await sql`
          UPDATE "User"
          SET full_name = ${fullName}, city = 'Mission Viejo', zip_code = '92692'
          WHERE email = ${email}
        `;
      }
      return send(res, 200, { success: true, message: "Profiles successfully updated." });
    }

    if (url === '/api/v1/migrate-task3' && method === 'GET') {
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "contact_name" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "phone" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "website" TEXT`;
      await sql`ALTER TABLE "MerchantLocation" ADD COLUMN IF NOT EXISTS "suite" TEXT`;
      return send(res, 200, { success: true, message: "Task 3 DB fields added!" });
    }

    if (url === '/api/v1/migrate-task2' && method === 'GET') {
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "logo_url" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "city" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "zip_code" TEXT`;
      await sql`
        CREATE TABLE IF NOT EXISTS "MerchantMember" (
          id TEXT DEFAULT gen_random_uuid()::text PRIMARY KEY,
          merchant_id TEXT NOT NULL REFERENCES "Merchant"("id") ON DELETE CASCADE,
          user_id TEXT NOT NULL REFERENCES "User"("id") ON DELETE CASCADE,
          created_at TIMESTAMP DEFAULT NOW(),
          UNIQUE(merchant_id, user_id)
        )
      `;
      return send(res, 200, { success: true, message: "Task 2 DB migrated!" });
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
