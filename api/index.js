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
  'capacitor://localhost',   // Capacitor iOS WKWebView origin
  'https://localhost',       // Capacitor iOS fallback
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

async function autoEnrollUser(sql, userId, publicCode) {
  if (!publicCode || !userId) return;
  try {
    const [qrData] = await sql`SELECT merchant_id FROM "QrCode" WHERE public_code = ${publicCode} AND status = 'active'`;
    if (!qrData) return;
    
    // 1. Add to member list
    await sql`
      INSERT INTO "MerchantMember" (id, merchant_id, user_id, created_at)
      VALUES (gen_random_uuid()::text, ${qrData.merchant_id}, ${userId}, NOW())
      ON CONFLICT DO NOTHING
    `;

    // 2. Assign all missing active campaigns as 'created' (Pending Offers)
    await sql`
      INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed, status)
      SELECT gen_random_uuid()::text, ${userId}, c.id, gen_random_uuid()::text, NOW(), c.end_at, false, 'created'
      FROM "Campaign" c
      WHERE c.merchant_id = ${qrData.merchant_id}
        AND c.status = 'active'
        AND c.end_at > NOW()
        AND c.discount_percentage >= 0
        AND NOT EXISTS (
          SELECT 1 FROM "Redemption" r2 
          WHERE r2.campaign_id = c.id 
            AND r2.user_id = ${userId}
        )
    `;
  } catch (e) {
    console.error("Auto-enrollment failed during auth", e);
  }
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

    // ── One-time migration: campaign_type column ───────────────────
    // NOTE: status column on Redemption already exists in production.
    // Do NOT bulk-update statuses here — it runs on every Vercel cold start
    // and would revert valid 'pending' rows back to 'created'.
    if (!global._campaignTypeMigrated) {
      try {
        await sql`ALTER TABLE "Campaign" ADD COLUMN IF NOT EXISTS campaign_type TEXT DEFAULT 'perk'`;
        await sql`UPDATE "Campaign" SET campaign_type='announcement' WHERE discount_percentage = -1 AND campaign_type IS DISTINCT FROM 'announcement'`;
        global._campaignTypeMigrated = true;
      } catch (migErr) { /* column may already exist or non-critical */ }
    }

    // ── One-time migration: add social login columns to User ──────
    if (!global._userSocialMigrated) {
      try {
        await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS apple_sub TEXT UNIQUE`;
        await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS google_sub TEXT UNIQUE`;
        global._userSocialMigrated = true;
      } catch (migErr) { /* columns may already exist */ }
    }


    // ── Health check ──────────────────────────────────────────────
    if (method === 'GET' && (url === '/' || url === '/health' || url.endsWith('/health'))) {
      await sql`SELECT 1`;
      return send(res, 200, { ok: true, status: 'healthy', db: 'connected', version: 'test-2026', timestamp: new Date().toISOString() });
    }

    // ── POST /api/v1/merchants/signup ─────────────────────────────
    if (method === 'POST' && url.endsWith('/merchants/signup')) {
      const data = req.body || {};

      // Validate all required fields
      const missing = [];
      if (!data.name)        missing.push('Store Name');
      if (!data.contactName) missing.push('Contact Name');
      if (!data.phone)       missing.push('Phone Number');
      if (!data.email)       missing.push('Email');
      if (!data.password)    missing.push('Password');
      if (!data.address)     missing.push('Street Address');
      if (!data.city)        missing.push('City');
      if (!data.state)       missing.push('State');
      if (!data.zip)         missing.push('ZIP Code');

      if (missing.length > 0) {
        return send(res, 400, { success: false, error: `Missing required fields: ${missing.join(', ')}` });
      }

      // Validate formats
      const phoneRegex = /^\d{3}-\d{3}-\d{4}$/;
      if (!phoneRegex.test(data.phone)) {
        return send(res, 400, { success: false, error: 'Phone number must be in xxx-xxx-xxxx format.' });
      }
      const zipRegex = /^\d{5}$/;
      if (!zipRegex.test(data.zip)) {
        return send(res, 400, { success: false, error: 'ZIP Code must be a 5-digit number.' });
      }
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(data.email)) {
        return send(res, 400, { success: false, error: 'Please provide a valid email address.' });
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

      // Insert merchant (required fields used directly; optional fields use || '')
      const [merchant] = await sql`
        INSERT INTO "Merchant" (id, business_name, contact_name, phone, website, subscription_tier, status, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${data.name}, ${data.contactName}, ${data.phone}, ${data.website || ''}, ${data.tier || 'trial'}, 'active', ${now}, ${now})
        RETURNING id, business_name, subscription_tier
      `;

      // Insert owner user
      const [merchantUser] = await sql`
        INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${email}, ${password_hash}, 'owner', 'active', ${now})
        RETURNING id, merchant_id, email, role, status, created_at
      `;

      // Insert location (required fields direct; suite is optional)
      await sql`
        INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.address}, ${data.suite || ''}, ${data.city}, ${data.state}, ${data.zip}, 'US', true, ${now})
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

    // ── GET /api/v1/debug/status (TEMPORARY — remove after verification) ──
    if (url.endsWith('/debug/status')) {
      const rs = await sql`SELECT id, user_id, campaign_id, status, redeemed, expires_at, issued_at FROM "Redemption" ORDER BY issued_at DESC LIMIT 20`;
      return send(res, 200, { success: true, data: rs });
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

    // ── GET /api/v1/merchants/search?zip=XXXXX ────────────────────
    if (method === 'GET' && url.startsWith('/api/v1/merchants/search')) {
      // NOTE: `url` at line 36 strips the query string, so parse from req.url directly
      const qs = (req.url || '').split('?')[1] || '';
      const zipParam = new URLSearchParams(qs).get('zip');
      if (!zipParam || !/^\d{5}$/.test(zipParam.trim())) {
        return send(res, 400, { success: false, error: 'Please provide a valid 5-digit ZIP code.' });
      }
      const zip = zipParam.trim();

      // Correlated subqueries: one row per merchant, no DISTINCT ON row-multiplication risk
      const merchants = await sql`
        SELECT
          m.id,
          m.business_name,
          m.logo_url,
          l.address,
          l.city,
          l.state,
          l.postal_code,
          (SELECT c.title
             FROM "Campaign" c
            WHERE c.merchant_id = m.id
              AND c.status = 'active'
              AND c.discount_percentage >= 0
            ORDER BY c.created_at ASC
            LIMIT 1) AS welcome_perk,
          (SELECT q.public_code
             FROM "QrCode" q
            WHERE q.merchant_id = m.id
              AND q.status = 'active'
            LIMIT 1) AS public_code
        FROM "Merchant" m
        JOIN "MerchantLocation" l
          ON l.merchant_id = m.id
         AND l.is_active = true
        WHERE TRIM(l.postal_code) = TRIM(${zip})
        ORDER BY m.business_name ASC
      `;

      return send(res, 200, { success: true, zip, count: merchants.length, data: merchants });
    }

    // ── GET /api/v1/qr/resolve/:code ──────────────────────────────
    const qrMatch = url.match(/\/api\/v1\/qr\/resolve\/([a-zA-Z0-9_-]+)/);

    if (method === 'GET' && qrMatch) {
      const public_code = qrMatch[1];
      const [qrCode] = await sql`SELECT * FROM "QrCode" WHERE public_code = ${public_code} AND status = 'active' LIMIT 1`;
      if (!qrCode) return send(res, 404, { success: false, error: 'QR code not found or inactive' });

      const [merchant] = await sql`SELECT id, business_name, logo_url FROM "Merchant" WHERE id = ${qrCode.merchant_id} LIMIT 1`;
      const [location] = await sql`SELECT address, city, state, postal_code FROM "MerchantLocation" WHERE merchant_id = ${qrCode.merchant_id} AND is_active = true LIMIT 1`;

      let campaigns = [];

      // If user is authenticated, return ONLY their assigned campaigns for this merchant
      const authHeader = req.headers.authorization;
      if (authHeader && authHeader.startsWith('Bearer ')) {
        try {
          const decoded = jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET);
          const userId = decoded.userId;
          // Auto-enroll the user into the merchant's member list if they aren't already
          await sql`
            INSERT INTO "MerchantMember" (id, merchant_id, user_id, created_at)
            VALUES (gen_random_uuid()::text, ${qrCode.merchant_id}, ${userId}, NOW())
            ON CONFLICT DO NOTHING
          `;

          // Auto-assign any active campaigns for this merchant that the user doesn't already have
          await sql`
            INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed, status)
            SELECT gen_random_uuid()::text, ${userId}, c.id, gen_random_uuid()::text, NOW(), c.end_at, false, 'created'
            FROM "Campaign" c
            WHERE c.merchant_id = ${qrCode.merchant_id}
              AND c.status = 'active'
              AND c.end_at > NOW()
              AND c.discount_percentage >= 0
              AND NOT EXISTS (
                SELECT 1 FROM "Redemption" r2 
                WHERE r2.campaign_id = c.id 
                  AND r2.user_id = ${userId}
              )
          `;

          // Find Redemption rows for this user + this merchant that are in 'created' status
          // (assigned to user, not yet activated — 'created' is the canonical pending state)
          const memberCampaigns = await sql`
            SELECT c.id, c.title, c.discount_percentage, c.terms, c.status as campaign_status,
                   c.start_at, c.end_at,
                   r.id as redemption_id, r.token, r.expires_at as redemption_expires_at,
                   r.redeemed, r.status as redemption_status
            FROM "Redemption" r
            JOIN "Campaign" c ON c.id = r.campaign_id
            WHERE r.user_id = ${userId}
              AND c.merchant_id = ${qrCode.merchant_id}
              AND r.status = 'created'
              AND r.redeemed = false
              AND c.status = 'active'
              AND c.end_at > NOW()
              AND c.discount_percentage >= 0
            ORDER BY c.created_at ASC
          `;
          // Remap so frontend sees c.status field as usual
          campaigns = memberCampaigns.map(row => ({ ...row, status: row.campaign_status }));
        } catch (jwtErr) {
          // Token invalid or expired — fall through to public campaigns
        }
      }

      // Fallback for unauthenticated or if member has no assigned campaigns:
      // return all active campaigns for this merchant so the QR page can still show something
      if (campaigns.length === 0) {
        campaigns = await sql`
          SELECT id, title, discount_percentage, terms, status, start_at, end_at
          FROM "Campaign"
          WHERE merchant_id = ${qrCode.merchant_id}
            AND status = 'active'
            AND end_at > NOW()
            AND discount_percentage >= 0
          ORDER BY created_at DESC
          LIMIT 5
        `;
      }

      return send(res, 200, { success: true, data: { qrCode, merchant, location, campaigns } });
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
        WHERE merchant_id = ${merchantId}
          AND status = 'active'
          AND discount_percentage >= 0
        ORDER BY created_at ASC
        LIMIT 1
      `;

      merchantData.qr_public_code = qrData ? qrData.public_code : null;
      merchantData.qr_url = qrData ? `https://perkfinity-app.vercel.app/qr/${qrData.public_code}` : null;
      merchantData.perk = campaignData ? campaignData.title : 'Welcome Perk';

      return send(res, 200, { success: true, data: merchantData });
    }

    // ── POST /api/v1/merchants/:id/promotions ──────────────────────
    const promoMatch = url.match(/^\/api\/v1\/merchants\/([^/]+)\/promotions$/);
    if (method === 'POST' && promoMatch) {
      const auth = req.headers.authorization;
      if (!auth || !auth.startsWith('Bearer ')) return send(res, 401, { success: false, error: 'Unauthorized' });
      const JWT_SECRET = process.env.JWT_SECRET;
      let decoded;
      try { decoded = jwt.verify(auth.split(' ')[1], JWT_SECRET); } catch(e) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const targetMerchantId = promoMatch[1];
      if (decoded.merchantId !== targetMerchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const data = req.body || {};
      if (!data.title || !data.type || !data.delivery || !data.audience) {
        return send(res, 400, { success: false, error: 'Missing required fields: title, type, delivery, audience' });
      }

      const now = new Date();
      const expiresAt = data.expires_at ? new Date(data.expires_at) : new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

      // Save as a Campaign so it appears in campaign history.
      // Announcements use discount_percentage = -1 as a permanent type marker
      // so they can be filtered out anywhere Redemption rows are not sufficient.
      const [campaign] = await sql`
        INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, terms, status, start_at, end_at, campaign_type, created_at, updated_at)
        VALUES (
          gen_random_uuid()::text,
          ${targetMerchantId},
          ${data.title},
          ${data.type === 'announcement' ? -1 : 0},
          ${data.condition_detail || ''},
          'active',
          ${now},
          ${expiresAt},
          ${data.type === 'announcement' ? 'announcement' : 'perk'},
          ${now},
          ${now}
        )
        RETURNING id, title, status, start_at, end_at
      `;

      // ── Audience-based Redemption creation (status='created') ────
      // We create Redemption rows for all campaigns, including announcements,
      // so they appear in the merchant member list. The app filters out announcements from the activate UI.
      let qualifyingUsers = [];
      const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
      const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);

      if (data.audience === 'all') {
        qualifyingUsers = await sql`
          SELECT DISTINCT mm.user_id FROM "MerchantMember" mm WHERE mm.merchant_id = ${targetMerchantId}
        `;
      } else if (data.audience === 'redeemed_30') {
        qualifyingUsers = await sql`
          SELECT DISTINCT r.user_id FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          WHERE c.merchant_id = ${targetMerchantId}
            AND (r.status = 'redeemed' OR r.redeemed = true)
            AND r.redeemed_at >= ${thirtyDaysAgo}
        `;
      } else if (data.audience === 'expired_30') {
        qualifyingUsers = await sql`
          SELECT DISTINCT r.user_id FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          WHERE c.merchant_id = ${targetMerchantId}
            AND (r.status = 'expired' OR (r.redeemed = false AND r.expires_at < NOW() AND r.expires_at >= ${thirtyDaysAgo} AND COALESCE(r.status,'pending') != 'created'))
        `;
      } else if (data.audience === 'never_redeemed') {
        qualifyingUsers = await sql`
          SELECT mm.user_id FROM "MerchantMember" mm
          WHERE mm.merchant_id = ${targetMerchantId}
            AND NOT EXISTS (
              SELECT 1 FROM "Redemption" r2 JOIN "Campaign" c2 ON c2.id = r2.campaign_id
              WHERE c2.merchant_id = ${targetMerchantId} AND r2.user_id = mm.user_id
                AND r2.status IN ('pending', 'redeemed', 'expired')
            )
        `;
      } else if (data.audience === 'redeemed_90') {
        qualifyingUsers = await sql`
          SELECT DISTINCT r.user_id FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          WHERE c.merchant_id = ${targetMerchantId}
            AND (r.status = 'redeemed' OR r.redeemed = true)
            AND r.redeemed_at >= ${ninetyDaysAgo}
        `;
      }
      // Create Redemption rows for all qualifying users, including announcements
      // so they appear in the merchant member list. The app filters out announcements from the activate UI.
      let assignedCount = 0;
      for (const u of qualifyingUsers) {
        try {
          await sql`
            INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed, status)
            VALUES (gen_random_uuid()::text, ${u.user_id}, ${campaign.id}, gen_random_uuid()::text, ${now}, ${expiresAt}, false, 'created')
            ON CONFLICT DO NOTHING
          `;
          assignedCount++;
        } catch (insertErr) { /* skip on conflict */ }
      }

      // Save promotion config to AuditLog (single entry, after assignment)
      await sql`
        INSERT INTO "AuditLog" (id, actor_type, actor_id, merchant_id, action, target_type, target_id, metadata, created_at)
        VALUES (
          gen_random_uuid()::text,
          'merchant_user',
          ${decoded.userId},
          ${targetMerchantId},
          'promotion_created',
          'Campaign',
          ${campaign.id},
          ${JSON.stringify({ type: data.type, condition: data.condition, delivery: data.delivery, audience: data.audience, expires_at: expiresAt.toISOString(), assigned_count: assignedCount })}::jsonb,
          ${now}
        )
      `;

      return send(res, 201, { success: true, data: { campaign, assigned_count: assignedCount, message: `Promotion created and assigned to ${assignedCount} member(s) via ${data.delivery}.` } });
    }

    // ── POST /api/v1/consumers/apple-signin ────────────────────────
    if (method === 'POST' && url.endsWith('/consumers/apple-signin')) {
      const data = req.body || {};
      if (!data.identityToken) return send(res, 400, { success: false, error: 'Missing identityToken' });

      // Decode the Apple JWT payload (we trust Apple; full sig verification requires fetching Apple public keys)
      let appleSub, appleEmail;
      try {
        const payloadBase64 = data.identityToken.split('.')[1];
        const payloadJson = Buffer.from(payloadBase64, 'base64').toString('utf8');
        const applePayload = JSON.parse(payloadJson);
        appleSub = applePayload.sub;   // stable unique Apple user ID
        appleEmail = applePayload.email; // only present on first sign-in
      } catch (e) {
        return send(res, 400, { success: false, error: 'Invalid Apple identity token' });
      }

      if (!appleSub) return send(res, 400, { success: false, error: 'Could not extract Apple user ID' });

      const JWT_SECRET = process.env.JWT_SECRET;
      if (!JWT_SECRET) return send(res, 500, { success: false, error: 'JWT_SECRET not configured' });

      // Find existing user by apple_sub, or by email, or create new
      let [user] = await sql`SELECT * FROM "User" WHERE apple_sub = ${appleSub} LIMIT 1`;

      if (!user && appleEmail) {
        [user] = await sql`SELECT * FROM "User" WHERE email = ${appleEmail.toLowerCase()} LIMIT 1`;
        if (user) {
          // Link the Apple sub to the existing email account
          await sql`UPDATE "User" SET apple_sub = ${appleSub} WHERE id = ${user.id}`;
        }
      }

      if (!user) {
        // Create new user
        const email = appleEmail ? appleEmail.toLowerCase() : `apple_${appleSub}@perkfinity.internal`;
        const fullName = data.fullName || '';
        [user] = await sql`
          INSERT INTO "User" (id, email, apple_sub, full_name, created_at, last_active)
          VALUES (gen_random_uuid()::text, ${email}, ${appleSub}, ${fullName}, NOW(), NOW())
          ON CONFLICT (email) DO UPDATE SET apple_sub = ${appleSub}, last_active = NOW()
          RETURNING *
        `;
      } else {
        await sql`UPDATE "User" SET last_active = NOW() WHERE id = ${user.id}`;
      }

      const token = jwt.sign({ userId: user.id, role: 'consumer' }, JWT_SECRET, { expiresIn: '30d' });
      await autoEnrollUser(sql, user.id, data.qrCode);
      const { password_hash: _pw, ...safeUser } = user;
      return send(res, 200, { success: true, data: { user: safeUser, accessToken: token } });
    }

    // ── POST /api/v1/consumers/google-signin ───────────────────────
    if (method === 'POST' && url.endsWith('/consumers/google-signin')) {
      const data = req.body || {};
      if (!data.idToken) return send(res, 400, { success: false, error: 'Missing idToken' });

      let googleSub, googleEmail, googleName;
      try {
        const payloadBase64 = data.idToken.split('.')[1];
        const payloadJson = Buffer.from(payloadBase64, 'base64').toString('utf8');
        const googlePayload = JSON.parse(payloadJson);
        googleSub = googlePayload.sub;
        googleEmail = googlePayload.email;
        googleName = googlePayload.name || '';
      } catch (e) {
        return send(res, 400, { success: false, error: 'Invalid Google ID token' });
      }

      if (!googleSub) return send(res, 400, { success: false, error: 'Could not extract Google user ID' });

      const JWT_SECRET = process.env.JWT_SECRET;
      if (!JWT_SECRET) return send(res, 500, { success: false, error: 'JWT_SECRET not configured' });

      let [user] = await sql`SELECT * FROM "User" WHERE google_sub = ${googleSub} LIMIT 1`;

      if (!user && googleEmail) {
        [user] = await sql`SELECT * FROM "User" WHERE email = ${googleEmail.toLowerCase()} LIMIT 1`;
        if (user) {
          await sql`UPDATE "User" SET google_sub = ${googleSub} WHERE id = ${user.id}`;
        }
      }

      if (!user) {
        const email = googleEmail ? googleEmail.toLowerCase() : `google_${googleSub}@perkfinity.internal`;
        [user] = await sql`
          INSERT INTO "User" (id, email, google_sub, full_name, created_at, last_active)
          VALUES (gen_random_uuid()::text, ${email}, ${googleSub}, ${googleName}, NOW(), NOW())
          ON CONFLICT (email) DO UPDATE SET google_sub = ${googleSub}, last_active = NOW()
          RETURNING *
        `;
      } else {
        await sql`UPDATE "User" SET last_active = NOW() WHERE id = ${user.id}`;
      }

      const gtoken = jwt.sign({ userId: user.id, role: 'consumer' }, JWT_SECRET, { expiresIn: '30d' });
      await autoEnrollUser(sql, user.id, data.qrCode);
      const { password_hash: _gpw, ...safeGUser } = user;
      return send(res, 200, { success: true, data: { user: safeGUser, accessToken: gtoken } });
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
      
      await autoEnrollUser(sql, user.id, data.qrCode);
      
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
      
      await autoEnrollUser(sql, user.id, data.qrCode);
      
      const { password_hash: _pw, ...safeUser } = user;
      return send(res, 200, { success: true, data: { user: safeUser, accessToken: token } });
    }

    // ── POST /api/v1/consumers/forgot-password ─────────────────────
    if (method === 'POST' && url.endsWith('/consumers/forgot-password')) {
      const data = req.body || {};
      if (!data.email) return send(res, 400, { success: false, error: 'Email is required' });

      const [user] = await sql`SELECT id, email, password_hash, apple_sub, google_sub FROM "User" WHERE email = ${data.email.toLowerCase()} LIMIT 1`;

      // If user exists and signed up via email (has password_hash), send reset email
      if (user && user.password_hash) {
        // TODO: Generate a reset token, save it to DB, and send email via Microsoft SMTP
        // For now, just log so we can verify the endpoint works
        console.log(`[FORGOT-PASSWORD] Reset requested for: ${user.email} (user: ${user.id})`);
      }

      // Always return success to not leak whether the email exists
      return send(res, 200, { success: true, message: 'If an account exists with that email, a reset link has been sent.' });
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
         SELECT DISTINCT ON (m.id)
           m.id as id,
           m.business_name as merchant_name,
           m.logo_url,
           l.postal_code as zip_code,
           q.public_code as qr_code,
           c.title as discount,
           (SELECT COUNT(*) FROM "Campaign" c2
            WHERE c2.merchant_id = m.id AND c2.status = 'active' AND c2.end_at > NOW()) as offer_count
         FROM "Campaign" c
         JOIN "Merchant" m ON m.id = c.merchant_id
         LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
         LEFT JOIN "QrCode" q ON q.merchant_id = m.id AND q.status = 'active'
         WHERE c.status = 'active' AND m.status = 'active' AND c.end_at > NOW()
         ORDER BY m.id, c.created_at DESC
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
      
      // UPDATE the most-recent non-redeemed Redemption row → 'pending'
      // Use CTE + LIMIT 1 to guarantee only ONE row is touched (avoids @unique token violation)
      const updated = await sql`
        WITH target AS (
          SELECT id FROM "Redemption"
          WHERE user_id = ${payload.userId}
            AND campaign_id = ${campaignId}
            AND status != 'redeemed'
            AND redeemed = false
          ORDER BY issued_at DESC
          LIMIT 1
        )
        UPDATE "Redemption"
        SET expires_at = NOW() + INTERVAL '5 minutes',
            status = 'pending',
            issued_at = NOW(),
            token = ${code}
        FROM target
        WHERE "Redemption".id = target.id
        RETURNING *
      `;

      let redemption;
      if (updated.length > 0) {
        redemption = updated[0];
      } else {
        // True fallback: no prior assignment row at all — insert fresh
        const [inserted] = await sql`
          INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed, status)
          VALUES (gen_random_uuid()::text, ${payload.userId}, ${campaignId}, ${code}, NOW(), NOW() + INTERVAL '5 minutes', false, 'pending')
          RETURNING *
        `;
        redemption = inserted;
      }

      return send(res, 201, { success: true, data: { activation: redemption } });
    }

    // ── POST /api/v1/campaigns/:id/expire ─────────────────────────
    const expireMatch = url.match(/\/api\/v1\/campaigns\/([a-zA-Z0-9_-]+)\/expire/);
    if (method === 'POST' && expireMatch) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });

      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const expireCampaignId = expireMatch[1];

      // Set the most-recent pending Redemption → 'expired'
      const expired = await sql`
        WITH target AS (
          SELECT id FROM "Redemption"
          WHERE user_id    = ${payload.userId}
            AND campaign_id = ${expireCampaignId}
            AND status      = 'pending'
            AND redeemed    = false
          ORDER BY issued_at DESC
          LIMIT 1
        )
        UPDATE "Redemption"
        SET status = 'expired'
        FROM target
        WHERE "Redemption".id = target.id
        RETURNING *
      `;

      return send(res, 200, { success: true, data: { expired: expired[0] || null } });
    }

    // ── POST /api/v1/campaigns/:id/cancel-activation ───────────────
    const cancelActivateMatch = url.match(/\/api\/v1\/campaigns\/([a-zA-Z0-9_-]+)\/cancel-activation/);
    if (method === 'POST' && cancelActivateMatch) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });

      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const cancelCampaignId = cancelActivateMatch[1];

      // Revert the most-recent pending Redemption → 'created', restore expires_at to campaign end date
      // Then delete any extra duplicate rows (cleanup from previous bug)
      const cancelled = await sql`
        WITH target AS (
          SELECT id FROM "Redemption"
          WHERE user_id    = ${payload.userId}
            AND campaign_id = ${cancelCampaignId}
            AND status      = 'pending'
            AND redeemed    = false
          ORDER BY issued_at DESC
          LIMIT 1
        )
        UPDATE "Redemption"
        SET status = 'created',
            expires_at = (SELECT end_at FROM "Campaign" WHERE id = ${cancelCampaignId})
        FROM target
        WHERE "Redemption".id = target.id
        RETURNING *
      `;

      // Delete any leftover duplicate non-redeemed rows for the same user/campaign
      // (keeping only the one we just reverted, identified by token)
      if (cancelled.length > 0) {
        await sql`
          DELETE FROM "Redemption"
          WHERE user_id    = ${payload.userId}
            AND campaign_id = ${cancelCampaignId}
            AND id         != ${cancelled[0].id}
            AND redeemed    = false
        `;
      }

      // Return 200 regardless — nothing to cancel is still a success from the user's perspective
      return send(res, 200, { success: true, data: { cancelled: cancelled[0] || null } });
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
        SET redeemed = true, redeemed_at = NOW(), status = 'redeemed'
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
                'id', r.id,
                'campaign_title', c.title,
                'token', r.token,
                'expires_at', r.expires_at,
                'redeemed_at', r.redeemed_at,
                'status', CASE
                  WHEN c.campaign_type = 'announcement' OR c.discount_percentage = -1 THEN 'Announcement'
                  WHEN r.status = 'redeemed' OR r.redeemed = true THEN 'Redeemed'
                  WHEN r.status = 'expired' OR (r.redeemed = false AND r.expires_at < NOW()) THEN 'Expired'
                  WHEN r.status = 'pending' THEN 'Pending'
                  ELSE 'Created'
                END
              )
            ) FILTER (WHERE r.id IS NOT NULL AND c.id IS NOT NULL), '[]'
          ) as promotions
        FROM "MerchantMember" mm
        JOIN "User" u ON u.id = mm.user_id
        LEFT JOIN "Redemption" r ON r.user_id = u.id
        LEFT JOIN "Campaign" c ON c.id = r.campaign_id AND c.merchant_id = mm.merchant_id AND c.status = 'active'
        WHERE mm.merchant_id = ${merchantId}
        GROUP BY u.id, u.city, u.zip_code, u.full_name
      `;
      
      return send(res, 200, { success: true, data: membersResult });
    }

    // ── GET /api/v1/merchants/:id/promotions/history ──────────────
    const promoHistoryMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/promotions\/history/);
    if (method === 'GET' && promoHistoryMatch) {
      const hMerchantId = promoHistoryMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let hPayload;
      try { hPayload = jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET); }
      catch (e) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (hPayload.merchantId !== hMerchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const history = await sql`
        SELECT c.id, c.title, c.status, c.start_at, c.end_at, c.created_at,
               (SELECT a.metadata FROM "AuditLog" a
                WHERE a.target_id = c.id AND a.action = 'promotion_created'
                ORDER BY a.created_at DESC LIMIT 1) as metadata,
               (SELECT COUNT(*) FROM "Redemption" r
                WHERE r.campaign_id = c.id AND (r.status = 'redeemed' OR r.redeemed = true)) as redeemed_count
        FROM "Campaign" c
        WHERE c.merchant_id = ${hMerchantId}
        ORDER BY c.created_at DESC
        LIMIT 50
      `;
      return send(res, 200, { success: true, data: history });
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
