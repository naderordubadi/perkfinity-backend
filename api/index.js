/**
 * Perkfinity Backend — Vercel Serverless + Neon
 * Uses @neondatabase/serverless: HTTP-based, no TCP, no build step, works everywhere.
 */

const { neon } = require('@neondatabase/serverless');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const SibApiV3Sdk = require('sib-api-v3-sdk');
const Stripe = require('stripe');

// ── Firebase Admin Init ──────────────────────────────────────────
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

let firebaseInitialized = false;
try {
  let cert;
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    let raw = process.env.FIREBASE_SERVICE_ACCOUNT;
    raw = raw.replace(/\\\\n/g, '\\n');
    cert = JSON.parse(raw);
    if (cert.private_key) cert.private_key = cert.private_key.replace(/\\n/g, '\n');
  } else {
    const serviceAccountPath = path.join(process.cwd(), 'firebase-service-account.json');
    if (fs.existsSync(serviceAccountPath)) {
      cert = require(serviceAccountPath);
    }
  }

  if (cert && !admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(cert)
    });
    firebaseInitialized = true;
  } else if (admin.apps.length) {
    firebaseInitialized = true;
  }
} catch (err) {
  console.error('Firebase Admin init error:', err);
}

async function sendPushNotification(token, title, body) {
  if (!firebaseInitialized || !token) return;
  try {
    await admin.messaging().send({
      token,
      notification: { title, body },
      apns: {
        headers: { 'apns-priority': '10' },
        payload: {
          aps: {
            alert: { title, body },
            sound: 'default',
            badge: 1
          }
        }
      }
    });
  } catch (err) {
    console.error('Firebase push error:', err);
  }
}

const ALLOWED_ORIGINS = [
  'https://perkfinity.net',
  'https://www.perkfinity.net',
  'https://app.perkfinity.net',
  'https://perkfinity-app.vercel.app',  // legacy — keep for backwards compat
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

    // 2. Auto-tier upgrade: check if merchant hit their free member limit
    //    If they have a saved payment method, auto-charge via Stripe.
    try {
      const [merchant] = await sql`SELECT id, subscription_tier, member_limit, stripe_customer_id, stripe_payment_method_id, billing_status FROM "Merchant" WHERE id = ${qrData.merchant_id}`;
      if (merchant && (merchant.subscription_tier === 'trial' || merchant.subscription_tier === 'free')) {
        const limit = merchant.member_limit || 10;
        const [countRow] = await sql`SELECT COUNT(*)::int as cnt FROM "MerchantMember" WHERE merchant_id = ${qrData.merchant_id}`;
        if (countRow && countRow.cnt >= limit) {
          // If merchant has a saved payment method, create a Stripe subscription automatically
          const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
          const PRICE_ID = process.env.STRIPE_TIER1_PRICE_ID;
          if (STRIPE_KEY && PRICE_ID && merchant.stripe_customer_id && merchant.stripe_payment_method_id) {
            try {
              const stripeClient = Stripe(STRIPE_KEY);
              const subscription = await stripeClient.subscriptions.create({
                customer: merchant.stripe_customer_id,
                items: [{ price: PRICE_ID }],
                default_payment_method: merchant.stripe_payment_method_id,
                metadata: { merchant_id: merchant.id }
              });
              await sql`
                UPDATE "Merchant" 
                SET subscription_tier = 'tier1', 
                    stripe_subscription_id = ${subscription.id},
                    billing_status = 'active',
                    subscription_started_at = NOW(),
                    next_billing_date = NOW() + INTERVAL '30 days',
                    updated_at = NOW() 
                WHERE id = ${qrData.merchant_id}
              `;
              console.log(`Auto-upgraded merchant ${qrData.merchant_id} to tier1 via Stripe (${countRow.cnt} members, limit was ${limit})`);
            } catch (stripeErr) {
              console.error(`Stripe auto-charge failed for merchant ${qrData.merchant_id}:`, stripeErr.message);
              // Still upgrade tier in DB so they aren't stuck, but flag billing as failed
              await sql`UPDATE "Merchant" SET subscription_tier = 'tier1', billing_status = 'payment_failed', updated_at = NOW() WHERE id = ${qrData.merchant_id}`;
            }
          } else {
            // No Stripe setup — just upgrade tier (legacy behavior)
            await sql`UPDATE "Merchant" SET subscription_tier = 'tier1', updated_at = NOW() WHERE id = ${qrData.merchant_id}`;
            console.log(`Auto-upgraded merchant ${qrData.merchant_id} to tier1 (no Stripe — legacy) (${countRow.cnt} members, limit was ${limit})`);
          }
        }
      }
    } catch (upgradeErr) {
      console.error('Auto-tier upgrade check failed:', upgradeErr);
    }

    // 3. Assign only welcome campaigns (not merchant-targeted promotions) to new members.
    //    Targeted promotions have an AuditLog entry (action='promotion_created');
    //    welcome campaigns created at merchant signup do not.
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
        AND NOT EXISTS (
          SELECT 1 FROM "AuditLog" al
          WHERE al.target_id = c.id
            AND al.action = 'promotion_created'
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

      // Promo code validation → set member_limit
      let memberLimit = 10;
      let promoCode = (data.promo_code || '').trim().toUpperCase();
      if (promoCode) {
        const validCodes = { 'MEMBER-15': 15, 'MEMBER-20': 20 };
        if (!validCodes[promoCode]) {
          return send(res, 400, { success: false, error: 'Invalid promo code.' });
        }
        memberLimit = validCodes[promoCode];
      } else {
        promoCode = null;
      }

      // Insert merchant (required fields used directly; optional fields use || '')
      const [merchant] = await sql`
        INSERT INTO "Merchant" (id, business_name, contact_name, phone, website, subscription_tier, member_limit, promo_code, status, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${data.name}, ${data.contactName}, ${data.phone}, ${data.website || ''}, ${data.tier || 'trial'}, ${memberLimit}, ${promoCode}, 'active', ${now}, ${now})
        RETURNING id, business_name, subscription_tier, member_limit
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

    // ── POST /api/v1/merchants/forgot-password ─────────────────────
    if (method === 'POST' && url.endsWith('/merchants/forgot-password')) {
      const data = req.body || {};
      if (!data.email) return send(res, 400, { success: false, error: 'Email is required' });

      // Find the merchant user
      const [user] = await sql`SELECT id, merchant_id, role FROM "MerchantUser" WHERE email = ${data.email.toLowerCase()} LIMIT 1`;
      
      if (user) {
        // Generate a fast token
        const rawToken = crypto.randomBytes(32).toString('hex');
        const expiresAt = new Date(Date.now() + 60 * 60 * 1000); // 1 hour

        // Save token to DB
        await sql`
          UPDATE "MerchantUser" 
          SET reset_token = ${rawToken}, reset_expires_at = ${expiresAt}
          WHERE id = ${user.id}
        `;

        // Send email via Brevo
        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY) {
          try {
            const brevoClient = SibApiV3Sdk.ApiClient.instance;
            brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
            const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
            
            const sendSmtpEmail = new SibApiV3Sdk.SendSmtpEmail();
            sendSmtpEmail.sender = { name: 'Perkfinity', email: 'noreply@perkfinity.net' };
            sendSmtpEmail.to = [{ email: data.email.toLowerCase() }];
            sendSmtpEmail.subject = 'Reset your Perkfinity Password';
            
            const resetLink = `https://perkfinity.net/reset-password.html?token=${rawToken}`;
            
            sendSmtpEmail.htmlContent = `
              <div style="font-family:'Helvetica Neue',Arial,sans-serif; max-width:520px; margin:0 auto; background:#ffffff; border-radius:16px; overflow:hidden; border:1px solid #eee;">
                <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf); padding:28px 24px; text-align:center;">
                  <div style="color:#fff; font-size:24px; font-weight:800;">Perkfinity</div>
                </div>
                <div style="padding:28px 24px;">
                  <div style="font-size:20px; font-weight:700; color:#1a1a2e; margin-bottom:16px;">Password Reset Request</div>
                  <p style="font-size:15px; color:#555; line-height:1.6; margin-bottom:24px;">
                    We received a request to reset the password for your Perkfinity merchant account. Click the button below to choose a new password. This link will expire in 1 hour.
                  </p>
                  <div style="text-align:center; margin-bottom:24px;">
                    <a href="${resetLink}" style="display:inline-block; background:#5b3fa5; color:#fff; font-weight:600; text-decoration:none; padding:14px 28px; border-radius:10px;">Reset Password</a>
                  </div>
                  <p style="font-size:13px; color:#aaa; text-align:center;">If you did not request this, you can safely ignore this email.</p>
                </div>
              </div>
            `;
            
            await emailApi.sendTransacEmail(sendSmtpEmail);
          } catch (brevoErr) {
            console.error('Brevo reset email failed:', brevoErr.message || brevoErr);
          }
        }
      }

      // Always return success even if user not found to prevent email enumeration
      return send(res, 200, { success: true, message: 'If an account exists with that email, a reset link has been sent.' });
    }

    // ── POST /api/v1/merchants/reset-password ──────────────────────
    if (method === 'POST' && url.endsWith('/merchants/reset-password')) {
      const data = req.body || {};
      if (!data.token || !data.password) return send(res, 400, { success: false, error: 'Token and new password are required' });

      const [user] = await sql`
        SELECT id FROM "MerchantUser" 
        WHERE reset_token = ${data.token} 
          AND reset_expires_at > NOW() 
        LIMIT 1
      `;

      if (!user) {
        return send(res, 400, { success: false, error: 'Invalid or expired reset token. Please request a new one.' });
      }

      const password_hash = await bcrypt.hash(data.password, 12);

      await sql`
        UPDATE "MerchantUser" 
        SET password_hash = ${password_hash}, reset_token = NULL, reset_expires_at = NULL 
        WHERE id = ${user.id}
      `;

      return send(res, 200, { success: true, message: 'Your password has been successfully reset. You can now log in.' });
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
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "reset_token" TEXT`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "reset_expires_at" TIMESTAMP`;
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "push_token" TEXT`;
      await sql`ALTER TABLE "MerchantUser" ADD COLUMN IF NOT EXISTS "reset_token" TEXT`;
      await sql`ALTER TABLE "MerchantUser" ADD COLUMN IF NOT EXISTS "reset_expires_at" TIMESTAMP`;
      // -- Daily Digest: NotificationQueue + delivery_channel --
      await sql`ALTER TABLE "Campaign" ADD COLUMN IF NOT EXISTS "delivery_channel" TEXT DEFAULT 'both'`;
      await sql`
        CREATE TABLE IF NOT EXISTS "NotificationQueue" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          user_id TEXT NOT NULL,
          campaign_id TEXT NOT NULL,
          merchant_id TEXT NOT NULL,
          store_name TEXT NOT NULL,
          store_address TEXT,
          logo_url TEXT,
          title TEXT NOT NULL,
          body TEXT,
          channels TEXT NOT NULL DEFAULT 'both',
          created_at TIMESTAMPTZ DEFAULT NOW(),
          sent BOOLEAN DEFAULT false
        )
      `;
      await sql`ALTER TABLE "NotificationQueue" ADD COLUMN IF NOT EXISTS "store_address" TEXT`;
      await sql`ALTER TABLE "NotificationQueue" ADD COLUMN IF NOT EXISTS "offer_expires_at" TIMESTAMPTZ`;
      // -- Notification History: persists sent notifications for in-app viewing --
      await sql`
        CREATE TABLE IF NOT EXISTS "NotificationHistory" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          user_id TEXT NOT NULL,
          title TEXT NOT NULL,
          body TEXT,
          type TEXT NOT NULL DEFAULT 'digest',
          payload JSONB DEFAULT '[]'::jsonb,
          read BOOLEAN DEFAULT false,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `;
      await sql`CREATE INDEX IF NOT EXISTS idx_notif_history_user ON "NotificationHistory" (user_id, created_at DESC)`;
      return send(res, 200, { success: true, message: "DB table migrations strictly applied!" });
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

          // Auto-assign only welcome campaigns (not merchant-targeted promotions) to new members.
          // Targeted promotions have an AuditLog entry; welcome campaigns do not.
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
              AND NOT EXISTS (
                SELECT 1 FROM "AuditLog" al
                WHERE al.target_id = c.id
                  AND al.action = 'promotion_created'
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
        SELECT m.business_name, m.contact_name, m.phone, m.website, m.logo_url, l.address, l.suite, l.city, l.state, l.postal_code, u.email
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
      merchantData.qr_url = qrData ? `https://app.perkfinity.net/qr/${qrData.public_code}` : null;
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
      // Create Redemption rows for all qualifying users
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

      // ── Queue notifications for daily digest ──────────────────────
      const deliveryChannel = data.delivery_channel || 'both'; // 'email', 'push', 'both'

      // Save delivery_channel to campaign
      await sql`UPDATE "Campaign" SET delivery_channel = ${deliveryChannel} WHERE id = ${campaign.id}`;

      let queuedCount = 0;

      if (qualifyingUsers.length > 0) {
        try {
          // Fetch merchant info for the queue
          const [merchantInfo] = await sql`
            SELECT m.business_name, m.logo_url, l.address, l.city, l.state, l.postal_code
            FROM "Merchant" m
            LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
            WHERE m.id = ${targetMerchantId}
            LIMIT 1
          `;
          const storeName = merchantInfo?.business_name || 'Your Local Store';
          const logoUrl = merchantInfo?.logo_url || '';
          const storeAddress = merchantInfo ? [merchantInfo.address, merchantInfo.city, merchantInfo.state, merchantInfo.postal_code].filter(Boolean).join(', ') : '';
          const headline = data.title || 'New Offer';
          const condLine = data.condition_detail || '';
          const bodyText = condLine || headline;

          // Insert into NotificationQueue for each qualifying user
          const userIds = qualifyingUsers.map(u => u.user_id);
          for (const userId of userIds) {
            try {
              await sql`
                INSERT INTO "NotificationQueue" (user_id, campaign_id, merchant_id, store_name, store_address, logo_url, title, body, channels, offer_expires_at)
                VALUES (${userId}, ${campaign.id}, ${targetMerchantId}, ${storeName}, ${storeAddress}, ${logoUrl}, ${headline}, ${bodyText}, ${deliveryChannel}, ${campaign.end_at})
              `;
              queuedCount++;
            } catch (queueErr) {
              console.error(`Queue insert failed for user ${userId}:`, queueErr.message);
            }
          }
        } catch (setupErr) {
          console.error('Campaign queue setup error:', setupErr.message || setupErr);
        }
      }

      const channelMsg = deliveryChannel === 'email' ? `${queuedCount} email(s)` : deliveryChannel === 'push' ? `${queuedCount} push notification(s)` : `${queuedCount} email(s) and ${queuedCount} push notification(s)`;
      return send(res, 201, { success: true, data: { campaign, assigned_count: assignedCount, queued_count: queuedCount, delivery_channel: deliveryChannel, message: `Promotion created and assigned to ${assignedCount} member(s). ${channelMsg} queued for daily digest.` } });
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
      
      const [existing] = await sql`SELECT id, password_hash FROM "User" WHERE email = ${data.email.toLowerCase()} LIMIT 1`;
      
      if (existing) {
        if (existing.password_hash) {
          // User already fully signed up — suggest login
          return send(res, 400, { success: false, error: 'An account with this email already exists. Please use Log In instead.' });
        }
        // User was auto-created (via Apple/Google sign-in or auto-enrollment) — let them set a password
        const hash = await bcrypt.hash(data.password, 12);
        await sql`UPDATE "User" SET password_hash = ${hash}, last_active = NOW() WHERE id = ${existing.id}`;
        const JWT_SECRET = process.env.JWT_SECRET;
        const token = jwt.sign({ userId: existing.id, role: 'consumer' }, JWT_SECRET, { expiresIn: '30d' });
        await autoEnrollUser(sql, existing.id, data.qrCode);
        return send(res, 200, { success: true, data: { user: { id: existing.id, email: data.email.toLowerCase() }, accessToken: token } });
      }
      
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
        const rawToken = crypto.randomBytes(32).toString('hex');
        const expiresAt = new Date(Date.now() + 60 * 60 * 1000); // 1 hour

        await sql`
          UPDATE "User" 
          SET reset_token = ${rawToken}, reset_expires_at = ${expiresAt}
          WHERE id = ${user.id}
        `;

        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY) {
          try {
            const brevoClient = SibApiV3Sdk.ApiClient.instance;
            brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
            const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
            
            const sendSmtpEmail = new SibApiV3Sdk.SendSmtpEmail();
            sendSmtpEmail.sender = { name: 'Perkfinity', email: 'noreply@perkfinity.net' };
            sendSmtpEmail.to = [{ email: data.email.toLowerCase() }];
            sendSmtpEmail.subject = 'Reset your Perkfinity Member Password';
            
            const resetLink = `https://perkfinity.net/member-reset-password.html?token=${rawToken}`;
            
            sendSmtpEmail.htmlContent = `
              <div style="font-family:'Helvetica Neue',Arial,sans-serif; max-width:520px; margin:0 auto; background:#ffffff; border-radius:16px; overflow:hidden; border:1px solid #eee;">
                <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf); padding:28px 24px; text-align:center;">
                  <div style="color:#fff; font-size:24px; font-weight:800;">Perkfinity</div>
                </div>
                <div style="padding:28px 24px;">
                  <div style="font-size:20px; font-weight:700; color:#1a1a2e; margin-bottom:16px;">Password Reset Request</div>
                  <p style="font-size:15px; color:#555; line-height:1.6; margin-bottom:24px;">
                    We received a request to reset the password for your Perkfinity member account. Click the button below to choose a new password. This link will expire in 1 hour.
                  </p>
                  <div style="text-align:center; margin-bottom:24px;">
                    <a href="${resetLink}" style="display:inline-block; background:#5b3fa5; color:#fff; font-weight:600; text-decoration:none; padding:14px 28px; border-radius:10px;">Reset Password</a>
                  </div>
                  <p style="font-size:13px; color:#aaa; text-align:center;">If you did not request this, you can safely ignore this email.</p>
                </div>
              </div>
            `;
            
            await emailApi.sendTransacEmail(sendSmtpEmail);
            console.log(`[FORGOT-PASSWORD] Member reset email sent for: ${user.email}`);
          } catch (brevoErr) {
            console.error('Brevo consumer reset email failed:', brevoErr.message || brevoErr);
          }
        } else {
          console.warn('[FORGOT-PASSWORD] BREVO_KEY missing, skipping member reset email.');
        }
      }

      // Always return success to not leak whether the email exists
      return send(res, 200, { success: true, message: 'If an account exists with that email, a reset link has been sent.' });
    }

    // ── POST /api/v1/consumers/reset-password ──────────────────────
    if (method === 'POST' && url.endsWith('/consumers/reset-password')) {
      const data = req.body || {};
      if (!data.token || !data.password) return send(res, 400, { success: false, error: 'Token and new password are required' });

      const [user] = await sql`
        SELECT id FROM "User" 
        WHERE reset_token = ${data.token} 
          AND reset_expires_at > NOW() 
        LIMIT 1
      `;

      if (!user) {
        return send(res, 400, { success: false, error: 'Invalid or expired reset token. Please request a new one.' });
      }

      const password_hash = await bcrypt.hash(data.password, 12);

      await sql`
        UPDATE "User" 
        SET password_hash = ${password_hash}, reset_token = NULL, reset_expires_at = NULL 
        WHERE id = ${user.id}
      `;

      return send(res, 200, { success: true, message: 'Your password has been successfully reset. You can now log into the app.' });
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
           COALESCE(l.address, '') || CASE WHEN l.city IS NOT NULL THEN ', ' || l.city ELSE '' END || CASE WHEN l.state IS NOT NULL THEN ', ' || l.state ELSE '' END as store_address,
           c.title as latest_offer_title,
           c.end_at as offer_expires_at,
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

    // ── POST /api/v1/consumers/push-token ─────────────────────────
    if (method === 'POST' && url.endsWith('/consumers/push-token')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const data = req.body || {};
      if (!data.token) return send(res, 400, { success: false, error: 'Missing push token' });

      await sql`UPDATE "User" SET push_token = ${data.token} WHERE id = ${payload.userId}`;
      return send(res, 200, { success: true, message: 'Push token registered successfully' });
    }

    // ── GET /api/v1/consumers/notifications ────────────────────────
    if (method === 'GET' && url.endsWith('/consumers/notifications')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const notifications = await sql`
        SELECT id, title, body, type, payload, read, created_at
        FROM "NotificationHistory"
        WHERE user_id = ${payload.userId}
        ORDER BY created_at DESC
        LIMIT 50
      `;
      const unreadCount = notifications.filter(n => !n.read).length;
      return send(res, 200, { success: true, data: notifications, unread_count: unreadCount });
    }

    // ── POST /api/v1/consumers/notifications/read ──────────────────
    if (method === 'POST' && url.endsWith('/consumers/notifications/read')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const data = req.body || {};
      if (!data.id) return send(res, 400, { success: false, error: 'Missing notification id' });

      await sql`UPDATE "NotificationHistory" SET read = true WHERE id = ${data.id} AND user_id = ${payload.userId}`;
      return send(res, 200, { success: true });
    }

    // ── POST /api/v1/consumers/notifications/read-all ──────────────
    if (method === 'POST' && url.endsWith('/consumers/notifications/read-all')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      await sql`UPDATE "NotificationHistory" SET read = true WHERE user_id = ${payload.userId} AND read = false`;
      return send(res, 200, { success: true });
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
      // GUARD: Only cancel if activated more than 2 seconds ago.
      // React's route transition causes a spurious cancel-activation within 0.5-0.8s
      // of activation — the 2s guard silently ignores those while still allowing
      // legitimate cancels from tab navigation or app backgrounding.
      const cancelled = await sql`
        WITH target AS (
          SELECT id FROM "Redemption"
          WHERE user_id    = ${payload.userId}
            AND campaign_id = ${cancelCampaignId}
            AND status      = 'pending'
            AND redeemed    = false
            AND issued_at   < NOW() - INTERVAL '2 seconds'
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
      return send(res, 200, { success: true, data: { logo_url: data.logo_url } });
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
                'issued_at', r.issued_at,
                'expires_at', r.expires_at,
                'redeemed_at', r.redeemed_at,
                'status', CASE
                  WHEN c.campaign_type = 'announcement' OR c.discount_percentage = -1 THEN 'Announcement'
                  WHEN r.status = 'redeemed' OR r.redeemed = true THEN 'Redeemed'
                  WHEN c.end_at IS NOT NULL AND c.end_at < NOW() THEN 'Expired'
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
        LEFT JOIN "Campaign" c ON c.id = r.campaign_id AND c.merchant_id = mm.merchant_id
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
        SELECT c.id, c.title,
               CASE WHEN c.end_at IS NOT NULL AND c.end_at < NOW() THEN 'expired' ELSE c.status END as status,
               c.start_at, c.end_at, c.created_at,
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

    // ── TEMP DEBUG: check push token registration ──────────────────
    if (url === '/api/v1/debug/push-tokens' && method === 'GET') {
      const rows = await sql`
        SELECT email,
               CASE WHEN push_token IS NOT NULL THEN 'SET' ELSE 'NULL' END as push_status,
               LEFT(push_token, 20) as token_preview
        FROM "User"
        ORDER BY created_at DESC
        LIMIT 20
      `;
      return send(res, 200, { success: true, data: rows });
    }

    // ── TEMP DEBUG: check notification queue status ────────────────
    if (url === '/api/v1/debug/notification-queue' && method === 'GET') {
      const rows = await sql`
        SELECT nq.id, nq.user_id, u.email, nq.store_name, nq.title, nq.channels, nq.sent, nq.created_at
        FROM "NotificationQueue" nq
        JOIN "User" u ON u.id = nq.user_id
        ORDER BY nq.created_at DESC
        LIMIT 30
      `;
      return send(res, 200, { success: true, data: rows });
    }

    // ── TEMP DEBUG: test push notification to a specific email ─────
    if (url === '/api/v1/debug/test-push' && method === 'GET') {
      const targetEmail = new URL(req.url, 'http://x').searchParams.get('email');
      if (!targetEmail) return send(res, 400, { success: false, error: 'Provide ?email= parameter' });

      // Check Firebase availability
      let fbReady = false;
      try {
        const fbAdmin = require('firebase-admin');
        if (process.env.FIREBASE_SERVICE_ACCOUNT && !fbAdmin.apps.length) {
          let raw = process.env.FIREBASE_SERVICE_ACCOUNT;
          raw = raw.replace(/\\\\n/g, '\\n');
          const cert = JSON.parse(raw);
          if (cert.private_key) cert.private_key = cert.private_key.replace(/\\n/g, '\n');
          fbAdmin.initializeApp({ credential: fbAdmin.credential.cert(cert) });
        }
        fbReady = fbAdmin.apps.length > 0;
      } catch (fbErr) {
        return send(res, 500, { success: false, error: 'Firebase init failed', detail: fbErr.message });
      }

      if (!fbReady) return send(res, 500, { success: false, error: 'Firebase not initialized — check FIREBASE_SERVICE_ACCOUNT env var' });

      const [user] = await sql`SELECT id, email, push_token FROM "User" WHERE email = ${targetEmail}`;
      if (!user) return send(res, 404, { success: false, error: 'User not found' });
      if (!user.push_token) return send(res, 400, { success: false, error: 'User has no push token stored', email: user.email });

      try {
        const fbAdmin = require('firebase-admin');
        const result = await fbAdmin.messaging().send({
          token: user.push_token,
          notification: { title: '🧪 Perkfinity Test', body: 'Push notification is working!' },
          apns: {
            headers: { 'apns-priority': '10' },
            payload: { aps: { alert: { title: '🧪 Perkfinity Test', body: 'Push notification is working!' }, sound: 'default', badge: 1 } }
          }
        });
        return send(res, 200, { success: true, message: 'Push sent successfully', firebase_response: result });
      } catch (pushErr) {
        return send(res, 500, { success: false, error: 'Push send failed', detail: pushErr.message, code: pushErr.code });
      }
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

    // ── Promo code + auto-tier migration ──────────────────────────
    if (url === '/api/v1/migrate-promo' && method === 'GET') {
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "member_limit" INT DEFAULT 10`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "promo_code" TEXT`;
      return send(res, 200, { success: true, message: "Promo code columns added (member_limit, promo_code)!" });
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

    // ══════════════════════════════════════════════════════════════
    // ADMIN API ENDPOINTS
    // ══════════════════════════════════════════════════════════════

    // ── GET /api/v1/admin/merchants ─────────────────────────────
    if (method === 'GET' && url.endsWith('/admin/merchants')) {
      const merchants = await sql`
        SELECT m.*,
          (SELECT COUNT(*) FROM "MerchantMember" ml WHERE ml.merchant_id = m.id) as member_count,
          (SELECT COUNT(*) FROM "Campaign" c WHERE c.merchant_id = m.id) as campaign_count,
          (SELECT COUNT(*) FROM "Redemption" r JOIN "Campaign" c2 ON c2.id = r.campaign_id WHERE c2.merchant_id = m.id AND r.status = 'redeemed') as redemption_count,
          ml2.address as city
        FROM "Merchant" m
        LEFT JOIN "MerchantLocation" ml2 ON ml2.merchant_id = m.id AND ml2.is_active = true
        ORDER BY m.created_at DESC
      `;
      const active = merchants.filter(m => m.status !== 'inactive').length;
      return send(res, 200, {
        success: true,
        data: {
          merchants: merchants.map(m => ({ ...m, password_hash: undefined, tier: m.subscription_tier || 'free', status: m.status || 'active' })),
          stats: { total: merchants.length, active }
        }
      });
    }

    // ── GET /api/v1/admin/members ────────────────────────────────
    if (method === 'GET' && url.endsWith('/admin/members')) {
      const members = await sql`
        SELECT u.id, u.email, u.full_name, u.phone_number, u.city, u.zip_code, u.push_token,
          u.created_at, u.last_active,
          (SELECT COUNT(*) FROM "MerchantMember" ml WHERE ml.user_id = u.id) as merchant_count
        FROM "User" u
        WHERE u.role IS NULL OR u.role = 'consumer'
        ORDER BY u.created_at DESC
      `;
      const pushEnabled = members.filter(m => m.push_token).length;
      return send(res, 200, {
        success: true,
        data: {
          members,
          stats: { total: members.length, push_enabled: pushEnabled }
        }
      });
    }

    // ── GET /api/v1/admin/campaigns ──────────────────────────────
    if (method === 'GET' && url.endsWith('/admin/campaigns')) {
      const campaigns = await sql`
        SELECT c.*, m.business_name as merchant_name,
          (SELECT COUNT(*) FROM "Redemption" r WHERE r.campaign_id = c.id AND r.status = 'redeemed') as redemption_count
        FROM "Campaign" c
        LEFT JOIN "Merchant" m ON m.id = c.merchant_id
        ORDER BY c.created_at DESC
      `;
      const now = new Date();
      const active = campaigns.filter(c => c.end_at && new Date(c.end_at) > now).length;
      const totalRedemptions = campaigns.reduce((sum, c) => sum + (parseInt(c.redemption_count) || 0), 0);
      const rate = campaigns.length ? Math.round((totalRedemptions / campaigns.length) * 100) / 100 : 0;
      return send(res, 200, {
        success: true,
        data: {
          campaigns: campaigns.map(c => ({
            ...c,
            status: c.end_at && new Date(c.end_at) > now ? 'active' : 'expired'
          })),
          stats: { total: campaigns.length, active, redemptions: totalRedemptions, redemption_rate: rate }
        }
      });
    }

    // ══════════════════════════════════════════════════════════════
    // STRIPE BILLING ENDPOINTS
    // ══════════════════════════════════════════════════════════════

    // ── DB Migration: Stripe billing columns ─────────────────────
    if (url === '/api/v1/migrate-stripe' && method === 'GET') {
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "stripe_customer_id" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "stripe_subscription_id" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "stripe_payment_method_id" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "subscription_started_at" TIMESTAMPTZ`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "next_billing_date" TIMESTAMPTZ`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "billing_status" TEXT DEFAULT 'none'`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "account_blocked" BOOLEAN DEFAULT false`;
      await sql`
        CREATE TABLE IF NOT EXISTS "Invoice" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          merchant_id TEXT NOT NULL REFERENCES "Merchant"(id),
          stripe_invoice_id TEXT UNIQUE,
          amount_cents INTEGER NOT NULL DEFAULT 2999,
          currency TEXT DEFAULT 'usd',
          status TEXT DEFAULT 'pending',
          period_start TIMESTAMPTZ,
          period_end TIMESTAMPTZ,
          paid_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `;
      return send(res, 200, { success: true, message: 'Stripe billing DB migration complete!' });
    }

    // ── POST /api/v1/stripe/create-setup-intent (Trial merchants) ─
    if (method === 'POST' && url.endsWith('/stripe/create-setup-intent')) {
      const data = req.body || {};
      const merchantId = data.merchant_id;
      if (!merchantId) return send(res, 400, { success: false, error: 'merchant_id is required' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      // Look up the merchant
      const [merchant] = await sql`SELECT id, business_name, stripe_customer_id FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });

      // Get merchant email
      const [merchantUser] = await sql`SELECT email FROM "MerchantUser" WHERE merchant_id = ${merchantId} LIMIT 1`;

      // Create or reuse Stripe customer
      let customerId = merchant.stripe_customer_id;
      if (!customerId) {
        const customer = await stripeClient.customers.create({
          name: merchant.business_name,
          email: merchantUser?.email || undefined,
          metadata: { merchant_id: merchantId }
        });
        customerId = customer.id;
        await sql`UPDATE "Merchant" SET stripe_customer_id = ${customerId} WHERE id = ${merchantId}`;
      }

      // Create Setup Intent
      const setupIntent = await stripeClient.setupIntents.create({
        customer: customerId,
        payment_method_types: ['card'],
        metadata: { merchant_id: merchantId }
      });

      return send(res, 200, {
        success: true,
        data: {
          client_secret: setupIntent.client_secret,
          customer_id: customerId
        }
      });
    }

    // ── POST /api/v1/stripe/create-checkout-session (Tier 1) ──────
    if (method === 'POST' && url.endsWith('/stripe/create-checkout-session')) {
      const data = req.body || {};
      const merchantId = data.merchant_id;
      if (!merchantId) return send(res, 400, { success: false, error: 'merchant_id is required' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      const PRICE_ID = process.env.STRIPE_TIER1_PRICE_ID;
      if (!STRIPE_KEY || !PRICE_ID) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      const [merchant] = await sql`SELECT id, business_name, stripe_customer_id FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });

      const [merchantUser] = await sql`SELECT email FROM "MerchantUser" WHERE merchant_id = ${merchantId} LIMIT 1`;

      let customerId = merchant.stripe_customer_id;
      if (!customerId) {
        const customer = await stripeClient.customers.create({
          name: merchant.business_name,
          email: merchantUser?.email || undefined,
          metadata: { merchant_id: merchantId }
        });
        customerId = customer.id;
        await sql`UPDATE "Merchant" SET stripe_customer_id = ${customerId} WHERE id = ${merchantId}`;
      }

      // Create Checkout Session for $29.99/mo subscription
      const session = await stripeClient.checkout.sessions.create({
        customer: customerId,
        payment_method_types: ['card'],
        line_items: [{ price: PRICE_ID, quantity: 1 }],
        mode: 'subscription',
        success_url: `https://perkfinity.net/signup.html?payment=success&merchant_id=${merchantId}`,
        cancel_url: `https://perkfinity.net/signup.html?payment=cancelled&merchant_id=${merchantId}`,
        metadata: { merchant_id: merchantId }
      });

      return send(res, 200, {
        success: true,
        data: {
          checkout_url: session.url,
          session_id: session.id
        }
      });
    }

    // ── POST /api/v1/stripe/create-customer-portal ─────────────────
    if (method === 'POST' && url.endsWith('/stripe/create-customer-portal')) {
      const data = req.body || {};
      const merchantId = data.merchant_id;
      if (!merchantId) return send(res, 400, { success: false, error: 'merchant_id is required' });

      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      const [merchant] = await sql`SELECT stripe_customer_id FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
      if (!merchant || !merchant.stripe_customer_id) return send(res, 400, { success: false, error: 'No Stripe customer found' });

      try {
        const portalSession = await stripeClient.billingPortal.sessions.create({
          customer: merchant.stripe_customer_id,
          return_url: 'https://perkfinity.net/dashboard.html?tab=billing',
        });
        return send(res, 200, { success: true, data: { url: portalSession.url } });
      } catch (e) {
        return send(res, 500, { success: false, error: e.message });
      }
    }

    // ── POST /api/v1/merchants/:id/billing/cancel ─────────────────
    const cancelMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing\/cancel$/);
    if (method === 'POST' && cancelMatch) {
      const merchantId = cancelMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      const [merchant] = await sql`SELECT stripe_subscription_id FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
      if (!merchant || !merchant.stripe_subscription_id) {
        return send(res, 400, { success: false, error: 'No active Stripe subscription found' });
      }

      try {
        await stripeClient.subscriptions.update(merchant.stripe_subscription_id, {
          cancel_at_period_end: true,
        });

        // Update local state to pending_cancellation so UI knows
        await sql`
          UPDATE "Merchant"
          SET billing_status = 'pending_cancellation'
          WHERE id = ${merchantId}
        `;

        return send(res, 200, { success: true, message: 'Subscription will cancel at period end' });
      } catch (e) {
        return send(res, 500, { success: false, error: e.message });
      }
    }

    // ── GET /api/v1/merchants/:id/billing ─────────────────────────
    const billingMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing$/);
    if (method === 'GET' && billingMatch) {
      const merchantId = billingMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const [merchant] = await sql`
        SELECT id, business_name, subscription_tier, billing_status, account_blocked,
               stripe_customer_id, stripe_subscription_id, subscription_started_at,
               next_billing_date, member_limit, created_at
        FROM "Merchant"
        WHERE id = ${merchantId}
        LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });

      // Get member count
      const [countRow] = await sql`SELECT COUNT(*)::int as cnt FROM "MerchantMember" WHERE merchant_id = ${merchantId}`;

      // Get invoice history
      const invoices = await sql`
        SELECT id, stripe_invoice_id, amount_cents, currency, status, period_start, period_end, paid_at, created_at
        FROM "Invoice"
        WHERE merchant_id = ${merchantId}
        ORDER BY created_at DESC
        LIMIT 20
      `;

      return send(res, 200, {
        success: true,
        data: {
          tier: merchant.subscription_tier,
          billing_status: merchant.billing_status || 'none',
          account_blocked: merchant.account_blocked || false,
          member_count: countRow?.cnt || 0,
          member_limit: merchant.member_limit || 10,
          subscription_started_at: merchant.subscription_started_at,
          next_billing_date: merchant.next_billing_date,
          created_at: merchant.created_at,
          has_stripe: !!merchant.stripe_customer_id,
          has_subscription: !!merchant.stripe_subscription_id,
          invoices
        }
      });
    }

    // ── POST /api/v1/merchants/:id/billing/reactivate ─────────────
    const reactivateMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing\/reactivate$/);
    if (method === 'POST' && reactivateMatch) {
      const merchantId = reactivateMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      const PRICE_ID = process.env.STRIPE_TIER1_PRICE_ID;
      if (!STRIPE_KEY || !PRICE_ID) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      const [merchant] = await sql`
        SELECT id, stripe_customer_id, stripe_payment_method_id
        FROM "Merchant"
        WHERE id = ${merchantId}
        LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (!merchant.stripe_customer_id || !merchant.stripe_payment_method_id) {
        return send(res, 400, { success: false, error: 'No payment method on file. Please update your payment method first.' });
      }

      try {
        const subscription = await stripeClient.subscriptions.create({
          customer: merchant.stripe_customer_id,
          items: [{ price: PRICE_ID }],
          default_payment_method: merchant.stripe_payment_method_id,
          metadata: { merchant_id: merchantId }
        });

        await sql`
          UPDATE "Merchant"
          SET subscription_tier = 'tier1',
              stripe_subscription_id = ${subscription.id},
              billing_status = 'active',
              account_blocked = false,
              subscription_started_at = NOW(),
              next_billing_date = NOW() + INTERVAL '30 days',
              updated_at = NOW()
          WHERE id = ${merchantId}
        `;

        return send(res, 200, { success: true, message: 'Subscription reactivated successfully!' });
      } catch (stripeErr) {
        return send(res, 400, { success: false, error: `Reactivation failed: ${stripeErr.message}` });
      }
    }

    // ── DELETE /api/v1/merchants/:id/abandon ──────────────────────
    const abandonMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/abandon$/);
    if ((method === 'DELETE' || method === 'POST') && abandonMatch) {
      const merchantId = abandonMatch[1];

      // Safety: only delete if no payment method attached
      const [merchant] = await sql`
        SELECT id, stripe_customer_id, stripe_payment_method_id
        FROM "Merchant"
        WHERE id = ${merchantId}
        LIMIT 1
      `;

      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchant.stripe_payment_method_id) {
        return send(res, 400, { success: false, error: 'Cannot abandon — payment method already attached' });
      }

      // Delete Stripe customer if created
      if (merchant.stripe_customer_id) {
        const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
        if (STRIPE_KEY) {
          try {
            const stripeClient = Stripe(STRIPE_KEY);
            await stripeClient.customers.del(merchant.stripe_customer_id);
          } catch (delErr) {
            console.error('Failed to delete Stripe customer:', delErr.message);
          }
        }
      }

      // Delete all related data
      await sql`DELETE FROM "QrCode" WHERE merchant_id = ${merchantId}`;
      await sql`DELETE FROM "Campaign" WHERE merchant_id = ${merchantId}`;
      await sql`DELETE FROM "MerchantLocation" WHERE merchant_id = ${merchantId}`;
      await sql`DELETE FROM "MerchantUser" WHERE merchant_id = ${merchantId}`;
      await sql`DELETE FROM "Merchant" WHERE id = ${merchantId}`;

      return send(res, 200, { success: true, message: 'Abandoned signup cleaned up' });
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
