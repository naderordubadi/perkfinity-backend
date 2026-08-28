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
const cheerio = require('cheerio');

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
  'capacitor://perkfinity.net',  // Capacitor iOS WKWebView — actual prod origin (hostname set in capacitor.config.ts)
  'capacitor://localhost',       // Capacitor iOS fallback (default hostname)
  'https://localhost',           // Capacitor iOS https fallback
  'http://localhost',            // Capacitor Android fallback (default hostname)
];

function setCors(req, res) {
  const origin = req.headers.origin;
  const isAllowed = ALLOWED_ORIGINS.includes(origin) || (origin && origin.startsWith('http://localhost:'));
  // Only echo back the origin if it is explicitly allowed.
  // Unknown origins get no Access-Control-Allow-Origin header — browser blocks them.
  // Note: this does not stop curl/Postman (CORS is browser-only); rate limiting handles that.
  if (isAllowed) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, Idempotency-Key, x-admin-secret');
}

// ── Signup rate limiter (in-memory, per Vercel instance) ─────────
// Protects /consumers/signup from bot/bulk account creation.
// 5 attempts per IP per hour. Resets automatically after the window.
// Note: in-memory state resets on cold start — this is acceptable for
// basic protection. Upgrade to Redis/Upstash if stricter limits needed.
const _signupRateMap = new Map(); // ip -> { count: number, resetAt: number }
const SIGNUP_RATE_LIMIT = 5;
const SIGNUP_RATE_WINDOW_MS = 60 * 60 * 1000; // 1 hour

function checkSignupRateLimit(req) {
  const ip =
    req.headers['x-forwarded-for']?.split(',')[0]?.trim() ||
    req.socket?.remoteAddress ||
    'unknown';
  const now = Date.now();
  const entry = _signupRateMap.get(ip);
  if (!entry || now > entry.resetAt) {
    _signupRateMap.set(ip, { count: 1, resetAt: now + SIGNUP_RATE_WINDOW_MS });
    return { allowed: true };
  }
  if (entry.count >= SIGNUP_RATE_LIMIT) {
    const retryAfterSec = Math.ceil((entry.resetAt - now) / 1000);
    return { allowed: false, retryAfterSec };
  }
  entry.count++;
  return { allowed: true };
}

// ── Stripe price ID maps ─────────────────────────────────────
// Central helper so all billing paths (checkout, admin approval,
// reactivate, auto-charge) resolve the correct Stripe price in one place.
const MONTHLY_PRICE_IDS = {
  tier1:          () => process.env.STRIPE_TIER1_PRICE_ID,
  online_starter: () => process.env.STRIPE_ONLINE_STARTER_PRICE_ID,
  online_growth:  () => process.env.STRIPE_ONLINE_GROWTH_PRICE_ID,
  online_scale:   () => process.env.STRIPE_ONLINE_SCALE_PRICE_ID,
};
const ANNUAL_PRICE_IDS = {
  tier1:          () => process.env.STRIPE_TIER1_ANNUAL_PRICE_ID,
  online_starter: () => process.env.STRIPE_ONLINE_STARTER_ANNUAL_PRICE_ID,
  online_growth:  () => process.env.STRIPE_ONLINE_GROWTH_ANNUAL_PRICE_ID,
  online_scale:   () => process.env.STRIPE_ONLINE_SCALE_ANNUAL_PRICE_ID,
};
function getPriceId(tier, billingCycle) {
  const map = billingCycle === 'annual' ? ANNUAL_PRICE_IDS : MONTHLY_PRICE_IDS;
  return map[tier]?.() || null;
}

async function autoEnrollUser(sql, userId, publicCode) {
  if (!publicCode || !userId) return;
  try {
    const [qrData] = await sql`SELECT merchant_id FROM "QrCode" WHERE public_code = ${publicCode} AND status = 'active'`;
    if (!qrData) return;

    // Cap guard: block auto-enrollment for capped online tiers already at their member limit.
    // This mirrors the same check in GET /qr/resolve/:code so auth flows cannot bypass the cap.
    const _cappedAutoTiers = ['online_starter', 'online_growth'];
    const [_capMerchAuto] = await sql`SELECT subscription_tier, member_limit, billing_status FROM "Merchant" WHERE id = ${qrData.merchant_id} LIMIT 1`;
    if (_capMerchAuto && _cappedAutoTiers.includes(_capMerchAuto.subscription_tier) && _capMerchAuto.member_limit && _capMerchAuto.billing_status === 'active') {
      const [_capCntAuto] = await sql`SELECT COUNT(*)::int as cnt FROM "MerchantMember" WHERE merchant_id = ${qrData.merchant_id}`;
      if (_capCntAuto && _capCntAuto.cnt >= _capMerchAuto.member_limit) {
        console.log(`[MemberCap-Auth] Auto-enroll blocked for merchant ${qrData.merchant_id} (${_capCntAuto.cnt}/${_capMerchAuto.member_limit}) — user ${userId} not enrolled`);
        return;
      }
    }

    // 1. Add to member list
    await sql`
      INSERT INTO "MerchantMember" (id, merchant_id, user_id, join_source, created_at)
      VALUES (gen_random_uuid()::text, ${qrData.merchant_id}, ${userId}, 'qr_scan', NOW())
      ON CONFLICT DO NOTHING
    `;

    // 2. Auto-tier upgrade: check if merchant hit their free member limit
    //    If they have a saved payment method, auto-charge via Stripe.
    try {
      const [merchant] = await sql`SELECT id, business_name, subscription_tier, member_limit, stripe_customer_id, stripe_payment_method_id, billing_status, billing_starts_at_member_count, billing_cycle FROM "Merchant" WHERE id = ${qrData.merchant_id}`;
      // Check online promo billing trigger FIRST (separate from trial→tier1 logic)
      const onlinePromoTiers = ['online_starter', 'online_growth', 'online_scale'];
      if (merchant && onlinePromoTiers.includes(merchant.subscription_tier) && merchant.billing_starts_at_member_count) {
        const [countRow] = await sql`SELECT COUNT(*)::int as cnt FROM "MerchantMember" WHERE merchant_id = ${qrData.merchant_id}`;
        if (countRow && countRow.cnt >= merchant.billing_starts_at_member_count) {
          const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
          const priceId = getPriceId(merchant.subscription_tier, merchant.billing_cycle || 'monthly');
          if (STRIPE_KEY && priceId && merchant.stripe_customer_id && merchant.stripe_payment_method_id) {
            try {
              const stripeClient = Stripe(STRIPE_KEY);
              const subscription = await stripeClient.subscriptions.create({
                customer: merchant.stripe_customer_id,
                items: [{ price: priceId }],
                default_payment_method: merchant.stripe_payment_method_id,
                metadata: { merchant_id: merchant.id, trigger: 'promo_member_limit' }
              });
              // Fix C: reset member_limit to the correct tier cap now that billing has started.
              // During the promo period, member_limit was set to the promo threshold (e.g. 200).
              // Once billing is active, it must reflect the actual plan cap.
              const tierCapAfterPromo = merchant.subscription_tier === 'online_starter' ? 500
                : merchant.subscription_tier === 'online_growth' ? 2500 : null;
              // Use Stripe's period_end for the next billing date — always accurate for monthly and annual
              const promoNextBillingDate = subscription.current_period_end
                ? new Date(subscription.current_period_end * 1000)
                : null;
              await sql`
                UPDATE "Merchant"
                SET billing_starts_at_member_count = NULL,
                    member_limit = ${tierCapAfterPromo},
                    member_cap_notified = false,
                    stripe_subscription_id = ${subscription.id},
                    billing_status = 'active',
                    subscription_started_at = NOW(),
                    next_billing_date = ${promoNextBillingDate},
                    updated_at = NOW()
                WHERE id = ${qrData.merchant_id}
              `;
              // Send billing-started email
              try {
                const [mu] = await sql`SELECT email FROM "MerchantUser" WHERE merchant_id = ${qrData.merchant_id} LIMIT 1`;
                const BREVO_KEY = process.env.BREVO_API_KEY;
                if (BREVO_KEY && mu?.email) {
                  const brevoClient = SibApiV3Sdk.ApiClient.instance;
                  brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
                  const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
                  const emailObj = new SibApiV3Sdk.SendSmtpEmail();
                  emailObj.sender = { name: 'Perkfinity', email: 'support@perkfinity.net' };
                  emailObj.to = [{ email: mu.email }];
                  emailObj.subject = `🎉 You've reached ${merchant.billing_starts_at_member_count} members — billing has started!`;
                  emailObj.htmlContent = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;"><div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;"><div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div></div><div style="padding:28px 24px;"><div style="font-size:20px;font-weight:700;color:#5b3fa5;margin-bottom:16px;">🎉 Congratulations, ${merchant.business_name}!</div><p style="font-size:15px;color:#555;line-height:1.6;">You've reached your promo member threshold. Your subscription is now active and your card on file will be billed monthly going forward.</p><p style="font-size:15px;color:#555;">Thank you for growing with Perkfinity!</p></div></div>`;
                  await emailApi.sendTransacEmail(emailObj);
                }
              } catch (emailErr) { console.error('Billing-started email failed:', emailErr.message); }
              console.log(`Online promo billing triggered for merchant ${qrData.merchant_id} at ${countRow.cnt} members`);
            } catch (stripeErr) {
              console.error(`Online promo Stripe charge failed for ${qrData.merchant_id}:`, stripeErr.message);
            }
          }
        }
      } else if (merchant && (merchant.subscription_tier === 'trial' || merchant.subscription_tier === 'free')) {
        const limit = merchant.member_limit || 100;
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
                // Omit default_payment_method so Stripe safely falls back to the customer's portal-managed default card
                metadata: { merchant_id: merchant.id }
              });
              // Use Stripe's period_end for the next billing date (correct for both monthly and annual)
              const trialNextBillingDate = subscription.current_period_end
                ? new Date(subscription.current_period_end * 1000)
                : null;
              await sql`
                UPDATE "Merchant" 
                SET subscription_tier = 'tier1', 
                    stripe_subscription_id = ${subscription.id},
                    billing_status = 'active',
                    subscription_started_at = NOW(),
                    next_billing_date = ${trialNextBillingDate},
                    updated_at = NOW() 
                WHERE id = ${qrData.merchant_id}
              `;
              console.log(`Auto-upgraded merchant ${qrData.merchant_id} to tier1 via Stripe (${countRow.cnt} members, limit was ${limit})`);
            } catch (stripeErr) {
              console.error(`Stripe auto-charge failed for merchant ${qrData.merchant_id}:`, stripeErr.message);
              // Block account and record failure timestamp for the reminder job
              await sql`UPDATE "Merchant" SET subscription_tier = 'tier1', billing_status = 'payment_failed', account_blocked = true, payment_failed_at = NOW(), payment_failure_reminder_count = 0, updated_at = NOW() WHERE id = ${qrData.merchant_id}`;
              await sql`UPDATE "Campaign" SET status = 'expired', updated_at = NOW() WHERE merchant_id = ${qrData.merchant_id} AND status = 'active'`;
              // NOTE: billing_starts_at_member_count trigger (online promo) is checked separately below
              // Send Day-0 notification email to merchant immediately
              try {
                const [mu] = await sql`SELECT email FROM "MerchantUser" WHERE merchant_id = ${qrData.merchant_id} LIMIT 1`;
                const BREVO_KEY = process.env.BREVO_API_KEY;
                if (BREVO_KEY && mu?.email) {
                  const brevoClient = SibApiV3Sdk.ApiClient.instance;
                  brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
                  const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
                  const emailObj = new SibApiV3Sdk.SendSmtpEmail();
                  emailObj.sender = { name: 'Perkfinity Support', email: 'support@perkfinity.net' };
                  emailObj.to = [{ email: mu.email }];
                  emailObj.subject = 'Action Required: Payment Failed — Your Perkfinity Account Is Paused';
                  const bizName = merchant?.business_name ? ` ${merchant.business_name}` : '';
                  emailObj.htmlContent = `
                    <div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;background:#ffffff;border-radius:16px;overflow:hidden;border:1px solid #eee;">
                      <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;">
                        <div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div>
                      </div>
                      <div style="padding:28px 24px;">
                        <div style="font-size:20px;font-weight:700;color:#dc2626;margin-bottom:16px;">⚠️ Payment Failed — Action Required</div>
                        <p style="font-size:15px;color:#555;line-height:1.6;margin-bottom:16px;">
                          Hi${bizName},<br><br>
                          Your account has reached the free member limit and we attempted to automatically upgrade you to <strong>Perkfinity Connect</strong>. Unfortunately, your payment method was declined.
                        </p>
                        <div style="background:#fef2f2;border:1.5px solid #fecaca;border-radius:10px;padding:16px 20px;margin-bottom:20px;">
                          <div style="font-size:13px;font-weight:700;color:#dc2626;margin-bottom:8px;">Your account is currently paused:</div>
                          <ul style="margin:0;padding-left:18px;font-size:13px;color:#991b1b;line-height:2;">
                            <li>Members cannot redeem perks by scanning your QR code</li>
                            <li>Campaigns and promotions are frozen</li>
                            <li>Your member data is fully preserved</li>
                          </ul>
                        </div>
                        <p style="font-size:15px;color:#555;line-height:1.6;margin-bottom:24px;">
                          To restore full access, log in to your dashboard, update your payment method, and reactivate your account. The process takes less than a minute.
                        </p>
                        <div style="text-align:center;margin-bottom:24px;">
                          <a href="https://perkfinity.net/dashboard.html" style="display:inline-block;background:#5b3fa5;color:#fff;font-weight:700;text-decoration:none;padding:14px 32px;border-radius:10px;font-size:15px;">Update Payment &amp; Restore Access</a>
                        </div>
                        <p style="font-size:13px;color:#aaa;text-align:center;">Need help? Reply to this email and our team will assist you right away.</p>
                      </div>
                    </div>
                  `;
                  await emailApi.sendTransacEmail(emailObj);
                  console.log(`[PaymentFailed] Day-0 email sent to ${mu.email} for merchant ${qrData.merchant_id}`);
                }
              } catch (emailErr) {
                console.error('[PaymentFailed] Day-0 email send failed:', emailErr.message);
              }
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

async function fetchOpenGraphImage(targetUrl) {
  if (!targetUrl) return null;
  try {
    const urlStr = targetUrl.startsWith('http') ? targetUrl : 'https://' + targetUrl;
    const res = await fetch(urlStr, {
      headers: { 'User-Agent': 'PerkfinityBot/1.0 (+https://perkfinity.net)' }
    });
    if (!res.ok) return null;
    const html = await res.text();
    const $ = cheerio.load(html);
    const ogImage = $('meta[property="og:image"]').attr('content');
    return ogImage || null;
  } catch (e) {
    console.error('Error fetching OG image:', e.message);
    return null;
  }
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

    // ── One-time migration: revenue_type column on Invoice ─────────
    if (!global._invoiceRevenueTypeMigrated) {
      try {
        await sql`ALTER TABLE "Invoice" ADD COLUMN IF NOT EXISTS "revenue_type" TEXT DEFAULT 'platform'`;
        // Backfill existing invoices based on pricing
        await sql`UPDATE "Invoice" SET "revenue_type" = 'sponsorship' WHERE "amount_cents" IN (4999, 9999, 12999)`;
        await sql`UPDATE "Invoice" SET "revenue_type" = 'pouf' WHERE "merchant_id" IN (SELECT "id" FROM "Merchant" WHERE "billing_cycle" = 'lifetime') OR "stripe_invoice_id" LIKE 'cs_%'`;
        global._invoiceRevenueTypeMigrated = true;
      } catch (migErr) { /* non-critical */ }
    }


    // ── One-time migration: AnnouncementLog table ─────────────────
    if (!global._announcementLogMigrated) {
      try {
        await sql`
          CREATE TABLE IF NOT EXISTS "AnnouncementLog" (
            id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
            subject TEXT NOT NULL,
            sender TEXT NOT NULL,
            audience_type TEXT,
            filters JSONB,
            recipient_count INTEGER DEFAULT 0,
            external_count INTEGER DEFAULT 0,
            has_attachments BOOLEAN DEFAULT false,
            status TEXT DEFAULT 'sent',
            html_body TEXT,
            scheduled_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
          )
        `;
        // Add html_body if table already exists without it
        await sql`ALTER TABLE "AnnouncementLog" ADD COLUMN IF NOT EXISTS html_body TEXT`;
        global._announcementLogMigrated = true;
      } catch (migErr) { /* table may already exist */ }
    }


    // ── Health check ──────────────────────────────────────────────
    if (method === 'GET' && (url === '/' || url === '/health' || url.endsWith('/health'))) {
      await sql`SELECT 1`;
      return send(res, 200, { ok: true, status: 'healthy', db: 'connected', version: 'test-2026', timestamp: new Date().toISOString() });
    }

    // ── POST /api/v1/contact ─────────────────────────────────────
    if (method === 'POST' && url.endsWith('/contact')) {
      const data = req.body || {};
      const { name, email, subject, message, attachment, hp } = data;
      // Honeypot — bots fill this hidden field, humans don't
      if (hp) return send(res, 200, { success: true });
      // Validation
      if (!name || !email || !subject || !message) {
        return send(res, 400, { success: false, error: 'All fields are required.' });
      }
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        return send(res, 400, { success: false, error: 'Please enter a valid email address.' });
      }
      // Send via Brevo
      const BREVO_KEY = process.env.BREVO_API_KEY;
      if (BREVO_KEY) {
        const brevoClient = SibApiV3Sdk.ApiClient.instance;
        brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
        const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
        const emailObj = new SibApiV3Sdk.SendSmtpEmail();
        emailObj.sender = { name: 'Perkfinity', email: 'support@perkfinity.net' };
        emailObj.to = [{ email: 'hello@perkfinity.net' }];
        emailObj.replyTo = { email: email.trim(), name: name.trim() };
        emailObj.subject = `[Contact] ${subject}`;
        const safeMsg = String(message).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\n/g, '<br>');
        emailObj.htmlContent = `
          <div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:600px;margin:0 auto;border-radius:12px;overflow:hidden;border:1px solid #e8e4f8">
            <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 32px">
              <div style="color:#fff;font-size:22px;font-weight:800;letter-spacing:-0.5px">Perkfinity</div>
              <div style="color:rgba(255,255,255,0.75);font-size:13px;margin-top:4px">New message via Contact Form</div>
            </div>
            <div style="padding:28px 32px;background:#fff">
              <table style="width:100%;border-collapse:collapse;margin-bottom:24px">
                <tr><td style="padding:9px 0;color:#888;font-size:13px;font-weight:600;width:28%;border-bottom:1px solid #f5f3ff">From</td><td style="padding:9px 0;font-size:14px;font-weight:700;border-bottom:1px solid #f5f3ff">${name.trim()}</td></tr>
                <tr><td style="padding:9px 0;color:#888;font-size:13px;font-weight:600;border-bottom:1px solid #f5f3ff">Email</td><td style="padding:9px 0;font-size:14px;border-bottom:1px solid #f5f3ff"><a href="mailto:${email.trim()}" style="color:#5b3fa5;font-weight:700">${email.trim()}</a></td></tr>
                <tr><td style="padding:9px 0;color:#888;font-size:13px;font-weight:600">Subject</td><td style="padding:9px 0;font-size:14px;font-weight:700">${String(subject).replace(/</g, '&lt;')}</td></tr>
              </table>
              <div style="background:#f8f7ff;border-radius:10px;padding:20px 24px;margin-bottom:20px">
                <div style="font-size:12px;font-weight:700;color:#7c3aed;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px">Message</div>
                <div style="font-size:15px;color:#1a1a2e;line-height:1.75">${safeMsg}</div>
              </div>
              ${attachment ? `<div style="font-size:12px;color:#888;margin-bottom:20px">📎 Attachment included: <strong>${String(attachment.name || 'file').replace(/</g, '&lt;')}</strong></div>` : ''}
              <div style="font-size:12px;color:#aaa;border-top:1px solid #f0eeff;padding-top:16px">
                💡 Hit <strong>Reply</strong> to respond directly to <strong>${name.trim()}</strong> at <strong>${email.trim()}</strong>
              </div>
            </div>
          </div>`;
        if (attachment && attachment.content && attachment.name) {
          emailObj.attachment = [{ content: attachment.content, name: attachment.name }];
        }
        await emailApi.sendTransacEmail(emailObj);
      }
      return send(res, 200, { success: true });
    }

    // ── DB Migration: Access Codes ──────────────────────────────
    if (url === '/api/v1/migrate-codes' && method === 'GET') {
      await sql`ALTER TABLE "AdminAccessCode" ADD COLUMN IF NOT EXISTS "type" TEXT DEFAULT 'free_for_life'`;
      await sql`ALTER TABLE "AdminAccessCode" ADD COLUMN IF NOT EXISTS "member_limit" INTEGER`;
      await sql`ALTER TABLE "AdminAccessCode" ADD COLUMN IF NOT EXISTS "use_count" INTEGER DEFAULT 0`;
      return send(res, 200, { success: true, message: 'Access codes DB migration complete!' });
    }

    // ── POST /api/v1/merchants/signup ─────────────────────────────
    if (method === 'POST' && url.endsWith('/merchants/signup')) {
      const data = req.body || {};
      const presence = (data.business_presence || 'physical').toLowerCase();
      if (!['physical', 'mobile', 'online', 'hybrid'].includes(presence)) {
        return send(res, 400, { success: false, error: 'Invalid business_presence. Must be physical, mobile, online, or hybrid.' });
      }

      // Validate core required fields (all presence types)
      const missing = [];
      if (!data.name) missing.push('Store Name');
      if (!data.contactName) missing.push('Contact Name');
      if (!data.phone) missing.push('Phone Number');
      if (!data.email) missing.push('Email');
      if (!data.password) missing.push('Password');
      if (!data.welcome_offer_text) missing.push('Welcome Offer Description');

      // Dynamic address/website validation per presence type
      if (presence === 'physical') {
        if (!data.address) missing.push('Street Address');
        if (!data.city) missing.push('City');
        if (!data.state) missing.push('State');
        if (!data.zip) missing.push('ZIP Code');
      } else if (presence === 'mobile') {
        if (!data.city) missing.push('City');
        if (!data.state) missing.push('State');
        if (!data.zip) missing.push('ZIP Code');
      } else if (presence === 'online') {
        if (!data.website) missing.push('Website URL');
      }

      if (missing.length > 0) {
        return send(res, 400, { success: false, error: `Missing required fields: ${missing.join(', ')}` });
      }

      // Validate formats
      const phoneRegex = /^\d{3}-\d{3}-\d{4}$/;
      if (!phoneRegex.test(data.phone)) {
        return send(res, 400, { success: false, error: 'Phone number must be in xxx-xxx-xxxx format.' });
      }
      if (presence !== 'online') {
        const zipRegex = /^\d{5}$/;
        if (!zipRegex.test(data.zip)) {
          return send(res, 400, { success: false, error: 'ZIP Code must be a 5-digit number.' });
        }
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

      // Auto-generate welcome promo code for online merchants only
      const welcomePromoCode = presence === 'online'
        ? 'HELLO-' + data.name.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12)
        : null;

      // Admin promo code validation → member_limit / tier
      let memberLimit = 100;
      let selectedTier = data.tier || 'trial';
      let skipStripe = false;
      let promoCode = (data.promo_code || '').trim().toUpperCase();
      let extendedTrial = false;
      if (promoCode) {
        const [accessCode] = await sql`
          SELECT id, label, type, member_limit, used
          FROM "AdminAccessCode"
          WHERE code = ${promoCode} AND expires_at > NOW()
          LIMIT 1
        `;
        if (!accessCode) return send(res, 400, { success: false, error: 'Invalid or expired promo code.' });
        if (accessCode.type === 'free_for_life') {
          if (accessCode.used) return send(res, 400, { success: false, error: 'This promo code has already been used.' });
          memberLimit = 999999; selectedTier = 'free_for_life'; skipStripe = true;
        } else if (accessCode.type === 'extended_trial') {
          memberLimit = accessCode.member_limit || 100; extendedTrial = true;
        } else if (accessCode.type === 'pouf') {
          if (data.billing_cycle !== 'annual') return send(res, 400, { success: false, error: 'POUF Lifetime codes require Annual billing. Please switch to Annual.' });
        } else {
          return send(res, 400, { success: false, error: 'Unrecognized promo code type.' });
        }
      } else {
        promoCode = null;
      }

      // Insert merchant with presence and welcome offer fields
      const [merchant] = await sql`
        INSERT INTO "Merchant" (id, business_name, contact_name, phone, public_phone, public_email, website, pos_system, business_presence, welcome_promo_code, welcome_offer_text, subscription_tier, member_limit, promo_code, business_category, status, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${data.name}, ${data.contactName}, ${data.phone}, ${data.public_phone ? data.public_phone.trim() : null}, ${data.public_email ? data.public_email.trim().toLowerCase() : null}, ${data.website || ''}, ${data.pos_system || ''}, ${presence}, ${welcomePromoCode}, ${data.welcome_offer_text}, ${selectedTier}, ${memberLimit}, ${promoCode}, ${data.business_category || null}, 'active', ${now}, ${now})
        RETURNING id, business_name, subscription_tier, member_limit, welcome_promo_code
      `;

      if (skipStripe && promoCode) {
        await sql`UPDATE "AdminAccessCode" SET used = true, used_by = ${merchant.id}, used_at = NOW(), use_count = use_count + 1 WHERE code = ${promoCode} AND type = 'free_for_life'`;
      }
      if (extendedTrial && promoCode) {
        await sql`UPDATE "AdminAccessCode" SET use_count = use_count + 1, used_at = NOW() WHERE code = ${promoCode} AND type = 'extended_trial'`;
      }

      // Insert owner user
      const [merchantUser] = await sql`
        INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${email}, ${password_hash}, 'owner', 'active', ${now})
        RETURNING id, merchant_id, email, role, status, created_at
      `;

      // Insert location — conditional per presence type
      if (presence === 'physical') {
        await sql`
          INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.address}, ${data.suite || ''}, ${data.city}, ${data.state}, ${data.zip}, 'US', true, ${now})
        `;
      } else if (presence === 'mobile') {
        await sql`
          INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, NULL, '', ${data.city}, ${data.state}, ${data.zip}, 'US', true, ${now})
        `;
      } else {
        // online: stub location row so relations remain intact
        await sql`
          INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, NULL, '', NULL, NULL, NULL, 'US', true, ${now})
        `;
      }

      // Insert welcome campaign — end_at = NULL (infinite) for ALL presence types
      await sql`
        INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, status, campaign_type, start_at, end_at, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.welcome_offer_text || 'Welcome Perk'}, 10, 'active', 'initial', ${now}, NULL, ${now}, ${now})
      `;

      // Insert QR code
      const public_code = crypto.randomBytes(9).toString('base64url');
      await sql`
        INSERT INTO "QrCode" (id, merchant_id, public_code, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${public_code}, 'active', ${now})
      `;

      // ── Contractor attribution (Task 3) ────────────────────────────
      const _signupRefCode = (data.contractor_code || '').toUpperCase().trim();
      if (_signupRefCode) {
        const [_signupContr] = await sql`SELECT id FROM "Contractor" WHERE referral_code = ${_signupRefCode} AND status = 'active' LIMIT 1`;
        if (_signupContr) {
          await sql`
            INSERT INTO "ContractorMerchantAttribution" (id, contractor_id, merchant_id, source, created_at, updated_at)
            VALUES (gen_random_uuid()::text, ${_signupContr.id}, ${merchant.id}, 'self', NOW(), NOW())
            ON CONFLICT (merchant_id) DO NOTHING
          `;
        }
      }
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
          qr_url: `https://www.perkfinity.net/qr/${public_code}`,
          skip_stripe: skipStripe,
          welcome_promo_code: welcomePromoCode,
        }
      });
    }

    // ── POST /api/v1/merchants/apply — Online Brand / Hybrid Application ────
    if (method === 'POST' && url.endsWith('/merchants/apply')) {
      const data = req.body || {};

      // Determine presence type — 'online' or 'hybrid'
      const applyPresence = (data.business_presence || 'online').toLowerCase();
      if (!['online', 'hybrid'].includes(applyPresence)) {
        return send(res, 400, { success: false, error: 'business_presence must be online or hybrid.' });
      }
      const isHybridApply = applyPresence === 'hybrid';

      const required = ['name', 'website', 'category', 'welcome_offer_text', 'tier', 'email', 'password', 'stripe_payment_method_id'];
      for (const f of required) {
        if (!data[f]) return send(res, 400, { success: false, error: `${f} is required` });
      }

      // Both Online and Hybrid: backend auto-generates HELLO-[BRANDNAME] as the welcome promo code.
      // No separate welcome_campaign_promo field needed from the frontend.

      // Hybrid single-location: address is required
      const isMultiLocation = data.is_multi_location === true;
      if (isHybridApply && !isMultiLocation) {
        if (!data.address || !data.city || !data.state || !data.zip) {
          return send(res, 400, { success: false, error: 'address, city, state, and zip are required for single-location hybrid businesses.' });
        }
      }

      const email = data.email.toLowerCase().trim();
      const [existing] = await sql`SELECT id FROM "MerchantUser" WHERE email = ${email} LIMIT 1`;
      if (existing) return send(res, 409, { success: false, error: 'An account with this email already exists.' });

      const password_hash = await bcrypt.hash(data.password, 12);
      const now = new Date();

      // Welcome promo code: HELLO-BRANDNAME (auto-generated system code for QR join flow)
      const welcomePromoCode = 'HELLO-' + data.name.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);

      // Promo code validation (extended_trial only)
      let memberLimit = data.tier === 'online_starter' ? 500 : data.tier === 'online_growth' ? 2500 : null;
      let billingStartsAt = null;
      let promoCode = (data.promo_code || '').trim().toUpperCase() || null;
      if (promoCode) {
        const [accessCode] = await sql`
          SELECT id, type, member_limit FROM "AdminAccessCode"
          WHERE code = ${promoCode} AND expires_at > NOW() LIMIT 1
        `;
        if (!accessCode) return send(res, 400, { success: false, error: 'Invalid or expired promo code.' });
        if (accessCode.type !== 'extended_trial') return send(res, 400, { success: false, error: 'Only extended trial promo codes are accepted.' });
        memberLimit = accessCode.member_limit || 150;
        billingStartsAt = memberLimit;
        await sql`UPDATE "AdminAccessCode" SET use_count = use_count + 1, used_at = NOW() WHERE code = ${promoCode}`;
      }

      // Create Stripe customer and attach payment method
      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);
      const customer = await stripeClient.customers.create({
        name: data.name,
        email: email,
        payment_method: data.stripe_payment_method_id,
        invoice_settings: { default_payment_method: data.stripe_payment_method_id },
        metadata: { source: isHybridApply ? 'hybrid_application' : 'online_brand_application' }
      });
      await stripeClient.paymentMethods.attach(data.stripe_payment_method_id, { customer: customer.id }).catch(() => { });

      // Accept logo_url (base64 string) if provided
      const logoUrl = data.logo_url || null;

      let coverPhotoUrl = null;
      if (data.website && ['online', 'hybrid'].includes(applyPresence)) {
        coverPhotoUrl = await fetchOpenGraphImage(data.website);
      }

      // Create merchant row
      const billingCycleValue = data.billing_cycle === 'annual' ? 'annual' : 'monthly';

      const [merchant] = await sql`
        INSERT INTO "Merchant" (
          id, business_name, contact_name, phone, public_phone, public_email, website, business_presence,
          welcome_promo_code, welcome_offer_text, subscription_tier, member_limit,
          promo_code, status, billing_status, application_status,
          business_category, billing_starts_at_member_count, is_multi_location,
          billing_cycle,
          stripe_customer_id, stripe_payment_method_id, logo_url, cover_photo_url, created_at, updated_at
        ) VALUES (
          gen_random_uuid()::text, ${data.name}, ${data.contactName || ''}, ${data.phone || ''},
          ${data.public_phone ? data.public_phone.trim() : null}, ${data.public_email ? data.public_email.trim().toLowerCase() : null},
          ${data.website}, ${applyPresence}, ${welcomePromoCode}, ${data.welcome_offer_text},
          ${data.tier}, ${memberLimit}, ${promoCode}, 'active', 'trial',
          'pending', ${data.category},
          ${billingStartsAt}, ${isMultiLocation},
          ${billingCycleValue},
          ${customer.id}, ${data.stripe_payment_method_id}, ${logoUrl}, ${coverPhotoUrl}, ${now}, ${now}
        )
        RETURNING id, business_name, subscription_tier, member_limit, welcome_promo_code, application_status
      `;

      // Create MerchantUser
      await sql`
        INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${email}, ${password_hash}, 'owner', 'active', ${now})
      `;

      // Location row — real address for single-location hybrid; stub for online or multi-location hybrid
      if (isHybridApply && !isMultiLocation) {
        // Single-location hybrid: store real address for localperks discovery
        await sql`
          INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.address}, ${data.suite || ''}, ${data.city}, ${data.state}, ${data.zip}, 'US', true, ${now})
        `;
      } else {
        // Online or multi-location hybrid: stub row (no physical address)
        await sql`
          INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, NULL, '', NULL, NULL, NULL, 'US', true, ${now})
        `;
      }

      // Initial welcome campaign — use auto-generated HELLO code as the promo for both Online and Hybrid
      const campaignPromo = welcomePromoCode;
      await sql`
        INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, status, campaign_type, promo_code, start_at, end_at, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.welcome_offer_text || 'Welcome Perk'}, 10, 'active', 'initial',
                ${campaignPromo}, ${now}, NULL, ${now}, ${now})
      `;

      // QR code
      const public_code = crypto.randomBytes(9).toString('base64url');
      await sql`
        INSERT INTO "QrCode" (id, merchant_id, public_code, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${public_code}, 'active', ${now})
      `;

      // Notify admin of new application
      try {
        const BREVO_KEY = process.env.BREVO_API_KEY;
        const ADMIN_EMAIL_ADDR = process.env.ADMIN_EMAIL || 'admin@perkfinity.net';
        if (BREVO_KEY) {
          const brevoClient = SibApiV3Sdk.ApiClient.instance;
          brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
          const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
          const emailObj = new SibApiV3Sdk.SendSmtpEmail();
          emailObj.sender = { name: 'Perkfinity System', email: 'support@perkfinity.net' };
          emailObj.to = [{ email: ADMIN_EMAIL_ADDR }];
          const presenceLabel = isHybridApply ? `Hybrid${isMultiLocation ? ' (Multi-Location)' : ' (Single-Location)'}` : 'Online';
          emailObj.subject = `[New Application] ${data.name} — ${presenceLabel} — ${data.tier}`;
          emailObj.htmlContent = `<p>New ${presenceLabel} brand application received.</p><ul><li><b>Brand:</b> ${data.name}</li><li><b>Type:</b> ${presenceLabel}</li><li><b>Website:</b> ${data.website}</li><li><b>Category:</b> ${data.category}</li><li><b>Tier:</b> ${data.tier}</li><li><b>Promo Code:</b> ${promoCode || 'None'}</li><li><b>Contact:</b> ${email}</li>${isHybridApply && !isMultiLocation ? `<li><b>Address:</b> ${data.address}, ${data.city}, ${data.state} ${data.zip}</li>` : ''}</ul><p>Review in Admin Dashboard → Applications tab.</p>`;
          await emailApi.sendTransacEmail(emailObj);
        }
      } catch (notifyErr) { console.error('Admin notification email failed:', notifyErr.message); }

      return send(res, 201, {
        success: true,
        data: {
          merchant_id: merchant.id,
          business_name: merchant.business_name,
          application_status: merchant.application_status,
          welcome_promo_code: merchant.welcome_promo_code,
          qr_public_code: public_code,
        }
      });
    }

    // ── POST /api/v1/merchants/apply-create-account ───────────────
    // NEW 5-step flow Step 3: creates merchant + user + location + campaign
    // with application_status='in_progress'. Auto-logs the merchant in.
    if (method === 'POST' && url.endsWith('/merchants/apply-create-account')) {
      const data = req.body || {};
      const applyPresence = (data.business_presence || 'online').toLowerCase();
      if (!['online', 'hybrid'].includes(applyPresence)) {
        return send(res, 400, { success: false, error: 'business_presence must be online or hybrid.' });
      }
      const isHybridApply = applyPresence === 'hybrid';
      const isMultiLocation = data.is_multi_location === true;

      const required = ['name', 'website', 'category', 'welcome_offer_text', 'tier', 'email', 'password', 'contactName'];
      for (const f of required) {
        if (!data[f]) return send(res, 400, { success: false, error: `${f} is required` });
      }
      if (isHybridApply && !isMultiLocation) {
        if (!data.address || !data.city || !data.state || !data.zip) {
          return send(res, 400, { success: false, error: 'Address fields are required for single-location hybrid.' });
        }
      }

      const email = data.email.toLowerCase().trim();
      const [existing] = await sql`SELECT id FROM "MerchantUser" WHERE email = ${email} LIMIT 1`;
      if (existing) return send(res, 409, { success: false, error: 'An account with this email already exists.' });

      const password_hash = await bcrypt.hash(data.password, 12);
      const now = new Date();
      const welcomePromoCode = 'HELLO-' + data.name.toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);

      let memberLimit = data.tier === 'online_starter' ? 500 : data.tier === 'online_growth' ? 2500 : null;
      let billingStartsAt = null;
      let skipStripe = false;
      let promoCode = (data.promo_code || '').trim().toUpperCase() || null;
      if (promoCode) {
        const [accessCode] = await sql`
          SELECT id, type, member_limit, used FROM "AdminAccessCode"
          WHERE code = ${promoCode} AND expires_at > NOW() LIMIT 1
        `;
        if (!accessCode) return send(res, 400, { success: false, error: 'Invalid or expired promo code.' });
        if (accessCode.type === 'free_for_life') {
          if (accessCode.used) return send(res, 400, { success: false, error: 'This promo code has already been used.' });
          memberLimit = 999999; skipStripe = true;
          data.tier = 'free_for_life';
        } else if (accessCode.type === 'extended_trial') {
          memberLimit = accessCode.member_limit || 150;
          billingStartsAt = memberLimit;
          await sql`UPDATE "AdminAccessCode" SET use_count = use_count + 1, used_at = NOW() WHERE code = ${promoCode}`;
        } else if (accessCode.type === 'pouf') {
          if (data.billing_cycle && data.billing_cycle !== 'annual') {
            return send(res, 400, { success: false, error: 'POUF promo codes require an annual billing cycle.' });
          }
        } else {
          return send(res, 400, { success: false, error: 'Unrecognized promo code type.' });
        }
      }

      // For FFL: skip Stripe customer creation entirely
      let customer = { id: null };
      if (!skipStripe) {
        const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
        const stripeClient = Stripe(STRIPE_KEY);
        customer = await stripeClient.customers.create({
          name: data.name,
          email: email,
          metadata: { source: isHybridApply ? 'hybrid_application' : 'online_brand_application' }
        });
      }

      const billingCycleValue2 = data.billing_cycle === 'annual' ? 'annual' : 'monthly';

      let coverPhotoUrl2 = null;
      if (data.website && ['online', 'hybrid'].includes(applyPresence)) {
        coverPhotoUrl2 = await fetchOpenGraphImage(data.website);
      }

      const [merchant] = await sql`
        INSERT INTO "Merchant" (
          id, business_name, contact_name, phone, public_phone, public_email, website, business_presence,
          welcome_promo_code, welcome_offer_text, subscription_tier, member_limit,
          promo_code, status, billing_status, application_status,
          business_category, billing_starts_at_member_count, is_multi_location,
          billing_cycle,
          stripe_customer_id, logo_url, cover_photo_url, created_at, updated_at
        ) VALUES (
          gen_random_uuid()::text, ${data.name}, ${data.contactName || ''}, ${data.phone || ''},
          ${data.public_phone ? data.public_phone.trim() : null}, ${data.public_email ? data.public_email.trim().toLowerCase() : null},
          ${data.website}, ${applyPresence}, ${welcomePromoCode}, ${data.welcome_offer_text},
          ${data.tier}, ${memberLimit}, ${promoCode}, 'active', null,
          'in_progress', ${data.category},
          ${billingStartsAt}, ${isMultiLocation},
          ${billingCycleValue2},
          ${customer.id}, null, ${coverPhotoUrl2}, ${now}, ${now}
        )
        RETURNING id, business_name, subscription_tier, member_limit, welcome_promo_code, stripe_customer_id
      `;

      const [muRow] = await sql`
        INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${email}, ${password_hash}, 'owner', 'active', ${now})
        RETURNING id
      `;

      if (isHybridApply && !isMultiLocation) {
        await sql`INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.address}, ${data.suite || ''}, ${data.city}, ${data.state}, ${data.zip}, 'US', true, ${now})`;
      } else {
        await sql`INSERT INTO "MerchantLocation" (id, merchant_id, address, suite, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, NULL, '', NULL, NULL, NULL, 'US', true, ${now})`;
      }

      await sql`INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, status, campaign_type, promo_code, start_at, end_at, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.welcome_offer_text || 'Welcome Perk'}, 10, 'active', 'initial', ${welcomePromoCode}, ${now}, NULL, ${now}, ${now})`;

      const public_code = crypto.randomBytes(9).toString('base64url');
      await sql`INSERT INTO "QrCode" (id, merchant_id, public_code, status, created_at)
        VALUES (gen_random_uuid()::text, ${merchant.id}, ${public_code}, 'active', ${now})`;

      // ── Contractor attribution (Task 3) ────────────────────────────
      const _applyRefCode = (data.contractor_code || '').toUpperCase().trim();
      if (_applyRefCode) {
        const [_applyContr] = await sql`SELECT id FROM "Contractor" WHERE referral_code = ${_applyRefCode} AND status = 'active' LIMIT 1`;
        if (_applyContr) {
          await sql`
            INSERT INTO "ContractorMerchantAttribution" (id, contractor_id, merchant_id, source, created_at, updated_at)
            VALUES (gen_random_uuid()::text, ${_applyContr.id}, ${merchant.id}, 'self', NOW(), NOW())
            ON CONFLICT (merchant_id) DO NOTHING
          `;
        }
      }
      const JWT_SECRET = process.env.JWT_SECRET;
      const accessToken = jwt.sign(
        { userId: muRow.id, merchantId: merchant.id, role: 'owner' },
        JWT_SECRET, { expiresIn: '8h' }
      );

      // Mark FFL code as used (after merchant row created so we have merchant.id)
      if (skipStripe && promoCode) {
        await sql`UPDATE "AdminAccessCode" SET used = true, used_by = ${merchant.id}, used_at = NOW(), use_count = use_count + 1 WHERE code = ${promoCode} AND type = 'free_for_life'`;
      }

      return send(res, 201, {
        success: true,
        data: { merchant_id: merchant.id, access_token: accessToken, welcome_promo_code: merchant.welcome_promo_code, qr_public_code: public_code, skip_stripe: skipStripe }
      });
    }

    // ── POST /api/v1/stripe/save-apply-payment ────────────────────
    // Step 4 of new apply flow: attaches Stripe PM to customer + saves to DB.
    // Does NOT set billing_status (billing starts only after admin approval).
    if (method === 'POST' && url.endsWith('/stripe/save-apply-payment')) {
      const data = req.body || {};
      const { merchant_id, payment_method_id } = data;
      if (!merchant_id || !payment_method_id) {
        return send(res, 400, { success: false, error: 'merchant_id and payment_method_id are required' });
      }
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) return send(res, 401, { success: false, error: 'Unauthorized' });
      let decoded;
      try { decoded = jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET); }
      catch (e) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (decoded.merchantId !== merchant_id) return send(res, 403, { success: false, error: 'Forbidden' });

      const [merchant] = await sql`SELECT id, stripe_customer_id FROM "Merchant" WHERE id = ${merchant_id} LIMIT 1`;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (STRIPE_KEY && merchant.stripe_customer_id) {
        try {
          const stripeClient = Stripe(STRIPE_KEY);
          await stripeClient.paymentMethods.attach(payment_method_id, { customer: merchant.stripe_customer_id }).catch(() => { });
          await stripeClient.customers.update(merchant.stripe_customer_id, {
            invoice_settings: { default_payment_method: payment_method_id }
          });
        } catch (e) { console.error('Stripe PM attach error:', e.message); }
      }

      await sql`UPDATE "Merchant" SET stripe_payment_method_id = ${payment_method_id}, updated_at = NOW() WHERE id = ${merchant_id}`;
      return send(res, 200, { success: true });
    }

    // ── POST /api/v1/merchants/:id/submit-application ─────────────
    // Step 5 of new apply flow: updates merchant fields from review,
    // saves optional logo, changes application_status → 'pending'.
    const submitAppMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/submit-application$/);
    if (method === 'POST' && submitAppMatch) {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) return send(res, 401, { success: false, error: 'Unauthorized' });
      let decoded;
      try { decoded = jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET); }
      catch (e) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      const merchantId = submitAppMatch[1];
      if (decoded.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const data = req.body || {};
      const [merchant] = await sql`
        SELECT m.*, mu.email as contact_email
        FROM "Merchant" m LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
        WHERE m.id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchant.application_status === 'pending') return send(res, 409, { success: false, error: 'Application already submitted.' });
      if (merchant.application_status !== 'in_progress') return send(res, 400, { success: false, error: 'Cannot submit application in its current state.' });
      if (!merchant.stripe_payment_method_id && merchant.subscription_tier !== 'free_for_life') {
        return send(res, 400, { success: false, error: 'Payment method is required before submitting.' });
      }

      // Full field update + status change in one query
      await sql`
        UPDATE "Merchant" SET
          business_name      = COALESCE(${data.name || null}, business_name),
          website            = COALESCE(${data.website || null}, website),
          welcome_offer_text = COALESCE(${data.welcome_offer_text || null}, welcome_offer_text),
          business_category  = COALESCE(${data.category || null}, business_category),
          logo_url           = CASE WHEN ${data.logo_url !== undefined} THEN ${data.logo_url || null} ELSE logo_url END,
          application_status = 'pending',
          updated_at         = NOW()
        WHERE id = ${merchantId}
      `;

      if (data.welcome_offer_text) {
        await sql`UPDATE "Campaign" SET title = ${data.welcome_offer_text}, updated_at = NOW()
          WHERE merchant_id = ${merchantId} AND campaign_type = 'initial'`;
      }

      // Confirmation email to merchant
      try {
        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY && merchant.contact_email) {
          const brevoClient = SibApiV3Sdk.ApiClient.instance;
          brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
          const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
          const confirmEmail = new SibApiV3Sdk.SendSmtpEmail();
          confirmEmail.sender = { name: 'Perkfinity', email: 'support@perkfinity.net' };
          confirmEmail.to = [{ email: merchant.contact_email }];
          confirmEmail.subject = `✅ Application Received — ${merchant.business_name}`;
          confirmEmail.htmlContent = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto"><div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center"><div style="color:#fff;font-size:24px;font-weight:800">Perkfinity</div></div><div style="padding:28px 24px"><p style="font-size:20px;font-weight:700;color:#5b3fa5">Application received! 🎉</p><p style="font-size:15px;color:#555;line-height:1.6">Thank you, <strong>${merchant.contact_name}</strong>! We received your application for <strong>${merchant.business_name}</strong>.</p><p style="font-size:15px;color:#555">Our team reviews within <strong>1–2 business days</strong>. You'll receive an email once a decision is made.</p><p style="font-size:13px;color:#888">Questions? Contact us at <a href="mailto:support@perkfinity.net">support@perkfinity.net</a>.</p></div></div>`;
          await emailApi.sendTransacEmail(confirmEmail);
        }
      } catch (e) { console.error('Merchant confirmation email failed:', e.message); }

      // Admin notification
      try {
        const BREVO_KEY = process.env.BREVO_API_KEY;
        const ADMIN_EMAIL_ADDR = process.env.ADMIN_EMAIL || 'admin@perkfinity.net';
        if (BREVO_KEY) {
          const brevoClient = SibApiV3Sdk.ApiClient.instance;
          brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
          const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
          const adminEmail = new SibApiV3Sdk.SendSmtpEmail();
          adminEmail.sender = { name: 'Perkfinity System', email: 'support@perkfinity.net' };
          adminEmail.to = [{ email: ADMIN_EMAIL_ADDR }];
          const presenceLabel = merchant.business_presence === 'hybrid' ? 'Hybrid' : 'Online';
          adminEmail.subject = `[New Application] ${merchant.business_name} — ${presenceLabel}`;
          adminEmail.htmlContent = `<p>New ${presenceLabel} application ready for review.</p><ul><li><b>Brand:</b> ${merchant.business_name}</li><li><b>Tier:</b> ${merchant.subscription_tier}</li><li><b>Contact:</b> ${merchant.contact_email}</li></ul><p>Review in Admin Dashboard → Applications tab.</p>`;
          await emailApi.sendTransacEmail(adminEmail);
        }
      } catch (e) { console.error('Admin notification failed:', e.message); }

      return send(res, 200, { success: true, message: 'Application submitted' });
    }

    // ── POST /api/v1/auth/login ────────────────────────────────────

    if (method === 'POST' && (url.endsWith('/auth/login') || url.endsWith('/merchants/login'))) {
      const data = req.body || {};
      if (!data.email || !data.password) {
        return send(res, 400, { success: false, error: 'email and password are required' });
      }

      const [user] = await sql`
        SELECT u.*, m.business_name, m.subscription_tier, m.status as merchant_status, m.logo_url,
               m.stripe_payment_method_id, m.stripe_subscription_id, m.billing_status, m.billing_cycle,
               m.business_presence, m.onboarding_complete, m.application_status, m.is_presetup, m.is_claimed
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

      // ── Gate 1: No payment method on file (physical/mobile, any billing state) ─
      // Fires if PM is missing for any non-FFL, non-online/hybrid merchant that hasn't
      // been fully cancelled or is winding down. Catches both:
      //   (a) Merchant never completed payment setup (new signup)
      //   (b) Merchant deleted their card in Stripe after setup (payment_method.detached
      //       cleared our DB field via webhook)
      // Excludes pending_cancellation: their account ends at period end, no future charge needed.
      // Excludes onboarding_complete=true: already-onboarded merchants (e.g. pre-existing
      //   merchants who signed up before stripe_payment_method_id column existed) are never
      //   routed back to payment capture — they access their dashboard normally.
      const setupIncomplete = (
        !user.stripe_payment_method_id &&
        !user.onboarding_complete &&
        user.subscription_tier !== 'free_for_life' &&
        !['online', 'hybrid'].includes(user.business_presence) &&
        !['cancelled', 'deleted', 'pending_cancellation'].includes(user.billing_status)
      );

      // ── Gate 2: Logo/QR step not completed (physical/mobile/FFL) ─
      const needsStep5 = (
        !user.onboarding_complete &&
        !setupIncomplete &&
        !['online', 'hybrid'].includes(user.business_presence) &&
        !['cancelled', 'deleted'].includes(user.billing_status)
      );

      // ── Gate 3: Online/Hybrid pending approval ────────────────────
      const pendingApproval = (
        ['online', 'hybrid'].includes(user.business_presence) &&
        user.application_status === 'pending'
      );

      // ── Gate 4: Online/Hybrid application declined ────────────────
      const applicationDeclined = (
        ['online', 'hybrid'].includes(user.business_presence) &&
        user.application_status === 'declined'
      );

      // ── Gate 5: Online/Hybrid in-progress, payment not yet saved ─
      const applicationNeedsPayment = (
        ['online', 'hybrid'].includes(user.business_presence) &&
        user.application_status === 'in_progress' &&
        !user.stripe_payment_method_id
      );

      // ── Gate 6: Online/Hybrid in-progress, payment saved, not submitted
      const applicationNeedsSubmit = (
        ['online', 'hybrid'].includes(user.business_presence) &&
        user.application_status === 'in_progress' &&
        !!user.stripe_payment_method_id
      );

      // ── Gate 7: Online/Hybrid approved but payment method was removed ─
      // Mirrors Gate 1 for online/hybrid: approved merchant deleted their card.
      // Excludes pending_cancellation (winding down, no future charge needed).
      // Excludes cancelled/deleted (have their own scenes in dashboard).
      // Excludes free_for_life: FFL merchants never go through Stripe setup,
      //   so the absence of a payment method is expected and correct for them.
      const isDemoAccount = (
        user.billing_status === 'active' && 
        !user.stripe_subscription_id && 
        !user.stripe_payment_method_id &&
        ['online', 'hybrid'].includes(user.business_presence)
      );

      const onlineNeedsPaymentUpdate = (
        ['online', 'hybrid'].includes(user.business_presence) &&
        user.application_status === 'approved' &&
        !user.stripe_payment_method_id &&
        user.subscription_tier !== 'free_for_life' &&
        user.billing_cycle !== 'lifetime' &&
        !['cancelled', 'deleted', 'pending_cancellation'].includes(user.billing_status) &&
        !isDemoAccount &&
        !user.is_presetup
      );

      const { password_hash: _pw, ...safeUser } = user;
      return send(res, 200, {
        success: true,
        data: {
          merchantUser: safeUser,
          accessToken,
          setup_incomplete: setupIncomplete,
          needs_step5: needsStep5,
          pending_approval: pendingApproval,
          application_declined: applicationDeclined,
          application_needs_payment: applicationNeedsPayment,
          application_needs_submit: applicationNeedsSubmit,
          online_needs_payment_update: onlineNeedsPaymentUpdate
        }
      });
    }
    // ── POST /api/v1/merchants/:id/mark-onboarded ─────────────────
    // Called by signup.html when the merchant reaches the QR sign screen (Step 5 complete).
    // Sets onboarding_complete = true so next login goes to dashboard, not step5 resume.
    const markOnboardedMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/mark-onboarded$/);
    if (method === 'POST' && markOnboardedMatch) {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return send(res, 401, { success: false, error: 'Unauthorized' });
      }
      let decoded;
      try {
        decoded = jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET);
      } catch (e) {
        return send(res, 401, { success: false, error: 'Invalid token' });
      }
      const merchantId = markOnboardedMatch[1];
      if (decoded.merchantId !== merchantId) {
        return send(res, 403, { success: false, error: 'Forbidden' });
      }
      await sql`
        UPDATE "Merchant"
        SET onboarding_complete = true, updated_at = NOW()
        WHERE id = ${merchantId}
      `;
      return send(res, 200, { success: true });
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
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "email_unsubscribed" BOOLEAN DEFAULT false`;
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
      await sql`ALTER TABLE "NotificationQueue" ADD COLUMN IF NOT EXISTS "disclaimer" TEXT`;
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
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "pos_system" TEXT`;

      // ── Tasks 8 & 9: Multi Business Presence + Merchant Discovery ──
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS business_presence VARCHAR(10) DEFAULT 'physical'`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS welcome_promo_code VARCHAR(50)`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS welcome_offer_text VARCHAR(200)`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS review_url VARCHAR(500)`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS order_url VARCHAR(500)`;
      await sql`ALTER TABLE "Campaign" ADD COLUMN IF NOT EXISTS promo_code VARCHAR(100)`;
      await sql`ALTER TABLE "Redemption" ALTER COLUMN expires_at DROP NOT NULL`;
      await sql`ALTER TABLE "Redemption" ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ`;
      await sql`ALTER TABLE "NotificationQueue" ADD COLUMN IF NOT EXISTS promo_code VARCHAR(100)`;
      await sql`ALTER TABLE "NotificationQueue" ADD COLUMN IF NOT EXISTS is_online_merchant BOOLEAN DEFAULT false`;
      await sql`ALTER TABLE "NotificationQueue" ADD COLUMN IF NOT EXISTS website TEXT`;
      // Retroactive: tag existing welcome campaigns (no AuditLog entry = initial campaign)
      await sql`
        UPDATE "Campaign" SET campaign_type = 'initial'
        WHERE (campaign_type IS NULL OR campaign_type = '')
          AND id NOT IN (
            SELECT DISTINCT target_id FROM "AuditLog"
            WHERE target_type = 'Campaign' AND target_id IS NOT NULL
          )
      `;
      // Retroactive: set infinite expiration on all initial campaigns
      await sql`UPDATE "Campaign" SET end_at = NULL WHERE campaign_type = 'initial'`;
      // Retroactive: null out expires_at on redemptions for initial campaigns
      await sql`
        UPDATE "Redemption" SET expires_at = NULL
        WHERE campaign_id IN (SELECT id FROM "Campaign" WHERE campaign_type = 'initial')
      `;

      // ── Online Merchant Platform: New Merchant columns ────────────
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS business_category VARCHAR(50)`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "cover_photo_url" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "promo_banner_url" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "is_fullpage_sponsored" BOOLEAN DEFAULT false`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "fullpage_sponsored_until" TIMESTAMPTZ`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "stripe_fullpage_sponsor_subscription_id" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "rating_score" VARCHAR(10)`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "rating_count" VARCHAR(20)`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "rating_platform" VARCHAR(20)`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "promo_description" TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS application_status VARCHAR(20) DEFAULT NULL`;
      await sql`ALTER TABLE "Merchant" DROP COLUMN IF EXISTS sales_volume_range`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS application_notes TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS billing_starts_at_member_count INT DEFAULT NULL`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS member_cap_notified BOOLEAN DEFAULT FALSE`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS cap_block_count INT DEFAULT 0`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS public_phone TEXT`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS public_email TEXT`;

      // ── Onboarding completion tracking ───────────────────────────
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS onboarding_complete BOOLEAN DEFAULT false`;
      // Backfill: all currently active/billing merchants are already through onboarding
      await sql`
        UPDATE "Merchant"
        SET onboarding_complete = true
        WHERE onboarding_complete = false
          AND billing_status IS NOT NULL
          AND billing_status NOT IN ('none', 'deleted')
      `;
      // Online/Hybrid approved merchants are also complete
      await sql`
        UPDATE "Merchant"
        SET onboarding_complete = true
        WHERE onboarding_complete = false
          AND business_presence IN ('online', 'hybrid')
          AND application_status = 'approved'
      `;

      // ── Multi-location flag (Online/Hybrid merchants) ─────────────
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS is_multi_location BOOLEAN NOT NULL DEFAULT false`;

      // ── Annual billing cycle tracking (Task 4) ────────────────
      // 'monthly' | 'annual'. Existing merchants default to 'monthly' safely.
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS billing_cycle TEXT DEFAULT 'monthly'`;

      // Retroactive: backfill Merchant.welcome_offer_text from active welcome/initial campaigns if null
      await sql`
        UPDATE "Merchant" m
        SET welcome_offer_text = (
          SELECT c.title 
          FROM "Campaign" c 
          WHERE c.merchant_id = m.id 
            AND c.status = 'active' 
            AND c.campaign_type IN ('initial', 'perk')
          ORDER BY c.created_at ASC 
          LIMIT 1
        )
        WHERE m.welcome_offer_text IS NULL
      `;

      return send(res, 200, { success: true, message: "DB table migrations strictly applied!" });
    }

    // ── GET /api/v1/merchants/sponsored ──────────────────────────────
    if (method === 'GET' && url.startsWith('/api/v1/merchants/sponsored')) {
      const qs = (req.url || '').split('?')[1] || '';
      const platform = new URLSearchParams(qs).get('platform');
      
      try {
        let sponsors;
        if (platform === 'app') {
          sponsors = await sql`
            SELECT DISTINCT ON (m.id)
              m.id, m.business_name, m.business_name as merchant_name, REPLACE(m.logo_url, 'http://', 'https://') as logo_url, REPLACE(m.cover_photo_url, 'http://', 'https://') as cover_photo_url, REPLACE(m.promo_banner_url, 'http://', 'https://') as promo_banner_url, m.website, m.review_url, m.order_url, m.business_presence,
              m.public_phone, m.public_email,
              m.is_fullpage_sponsored, m.fullpage_sponsored_until, m.promo_description, m.rating_score, m.rating_count, m.rating_platform,
              l.address, l.city, l.state, l.postal_code,
              (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code, m.welcome_offer_text,
              (SELECT c.title FROM "Campaign" c WHERE c.merchant_id = m.id AND c.status = 'active' AND (c.end_at IS NULL OR c.end_at > NOW()) ORDER BY c.created_at DESC LIMIT 1) as latest_offer_title,
              (SELECT c.title FROM "Campaign" c WHERE c.merchant_id = m.id AND c.status = 'active' AND c.discount_percentage >= 0 ORDER BY c.created_at ASC LIMIT 1) as discount,
              CASE
                WHEN m.business_presence = 'online' THEN 'Online Only'
                WHEN m.business_presence = 'mobile' THEN concat_ws(', ', NULLIF(l.city, ''), NULLIF(l.state, ''))
                ELSE concat_ws(', ', NULLIF(l.address, ''), NULLIF(l.city, ''), NULLIF(l.state, ''))
              END as store_address
            FROM "Merchant" m
            LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
            WHERE m.status = 'active' AND m.is_hidden = false AND m.is_app_sponsored = true AND m.app_sponsored_until > NOW()
          `;
        } else {
          sponsors = await sql`
            SELECT DISTINCT ON (m.id)
              m.id, m.business_name, m.business_name as merchant_name, REPLACE(m.logo_url, 'http://', 'https://') as logo_url, REPLACE(m.cover_photo_url, 'http://', 'https://') as cover_photo_url, REPLACE(m.promo_banner_url, 'http://', 'https://') as promo_banner_url, m.website, m.review_url, m.order_url, m.business_presence,
              m.public_phone, m.public_email,
              m.is_fullpage_sponsored, m.fullpage_sponsored_until, m.promo_description, m.rating_score, m.rating_count, m.rating_platform,
              l.address, l.city, l.state, l.postal_code,
              (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code, m.welcome_offer_text,
              (SELECT c.title FROM "Campaign" c WHERE c.merchant_id = m.id AND c.status = 'active' AND (c.end_at IS NULL OR c.end_at > NOW()) ORDER BY c.created_at DESC LIMIT 1) as latest_offer_title,
              (SELECT c.title FROM "Campaign" c WHERE c.merchant_id = m.id AND c.status = 'active' AND c.discount_percentage >= 0 ORDER BY c.created_at ASC LIMIT 1) as discount,
              CASE
                WHEN m.business_presence = 'online' THEN 'Online Only'
                WHEN m.business_presence = 'mobile' THEN concat_ws(', ', NULLIF(l.city, ''), NULLIF(l.state, ''))
                ELSE concat_ws(', ', NULLIF(l.address, ''), NULLIF(l.city, ''), NULLIF(l.state, ''))
              END as store_address
            FROM "Merchant" m
            LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
            WHERE m.status = 'active' AND m.is_hidden = false AND m.is_web_sponsored = true AND m.web_sponsored_until > NOW()
          `;
        }

        // Shuffle using Fisher-Yates
        for (let i = sponsors.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [sponsors[i], sponsors[j]] = [sponsors[j], sponsors[i]];
        }

        return send(res, 200, { success: true, data: sponsors.slice(0, 8) });
      } catch (err) {
        console.error('Fetch sponsored merchants error:', err);
        return send(res, 500, { success: false, error: err.message });
      }
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
      // Excludes blocked and deleted merchants from user-facing discovery
      const merchants = await sql`
        SELECT
          m.id,
          m.business_name,
          REPLACE(m.logo_url, 'http://', 'https://') as logo_url,
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
          AND m.account_blocked = false
          AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
        ORDER BY m.business_name ASC
      `;

      return send(res, 200, { success: true, zip, count: merchants.length, data: merchants });
    }

    // ── GET /api/v1/public/online-merchants — for /codes page ─────
    if (method === 'GET' && url.startsWith('/api/v1/public/online-merchants')) {
      const qs = (req.url || '').split('?')[1] || '';
      const params = new URLSearchParams(qs);
      const category = params.get('category') || null;

      let merchants;
      if (category && category !== 'all') {
        merchants = await sql`
          SELECT m.id, m.business_name, m.logo_url, m.cover_photo_url, m.website, m.welcome_offer_text,
                 m.business_category, m.welcome_promo_code, m.public_phone, m.public_email,
                 m.is_fullpage_sponsored, m.fullpage_sponsored_until, m.promo_banner_url, m.promo_description,
                 m.rating_score, m.rating_count, m.rating_platform, m.order_url, m.review_url,
                 l.address, l.city, l.state, l.postal_code,
                 (SELECT q.public_code FROM "QrCode" q WHERE q.merchant_id = m.id AND q.status = 'active' LIMIT 1) AS qr_public_code,
                 (SELECT COUNT(*) FROM "Campaign" c WHERE c.merchant_id = m.id AND c.status = 'active') AS active_campaign_count
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('online', 'hybrid')
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.account_blocked = false
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND m.business_category = ${category}
          ORDER BY m.business_name ASC
        `;
      } else {
        merchants = await sql`
          SELECT m.id, m.business_name, m.logo_url, m.cover_photo_url, m.website, m.welcome_offer_text,
                 m.business_category, m.welcome_promo_code, m.public_phone, m.public_email,
                 m.is_fullpage_sponsored, m.fullpage_sponsored_until, m.promo_banner_url, m.promo_description,
                 m.rating_score, m.rating_count, m.rating_platform, m.order_url, m.review_url,
                 l.address, l.city, l.state, l.postal_code,
                 (SELECT q.public_code FROM "QrCode" q WHERE q.merchant_id = m.id AND q.status = 'active' LIMIT 1) AS qr_public_code,
                 (SELECT COUNT(*) FROM "Campaign" c WHERE c.merchant_id = m.id AND c.status = 'active') AS active_campaign_count
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('online', 'hybrid')
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.account_blocked = false
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
          ORDER BY m.business_name ASC
        `;
      }
      return send(res, 200, { success: true, count: merchants.length, data: merchants });
    }

    // ── GET /api/v1/public/local-merchants — for /localperks page ─
    if (method === 'GET' && url.startsWith('/api/v1/public/local-merchants')) {
      const qs = (req.url || '').split('?')[1] || '';
      const params = new URLSearchParams(qs);
      const q = (params.get('q') || '').trim();
      const cities = params.getAll('city').map(c => c.trim().toLowerCase()).filter(Boolean);
      const zips = params.getAll('zip').map(z => z.trim()).filter(Boolean);

      // postgres.js does not support composing fragments dynamically.
      // Use complete self-contained queries per filter branch (same pattern as online-merchants).
      let merchants;
      const like = '%' + q + '%';

      if (q && cities.length > 0 && zips.length > 0) {
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND m.business_name ILIKE ${like} AND LOWER(l.city)=ANY(${cities}) AND l.postal_code=ANY(${zips})
          ORDER BY m.business_name ASC LIMIT 100`;
      } else if (q && cities.length > 0) {
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND m.business_name ILIKE ${like} AND LOWER(l.city)=ANY(${cities})
          ORDER BY m.business_name ASC LIMIT 100`;
      } else if (q && zips.length > 0) {
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND m.business_name ILIKE ${like} AND l.postal_code=ANY(${zips})
          ORDER BY m.business_name ASC LIMIT 100`;
      } else if (cities.length > 0 && zips.length > 0) {
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND LOWER(l.city)=ANY(${cities}) AND l.postal_code=ANY(${zips})
          ORDER BY m.business_name ASC LIMIT 100`;
      } else if (q) {
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND m.business_name ILIKE ${like}
          ORDER BY m.business_name ASC LIMIT 100`;
      } else if (cities.length > 0) {
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND LOWER(l.city)=ANY(${cities})
          ORDER BY m.business_name ASC LIMIT 100`;
      } else if (zips.length > 0) {
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
            AND l.postal_code=ANY(${zips})
          ORDER BY m.business_name ASC LIMIT 100`;
      } else {
        // No filters — return all local/mobile merchants
        merchants = await sql`
          SELECT m.id,m.business_name,m.logo_url,m.cover_photo_url,m.business_presence,m.business_category,
                 m.is_fullpage_sponsored,m.fullpage_sponsored_until,m.promo_banner_url,m.promo_description,
                 m.rating_score,m.rating_count,m.rating_platform,m.website,m.order_url,m.review_url,m.public_phone,m.public_email,
                 COALESCE(m.welcome_offer_text,(SELECT c.title FROM "Campaign" c WHERE c.merchant_id=m.id AND c.status='active' AND c.campaign_type='initial' ORDER BY c.created_at ASC LIMIT 1)) AS welcome_offer_text,
                 l.address,l.city,l.state,l.postal_code,
                 (SELECT q2.public_code FROM "QrCode" q2 WHERE q2.merchant_id=m.id AND q2.status='active' LIMIT 1) AS qr_public_code
          FROM "Merchant" m LEFT JOIN "MerchantLocation" l ON l.merchant_id=m.id AND l.is_active=true
          WHERE m.business_presence IN ('physical','mobile','hybrid') AND m.account_blocked=false
            AND (m.application_status IS NULL OR m.application_status = 'approved')
            AND m.business_name != '[Deleted]'
            AND m.is_hidden = false
          ORDER BY m.business_name ASC LIMIT 100`;
      }

      return send(res, 200, { success: true, count: merchants.length, data: merchants });
    }

    // ── GET /api/v1/qr/resolve/:code ──────────────────────────────
    const qrMatch = url.match(/\/api\/v1\/qr\/resolve\/([a-zA-Z0-9_-]+)/);

    if (method === 'GET' && qrMatch) {
      const public_code = qrMatch[1];
      const [qrCode] = await sql`SELECT * FROM "QrCode" WHERE public_code = ${public_code} AND status = 'active' LIMIT 1`;
      if (!qrCode) return send(res, 404, { success: false, error: 'QR code not found or inactive' });

      const [merchant] = await sql`SELECT id, business_name, logo_url, welcome_offer_text, account_blocked, billing_status, subscription_tier, member_limit, member_cap_notified, cap_block_count FROM "Merchant" WHERE id = ${qrCode.merchant_id} LIMIT 1`;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchant.billing_status === 'deleted') return send(res, 403, { success: false, error: 'This store is no longer available' });
      if (merchant.account_blocked) return send(res, 403, { success: false, error: 'This merchant is currently inactive' });

      // Fix A: Enforce online plan tier member cap (online_starter=500, online_growth=2500, online_scale=unlimited).
      // Block new joins when a capped tier merchant is at their limit. Does not affect physical/mobile/trial merchants.
      const _cappedOnlineTiers = ['online_starter', 'online_growth'];
      if (_cappedOnlineTiers.includes(merchant.subscription_tier) && merchant.member_limit && merchant.billing_status === 'active') {
        const [_capCount] = await sql`SELECT COUNT(*)::int as cnt FROM "MerchantMember" WHERE merchant_id = ${qrCode.merchant_id}`;
        if (_capCount && _capCount.cnt >= merchant.member_limit) {
          console.log(`[MemberCap] Merchant ${qrCode.merchant_id} at tier cap (${_capCount.cnt}/${merchant.member_limit}). Join blocked.`);
          // Notification: fires on first blocked join, then every 25 blocked joins after that.
          const _preIncCount = merchant.cap_block_count || 0;
          const _isFirstBlock = !merchant.member_cap_notified;
          const _isPeriodicBlock = !_isFirstBlock && (_preIncCount + 1) % 25 === 0;
          await sql`UPDATE "Merchant" SET cap_block_count = COALESCE(cap_block_count, 0) + 1, member_cap_notified = true, updated_at = NOW() WHERE id = ${qrCode.merchant_id}`;
          if (_isFirstBlock || _isPeriodicBlock) {
            try {
              const [_capMu] = await sql`SELECT email FROM "MerchantUser" WHERE merchant_id = ${qrCode.merchant_id} LIMIT 1`;
              const BREVO_KEY = process.env.BREVO_API_KEY;
              if (BREVO_KEY && _capMu?.email) {
                const brevoClient = SibApiV3Sdk.ApiClient.instance;
                brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
                const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
                const emailObj = new SibApiV3Sdk.SendSmtpEmail();
                emailObj.sender = { name: 'Perkfinity Support', email: 'support@perkfinity.net' };
                emailObj.to = [{ email: _capMu.email }];
                const _tierLabel = merchant.subscription_tier === 'online_starter' ? 'Starter (500 members)' : 'Growth (2,500 members)';
                const _nextTier = merchant.subscription_tier === 'online_starter' ? 'Growth' : 'Scale';
                emailObj.subject = _isFirstBlock
                  ? `⚠️ A new member couldn't join ${merchant.business_name} — your plan is full`
                  : `⚠️ Reminder: New members are still being turned away from ${merchant.business_name}`;
                emailObj.htmlContent = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;background:#ffffff;border-radius:16px;overflow:hidden;border:1px solid #eee;"><div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;"><div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div></div><div style="padding:28px 24px;"><div style="font-size:20px;font-weight:700;color:#e67e22;margin-bottom:16px;">⚠️ A member just tried to join — but couldn't</div><p style="font-size:15px;color:#555;line-height:1.6;">Hi <strong>${merchant.business_name}</strong>,<br><br>Someone new just tried to join your Perkfinity page but was turned away because your <strong>${_tierLabel}</strong> plan has reached its ${merchant.member_limit}-member limit.</p><div style="background:#fff7ed;border:1.5px solid #fed7aa;border-radius:10px;padding:16px 20px;margin-bottom:20px;"><div style="font-size:13px;font-weight:700;color:#c2410c;margin-bottom:8px;">What's happening right now:</div><ul style="margin:0;padding-left:18px;font-size:13px;color:#7c2d12;line-height:2;"><li>New potential members are being turned away from your page</li><li><strong>This will keep happening</strong> until you upgrade your plan</li><li>Your current ${merchant.member_limit} members and all active campaigns are unaffected</li></ul></div><p style="font-size:15px;color:#555;line-height:1.6;margin-bottom:16px;">Don't lose customers — upgrade now to keep your doors open to new members:</p><table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;"><tr><td style="padding-bottom:10px;"><a href="https://perkfinity.net/dashboard.html" style="display:block;background:#5b3fa5;color:#fff;font-weight:700;text-decoration:none;padding:14px 20px;border-radius:10px;font-size:14px;text-align:center;">Upgrade to Growth — Up to 2,500 Members</a></td></tr><tr><td><a href="https://perkfinity.net/dashboard.html" style="display:block;background:#1e1b4b;color:#fff;font-weight:700;text-decoration:none;padding:14px 20px;border-radius:10px;font-size:14px;text-align:center;">Go Scale — Unlimited Members</a></td></tr></table><p style="font-size:13px;color:#aaa;text-align:center;">Need help choosing? Reply to this email and our team will assist you right away.</p></div></div>`;
                await emailApi.sendTransacEmail(emailObj);
                console.log(`[MemberCap] ${_isFirstBlock ? 'First' : 'Periodic'} cap notification sent to ${_capMu.email} for merchant ${qrCode.merchant_id} (block #${_preIncCount + 1})`);
              }
            } catch (_capEmailErr) {
              console.error('[MemberCap] Upgrade email failed:', _capEmailErr.message);
            }
          }
          return send(res, 403, { success: false, error: 'This store has reached its current member capacity. Please check back later.' });
        }
      }

      const [location] = await sql`SELECT address, city, state, postal_code FROM "MerchantLocation" WHERE merchant_id = ${qrCode.merchant_id} AND is_active = true LIMIT 1`;

      let campaigns = [];
      let resolvedUserId = null; // tracks successfully authenticated user — controls fallback eligibility

      // If user is authenticated, return ONLY their assigned campaigns for this merchant
      const authHeader = req.headers.authorization;
      if (authHeader && authHeader.startsWith('Bearer ')) {
        try {
          const decoded = jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET);
          const userId = decoded.userId;
          resolvedUserId = userId; // mark as successfully authenticated
          // Auto-enroll the user into the merchant's member list if they aren't already
          await sql`
            INSERT INTO "MerchantMember" (id, merchant_id, user_id, join_source, created_at)
            VALUES (gen_random_uuid()::text, ${qrCode.merchant_id}, ${userId}, 'qr_scan', NOW())
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
              AND (c.end_at IS NULL OR c.end_at > NOW())
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
                   c.start_at, c.end_at, c.campaign_type, c.promo_code,
                   r.id as redemption_id, r.token, r.expires_at as redemption_expires_at,
                   r.redeemed, r.status as redemption_status
            FROM "Redemption" r
            JOIN "Campaign" c ON c.id = r.campaign_id
            WHERE r.user_id = ${userId}
              AND c.merchant_id = ${qrCode.merchant_id}
              AND r.status IN ('created', 'claimed')
              AND r.redeemed = false
              AND c.status = 'active'
              AND (c.end_at IS NULL OR c.end_at > NOW())
              AND c.discount_percentage >= 0
            ORDER BY c.created_at ASC
          `;
          // Remap so frontend sees c.status field as usual
          campaigns = memberCampaigns.map(row => ({ ...row, status: row.campaign_status }));
        } catch (jwtErr) {
          // Token invalid or expired — fall through to public campaigns
        }
      }

      // Fallback: ONLY for truly unauthenticated users (no valid JWT).
      // Authenticated users with no available campaigns get an empty result — they have nothing left to activate.
      // This prevents redeemed campaigns from reappearing and targeted campaigns from leaking to wrong users.
      if (campaigns.length === 0 && !resolvedUserId) {
        campaigns = await sql`
          SELECT id, title, discount_percentage, terms, status, start_at, end_at, campaign_type, promo_code
          FROM "Campaign" c
          WHERE c.merchant_id = ${qrCode.merchant_id}
            AND c.status = 'active'
            AND (c.end_at IS NULL OR c.end_at > NOW())
            AND c.discount_percentage >= 0
            AND NOT EXISTS (
              SELECT 1 FROM "AuditLog" al
              WHERE al.target_id = c.id
                AND al.action = 'promotion_created'
            )
          ORDER BY c.created_at ASC
          LIMIT 5
        `;
      }

      // When authenticated user has no available campaigns, check if they've redeemed
      // something from this merchant so we can show a contextual message vs. silent redirect.
      let allRedeemed = false;
      if (campaigns.length === 0 && resolvedUserId) {
        const [redeemedCheck] = await sql`
          SELECT 1 FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          WHERE r.user_id = ${resolvedUserId}
            AND c.merchant_id = ${qrCode.merchant_id}
            AND r.redeemed = true
          LIMIT 1
        `;
        allRedeemed = !!redeemedCheck;
      }

      return send(res, 200, { success: true, data: { qrCode, merchant, location, campaigns, all_redeemed: allRedeemed } });
    }


    // ── POST /api/v1/merchants/claim ──────────────────────────────
    if (method === 'POST' && url === '/api/v1/merchants/claim') {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });

      let payload;
      try { 
        payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); 
      } catch (err) { 
        return send(res, 401, { success: false, error: 'Invalid or expired session token' }); 
      }

      const merchantId = payload.merchantId;
      const data = req.body || {};
      const { contact_name, email, phone, password } = data;

      // Validation
      const missing = [];
      if (!contact_name) missing.push('Full Name');
      if (!email) missing.push('Email Address');
      if (!phone) missing.push('Phone Number');
      if (!password) missing.push('New Password');

      if (missing.length > 0) {
        return send(res, 400, { success: false, error: `Missing required fields: ${missing.join(', ')}` });
      }

      const cleanEmail = email.toLowerCase().trim();
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(cleanEmail)) {
        return send(res, 400, { success: false, error: 'Please enter a valid email address.' });
      }

      let cleanPhone = phone.trim().replace(/\D/g, '');
      if (cleanPhone.length === 11 && cleanPhone.startsWith('1')) cleanPhone = cleanPhone.slice(1);
      if (cleanPhone.length !== 10) {
        return send(res, 400, { success: false, error: 'Please enter a valid 10-digit phone number.' });
      }
      const formattedPhone = `${cleanPhone.slice(0,3)}-${cleanPhone.slice(3,6)}-${cleanPhone.slice(6)}`;

      if (password.length < 6) {
        return send(res, 400, { success: false, error: 'Password must be at least 6 characters.' });
      }

      // Check if email already used by another merchant user
      const [existingUser] = await sql`
        SELECT id FROM "MerchantUser" 
        WHERE LOWER(email) = ${cleanEmail} AND merchant_id != ${merchantId}
        LIMIT 1
      `;
      if (existingUser) {
        return send(res, 400, { success: false, error: 'An account with this email address already exists.' });
      }

      const newPasswordHash = await bcrypt.hash(password, 12);

      // Update MerchantUser
      await sql`
        UPDATE "MerchantUser"
        SET email = ${cleanEmail},
            password_hash = ${newPasswordHash}
        WHERE merchant_id = ${merchantId} AND id = ${payload.userId}
      `;

      // Update Merchant
      await sql`
        UPDATE "Merchant"
        SET contact_name = ${contact_name.trim()},
            phone = ${formattedPhone},
            is_claimed = true,
            is_hidden = false,
            temp_password_plain = NULL,
            presetup_claimed_at = NOW(),
            onboarding_complete = true,
            updated_at = NOW()
        WHERE id = ${merchantId}
      `;

      // Generate fresh tokens
      const token = jwt.sign(
        { userId: payload.userId, merchantId, role: 'owner', email: cleanEmail },
        process.env.JWT_SECRET,
        { expiresIn: process.env.JWT_EXPIRY || '8h' }
      );
      const refreshToken = jwt.sign(
        { userId: payload.userId, merchantId, type: 'refresh' },
        process.env.JWT_SECRET,
        { expiresIn: process.env.JWT_REFRESH_EXPIRY || '7d' }
      );

      return send(res, 200, {
        success: true,
        message: 'Profile claimed successfully!',
        token,
        refreshToken,
        email: cleanEmail,
        merchant_id: merchantId,
        is_claimed: true,
        is_hidden: false
      });
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
        SELECT m.business_name, m.contact_name, m.phone, m.public_phone, m.public_email, m.website, m.logo_url, m.cover_photo_url, m.promo_banner_url, m.promo_description, m.subscription_tier,
               m.stripe_payment_method_id, m.billing_status, m.business_presence, m.welcome_promo_code,
               m.welcome_offer_text, m.review_url, m.order_url, m.is_multi_location, m.onboarding_complete,
               m.is_web_sponsored, m.web_sponsored_until, m.is_app_sponsored, m.app_sponsored_until,
               m.is_fullpage_sponsored, m.fullpage_sponsored_until,
               m.is_presetup, m.is_claimed, m.member_limit, m.is_hidden,
               l.address, l.suite, l.city, l.state, l.postal_code, u.email
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

      // Fetch associated initial campaign perk title
      const [campaignData] = await sql`
        SELECT title FROM "Campaign"
        WHERE merchant_id = ${merchantId}
          AND status = 'active'
          AND campaign_type = 'initial'
        ORDER BY created_at ASC
        LIMIT 1
      `;

      // Fetch attributed sales rep (if any)
      const [contractorData] = await sql`
        SELECT c.full_name as contractor_name, c.phone as contractor_phone, c.email as contractor_email
        FROM "ContractorMerchantAttribution" a
        JOIN "Contractor" c ON c.id = a.contractor_id
        WHERE a.merchant_id = ${merchantId}
        LIMIT 1
      `;

      merchantData.qr_public_code = qrData ? qrData.public_code : null;
      merchantData.qr_url = qrData ? `https://www.perkfinity.net/qr/${qrData.public_code}` : null;
      merchantData.perk = campaignData ? campaignData.title : (merchantData.welcome_offer_text || 'Welcome Perk');
      merchantData.contractor_name = contractorData ? contractorData.contractor_name : null;
      merchantData.contractor_phone = contractorData ? contractorData.contractor_phone : null;
      merchantData.contractor_email = contractorData ? contractorData.contractor_email : null;
      
      // Apply active sponsorship logic
      merchantData.is_web_sponsored = merchantData.is_web_sponsored && (!merchantData.web_sponsored_until || new Date(merchantData.web_sponsored_until) >= new Date()) ? true : false;
      merchantData.is_app_sponsored = merchantData.is_app_sponsored && (!merchantData.app_sponsored_until || new Date(merchantData.app_sponsored_until) >= new Date()) ? true : false;

      return send(res, 200, { success: true, data: merchantData });
    }

    // ── POST /api/v1/merchants/:id/promotions ──────────────────────
    const promoMatch = url.match(/^\/api\/v1\/merchants\/([^/]+)\/promotions$/);
    if (method === 'POST' && promoMatch) {
      const auth = req.headers.authorization;
      if (!auth || !auth.startsWith('Bearer ')) return send(res, 401, { success: false, error: 'Unauthorized' });
      const JWT_SECRET = process.env.JWT_SECRET;
      let decoded;
      try { decoded = jwt.verify(auth.split(' ')[1], JWT_SECRET); } catch (e) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const targetMerchantId = promoMatch[1];
      if (decoded.merchantId !== targetMerchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const [merchantGuard] = await sql`SELECT account_blocked FROM "Merchant" WHERE id = ${targetMerchantId} LIMIT 1`;
      if (merchantGuard?.account_blocked) return send(res, 403, { success: false, error: 'Account suspended. Reactivate your subscription to create campaigns.' });

      const data = req.body || {};
      if (!data.title || !data.type || !data.delivery || !data.audience) {
        return send(res, 400, { success: false, error: 'Missing required fields: title, type, delivery, audience' });
      }

      const now = new Date();
      const expiresAt = data.expires_at ? new Date(data.expires_at) : new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

      // Sanitize promo code: uppercase, alphanumeric + dash + underscore only, max 18 chars
      const sanitizedPromoCode = data.promo_code
        ? data.promo_code.trim().toUpperCase().replace(/[^A-Z0-9_-]/g, '').slice(0, 18) || null
        : null;

      // Save as a Campaign so it appears in campaign history.
      // Announcements use discount_percentage = -1 as a permanent type marker
      // so they can be filtered out anywhere Redemption rows are not sufficient.
      const [campaign] = await sql`
        INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, terms, status, start_at, end_at, campaign_type, promo_code, created_at, updated_at)
        VALUES (
          gen_random_uuid()::text,
          ${targetMerchantId},
          ${data.title},
          ${data.type === 'announcement' ? -1 : 0},
          ${data.condition_detail || ''},
          'active',
          ${now},
          ${expiresAt},
          ${data.type || 'perk'},
          ${sanitizedPromoCode},
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
          SELECT DISTINCT mm.user_id, u.email_unsubscribed, u.push_notifications_enabled, u.push_token 
          FROM "MerchantMember" mm 
          JOIN "User" u ON u.id = mm.user_id 
          WHERE mm.merchant_id = ${targetMerchantId}
        `;
      } else if (data.audience === 'redeemed_30') {
        qualifyingUsers = await sql`
          SELECT DISTINCT r.user_id, u.email_unsubscribed, u.push_notifications_enabled, u.push_token 
          FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          JOIN "User" u ON u.id = r.user_id
          WHERE c.merchant_id = ${targetMerchantId}
            AND (r.status = 'redeemed' OR r.redeemed = true)
            AND r.redeemed_at >= ${thirtyDaysAgo}
        `;
      } else if (data.audience === 'expired_30') {
        qualifyingUsers = await sql`
          SELECT DISTINCT r.user_id, u.email_unsubscribed, u.push_notifications_enabled, u.push_token 
          FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          JOIN "User" u ON u.id = r.user_id
          WHERE c.merchant_id = ${targetMerchantId}
            AND (r.status = 'expired' OR (r.redeemed = false AND r.expires_at < NOW() AND r.expires_at >= ${thirtyDaysAgo} AND COALESCE(r.status,'pending') != 'created'))
        `;
      } else if (data.audience === 'never_redeemed') {
        qualifyingUsers = await sql`
          SELECT mm.user_id, u.email_unsubscribed, u.push_notifications_enabled, u.push_token 
          FROM "MerchantMember" mm
          JOIN "User" u ON u.id = mm.user_id
          WHERE mm.merchant_id = ${targetMerchantId}
            AND NOT EXISTS (
              SELECT 1 FROM "Redemption" r2 JOIN "Campaign" c2 ON c2.id = r2.campaign_id
              WHERE c2.merchant_id = ${targetMerchantId} AND r2.user_id = mm.user_id
                AND r2.status IN ('pending', 'redeemed', 'expired')
            )
        `;
      } else if (data.audience === 'redeemed_90') {
        qualifyingUsers = await sql`
          SELECT DISTINCT r.user_id, u.email_unsubscribed, u.push_notifications_enabled, u.push_token 
          FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          JOIN "User" u ON u.id = r.user_id
          WHERE c.merchant_id = ${targetMerchantId}
            AND (r.status = 'redeemed' OR r.redeemed = true)
            AND r.redeemed_at >= ${ninetyDaysAgo}
        `;
      } else if (data.audience === 'by_location') {
        const cities = Array.isArray(data.audience_cities) ? data.audience_cities.filter(c => c) : [];
        const zips = Array.isArray(data.audience_zips) ? data.audience_zips.filter(z => z) : [];
        // Fetch all members with their city/zip, then filter in JS
        const allWithLocation = await sql`
          SELECT DISTINCT mm.user_id, u.city, u.zip_code, u.email_unsubscribed, u.push_notifications_enabled, u.push_token 
          FROM "MerchantMember" mm
          JOIN "User" u ON u.id = mm.user_id
          WHERE mm.merchant_id = ${targetMerchantId}
        `;
        qualifyingUsers = allWithLocation.filter(u => {
          const cityMatch = cities.length === 0 || cities.includes(u.city);
          const zipMatch = zips.length === 0 || zips.includes(u.zip_code);
          return (cities.length > 0 && cityMatch) || (zips.length > 0 && zipMatch);
        });
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
      let emailCount = 0;
      let pushCount = 0;
      let queueErrors = [];
      if (qualifyingUsers.length > 0) {
        try {
          // Fetch merchant info for the queue — include presence, website, and campaign promo_code
          const [merchantInfo] = await sql`
            SELECT m.business_name, m.logo_url, m.business_presence, m.website,
                   l.address, l.city, l.state, l.postal_code
            FROM "Merchant" m
            LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
            WHERE m.id = ${targetMerchantId}
            LIMIT 1
          `;
          const storeName = merchantInfo?.business_name || 'Your Local Store';
          const logoUrl = merchantInfo?.logo_url || '';
          const presence = merchantInfo?.business_presence || 'physical';
          const isOnline = presence === 'online';
          const merchantWebsite = merchantInfo?.website || '';
          // store_address: online → empty (website used instead), mobile → label, physical → full address
          const storeAddress = isOnline
            ? ''
            : presence === 'mobile'
              ? 'Mobile Business'
              : merchantInfo ? [merchantInfo.address, merchantInfo.city, merchantInfo.state, merchantInfo.postal_code].filter(Boolean).join(', ') : '';
          const campaignPromoCode = isOnline ? (data.promo_code || null) : null;
          const headline = data.title || 'New Offer';
          const condLine = data.condition_detail || '';
          const bodyText = condLine || headline;

          // Insert into NotificationQueue for each qualifying user
          for (const u of qualifyingUsers) {
            const userId = u.user_id;
            try {
              await sql`
                INSERT INTO "NotificationQueue" (user_id, campaign_id, merchant_id, store_name, store_address, logo_url, title, body, channels, offer_expires_at, disclaimer, is_online_merchant, website, promo_code)
                VALUES (${userId}, ${campaign.id}, ${targetMerchantId}, ${storeName}, ${storeAddress}, ${logoUrl}, ${headline}, ${bodyText}, ${deliveryChannel}, ${campaign.end_at}, ${data.disclaimer || null}, ${isOnline}, ${merchantWebsite}, ${campaignPromoCode})
              `;
              queuedCount++;
              if (u.email_unsubscribed !== true) emailCount++;
              if (u.push_notifications_enabled === true && u.push_token) pushCount++;
            } catch (queueErr) {
              console.error(`Queue insert failed for user ${userId}:`, queueErr.message);
              queueErrors.push(`insert: ${queueErr.message}`);
            }
          }
        } catch (setupErr) {
          console.error('Campaign queue setup error:', setupErr.message || setupErr);
          queueErrors.push(`setup: ${setupErr.message || setupErr}`);
        }
      }

      const channelMsg = deliveryChannel === 'email' ? `${emailCount} email(s)` : deliveryChannel === 'push' ? `${pushCount} push notification(s)` : `${emailCount} email(s) and ${pushCount} push notification(s)`;
      return send(res, 201, { success: true, data: { campaign, assigned_count: assignedCount, queued_count: queuedCount, delivery_channel: deliveryChannel, queue_errors: queueErrors, message: `Promotion created and assigned to ${assignedCount} member(s). ${channelMsg} queued for daily digest.` } });
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

      const ua = (req.headers['user-agent'] || '').toLowerCase();
      const platform = ua.includes('android') ? 'android' : (ua.includes('iphone') || ua.includes('ipad') || ua.includes('ipod') ? 'ios' : 'web');
      if (!user) {
        // Create new user
        const email = appleEmail ? appleEmail.toLowerCase() : `apple_${appleSub}@perkfinity.internal`;
        const fullName = data.fullName || '';
        [user] = await sql`
          INSERT INTO "User" (id, email, apple_sub, full_name, created_at, last_active, device_platform)
          VALUES (gen_random_uuid()::text, ${email}, ${appleSub}, ${fullName}, NOW(), NOW(), ${platform})
          ON CONFLICT (email) DO UPDATE SET apple_sub = ${appleSub}, last_active = NOW(), device_platform = ${platform}
          RETURNING *
        `;
      } else {
        await sql`UPDATE "User" SET last_active = NOW(), device_platform = COALESCE(device_platform, ${platform}) WHERE id = ${user.id}`;
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

      const ua = (req.headers['user-agent'] || '').toLowerCase();
      const platform = ua.includes('android') ? 'android' : (ua.includes('iphone') || ua.includes('ipad') || ua.includes('ipod') ? 'ios' : 'web');
      if (!user) {
        const email = googleEmail ? googleEmail.toLowerCase() : `google_${googleSub}@perkfinity.internal`;
        [user] = await sql`
          INSERT INTO "User" (id, email, google_sub, full_name, created_at, last_active, device_platform)
          VALUES (gen_random_uuid()::text, ${email}, ${googleSub}, ${googleName}, NOW(), NOW(), ${platform})
          ON CONFLICT (email) DO UPDATE SET google_sub = ${googleSub}, last_active = NOW(), device_platform = ${platform}
          RETURNING *
        `;
      } else {
        await sql`UPDATE "User" SET last_active = NOW(), device_platform = COALESCE(device_platform, ${platform}) WHERE id = ${user.id}`;
      }

      const gtoken = jwt.sign({ userId: user.id, role: 'consumer' }, JWT_SECRET, { expiresIn: '30d' });
      await autoEnrollUser(sql, user.id, data.qrCode);
      const { password_hash: _gpw, ...safeGUser } = user;
      return send(res, 200, { success: true, data: { user: safeGUser, accessToken: gtoken } });
    }

    // ── POST /api/v1/consumers/signup ─────────────────────────────

    if (method === 'POST' && url.endsWith('/consumers/signup')) {
      // Rate limit: 5 sign-up attempts per IP per hour
      const rl = checkSignupRateLimit(req);
      if (!rl.allowed) {
        res.setHeader('Retry-After', rl.retryAfterSec);
        return send(res, 429, { success: false, error: `Too many sign-up attempts. Please try again in ${Math.ceil(rl.retryAfterSec / 60)} minute(s).` });
      }

      const data = req.body || {};
      if (!data.email || !data.password) return send(res, 400, { success: false, error: 'Missing email or password' });

      const [existing] = await sql`SELECT id, password_hash FROM "User" WHERE email = ${data.email.toLowerCase()} LIMIT 1`;

      if (existing) {
        if (existing.password_hash) {
          // User already fully signed up — suggest login
          return send(res, 400, { success: false, error: 'An account with this email already exists. Please use Log In instead.' });
        }
        // User was auto-created (via Apple/Google sign-in or auto-enrollment) — let them set a password
        const ua = (req.headers['user-agent'] || '').toLowerCase();
        const platform = ua.includes('android') ? 'android' : (ua.includes('iphone') || ua.includes('ipad') || ua.includes('ipod') ? 'ios' : 'web');
        const hash = await bcrypt.hash(data.password, 12);
        await sql`UPDATE "User" SET password_hash = ${hash}, last_active = NOW(), device_platform = COALESCE(device_platform, ${platform}) WHERE id = ${existing.id}`;
        const JWT_SECRET = process.env.JWT_SECRET;
        const token = jwt.sign({ userId: existing.id, role: 'consumer' }, JWT_SECRET, { expiresIn: '30d' });
        await autoEnrollUser(sql, existing.id, data.qrCode);
        return send(res, 200, { success: true, data: { user: { id: existing.id, email: data.email.toLowerCase() }, accessToken: token } });
      }

      const ua = (req.headers['user-agent'] || '').toLowerCase();
      const platform = ua.includes('android') ? 'android' : (ua.includes('iphone') || ua.includes('ipad') || ua.includes('ipod') ? 'ios' : 'web');
      const hash = await bcrypt.hash(data.password, 12);
      const [user] = await sql`
        INSERT INTO "User" (id, email, password_hash, created_at, last_active, device_platform)
        VALUES (gen_random_uuid()::text, ${data.email.toLowerCase()}, ${hash}, NOW(), NOW(), ${platform})
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

      const ua = (req.headers['user-agent'] || '').toLowerCase();
      const platform = ua.includes('android') ? 'android' : (ua.includes('iphone') || ua.includes('ipad') || ua.includes('ipod') ? 'ios' : 'web');
      await sql`UPDATE "User" SET last_active = NOW(), device_platform = COALESCE(device_platform, ${platform}) WHERE id = ${user.id}`;
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
      // Optionally resolve the logged-in user so we can flag is_member per merchant.
      // Unauthenticated callers get is_member: false for all merchants.
      let authedUserId = null;
      const authHeader = req.headers.authorization;
      if (authHeader && authHeader.startsWith('Bearer ')) {
        try {
          const decoded = jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET);
          authedUserId = decoded.userId || null;
        } catch { /* invalid/expired token — treat as unauthenticated */ }
      }

      // Fetch the list of merchant IDs this user has already joined
      let memberMerchantIds = new Set();
      if (authedUserId) {
        const memberRows = await sql`
          SELECT merchant_id FROM "MerchantMember" WHERE user_id = ${authedUserId}
        `;
        memberMerchantIds = new Set(memberRows.map(r => r.merchant_id));
      }

      // Fetch claimed initial redemptions for this user (online merchants)
      let claimedMerchantMap = new Map(); // merchant_id → claimed_at
      if (authedUserId) {
        const claimedRows = await sql`
          SELECT c.merchant_id, r.claimed_at
          FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          WHERE r.user_id = ${authedUserId}
            AND c.campaign_type = 'initial'
            AND r.status = 'claimed'
        `;
        claimedRows.forEach(row => claimedMerchantMap.set(row.merchant_id, row.claimed_at));
      }

      const campaigns = await sql`
         SELECT DISTINCT ON (m.id)
           m.id as id,
           c.id as campaign_id,
           m.business_name as merchant_name,
           m.logo_url,
           m.cover_photo_url,
           m.business_presence,
           m.business_category,
           m.website,
           m.welcome_promo_code,
           m.welcome_offer_text,
           m.review_url,
           m.order_url,
           m.is_fullpage_sponsored,
           m.fullpage_sponsored_until,
           m.promo_banner_url,
           m.promo_description,
           m.rating_score,
           m.rating_count,
           m.rating_platform,
           l.postal_code as zip_code,
           q.public_code as qr_code,
           c.title as discount,
           CASE
             WHEN m.business_presence = 'online' THEN 'Online Only'
             WHEN m.business_presence = 'mobile' THEN concat_ws(', ', NULLIF(l.city, ''), NULLIF(l.state, ''))
             ELSE concat_ws(', ', NULLIF(l.address, ''), NULLIF(l.city, ''), NULLIF(l.state, ''))
           END as store_address,
           c.title as latest_offer_title,
           c.end_at as offer_expires_at,
           (SELECT COUNT(*) FROM "Campaign" c2
            WHERE c2.merchant_id = m.id AND c2.status = 'active'
              AND (c2.end_at IS NULL OR c2.end_at > NOW())
              AND (
                ${authedUserId}::text IS NULL
                OR NOT EXISTS (SELECT 1 FROM "AuditLog" al WHERE al.target_id = c2.id AND al.action = 'promotion_created')
                OR EXISTS (SELECT 1 FROM "Redemption" r2 WHERE r2.campaign_id = c2.id AND r2.user_id = ${authedUserId})
              )
              AND NOT EXISTS (
                SELECT 1 FROM "Redemption" r3
                WHERE r3.campaign_id = c2.id
                  AND r3.user_id = ${authedUserId}
                  AND (r3.status = 'redeemed' OR r3.redeemed = true)
              )) as offer_count,
           (SELECT MAX(c2.created_at) FROM "Campaign" c2
            WHERE c2.merchant_id = m.id AND c2.status = 'active'
              AND (c2.end_at IS NULL OR c2.end_at > NOW())) as latest_offer_at
         FROM "Campaign" c
         JOIN "Merchant" m ON m.id = c.merchant_id
         LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
         LEFT JOIN "QrCode" q ON q.merchant_id = m.id AND q.status = 'active'
         WHERE c.status = 'active' AND m.status = 'active'
           AND m.is_hidden = false
           AND (c.end_at IS NULL OR c.end_at > NOW())
         ORDER BY m.id, c.created_at ASC
       `;

      const result = campaigns.map(c => ({
        ...c,
        is_member: memberMerchantIds.has(c.id),
        is_claimed: claimedMerchantMap.has(c.id),
        claimed_at: claimedMerchantMap.get(c.id) || null,
      }));

      return send(res, 200, { success: true, data: result });
    }

    // ── GET /api/v1/consumers/merchants/:id/campaigns ─────────────
    const consumerMerchCamsMatch = url.match(/^\/api\/v1\/consumers\/merchants\/([a-zA-Z0-9_-]+)\/campaigns$/);
    if (method === 'GET' && consumerMerchCamsMatch) {
      const merchantId = consumerMerchCamsMatch[1];
      let userId = null;
      const authHeader = req.headers.authorization;
      if (authHeader) {
        try {
          const decoded = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET);
          userId = decoded.userId || null;
        } catch { /* ignore */ }
      }
      const cams = await sql`
        SELECT
          c.id as campaign_id,
          c.title,
          c.discount_percentage,
          c.terms,
          c.end_at,
          c.campaign_type,
          m.business_presence,
          COALESCE(NULLIF(c.promo_code, ''), m.welcome_promo_code) as promo_code,
          r.id as redemption_id,
          r.status as redemption_status,
          r.claimed_at,
          r.redeemed,
          r.redeemed_at
        FROM "Campaign" c
        JOIN "Merchant" m ON m.id = c.merchant_id
        LEFT JOIN "Redemption" r ON r.campaign_id = c.id AND r.user_id = ${userId}
        WHERE c.merchant_id = ${merchantId}
          AND c.status = 'active'
          AND (c.end_at IS NULL OR c.end_at > NOW())
          AND NOT (
            r.id IS NOT NULL
            AND (r.status = 'redeemed' OR r.redeemed = true)
          )
          AND NOT (
            m.business_presence = 'online'
            AND c.campaign_type = 'initial'
            AND r.id IS NOT NULL
            AND r.status = 'claimed'
            AND r.claimed_at + INTERVAL '30 days' < NOW()
          )
          AND (
            -- Welcome/initial campaigns (no AuditLog): visible to all members
            NOT EXISTS (
              SELECT 1 FROM "AuditLog" al
              WHERE al.target_id = c.id AND al.action = 'promotion_created'
            )
            OR
            -- Targeted campaigns: only visible to members who were in the original audience
            r.id IS NOT NULL
          )
        ORDER BY c.created_at ASC
      `;
      return send(res, 200, { success: true, data: cams });
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
          r.claimed_at,
          r.status,
          c.title as campaign_title,
          m.business_name as merchant_name,
          m.business_presence
        FROM "Redemption" r
        JOIN "Campaign" c ON c.id = r.campaign_id
        JOIN "Merchant" m ON m.id = c.merchant_id
        WHERE r.user_id = ${payload.userId}
        ORDER BY COALESCE(r.redeemed_at, r.claimed_at, r.issued_at) DESC
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

      await sql`UPDATE "User" SET push_token = ${data.token}, device_platform = ${data.platform || null} WHERE id = ${payload.userId}`;
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

    // ── DELETE /api/v1/consumers/account ────────────────────────────
    // Permanently deletes the user's PII and removes them from all merchant member lists.
    // Keeps a shell User row + Redemption rows (anonymized) for merchant analytics.
    if (method === 'DELETE' && url.endsWith('/consumers/account')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const userId = payload.userId;

      // 1. Delete notification history (no value after account deletion)
      await sql`DELETE FROM "NotificationHistory" WHERE user_id = ${userId}`;

      // 2. Delete pending notification queue entries
      await sql`DELETE FROM "NotificationQueue" WHERE user_id = ${userId}`;

      // 3. Remove from all merchant member lists (member count drops accurately)
      await sql`DELETE FROM "MerchantMember" WHERE user_id = ${userId}`;

      // 4. Null out ALL personally identifiable information on the User row
      //    Keep the row as a shell so Redemption foreign keys stay valid
      await sql`
        UPDATE "User" SET
          email = NULL,
          full_name = NULL,
          phone_number = NULL,
          city = NULL,
          zip_code = NULL,
          password_hash = NULL,
          push_token = NULL,
          google_sub = NULL,
          apple_sub = NULL,
          location_sharing_enabled = false,
          push_notifications_enabled = false,
          reset_token = NULL,
          reset_expires_at = NULL
        WHERE id = ${userId}
      `;

      console.log(`[DELETE ACCOUNT] User ${userId} account data purged successfully`);
      return send(res, 200, { success: true, message: 'Account deleted successfully' });
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

      // Auto-join merchant member & verify campaign is active and unexpired
      const [campaign] = await sql`SELECT merchant_id, status, end_at FROM "Campaign" WHERE id = ${campaignId}`;
      if (!campaign) return send(res, 404, { success: false, error: 'Campaign not found' });

      if (campaign.status === 'expired' || (campaign.end_at && new Date(campaign.end_at) <= new Date())) {
        return send(res, 400, { success: false, error: 'This offer has expired and is no longer available.' });
      }

      await sql`
         INSERT INTO "MerchantMember" (id, merchant_id, user_id, join_source, created_at)
         VALUES (gen_random_uuid()::text, ${campaign.merchant_id}, ${payload.userId}, 'app_discovery', NOW())
         ON CONFLICT DO NOTHING
      `;

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
        SET expires_at = NOW() + INTERVAL '3 minutes',
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
        // Guard 1: Block re-activation of an already-redeemed campaign
        const [alreadyRedeemed] = await sql`
          SELECT 1 FROM "Redemption"
          WHERE user_id = ${payload.userId}
            AND campaign_id = ${campaignId}
            AND redeemed = true
          LIMIT 1
        `;
        if (alreadyRedeemed) {
          return send(res, 400, { success: false, error: 'You have already redeemed this offer.' });
        }

        // Guard 2: Block activation of targeted campaigns by non-audience members
        const [auditEntry] = await sql`
          SELECT 1 FROM "AuditLog"
          WHERE target_id = ${campaignId}
            AND action = 'promotion_created'
          LIMIT 1
        `;
        if (auditEntry) {
          return send(res, 403, { success: false, error: 'This offer is not available to you.' });
        }

        // Safe to insert — first-time activation for a valid initial/welcome campaign
        const [inserted] = await sql`
          INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed, status)
          VALUES (gen_random_uuid()::text, ${payload.userId}, ${campaignId}, ${code}, NOW(), NOW() + INTERVAL '3 minutes', false, 'pending')
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
        SET status = CASE WHEN claimed_at IS NOT NULL THEN 'claimed' ELSE 'created' END,
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

    // ── POST /api/v1/consumers/merchants/:merchant_id/claim-welcome ─
    const claimWelcomeMatch = url.match(/^\/api\/v1\/consumers\/merchants\/([a-zA-Z0-9_-]+)\/claim-welcome$/);
    if (method === 'POST' && claimWelcomeMatch) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const targetMerchantId = claimWelcomeMatch[1];

      // Verify merchant is online type
      const [merchantInfo] = await sql`
        SELECT id, business_presence, welcome_promo_code FROM "Merchant"
        WHERE id = ${targetMerchantId} LIMIT 1
      `;
      if (!merchantInfo) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchantInfo.business_presence !== 'online') {
        return send(res, 400, { success: false, error: 'Claim & Copy is only available for online merchants' });
      }

      // Find unclaimed initial redemption for this user + merchant
      const [redemption] = await sql`
        SELECT r.id FROM "Redemption" r
        JOIN "Campaign" c ON c.id = r.campaign_id
        WHERE r.user_id = ${payload.userId}
          AND c.merchant_id = ${targetMerchantId}
          AND c.campaign_type = 'initial'
          AND r.status = 'created'
        LIMIT 1
      `;
      if (!redemption) {
        // Check if already claimed — return the code again
        const [already] = await sql`
          SELECT r.claimed_at FROM "Redemption" r
          JOIN "Campaign" c ON c.id = r.campaign_id
          WHERE r.user_id = ${payload.userId}
            AND c.merchant_id = ${targetMerchantId}
            AND c.campaign_type = 'initial'
            AND r.status = 'claimed'
          LIMIT 1
        `;
        if (already) {
          return send(res, 200, { success: true, already_claimed: true, data: { promo_code: merchantInfo.welcome_promo_code, claimed_at: already.claimed_at } });
        }
        return send(res, 404, { success: false, error: 'No claimable welcome offer found. Make sure you have joined this merchant first.' });
      }

      // Mark as claimed
      await sql`UPDATE "Redemption" SET status = 'claimed', claimed_at = NOW() WHERE id = ${redemption.id}`;

      return send(res, 200, { success: true, data: { promo_code: merchantInfo.welcome_promo_code, claimed_at: new Date() } });
    }

    // ── POST /api/v1/consumers/redemptions/:id/claim ───────────────
    const claimRedemptionMatch = url.match(/^\/api\/v1\/consumers\/redemptions\/([a-zA-Z0-9_-]+)\/claim$/);
    if (method === 'POST' && claimRedemptionMatch) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const redemptionId = claimRedemptionMatch[1];
      const [redemption] = await sql`
        SELECT r.id, r.user_id, r.status, c.promo_code, c.id as campaign_id
        FROM "Redemption" r
        JOIN "Campaign" c ON c.id = r.campaign_id
        WHERE r.id = ${redemptionId}
        LIMIT 1
      `;
      if (!redemption) return send(res, 404, { success: false, error: 'Redemption not found' });
      if (redemption.user_id !== payload.userId) return send(res, 403, { success: false, error: 'Forbidden' });
      if (redemption.status === 'claimed') {
        return send(res, 200, { success: true, already_claimed: true, data: { promo_code: redemption.promo_code } });
      }
      if (redemption.status !== 'created') {
        return send(res, 400, { success: false, error: `Cannot claim a redemption with status: ${redemption.status}` });
      }
      if (!redemption.promo_code) {
        return send(res, 400, { success: false, error: 'This offer does not have a promo code to claim' });
      }
      await sql`UPDATE "Redemption" SET status = 'claimed', claimed_at = NOW() WHERE id = ${redemptionId}`;
      return send(res, 200, { success: true, data: { promo_code: redemption.promo_code, claimed_at: new Date() } });
    }

    // ── POST /api/v1/redemptions/claim ─────────────────────────────
    // Accepts { campaign_id } in body. Finds the user's Redemption for that
    // campaign, marks it claimed, and returns the promo_code. Idempotent.
    if (method === 'POST' && url.endsWith('/redemptions/claim')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const data = req.body || {};
      if (!data.campaign_id) return send(res, 400, { success: false, error: 'Missing campaign_id' });

      const [redemption] = await sql`
        SELECT r.id, r.user_id, r.status, r.claimed_at,
               COALESCE(NULLIF(c.promo_code, ''), m.welcome_promo_code) as promo_code
        FROM "Redemption" r
        JOIN "Campaign" c ON c.id = r.campaign_id
        JOIN "Merchant" m ON m.id = c.merchant_id
        WHERE r.campaign_id = ${data.campaign_id}
          AND r.user_id = ${payload.userId}
        LIMIT 1
      `;

      if (!redemption) {
        // No pre-existing Redemption row — attempt on-demand creation.
        // This covers two cases:
        //   (a) Targeted promotions that were excluded from QR-scan auto-assignment
        //       (they have an AuditLog 'promotion_created' entry, so the batch INSERT skips them).
        //   (b) Members who joined via the in-app 'Join Member List' button rather than
        //       scanning a QR code — autoEnrollUser creates MerchantMember but no Redemption rows.
        // We verify the campaign is valid and the user is a member before creating anything.

        const [campaign] = await sql`
          SELECT c.id, c.merchant_id, c.status, c.end_at,
                 COALESCE(NULLIF(c.promo_code, ''), m.welcome_promo_code) AS promo_code
          FROM "Campaign" c
          JOIN "Merchant" m ON m.id = c.merchant_id
          WHERE c.id = ${data.campaign_id}
            AND c.status IN ('active', 'created')
            AND (c.end_at IS NULL OR c.end_at > NOW())
          LIMIT 1
        `;
        if (!campaign) {
          return send(res, 404, { success: false, error: 'Campaign not found or has expired' });
        }

        // Block on-demand creation for targeted campaigns. Only members who were in the
        // original audience (and already have a Redemption row) may claim them.
        // Welcome/initial campaigns have no AuditLog entry and are open to all members.
        const [auditEntry] = await sql`
          SELECT 1 FROM "AuditLog"
          WHERE target_id = ${data.campaign_id}
            AND action = 'promotion_created'
          LIMIT 1
        `;
        if (auditEntry) {
          return send(res, 403, { success: false, error: 'This offer is not available to you.' });
        }

        if (!campaign.promo_code) {
          return send(res, 400, { success: false, error: 'This offer does not have a promo code' });
        }

        // Confirm the user is a member of the merchant that owns this campaign.
        const [membership] = await sql`
          SELECT id FROM "MerchantMember"
          WHERE merchant_id = ${campaign.merchant_id}
            AND user_id = ${payload.userId}
          LIMIT 1
        `;
        if (!membership) {
          return send(res, 403, { success: false, error: 'You must be a member of this merchant to reveal this offer' });
        }

        // Atomically insert a claimed Redemption row if none exists yet.
        // The INSERT ... SELECT ... WHERE NOT EXISTS guard prevents duplicate rows
        // in the rare case of two simultaneous requests for the same user+campaign.
        await sql`
          INSERT INTO "Redemption" (id, user_id, campaign_id, token, issued_at, expires_at, redeemed, status, claimed_at)
          SELECT
            gen_random_uuid()::text,
            ${payload.userId},
            ${data.campaign_id},
            gen_random_uuid()::text,
            NOW(),
            ${campaign.end_at || null},
            false,
            'claimed',
            NOW()
          WHERE NOT EXISTS (
            SELECT 1 FROM "Redemption"
            WHERE campaign_id = ${data.campaign_id}
              AND user_id = ${payload.userId}
          )
        `;

        return send(res, 200, { success: true, data: { promo_code: campaign.promo_code, claimed_at: new Date() } });
      }

      // Pre-existing Redemption found — safety check (WHERE already filters by userId, but belt-and-suspenders).
      if (redemption.user_id !== payload.userId) return send(res, 403, { success: false, error: 'Forbidden' });

      // Backend last line of defense — block re-claiming an already-redeemed offer.
      if (redemption.redeemed || redemption.status === 'redeemed') {
        return send(res, 400, { success: false, error: 'You have already redeemed this offer.' });
      }

      // Already claimed — return code idempotently without re-marking.
      if (redemption.status === 'claimed' || redemption.status === 'activated') {
        return send(res, 200, { success: true, already_claimed: true, data: { promo_code: redemption.promo_code, claimed_at: redemption.claimed_at } });
      }
      if (!redemption.promo_code) {
        return send(res, 400, { success: false, error: 'This offer does not have a promo code' });
      }
      await sql`UPDATE "Redemption" SET status = 'claimed', claimed_at = NOW() WHERE id = ${redemption.id}`;
      return send(res, 200, { success: true, data: { promo_code: redemption.promo_code, claimed_at: new Date() } });
    }


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
      // Allow null to support logo deletion
      if (data.logo_url === undefined) return send(res, 400, { success: false, error: 'Missing logo_url' });

      const logoValue = data.logo_url || null;
      await sql`UPDATE "Merchant" SET logo_url = ${logoValue} WHERE id = ${merchantId}`;
      return send(res, 200, { success: true, data: { logo_url: logoValue } });
    }

    // ── POST /api/v1/merchants/:id/cover-photo ────────────────────
    const coverPhotoMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/cover-photo/);
    if (method === 'POST' && coverPhotoMatch) {
      const merchantId = coverPhotoMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });

      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const data = req.body || {};
      if (data.cover_photo_url === undefined) return send(res, 400, { success: false, error: 'Missing cover_photo_url' });

      const coverValue = data.cover_photo_url || null;
      await sql`UPDATE "Merchant" SET cover_photo_url = ${coverValue} WHERE id = ${merchantId}`;
      return send(res, 200, { success: true, data: { cover_photo_url: coverValue } });
    }

    // ── POST /api/v1/merchants/:id/update-profile ──────────────────
    // Called from signup Step 5 when merchant edits Step 1 fields before Finish Setup.
    const updateProfileMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/update-profile$/);
    if (method === 'POST' && updateProfileMatch) {
      const merchantId = updateProfileMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const d = req.body || {};
      await sql`
        UPDATE "Merchant" SET
          business_name      = COALESCE(${d.name || null}, business_name),
          website            = COALESCE(${d.website || null}, website),
          welcome_offer_text = COALESCE(${d.welcome_offer_text || null}, welcome_offer_text),
          address            = COALESCE(${d.address || null}, address),
          suite              = COALESCE(${d.suite || null}, suite),
          city               = COALESCE(${d.city || null}, city),
          state              = COALESCE(${d.state || null}, state),
          zip_code           = COALESCE(${d.zip || null}, zip_code),
          updated_at         = NOW()
        WHERE id = ${merchantId}
      `;
      // Also update the initial campaign title if offer changed
      if (d.welcome_offer_text) {
        await sql`UPDATE "Campaign" SET title = ${d.welcome_offer_text}, updated_at = NOW()
          WHERE merchant_id = ${merchantId} AND campaign_type = 'initial'`;
      }
      return send(res, 200, { success: true });
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
          mm.join_source,
          COALESCE(
            json_agg(
              json_build_object(
                'id', r.id,
                'campaign_title', c.title,
                'token', r.token,
                'issued_at', r.issued_at,
                'expires_at', r.expires_at,
                'redeemed_at', r.redeemed_at,
                'claimed_at', r.claimed_at,
                'status', CASE
                  WHEN c.campaign_type = 'announcement' OR c.discount_percentage = -1 THEN 'Announcement'
                  WHEN r.status = 'redeemed' OR r.redeemed = true THEN 'Redeemed'
                  WHEN r.status = 'claimed' THEN 'Claimed'
                  WHEN c.end_at IS NOT NULL AND c.end_at < NOW() THEN 'Expired'
                  WHEN r.status = 'expired' THEN 'Expired'
                  WHEN r.status = 'pending' AND (r.expires_at IS NULL OR r.expires_at > NOW()) THEN 'Pending'
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
        GROUP BY u.id, u.city, u.zip_code, u.full_name, mm.join_source
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
               c.promo_code,
               (SELECT a.metadata FROM "AuditLog" a
                WHERE a.target_id = c.id AND a.action = 'promotion_created'
                ORDER BY a.created_at DESC LIMIT 1) as metadata,
               (SELECT COUNT(*) FROM "Redemption" r
                WHERE r.campaign_id = c.id AND (r.status = 'redeemed' OR r.redeemed = true)) as redeemed_count,
               (SELECT COUNT(*) FROM "Redemption" r
                WHERE r.campaign_id = c.id AND r.claimed_at IS NOT NULL) as claimed_count
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

      // Update Merchant Details (welcome_promo_code is intentionally excluded — never editable)
      if (data.business_name || data.contact_name || data.phone || data.public_phone !== undefined || data.public_email !== undefined || data.website !== undefined ||
        data.welcome_offer_text !== undefined || data.review_url !== undefined || data.order_url !== undefined) {
        // Build the update safely — always update all present fields
        const newWelcomeOfferText = data.welcome_offer_text !== undefined ? data.welcome_offer_text : null;
        const newReviewUrl = data.review_url !== undefined ? (data.review_url || null) : undefined;
        const newOrderUrl = data.order_url !== undefined ? (data.order_url || null) : undefined;

        await sql`
           UPDATE "Merchant"
           SET
             business_name = COALESCE(${data.business_name || null}, business_name),
             contact_name = COALESCE(${data.contact_name || null}, contact_name),
             phone = COALESCE(${data.phone || null}, phone),
             public_phone = ${data.public_phone !== undefined ? (data.public_phone || null) : sql`public_phone`},
             public_email = ${data.public_email !== undefined ? (data.public_email ? data.public_email.toLowerCase() : null) : sql`public_email`},
             website = COALESCE(${data.website !== undefined ? data.website : null}, website),
             welcome_offer_text = COALESCE(${newWelcomeOfferText}, welcome_offer_text)
           WHERE id = ${merchantId}
         `;
        // Update nullable URL fields separately (COALESCE would skip intentional clears)
        if (newReviewUrl !== undefined) {
          let detectedPlatform = null;
          if (newReviewUrl) {
            const rLower = newReviewUrl.toLowerCase();
            if (rLower.includes('yelp')) detectedPlatform = 'Yelp';
            else if (rLower.includes('google') || rLower.includes('g.page') || rLower.includes('maps.app.goo.gl')) detectedPlatform = 'Google';
            else if (rLower.includes('instagram.com') || rLower.includes('instagr.am')) detectedPlatform = 'Instagram';
            else if (rLower.includes('facebook.com') || rLower.includes('fb.me') || rLower.includes('fb.com')) detectedPlatform = 'Facebook';
            else if (rLower.includes('tiktok.com')) detectedPlatform = 'TikTok';
            else if (rLower.includes('twitter.com') || rLower.includes('x.com')) detectedPlatform = 'X';
            else if (rLower.includes('youtube.com') || rLower.includes('youtu.be')) detectedPlatform = 'YouTube';
            else if (rLower.includes('linkedin.com')) detectedPlatform = 'LinkedIn';
            else if (rLower.includes('pinterest.com') || rLower.includes('pin.it')) detectedPlatform = 'Pinterest';
            else if (rLower.includes('threads.net')) detectedPlatform = 'Threads';
            else if (rLower.includes('nextdoor.com')) detectedPlatform = 'Nextdoor';
            else if (rLower.includes('tripadvisor')) detectedPlatform = 'TripAdvisor';
            else if (rLower.includes('trustpilot')) detectedPlatform = 'Trustpilot';
            else if (rLower.includes('bbb.org')) detectedPlatform = 'BBB';
          }
          await sql`UPDATE "Merchant" SET review_url = ${newReviewUrl}, rating_platform = COALESCE(${detectedPlatform}, rating_platform) WHERE id = ${merchantId}`;
        }
        if (newOrderUrl !== undefined) {
          await sql`UPDATE "Merchant" SET order_url = ${newOrderUrl} WHERE id = ${merchantId}`;
        }
        if (data.public_phone !== undefined) {
          const newPublicPhone = data.public_phone ? data.public_phone.trim() : null;
          await sql`UPDATE "Merchant" SET public_phone = ${newPublicPhone} WHERE id = ${merchantId}`;
        }
        if (data.public_email !== undefined) {
          const newPublicEmail = data.public_email ? data.public_email.trim().toLowerCase() : null;
          await sql`UPDATE "Merchant" SET public_email = ${newPublicEmail} WHERE id = ${merchantId}`;
        }
        if (data.is_multi_location !== undefined) {
          await sql`UPDATE "Merchant" SET is_multi_location = ${!!data.is_multi_location} WHERE id = ${merchantId}`;
        }
        // Also sync the initial campaign title so the print sign stays up to date
        if (newWelcomeOfferText) {
          const updateResult = await sql`
            UPDATE "Campaign"
            SET title = ${newWelcomeOfferText}, updated_at = ${new Date().toISOString()}
            WHERE merchant_id = ${merchantId}
              AND campaign_type = 'initial'
              AND status = 'active'
          `;
          if (updateResult.count === 0) {
            const now = new Date().toISOString();
            await sql`
              INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, status, campaign_type, created_at, end_at, updated_at)
              VALUES (gen_random_uuid()::text, ${merchantId}, ${newWelcomeOfferText}, 10, 'active', 'initial', ${now}, NULL, ${now})
            `;
          }
        }
      }

      // Update Location Details (assuming 1 location for now per merchant, based on onboarding signup logic)
      if (data.address !== undefined || data.suite !== undefined || data.city !== undefined || data.state !== undefined || data.zip !== undefined) {
        await sql`
           UPDATE "MerchantLocation" 
           SET 
             address = ${data.address !== undefined ? (data.address ? data.address.trim() : null) : sql`address`},
             suite = ${data.suite !== undefined ? (data.suite ? data.suite.trim() : null) : sql`suite`},
             city = ${data.city !== undefined ? (data.city ? data.city.trim() : null) : sql`city`},
             state = ${data.state !== undefined ? (data.state ? data.state.trim().toUpperCase() : null) : sql`state`},
             postal_code = ${data.zip !== undefined ? (data.zip ? data.zip.trim() : null) : sql`postal_code`}
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
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "member_limit" INT DEFAULT 100`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "promo_code" TEXT`;
      return send(res, 200, { success: true, message: "Promo code columns added (member_limit, promo_code)!" });
    }

    // ── device_platform migration ──────────────────────────────────
    if (url === '/api/v1/admin/migrate-device-platform' && method === 'GET') {
      await sql`ALTER TABLE "User" ADD COLUMN IF NOT EXISTS device_platform TEXT`;
      return send(res, 200, { success: true, message: 'device_platform column added to User table.' });
    }

    // ── payment_failed_at migration ────────────────────────────────
    if (url === '/api/v1/admin/migrate-payment-failed-at' && method === 'GET') {
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS payment_failed_at TIMESTAMPTZ`;
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS payment_failure_reminder_count INTEGER DEFAULT 0`;
      return send(res, 200, { success: true, message: 'payment_failed_at and payment_failure_reminder_count added to Merchant table.' });
    }

    // ── Hybrid business_presence migration ────────────────────────
    if (url === '/api/v1/admin/migrate-hybrid-presence' && method === 'GET') {
      await sql`ALTER TABLE "Merchant" ALTER COLUMN business_presence TYPE VARCHAR(20)`;
      return send(res, 200, { success: true, message: 'business_presence column widened to VARCHAR(20) for hybrid support.' });
    }

    // ── AdminAccessCode migration ──────────────────────────────────
    if (url === '/api/v1/admin/migrate-access-codes' && method === 'GET') {
      await sql`
        CREATE TABLE IF NOT EXISTS "AdminAccessCode" (
          id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          code       TEXT UNIQUE NOT NULL,
          label      TEXT,
          used       BOOLEAN DEFAULT false,
          used_by    TEXT,
          used_at    TIMESTAMPTZ,
          expires_at TIMESTAMPTZ NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `;
      // Add cancelled_at to Merchant if not exists (needed by FFL cancel endpoint)
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS "cancelled_at" TIMESTAMPTZ`;
      return send(res, 200, { success: true, message: 'AdminAccessCode table created and Merchant.cancelled_at added.' });
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

    // ── Helper: verify admin JWT Bearer token ────────────────────
    function verifyAdminAuth(req) {
      const authHeader = req.headers['authorization'] || '';
      if (!authHeader.startsWith('Bearer ')) return false;
      try {
        const payload = jwt.verify(authHeader.slice(7), process.env.JWT_SECRET);
        return payload.role === 'admin';
      } catch (e) {
        return false;
      }
    }

    // ── POST /api/v1/admin/login ─────────────────────────────────
    if (method === 'POST' && url.endsWith('/admin/login')) {
      const data = req.body || {};
      const { email, password } = data;
      const ADMIN_EMAIL = process.env.ADMIN_EMAIL;
      const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;

      if (!ADMIN_EMAIL || !ADMIN_PASSWORD) {
        return send(res, 500, { success: false, error: 'Admin credentials not configured on server' });
      }
      if (!email || !password) {
        return send(res, 400, { success: false, error: 'Email and password are required' });
      }
      if (email.toLowerCase().trim() !== ADMIN_EMAIL.toLowerCase().trim() || password !== ADMIN_PASSWORD) {
        return send(res, 401, { success: false, error: 'Invalid email or password' });
      }
      const token = jwt.sign({ role: 'admin' }, process.env.JWT_SECRET, { expiresIn: '12h' });
      return send(res, 200, { success: true, token });
    }

    // ── Helper: verify rep JWT Bearer token ─────────────────────
    function verifyRepAuth(req) {
      const authHeader = req.headers['authorization'] || '';
      if (!authHeader.startsWith('Bearer ')) return null;
      try {
        const payload = jwt.verify(authHeader.slice(7), process.env.JWT_SECRET);
        return payload.role === 'rep' ? payload.contractorId : null;
      } catch (e) { return null; }
    }

    // ── POST /api/v1/rep/forgot-password ──────────────────────────
    if (method === 'POST' && url.endsWith('/rep/forgot-password')) {
      const data = req.body || {};
      if (!data.email) return send(res, 400, { success: false, error: 'Email is required.' });

      const emailClean = data.email.toLowerCase().trim();
      const [rep] = await sql`
        SELECT id, full_name, email FROM "Contractor"
        WHERE LOWER(email) = ${emailClean} AND status != 'terminated'
        LIMIT 1
      `;

      if (rep) {
        const rawToken = crypto.randomBytes(32).toString('hex');
        await sql`
          UPDATE "Contractor"
          SET invite_token = ${rawToken},
              invite_expires_at = NOW() + INTERVAL '1 hour',
              updated_at = NOW()
          WHERE id = ${rep.id}
        `;

        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY) {
          try {
            const brevoClient = SibApiV3Sdk.ApiClient.instance;
            brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
            const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();

            const sendSmtpEmail = new SibApiV3Sdk.SendSmtpEmail();
            sendSmtpEmail.sender = { name: 'Perkfinity', email: 'noreply@perkfinity.net' };
            sendSmtpEmail.to = [{ email: rep.email }];
            sendSmtpEmail.subject = 'Reset your Perkfinity Rep Portal Password';

            const origin = req.headers.origin || 'https://perkfinity.net';
            const resetLink = `${origin}/reps/index.html?token=${rawToken}`;

            sendSmtpEmail.htmlContent = `
              <div style="font-family:'Helvetica Neue',Arial,sans-serif; max-width:520px; margin:0 auto; background:#ffffff; border-radius:16px; overflow:hidden; border:1px solid #eee;">
                <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf); padding:28px 24px; text-align:center;">
                  <div style="color:#fff; font-size:24px; font-weight:800;">Perkfinity</div>
                </div>
                <div style="padding:28px 24px;">
                  <div style="font-size:20px; font-weight:700; color:#1a1a2e; margin-bottom:16px;">Rep Portal Password Reset</div>
                  <p style="font-size:15px; color:#555; line-height:1.6; margin-bottom:24px;">
                    Hi ${rep.full_name || 'there'},<br><br>
                    We received a request to reset the password for your Perkfinity Sales Rep account. Click the button below to choose a new password. This link expires in 1 hour.
                  </p>
                  <div style="text-align:center; margin-bottom:24px;">
                    <a href="${resetLink}" style="display:inline-block; background:#5b3fa5; color:#fff; font-weight:600; text-decoration:none; padding:14px 28px; border-radius:10px;">Reset Password</a>
                  </div>
                  <p style="font-size:13px; color:#aaa; text-align:center;">If you did not request this, you can safely ignore this email.</p>
                </div>
              </div>
            `;

            await emailApi.sendTransacEmail(sendSmtpEmail);
            console.log(`[Brevo] Rep reset password email sent to ${rep.email}`);
          } catch (brevoErr) {
            console.error('Brevo rep reset email failed:', brevoErr.message || brevoErr);
          }
        }
      }

      return send(res, 200, { success: true, message: 'If that email is registered, a reset link was sent. Check your inbox.' });
    }

    // ── POST /api/v1/rep/reset-password ──────────────────────────
    if (method === 'POST' && url.endsWith('/rep/reset-password')) {
      const { token, password } = req.body || {};
      if (!token || !password) return send(res, 400, { success: false, error: 'Token and password are required.' });
      if (password.length < 8) return send(res, 400, { success: false, error: 'Password must be at least 8 characters long.' });

      const [rep] = await sql`
        SELECT id FROM "Contractor"
        WHERE invite_token = ${token}
          AND invite_expires_at > NOW()
        LIMIT 1
      `;
      if (!rep) return send(res, 400, { success: false, error: 'Invalid or expired invite token. Please request a new one from the Admin.' });

      const hash = await bcrypt.hash(password, 10);
      await sql`
        UPDATE "Contractor"
        SET password_hash = ${hash},
            invite_token = NULL,
            invite_expires_at = NULL,
            updated_at = NOW()
        WHERE id = ${rep.id}
      `;

      return send(res, 200, { success: true, message: 'Password has been set successfully. You can now log in.' });
    }

    // ── POST /api/v1/rep/login ───────────────────────────────────
    if (method === 'POST' && url.endsWith('/rep/login')) {
      const { email, password } = req.body || {};
      if (!email || !password) return send(res, 400, { success: false, error: 'Email and password are required.' });
      const [repLoginRow] = await sql`
        SELECT id, full_name, email, referral_code, status, ica_status, stripe_onboarding_status, stripe_account_id, password_hash
        FROM "Contractor"
        WHERE email = ${email.toLowerCase().trim()}
        LIMIT 1
      `;
      if (!repLoginRow) return send(res, 401, { success: false, error: 'Invalid email or password.' });
      if (!repLoginRow.password_hash) return send(res, 401, { success: false, error: 'Portal access not yet set up. Please contact your administrator.' });
      const repMatch = await bcrypt.compare(password, repLoginRow.password_hash);
      if (!repMatch) return send(res, 401, { success: false, error: 'Invalid email or password.' });
      const repToken = jwt.sign({ role: 'rep', contractorId: repLoginRow.id }, process.env.JWT_SECRET, { expiresIn: '8h' });
      return send(res, 200, { success: true, token: repToken, contractor: {
        id: repLoginRow.id, full_name: repLoginRow.full_name, email: repLoginRow.email,
        referral_code: repLoginRow.referral_code, status: repLoginRow.status
      }});
    }

    // ── GET /api/v1/rep/profile ──────────────────────────────────
    if (method === 'GET' && url === '/api/v1/rep/profile') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const [repProfile] = await sql`
        SELECT id, full_name, legal_name, email, phone, referral_code, status, ica_status, stripe_onboarding_status, stripe_account_id, entity_type, created_at
        FROM "Contractor" WHERE id = ${repId} LIMIT 1
      `;
      if (!repProfile) return send(res, 404, { success: false, error: 'Not found.' });
      return send(res, 200, { success: true, data: repProfile });
    }

    // ── POST /api/v1/rep/sign-ica ────────────────────────────────
    if (method === 'POST' && url === '/api/v1/rep/sign-ica') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const data = req.body || {};
      if (!data.signatureName) return send(res, 400, { success: false, error: 'Signature name is required' });
      
      const [ctr] = await sql`SELECT id, full_name, email, ica_status FROM "Contractor" WHERE id = ${repId} LIMIT 1`;
      if (!ctr) return send(res, 404, { success: false, error: 'Not found.' });
      if (ctr.ica_status === 'signed') return send(res, 400, { success: false, error: 'ICA already signed.' });

      // Mark ICA signed; auto-activate if still pending or inactive
      await sql`
        UPDATE "Contractor"
        SET ica_status = 'signed',
            status     = CASE WHEN status IN ('pending', 'inactive') THEN 'active' ELSE status END,
            updated_at = NOW()
        WHERE id = ${ctr.id}
      `;
      // Auto-start quota period if not already running
      const [qExisting] = await sql`SELECT id FROM "ContractorQuotaPeriod" WHERE contractor_id = ${ctr.id} LIMIT 1`;
      if (!qExisting) {
        const [terr] = await sql`SELECT zip_codes FROM "ContractorTerritory" WHERE contractor_id = ${ctr.id} AND status = 'active' LIMIT 1`;
        const numZips = terr && Array.isArray(terr.zip_codes) && terr.zip_codes.length > 0 ? terr.zip_codes.length : 1;
        const initialQuota = Math.max(20, numZips * 10);
        
        await sql`
          INSERT INTO "ContractorQuotaPeriod"
            (id, contractor_id, period_start, period_end, quota_target, status, created_at, updated_at)
          VALUES
            (gen_random_uuid()::text, ${ctr.id}, CURRENT_DATE,
             CURRENT_DATE + INTERVAL '3 months', ${initialQuota}, 'active', NOW(), NOW())
        `;
        console.log(`[sign-ica] Quota period auto-started for contractor ${ctr.id} with target ${initialQuota}`);
      }
      // Notify admin
      try {
        const SibApiV3Sdk = require('sib-api-v3-sdk');
        const sibClient   = SibApiV3Sdk.ApiClient.instance;
        sibClient.authentications['api-key'].apiKey = process.env.BREVO_API_KEY;
        const transactional = new SibApiV3Sdk.TransactionalEmailsApi();
        await transactional.sendTransacEmail({
          sender:      { name: 'Perkfinity System', email: 'support@perkfinity.net' },
          to:          [{ email: 'support@perkfinity.net', name: 'Admin' }],
          subject:     `✅ ICA Fully Signed — ${ctr.full_name}`,
          htmlContent: `<h2>ICA Fully Executed</h2><p><strong>${ctr.full_name}</strong> (${ctr.email}) has electronically signed their ICA via the Rep Portal. Their quota period has been automatically started.</p>`,
        });
      } catch (emailErr) {
        console.error('[sign-ica] Admin notification email failed:', emailErr.message);
      }
      
      return send(res, 200, { success: true, message: 'ICA signed successfully' });
    }

    // ── GET /api/v1/rep/ica-pdf ────────────────────────────────
    if (method === 'GET' && url === '/api/v1/rep/ica-pdf') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const [ctr] = await sql`
        SELECT c.*, r.commission_rate, r.commission_duration_months, r.retainer_cents
        FROM "Contractor" c
        LEFT JOIN "ContractorCompensationRule" r ON r.contractor_id = c.id
        WHERE c.id = ${repId} LIMIT 1
      `;
      if (!ctr) return send(res, 404, { success: false, error: 'Not found.' });
      
      const [terr] = await sql`SELECT zip_codes FROM "ContractorTerritory" WHERE contractor_id = ${repId} AND status = 'active' LIMIT 1`;

      let stripeBusinessType = null;
      if (ctr.stripe_account_id) {
        try {
          const stripeKey = process.env.STRIPE_SECRET_KEY || null;
          if (stripeKey) {
            const stripeClient = Stripe(stripeKey);
            const stripeAcc = await stripeClient.accounts.retrieve(ctr.stripe_account_id);
            stripeBusinessType = stripeAcc.business_type;
          }
        } catch (e) {
          console.error('[ICA] Stripe business type lookup error:', e.message);
        }
      }

      const { generateICAPdf } = require('./lib/generate-ica.js');
      const pdfBuffer = await generateICAPdf({
        contractorName: ctr.legal_name || ctr.full_name,
        contractorEmail: ctr.email,
        entityType: ctr.entity_type,
        stripeBusinessType: stripeBusinessType,
        agreementDate: ctr.ica_status === 'signed' ? ctr.updated_at : new Date(),
        territoryZips: terr && terr.zip_codes ? terr.zip_codes : [], 
        commissionRate: ctr.commission_rate || 15,
        commissionDurationMonths: ctr.commission_duration_months || 12,
        retainerAmount: (ctr.retainer_cents || 0) / 100,
        isSigned: ctr.ica_status === 'signed',
        signatureName: ctr.full_name,
        companySignatory: ctr.ica_company_signatory,
        signedDate: ctr.updated_at
      });

      res.setHeader('Content-Type', 'application/pdf');
      res.setHeader('Content-Disposition', 'inline; filename="Independent_Contractor_Agreement.pdf"');
      return res.end(pdfBuffer);
    }

    // ── GET /api/v1/rep/merchants ────────────────────────────────
    if (method === 'GET' && url === '/api/v1/rep/merchants') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const repMerchants = await sql`
        SELECT a.id AS attribution_id, a.commission_start_date, a.commission_end_date,
               a.retention_bonuses_paid, a.source, a.created_at AS attributed_at,
               me.id AS merchant_id, me.business_name, me.subscription_tier AS tier,
               me.billing_status, me.billing_cycle, me.contact_name, me.application_status,
               me.stripe_subscription_id, me.stripe_payment_method_id, me.billing_starts_at_member_count, me.member_limit,
               me.is_presetup, me.is_claimed, me.presetup_claimed_at,
               (SELECT COUNT(*) FROM "MerchantMember" WHERE merchant_id = me.id) AS member_count
        FROM "ContractorMerchantAttribution" a
        JOIN "Merchant" me ON me.id = a.merchant_id
        WHERE a.contractor_id = ${repId}
        ORDER BY a.created_at DESC
      `;
      return send(res, 200, { success: true, data: repMerchants });
    }

    // ── GET /api/v1/rep/earnings ─────────────────────────────────
    if (method === 'GET' && url === '/api/v1/rep/earnings') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const [repW9] = await sql`SELECT stripe_onboarding_status FROM "Contractor" WHERE id = ${repId} LIMIT 1`;
      if (!repW9) return send(res, 404, { success: false, error: 'Not found.' });
      if (repW9.stripe_onboarding_status !== 'complete')
        return send(res, 200, { success: true, gated: true, message: 'Complete Stripe onboarding to view earnings.' });
      const repPayoutsAll = await sql`SELECT status, total_cents, commission_cents, retainer_cents FROM "ContractorPayout" WHERE contractor_id = ${repId}`;
      const total_earned_cents = repPayoutsAll.reduce((s, p) => s + (p.total_cents || 0), 0);
      const pending_cents = repPayoutsAll.filter(p => p.status === 'pending' || p.status === 'approved').reduce((s, p) => s + (p.total_cents || 0), 0);
      const paid_cents = repPayoutsAll.filter(p => p.status === 'paid').reduce((s, p) => s + (p.total_cents || 0), 0);
      const [repRule] = await sql`SELECT commission_rate, commission_duration_months, retainer_cents FROM "ContractorCompensationRule" WHERE contractor_id = ${repId} LIMIT 1`;
      const defaultRule = { commission_rate: 0.25, commission_duration_months: 12, retainer_cents: 0 };
      return send(res, 200, { success: true, gated: false, data: { total_earned_cents, pending_cents, paid_cents, rule: repRule || defaultRule } });
    }

    // ── GET /api/v1/rep/payouts ──────────────────────────────────
    if (method === 'GET' && url === '/api/v1/rep/payouts') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const [repPayW9] = await sql`SELECT stripe_onboarding_status FROM "Contractor" WHERE id = ${repId} LIMIT 1`;
      if (!repPayW9) return send(res, 404, { success: false, error: 'Not found.' });
      if (repPayW9.stripe_onboarding_status !== 'complete')
        return send(res, 200, { success: true, gated: true, message: 'Complete Stripe onboarding to view payouts.' });
      const repPayoutsList = await sql`
        SELECT id, period_start, period_end, commission_cents, retainer_cents,
               milestone_bonus_cents, retention_bonus_cents, special_bonus_cents,
               total_cents, status, payment_method, paid_at, created_at, breakdown
        FROM "ContractorPayout"
        WHERE contractor_id = ${repId}
        ORDER BY created_at DESC
      `;
      return send(res, 200, { success: true, data: repPayoutsList });
    }

    // ── GET /api/v1/rep/quota ────────────────────────────────────
    if (method === 'GET' && url === '/api/v1/rep/quota') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const [repQuota] = await sql`SELECT * FROM "ContractorQuotaPeriod" WHERE contractor_id = ${repId} LIMIT 1`;
      if (!repQuota) return send(res, 200, { success: true, data: null });
      const [repQCount] = await sql`
        SELECT COUNT(*)::int AS cnt FROM "ContractorMerchantAttribution" a
        JOIN "Merchant" m ON m.id = a.merchant_id
        WHERE a.contractor_id = ${repId} AND m.billing_status NOT IN ('cancelled', 'deleted')
      `;
      const rqCount = repQCount?.cnt || 0;
      const rqPercent = Math.min(Math.round((rqCount / (repQuota.quota_target || 30)) * 100), 100);
      const rqEnd = new Date(repQuota.period_end);
      const rqDays = Math.max(0, Math.ceil((rqEnd - new Date()) / 86400000));
      return send(res, 200, { success: true, data: { period: repQuota, current_count: rqCount, percent: rqPercent, days_remaining: rqDays } });
    }

    // ── GET /api/v1/rep/territory ────────────────────────────────
    if (method === 'GET' && url === '/api/v1/rep/territory') {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      const [repTerritory] = await sql`
        SELECT id, label, zip_codes, status, assigned_at
        FROM "ContractorTerritory"
        WHERE contractor_id = ${repId} AND status = 'active'
        LIMIT 1
      `;
      return send(res, 200, { success: true, data: repTerritory || null });
    }

    // ── GET /api/v1/admin/merchants ─────────────────────────────
    if (method === 'GET' && url.endsWith('/admin/merchants')) {
      const merchants = await sql`
        SELECT m.*,
          (SELECT COUNT(*) FROM "MerchantMember" ml WHERE ml.merchant_id = m.id) as member_count,
          (SELECT COUNT(*) FROM "Campaign" c WHERE c.merchant_id = m.id) as campaign_count,
          (SELECT COUNT(*) FROM "Redemption" r JOIN "Campaign" c2 ON c2.id = r.campaign_id WHERE c2.merchant_id = m.id AND r.status = 'redeemed') as redemption_count,
          (SELECT title FROM "Campaign" c3 WHERE c3.merchant_id = m.id AND c3.status = 'active' ORDER BY c3.created_at ASC LIMIT 1) as perk,
          mu.email as contact_email,
          ml2.address as location_address,
          ml2.suite as location_suite,
          ml2.city as location_city,
          ml2.state as location_state,
          ml2.postal_code as location_zip
        FROM "Merchant" m
        LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
        LEFT JOIN "MerchantLocation" ml2 ON ml2.merchant_id = m.id AND ml2.is_active = true
        WHERE (m.application_status IS NULL OR m.application_status = 'approved')
        ORDER BY m.created_at DESC
      `;
      const active = merchants.filter(m => m.status !== 'inactive' && m.billing_status !== 'deleted').length;
      return send(res, 200, {
        success: true,
        data: {
          merchants: merchants.map(m => ({
            ...m,
            password_hash: undefined,
            tier: m.subscription_tier || 'free',
            status: m.status || 'active',
            is_web_sponsored: m.is_web_sponsored && (!m.web_sponsored_until || new Date(m.web_sponsored_until) >= new Date()) ? true : false,
            is_app_sponsored: m.is_app_sponsored && (!m.app_sponsored_until || new Date(m.app_sponsored_until) >= new Date()) ? true : false
          })),
          stats: { total: merchants.length, active }
        }
      });
    }

    // ── PATCH /api/v1/admin/merchants/:id/toggle-visibility ──────
    const toggleVisMatch = url.match(/^\/api\/v1\/admin\/merchants\/([a-zA-Z0-9_-]+)\/toggle-visibility$/);
    if (method === 'PATCH' && toggleVisMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = toggleVisMatch[1];
      try {
        const body = req.body || {};
        if (typeof body.is_hidden !== 'boolean') {
          return send(res, 400, { success: false, error: 'is_hidden must be a boolean' });
        }
        await sql`UPDATE "Merchant" SET is_hidden = ${body.is_hidden} WHERE id = ${merchantId}`;
        return send(res, 200, { success: true, message: 'Visibility updated' });
      } catch (err) {
        console.error('Toggle visibility error:', err);
        return send(res, 500, { success: false, error: 'Failed to update visibility' });
      }
    }

    // ── PATCH /api/v1/admin/merchants/:id/sponsorship ───────────────
    const sponsorAdminMatch = url.match(/^\/api\/v1\/admin\/merchants\/([a-zA-Z0-9_-]+)\/sponsorship$/);
    if (method === 'PATCH' && sponsorAdminMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = sponsorAdminMatch[1];
      try {
        const body = req.body || {};
        await sql`
          UPDATE "Merchant" 
          SET 
            is_web_sponsored = ${body.is_web_sponsored !== undefined ? body.is_web_sponsored : sql`is_web_sponsored`},
            web_sponsored_until = ${body.web_sponsored_until !== undefined ? (body.web_sponsored_until ? new Date(body.web_sponsored_until) : null) : sql`web_sponsored_until`},
            is_app_sponsored = ${body.is_app_sponsored !== undefined ? body.is_app_sponsored : sql`is_app_sponsored`},
            app_sponsored_until = ${body.app_sponsored_until !== undefined ? (body.app_sponsored_until ? new Date(body.app_sponsored_until) : null) : sql`app_sponsored_until`},
            is_fullpage_sponsored = ${body.is_fullpage_sponsored !== undefined ? body.is_fullpage_sponsored : sql`is_fullpage_sponsored`},
            fullpage_sponsored_until = ${body.fullpage_sponsored_until !== undefined ? (body.fullpage_sponsored_until ? new Date(body.fullpage_sponsored_until) : null) : sql`fullpage_sponsored_until`}
          WHERE id = ${merchantId}
        `;
        return send(res, 200, { success: true, message: 'Sponsorship updated' });
      } catch (err) {
        console.error('Update sponsorship error:', err);
        return send(res, 500, { success: false, error: 'Failed to update sponsorship' });
      }
    }

    // ── PATCH /api/v1/admin/merchants/:id/creative ───────────────────
    const creativeAdminMatch = url.match(/^\/api\/v1\/admin\/merchants\/([a-zA-Z0-9_-]+)\/creative$/);
    if (method === 'PATCH' && creativeAdminMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = creativeAdminMatch[1];
      try {
        const body = req.body || {};
        const sanitizeImg = (u) => {
          if (!u || typeof u !== 'string') return null;
          const trimmed = u.trim();
          if (!trimmed) return null;
          if (trimmed.startsWith('http://')) return trimmed.replace(/^http:\/\//i, 'https://');
          return trimmed;
        };

        if (body.cover_photo_url !== undefined) {
          const coverVal = sanitizeImg(body.cover_photo_url);
          await sql`UPDATE "Merchant" SET cover_photo_url = ${coverVal}, updated_at = NOW() WHERE id = ${merchantId}`;
        }
        if (body.logo_url !== undefined) {
          const logoVal = sanitizeImg(body.logo_url);
          await sql`UPDATE "Merchant" SET logo_url = ${logoVal}, updated_at = NOW() WHERE id = ${merchantId}`;
        }
        return send(res, 200, { success: true, message: 'Creative updated successfully' });
      } catch (err) {
        console.error('Update creative error:', err);
        return send(res, 500, { success: false, error: 'Failed to update creative' });
      }
    }

    // ── POST /api/v1/admin/merchants/presetup ─────────────────────
    if (method === 'POST' && url === '/api/v1/admin/merchants/presetup') {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });

      const data = req.body || {};
      const missing = [];
      if (!data.business_name) missing.push('Business Name');
      if (!data.business_category) missing.push('Category');
      if (!data.welcome_offer_text) missing.push('Welcome Offer');

      if (missing.length > 0) {
        return send(res, 400, { success: false, error: `Missing required fields: ${missing.join(', ')}` });
      }

      try {
        const cleanSlug = (data.business_name || 'store').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 15) || 'merchant';
        const zip5 = data.zip ? data.zip.replace(/[^0-9]/g, '').slice(0, 5) : '00000';
        const rand4 = Math.floor(1000 + Math.random() * 9000);
        const tempEmail = `${cleanSlug}_${zip5 !== '00000' ? zip5 : rand4}_${rand4}@presetup.perkfinity.net`;

        const nameCap = cleanSlug.charAt(0).toUpperCase() + cleanSlug.slice(1);
        const tempPassword = `${nameCap}2026!`;
        const password_hash = await bcrypt.hash(tempPassword, 12);

        const memberLimit = parseInt(data.member_limit) || 50;
        const isHidden = data.is_hidden !== undefined ? !!data.is_hidden : true;
        const isWebSponsor = !!data.is_web_sponsored;
        const isAppSponsor = !!data.is_app_sponsored;
        const isFullpageSponsor = !!data.is_fullpage_sponsored;
        const sponsorUntil = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000); // 30 days trial

        const addressVal = data.address ? data.address.trim() : null;
        const cityVal = data.city ? data.city.trim() : null;
        const stateVal = data.state ? data.state.trim().toUpperCase() : null;
        const postalCodeVal = (data.zip && data.zip.trim()) ? data.zip.trim() : null;
        const isMultiLoc = !addressVal;

        const sanitizeImgUrl = (u) => {
          if (!u || typeof u !== 'string') return null;
          const trimmed = u.trim();
          if (!trimmed) return null;
          if (trimmed.startsWith('http://')) return trimmed.replace(/^http:\/\//i, 'https://');
          return trimmed;
        };

        const finalLogoUrl = sanitizeImgUrl(data.logo_url);
        const finalCoverUrl = sanitizeImgUrl(data.cover_photo_url);

        let detectedPlatform = null;
        if (data.review_url) {
          const rLower = data.review_url.toLowerCase();
          if (rLower.includes('yelp')) detectedPlatform = 'Yelp';
          else if (rLower.includes('google') || rLower.includes('g.page') || rLower.includes('maps.app.goo.gl')) detectedPlatform = 'Google';
          else if (rLower.includes('instagram.com') || rLower.includes('instagr.am')) detectedPlatform = 'Instagram';
          else if (rLower.includes('facebook.com') || rLower.includes('fb.me') || rLower.includes('fb.com')) detectedPlatform = 'Facebook';
          else if (rLower.includes('tiktok.com')) detectedPlatform = 'TikTok';
          else if (rLower.includes('twitter.com') || rLower.includes('x.com')) detectedPlatform = 'X';
          else if (rLower.includes('youtube.com') || rLower.includes('youtu.be')) detectedPlatform = 'YouTube';
          else if (rLower.includes('linkedin.com')) detectedPlatform = 'LinkedIn';
          else if (rLower.includes('pinterest.com') || rLower.includes('pin.it')) detectedPlatform = 'Pinterest';
          else if (rLower.includes('threads.net')) detectedPlatform = 'Threads';
          else if (rLower.includes('nextdoor.com')) detectedPlatform = 'Nextdoor';
          else if (rLower.includes('tripadvisor')) detectedPlatform = 'TripAdvisor';
          else if (rLower.includes('trustpilot')) detectedPlatform = 'Trustpilot';
          else if (rLower.includes('bbb.org')) detectedPlatform = 'BBB';
        }

        // 1. Create Merchant
        const [merchant] = await sql`
          INSERT INTO "Merchant" (
            id, business_name, contact_name, phone, public_phone, public_email,
            website, review_url, rating_platform, order_url, logo_url, cover_photo_url, promo_description,
            business_presence, business_category, welcome_offer_text, is_multi_location,
            subscription_tier, member_limit, status, application_status, is_hidden, is_presetup, is_claimed,
            temp_password_plain, is_web_sponsored, web_sponsored_until,
            is_app_sponsored, app_sponsored_until, is_fullpage_sponsored, fullpage_sponsored_until,
            created_at, updated_at
          ) VALUES (
            gen_random_uuid()::text, ${data.business_name.trim()}, ${data.contact_name ? data.contact_name.trim() : 'Store Owner'}, ${data.phone ? data.phone.trim() : null}, ${data.public_phone ? data.public_phone.trim() : null}, ${data.public_email ? data.public_email.trim().toLowerCase() : null},
            ${data.website ? data.website.trim() : ''}, ${data.review_url ? data.review_url.trim() : null}, ${detectedPlatform}, ${data.order_url ? data.order_url.trim() : null}, ${finalLogoUrl}, ${finalCoverUrl}, ${data.promo_description || null},
            'hybrid', ${data.business_category}, ${data.welcome_offer_text.trim()}, ${isMultiLoc},
            'presetup_50', ${memberLimit}, 'active', 'approved', ${isHidden}, true, false,
            ${tempPassword}, ${isWebSponsor}, ${isWebSponsor ? sponsorUntil : null},
            ${isAppSponsor}, ${isAppSponsor ? sponsorUntil : null}, ${isFullpageSponsor}, ${isFullpageSponsor ? sponsorUntil : null},
            NOW(), NOW()
          )
          RETURNING *
        `;

        // 2. Create Location
        await sql`
          INSERT INTO "MerchantLocation" (id, merchant_id, address, city, state, postal_code, country, is_active, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, ${addressVal}, ${cityVal}, ${stateVal}, ${postalCodeVal}, 'US', true, NOW())
        `;

        // 3. Create MerchantUser
        await sql`
          INSERT INTO "MerchantUser" (id, merchant_id, email, password_hash, role, status, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, ${tempEmail}, ${password_hash}, 'owner', 'active', NOW())
        `;

        // 4. Create QR Code
        const public_code = crypto.randomBytes(9).toString('base64url');
        await sql`
          INSERT INTO "QrCode" (id, merchant_id, public_code, status, created_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, ${public_code}, 'active', NOW())
        `;

        // 5. Create Welcome Campaign
        await sql`
          INSERT INTO "Campaign" (id, merchant_id, title, discount_percentage, terms, status, campaign_type, start_at, end_at, created_at, updated_at)
          VALUES (gen_random_uuid()::text, ${merchant.id}, ${data.welcome_offer_text.trim()}, 10, ${data.terms ? data.terms.trim() : 'Valid for first-time customers. Cannot be combined with other offers.'}, 'active', 'initial', NOW(), NULL, NOW(), NOW())
        `;

        // 6. Optional Sales Rep Attribution
        let contractorInfo = null;
        if (data.contractor_id && String(data.contractor_id).trim() !== '') {
          const [ctr] = await sql`
            SELECT id, full_name, referral_code, email, phone FROM "Contractor" WHERE id = ${data.contractor_id} LIMIT 1
          `;
          if (ctr) {
            contractorInfo = ctr;
            await sql`
              INSERT INTO "ContractorMerchantAttribution" (
                id, contractor_id, merchant_id, source, created_at, updated_at
              ) VALUES (
                gen_random_uuid()::text, ${ctr.id}, ${merchant.id}, 'presetup', NOW(), NOW()
              )
              ON CONFLICT (merchant_id) DO UPDATE SET contractor_id = ${ctr.id}, updated_at = NOW()
            `;
          }
        }

        return send(res, 200, {
          success: true,
          message: 'Pre-setup merchant created successfully!',
          data: {
            ...merchant,
            temp_email: tempEmail,
            temp_password: tempPassword,
            public_code,
            address: data.address,
            city: data.city,
            state: data.state,
            postal_code: zip5,
            contractor_id: contractorInfo ? contractorInfo.id : null,
            contractor_name: contractorInfo ? contractorInfo.full_name : null,
            contractor_referral_code: contractorInfo ? contractorInfo.referral_code : null,
            contractor_phone: contractorInfo ? contractorInfo.phone : null,
            contractor_email: contractorInfo ? contractorInfo.email : null
          }
        });
      } catch (err) {
        console.error('Error creating pre-setup merchant:', err);
        return send(res, 500, { success: false, error: 'Failed to create pre-setup merchant: ' + err.message });
      }
    }

    // ── GET /api/v1/admin/merchants/presetup ──────────────────────
    if (method === 'GET' && url === '/api/v1/admin/merchants/presetup') {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });

      try {
        const rows = await sql`
          SELECT m.*,
            mu.email as temp_email,
            (SELECT q.public_code FROM "QrCode" q WHERE q.merchant_id = m.id AND q.status = 'active' LIMIT 1) as public_code,
            (SELECT COUNT(*)::int FROM "MerchantMember" mm WHERE mm.merchant_id = m.id) as member_count,
            l.address, l.city, l.state, l.postal_code,
            c.id as contractor_id, c.full_name as contractor_name, c.referral_code as contractor_referral_code,
            c.email as contractor_email, c.phone as contractor_phone
          FROM "Merchant" m
          LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
          LEFT JOIN "MerchantLocation" l ON l.merchant_id = m.id AND l.is_active = true
          LEFT JOIN "ContractorMerchantAttribution" cma ON cma.merchant_id = m.id
          LEFT JOIN "Contractor" c ON c.id = cma.contractor_id
          WHERE m.is_presetup = true
          ORDER BY m.created_at DESC
        `;
        return send(res, 200, { success: true, data: rows });
      } catch (err) {
        console.error('Error fetching pre-setup merchants:', err);
        return send(res, 500, { success: false, error: 'Failed to fetch pre-setup merchants: ' + err.message });
      }
    }

    // ── GET /api/v1/admin/members ────────────────────────────────
    if (method === 'GET' && url.endsWith('/admin/members')) {
      const members = await sql`
        SELECT u.id, u.email, u.full_name, u.phone_number, u.city, u.zip_code, u.push_token, u.device_platform,
          u.created_at,
          (SELECT COUNT(*) FROM "MerchantMember" ml WHERE ml.user_id = u.id) as merchant_count,
          (SELECT COUNT(*) FROM "Redemption" r WHERE r.user_id = u.id AND r.status = 'redeemed') as redemption_count,
          (SELECT COUNT(*) FROM "MerchantMember" ml2 WHERE ml2.user_id = u.id AND ml2.created_at >= NOW() - INTERVAL '30 days') as merchants_30d,
          (SELECT COUNT(*) FROM "Redemption" r2 WHERE r2.user_id = u.id AND r2.status = 'redeemed' AND r2.redeemed_at >= NOW() - INTERVAL '30 days') as redeemed_30d
        FROM "User" u
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
          (SELECT COUNT(*) FROM "Redemption" r WHERE r.campaign_id = c.id AND r.status = 'redeemed') as redemption_count,
          (SELECT COUNT(*) FROM "Redemption" r2 WHERE r2.campaign_id = c.id AND r2.status = 'expired') as expired_count,
          (SELECT COUNT(*) FROM "Redemption" r3 WHERE r3.campaign_id = c.id) as total_sent
        FROM "Campaign" c
        LEFT JOIN "Merchant" m ON m.id = c.merchant_id
        ORDER BY c.created_at DESC
      `;
      const now = new Date();

      // Respect the DB status field as the source of truth.
      // Only override to 'expired' if DB says 'active' but end_at has already passed
      // (cron lag guard — nightly expire-campaigns cron should have caught these).
      const campaignsWithStatus = campaigns.map(c => ({
        ...c,
        status: c.status === 'active' && c.end_at && new Date(c.end_at) < now
          ? 'expired'
          : c.status
      }));

      const active = campaignsWithStatus.filter(c => c.status === 'active').length;
      const totalRedemptions = campaignsWithStatus.reduce((sum, c) => sum + (parseInt(c.redemption_count) || 0), 0);
      const rate = campaignsWithStatus.length ? Math.round((totalRedemptions / campaignsWithStatus.length) * 100) / 100 : 0;
      return send(res, 200, {
        success: true,
        data: {
          campaigns: campaignsWithStatus,
          stats: { total: campaignsWithStatus.length, active, redemptions: totalRedemptions, redemption_rate: rate }
        }
      });
    }


    // ── GET /api/v1/admin/billing ─────────────────────────────────
    if (method === 'GET' && url.endsWith('/admin/billing')) {
      // Invoices with merchant names and billing details
      const invoices = await sql`
        SELECT i.*, m.business_name as merchant_name, m.subscription_tier, m.next_billing_date, m.billing_status, m.billing_cycle
        FROM "Invoice" i
        LEFT JOIN "Merchant" m ON m.id = i.merchant_id
        ORDER BY i.created_at DESC
      `;

      // Billing stats from Merchant table (excluding Demo accounts)
      const [stats] = await sql`
        SELECT
          COUNT(*) FILTER (
            WHERE subscription_tier IN ('tier1','online_starter','online_growth','online_scale') 
              AND account_blocked = false 
              AND billing_status = 'active'
              AND stripe_subscription_id IS NOT NULL
          ) as paying_merchants,
          COUNT(*) FILTER (
            WHERE subscription_tier IN ('tier1','online_starter','online_growth','online_scale') 
              AND billing_status = 'pending_cancellation'
              AND NOT (billing_status = 'active' AND stripe_subscription_id IS NULL AND stripe_payment_method_id IS NULL AND member_limit IS NULL AND billing_cycle = 'monthly' AND subscription_tier != 'free_for_life')
          ) as pending_cancel,
          COUNT(*) FILTER (WHERE billing_status = 'payment_failed') as failed_payments,
          COUNT(*) FILTER (WHERE subscription_tier = 'free_for_life' AND account_blocked = false) as ffl_merchants,
          COUNT(*) FILTER (
            WHERE subscription_tier != 'free_for_life' 
              AND account_blocked = false 
              AND (billing_status != 'deleted' OR billing_status IS NULL)
              AND (stripe_subscription_id IS NULL OR billing_status != 'active')
              AND NOT (billing_status = 'active' AND stripe_subscription_id IS NULL AND stripe_payment_method_id IS NULL AND member_limit IS NULL AND billing_cycle = 'monthly' AND subscription_tier != 'free_for_life')
          ) as upgrade_eligible
        FROM "Merchant"
      `;

      // Fetch active sponsors (both paid and complimentary)
      const activeSponsors = await sql`
        SELECT id, business_name, subscription_tier, 
               is_web_sponsored, web_sponsored_until, 
               is_app_sponsored, app_sponsored_until,
               stripe_bundle_sponsor_subscription_id, 
               stripe_web_sponsor_subscription_id, 
               stripe_app_sponsor_subscription_id
        FROM "Merchant"
        WHERE ((is_web_sponsored = true AND (web_sponsored_until IS NULL OR web_sponsored_until > NOW()))
           OR (is_app_sponsored = true AND (app_sponsored_until IS NULL OR app_sponsored_until > NOW())))
          AND account_blocked = false
      `;

      // Filter complimentary sponsors specifically for the panel
      const complimentarySponsors = activeSponsors.filter(s => 
        !s.stripe_bundle_sponsor_subscription_id && 
        !s.stripe_web_sponsor_subscription_id && 
        !s.stripe_app_sponsor_subscription_id
      );

      // Calculate active paid sponsorship MRR run-rate
      let sponsorshipMrr = 0;
      activeSponsors.forEach(m => {
        if (m.stripe_bundle_sponsor_subscription_id) {
          sponsorshipMrr += 129.99;
        } else if (m.stripe_web_sponsor_subscription_id && m.stripe_app_sponsor_subscription_id && m.stripe_web_sponsor_subscription_id === m.stripe_app_sponsor_subscription_id) {
          // If both web and app fields point to the same bundle subscription ID, count as Everywhere Bundle
          sponsorshipMrr += 129.99;
        } else {
          if (m.stripe_web_sponsor_subscription_id) {
            sponsorshipMrr += 49.99;
          }
          if (m.stripe_app_sponsor_subscription_id) {
            sponsorshipMrr += 99.99;
          }
        }
      });

      // Count active POUF (lifetime) merchants
      const [poufStats] = await sql`
        SELECT COUNT(*)::int as count
        FROM "Merchant"
        WHERE billing_cycle = 'lifetime' AND account_blocked = false
      `;

      // MRR + ARR: split by billing_cycle using the most recent paid invoice per active merchant.
      const [revenueResult] = await sql`
        SELECT
          COALESCE(SUM(amount_cents) FILTER (WHERE billing_cycle IS NULL OR billing_cycle = 'monthly'), 0) AS mrr_cents,
          COALESCE(SUM(amount_cents) FILTER (WHERE billing_cycle = 'annual'), 0) AS arr_cents,
          COUNT(*) FILTER (WHERE billing_cycle = 'annual') AS annual_merchant_count
        FROM (
          SELECT DISTINCT ON (i.merchant_id) i.amount_cents, m.billing_cycle
          FROM "Invoice" i
          JOIN "Merchant" m ON m.id = i.merchant_id
          WHERE i.status = 'paid'
            AND m.billing_status = 'active'
            AND m.stripe_subscription_id IS NOT NULL
            AND m.account_blocked = false
            AND NOT (m.billing_status = 'active' AND m.stripe_subscription_id IS NULL AND m.stripe_payment_method_id IS NULL AND m.member_limit IS NULL AND m.billing_cycle = 'monthly' AND m.subscription_tier != 'free_for_life')
          ORDER BY i.merchant_id, i.paid_at DESC
        ) last_invoices
      `;

      const mrr = (parseInt(revenueResult.mrr_cents) || 0) / 100;
      const arr = (parseInt(revenueResult.arr_cents) || 0) / 100;
      const blendedMrr = mrr + arr / 12;
      const annualMerchantCount = parseInt(revenueResult.annual_merchant_count) || 0;
      const payingCount = parseInt(stats.paying_merchants) || 0;

      // Segmented revenues from Invoice list
      const totalRevenue = invoices
        .filter(i => i.status === 'paid')
        .reduce((sum, i) => sum + (parseInt(i.amount_cents) || 0), 0);

      const platformRevenue = invoices
        .filter(i => i.status === 'paid' && (i.revenue_type === 'platform' || !i.revenue_type))
        .reduce((sum, i) => sum + (parseInt(i.amount_cents) || 0), 0);

      const sponsorshipRevenue = invoices
        .filter(i => i.status === 'paid' && i.revenue_type === 'sponsorship')
        .reduce((sum, i) => sum + (parseInt(i.amount_cents) || 0), 0);

      const poufRevenue = invoices
        .filter(i => i.status === 'paid' && i.revenue_type === 'pouf')
        .reduce((sum, i) => sum + (parseInt(i.amount_cents) || 0), 0);

      return send(res, 200, {
        success: true,
        data: {
          invoices,
          complimentarySponsors,
          stats: {
            mrr: mrr.toFixed(2),
            arr: arr.toFixed(2),
            blended_mrr: blendedMrr.toFixed(2),
            annual_merchant_count: annualMerchantCount,
            paying_merchants: payingCount,
            pending_cancel: parseInt(stats.pending_cancel) || 0,
            failed_payments: parseInt(stats.failed_payments) || 0,
            ffl_merchants: parseInt(stats.ffl_merchants) || 0,
            upgrade_eligible: parseInt(stats.upgrade_eligible) || 0,
            total_revenue_cents: totalRevenue,
            platform_revenue_cents: platformRevenue,
            sponsorship_revenue_cents: sponsorshipRevenue,
            pouf_revenue_cents: poufRevenue,
            sponsorship_mrr: sponsorshipMrr.toFixed(2),
            sponsors_count: activeSponsors.length,
            pouf_count: poufStats.count
          }
        }
      });
    }

    // ═══════════════════════════════════════════════════════════════
    // ANNOUNCEMENT ENDPOINTS
    // ═══════════════════════════════════════════════════════════════

    // ── GET /api/v1/admin/audience-options ─────────────────────────
    if (method === 'GET' && url.endsWith('/admin/audience-options')) {
      const merchantCities = await sql`
        SELECT DISTINCT INITCAP(TRIM(ml.city)) as city FROM "MerchantLocation" ml
        WHERE ml.city IS NOT NULL AND TRIM(ml.city) != ''
        ORDER BY city
      `;
      const merchantZips = await sql`
        SELECT DISTINCT TRIM(ml.postal_code) as postal_code FROM "MerchantLocation" ml
        WHERE ml.postal_code IS NOT NULL AND TRIM(ml.postal_code) != ''
        ORDER BY postal_code
      `;
      const memberCities = await sql`
        SELECT DISTINCT INITCAP(TRIM(u.city)) as city FROM "User" u
        WHERE u.city IS NOT NULL AND TRIM(u.city) != ''
        ORDER BY city
      `;
      const memberZips = await sql`
        SELECT DISTINCT TRIM(u.zip_code) as zip_code FROM "User" u
        WHERE u.zip_code IS NOT NULL AND TRIM(u.zip_code) != ''
        ORDER BY zip_code
      `;
      return send(res, 200, {
        success: true,
        data: {
          merchant_cities: merchantCities.map(r => r.city),
          merchant_zips: merchantZips.map(r => r.postal_code),
          member_cities: memberCities.map(r => r.city),
          member_zips: memberZips.map(r => r.zip_code)
        }
      });
    }

    // ── GET /api/v1/admin/audience-preview ─────────────────────────
    if (method === 'GET' && url.endsWith('/admin/audience-preview')) {
      try {
        const qs = require('url').parse(req.url, true).query;
        const audience = qs.audience ? JSON.parse(qs.audience) : {};
        let recipients = [];

        // Merchant recipients — same query pattern as /admin/merchants
        if (audience.type === 'merchants' || audience.type === 'both') {
          const rows = await sql`
            SELECT m.id, m.business_name,
              mu.email as contact_email,
              m.subscription_tier, m.billing_status, m.account_blocked,
              m.business_presence,
              (SELECT COUNT(*) FROM "MerchantMember" mm WHERE mm.merchant_id = m.id) as member_count,
              m.created_at,
              ml.city as location_city, ml.postal_code as location_zip
            FROM "Merchant" m
            LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
            LEFT JOIN "MerchantLocation" ml ON ml.merchant_id = m.id AND ml.is_active = true
            ORDER BY m.created_at DESC
          `;
          let filtered = rows.filter(r => r.contact_email);
          if (audience.statuses && audience.statuses.length) {
            filtered = filtered.filter(r => {
              if (audience.statuses.includes('free_trial') && (r.subscription_tier === 'none' || r.subscription_tier === 'trial' || !r.subscription_tier)) return true;
              if (audience.statuses.includes('tier1') && r.subscription_tier === 'tier1') return true;
              if (audience.statuses.includes('free_for_life') && r.subscription_tier === 'free_for_life') return true;
              if (audience.statuses.includes('blocked') && r.account_blocked === true) return true;
              if (audience.statuses.includes('pending_cancellation') && r.billing_status === 'pending_cancellation') return true;
              return false;
            });
          }
          if (audience.cities && audience.cities.length) {
            const lc = audience.cities.map(c => c.toLowerCase().trim());
            filtered = filtered.filter(r => r.location_city && lc.includes(r.location_city.toLowerCase().trim()));
          }
          if (audience.zip_codes && audience.zip_codes.length) {
            const lz = audience.zip_codes.map(z => z.trim());
            filtered = filtered.filter(r => r.location_zip && lz.includes(r.location_zip.trim()));
          }
          if (audience.presences && audience.presences.length) {
            filtered = filtered.filter(r => audience.presences.includes(r.business_presence || 'physical'));
          }
          if (audience.joined_days) {
            const cutoff = new Date(Date.now() - parseInt(audience.joined_days) * 86400000);
            filtered = filtered.filter(r => new Date(r.created_at) >= cutoff);
          }
          if (audience.member_count_max != null) {
            filtered = filtered.filter(r => (parseInt(r.member_count) || 0) <= parseInt(audience.member_count_max));
          }
          recipients = recipients.concat(filtered.map(r => ({ name: r.business_name, email: r.contact_email, type: 'merchant' })));
        }

        // Member recipients — same query pattern as /admin/members
        if (audience.type === 'members' || audience.type === 'both') {
          const rows = await sql`
            SELECT u.id, u.full_name, u.email, u.city, u.zip_code, u.created_at,
              u.push_token, u.device_platform,
              (SELECT COUNT(*) FROM "Redemption" r WHERE r.user_id = u.id AND r.status IN ('redeemed','claimed')) as redemption_count
            FROM "User" u
            ORDER BY u.created_at DESC
          `;
          let filtered = rows.filter(r => r.email);
          if (audience.cities && audience.cities.length) {
            const lc = audience.cities.map(c => c.toLowerCase().trim());
            filtered = filtered.filter(r => r.city && lc.includes(r.city.toLowerCase().trim()));
          }
          if (audience.zip_codes && audience.zip_codes.length) {
            const lz = audience.zip_codes.map(z => z.trim());
            filtered = filtered.filter(r => r.zip_code && lz.includes(r.zip_code.trim()));
          }
          if (audience.joined_days) {
            const cutoff = new Date(Date.now() - parseInt(audience.joined_days) * 86400000);
            filtered = filtered.filter(r => new Date(r.created_at) >= cutoff);
          }
          if (audience.platforms && audience.platforms.length) {
            filtered = filtered.filter(r => audience.platforms.includes(r.device_platform || ''));
          }
          if (audience.push_enabled === 'yes') filtered = filtered.filter(r => !!(r.push_token));
          if (audience.push_enabled === 'no') filtered = filtered.filter(r => !(r.push_token));
          if (audience.has_redeemed === 'yes') filtered = filtered.filter(r => parseInt(r.redemption_count) > 0);
          if (audience.has_redeemed === 'no') filtered = filtered.filter(r => !(parseInt(r.redemption_count) > 0));
          recipients = recipients.concat(filtered.map(r => ({ name: r.full_name, email: r.email, type: 'member' })));
        }

        // Deduplicate by email
        const seen = new Set();
        recipients = recipients.filter(r => {
          if (!r.email || seen.has(r.email.toLowerCase())) return false;
          seen.add(r.email.toLowerCase());
          return true;
        });

        return send(res, 200, {
          success: true,
          data: {
            count: recipients.length,
            sample: recipients.slice(0, 10).map(r => ({ name: r.name, email: r.email, type: r.type }))
          }
        });
      } catch (previewErr) {
        console.error('audience-preview error:', previewErr);
        return send(res, 500, { success: false, error: previewErr.message || 'Preview failed' });
      }
    }

    // ── POST /api/v1/admin/send-announcement ──────────────────────
    if (method === 'POST' && url.endsWith('/admin/send-announcement')) {
      const data = req.body || {};
      const { subject, html_body, sender, audience, external_emails, attachments, scheduled_at } = data;

      if (!subject || !html_body) {
        return send(res, 400, { success: false, error: 'Subject and body are required' });
      }

      const BREVO_KEY = process.env.BREVO_API_KEY;
      if (!BREVO_KEY) {
        return send(res, 500, { success: false, error: 'Brevo API key not configured' });
      }

      const SibApiV3Sdk = require('sib-api-v3-sdk');
      const brevoClient = SibApiV3Sdk.ApiClient.instance;
      brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
      const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();

      // Sender mapping
      const senderMap = {
        'hello@perkfinity.net': { name: 'Perkfinity', email: 'hello@perkfinity.net' },
        'support@perkfinity.net': { name: 'Perkfinity Support', email: 'support@perkfinity.net' },
        'noreply@perkfinity.net': { name: 'Perkfinity', email: 'noreply@perkfinity.net' }
      };
      const senderObj = senderMap[sender] || senderMap['noreply@perkfinity.net'];

      // Build recipient list (same logic as preview)
      let recipients = [];
      const aud = audience || {};

      if (aud.type === 'merchants' || aud.type === 'both') {
        const rows = await sql`
          SELECT m.id, mu.email as contact_email,
            m.subscription_tier, m.billing_status, m.account_blocked,
            m.business_presence,
            (SELECT COUNT(*) FROM "MerchantMember" mm WHERE mm.merchant_id = m.id) as member_count,
            m.created_at,
            ml.city as location_city, ml.postal_code as location_zip
          FROM "Merchant" m
          LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
          LEFT JOIN "MerchantLocation" ml ON ml.merchant_id = m.id AND ml.is_active = true
          ORDER BY m.created_at DESC
        `;
        let filtered = rows.filter(r => r.contact_email);
        if (aud.statuses && aud.statuses.length) {
          filtered = filtered.filter(r => {
            if (aud.statuses.includes('free_trial') && (r.subscription_tier === 'none' || r.subscription_tier === 'trial' || !r.subscription_tier)) return true;
            if (aud.statuses.includes('tier1') && r.subscription_tier === 'tier1') return true;
            if (aud.statuses.includes('free_for_life') && r.subscription_tier === 'free_for_life') return true;
            if (aud.statuses.includes('blocked') && r.account_blocked === true) return true;
            if (aud.statuses.includes('pending_cancellation') && r.billing_status === 'pending_cancellation') return true;
            return false;
          });
        }
        if (aud.cities && aud.cities.length) { const lc = aud.cities.map(c => c.toLowerCase().trim()); filtered = filtered.filter(r => r.location_city && lc.includes(r.location_city.toLowerCase().trim())); }
        if (aud.zip_codes && aud.zip_codes.length) { const lz = aud.zip_codes.map(z => z.trim()); filtered = filtered.filter(r => r.location_zip && lz.includes(r.location_zip.trim())); }
        if (aud.presences && aud.presences.length) { filtered = filtered.filter(r => aud.presences.includes(r.business_presence || 'physical')); }
        if (aud.joined_days) {
          const cutoff = new Date(Date.now() - parseInt(aud.joined_days) * 86400000);
          filtered = filtered.filter(r => new Date(r.created_at) >= cutoff);
        }
        if (aud.member_count_max != null) filtered = filtered.filter(r => (parseInt(r.member_count) || 0) <= parseInt(aud.member_count_max));
        recipients = recipients.concat(filtered.map(r => r.contact_email));
      }

      if (aud.type === 'members' || aud.type === 'both') {
        const rows = await sql`
          SELECT u.id, u.email, u.city, u.zip_code, u.created_at,
            u.push_token, u.device_platform,
            (SELECT COUNT(*) FROM "Redemption" r WHERE r.user_id = u.id AND r.status IN ('redeemed','claimed')) as redemption_count
          FROM "User" u
          ORDER BY u.created_at DESC
        `;
        let filtered = rows.filter(r => r.email);
        if (aud.cities && aud.cities.length) { const lc = aud.cities.map(c => c.toLowerCase().trim()); filtered = filtered.filter(r => r.city && lc.includes(r.city.toLowerCase().trim())); }
        if (aud.zip_codes && aud.zip_codes.length) { const lz = aud.zip_codes.map(z => z.trim()); filtered = filtered.filter(r => r.zip_code && lz.includes(r.zip_code.trim())); }
        if (aud.joined_days) {
          const cutoff = new Date(Date.now() - parseInt(aud.joined_days) * 86400000);
          filtered = filtered.filter(r => new Date(r.created_at) >= cutoff);
        }
        if (aud.platforms && aud.platforms.length) { filtered = filtered.filter(r => aud.platforms.includes(r.device_platform || '')); }
        if (aud.push_enabled === 'yes') filtered = filtered.filter(r => !!(r.push_token));
        if (aud.push_enabled === 'no') filtered = filtered.filter(r => !(r.push_token));
        if (aud.has_redeemed === 'yes') filtered = filtered.filter(r => parseInt(r.redemption_count) > 0);
        if (aud.has_redeemed === 'no') filtered = filtered.filter(r => !(parseInt(r.redemption_count) > 0));
        recipients = recipients.concat(filtered.map(r => r.email));
      }

      // Add external emails
      const extEmails = (external_emails || []).filter(e => e && e.includes('@'));
      recipients = recipients.concat(extEmails);

      // Deduplicate
      recipients = [...new Set(recipients.map(e => e.toLowerCase()))];

      if (recipients.length === 0) {
        return send(res, 400, { success: false, error: 'No recipients found with the current filters' });
      }

      // Build Brevo attachments
      const brevoAttachments = (attachments || []).map(a => ({
        name: a.name,
        content: a.content // base64
      }));

      // Determine if this is a scheduled send
      const isScheduled = scheduled_at && new Date(scheduled_at) > new Date();

      // Send in batches of 50
      let sentCount = 0;
      let failCount = 0;
      const batchSize = 50;
      for (let i = 0; i < recipients.length; i += batchSize) {
        const batch = recipients.slice(i, i + batchSize);
        try {
          const sendSmtpEmail = new SibApiV3Sdk.SendSmtpEmail();
          sendSmtpEmail.sender = senderObj;
          // Use BCC for privacy — send to self, BCC all recipients
          sendSmtpEmail.to = [senderObj];
          sendSmtpEmail.bcc = batch.map(email => ({ email }));
          sendSmtpEmail.subject = subject;
          sendSmtpEmail.htmlContent = html_body;
          if (brevoAttachments.length > 0) sendSmtpEmail.attachment = brevoAttachments;
          // Schedule for later if scheduled_at is provided
          if (isScheduled) {
            sendSmtpEmail.scheduledAt = new Date(scheduled_at).toISOString();
          }
          await emailApi.sendTransacEmail(sendSmtpEmail);
          sentCount += batch.length;
        } catch (sendErr) {
          console.error('Brevo batch send error:', sendErr.message || sendErr);
          failCount += batch.length;
        }
      }

      // Log to AnnouncementLog
      const logStatus = isScheduled ? 'scheduled' : (failCount > 0 && sentCount === 0 ? 'failed' : failCount > 0 ? 'partial' : 'sent');
      try {
        await sql`
          INSERT INTO "AnnouncementLog" (subject, sender, audience_type, filters, recipient_count, external_count, has_attachments, status, html_body, scheduled_at)
          VALUES (
            ${subject},
            ${senderObj.email},
            ${aud.type || 'custom'},
            ${JSON.stringify(aud)}::jsonb,
            ${sentCount},
            ${extEmails.length},
            ${brevoAttachments.length > 0},
            ${logStatus},
            ${html_body},
            ${scheduled_at ? new Date(scheduled_at) : null}
          )
        `;
      } catch (logErr) {
        console.error('AnnouncementLog insert error:', logErr.message || logErr);
      }

      return send(res, 200, {
        success: true,
        data: { sent: sentCount, failed: failCount, total_recipients: recipients.length, scheduled: isScheduled }
      });
    }

    // ── POST /api/v1/admin/send-individual-email ──────────────────
    // Individual outreach: sends directly TO the recipient (no BCC).
    // Uses a clean plain-text-style email — no branded banner.
    if (method === 'POST' && url.endsWith('/admin/send-individual-email')) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const data = req.body || {};
      const { recipient_email, subject, body, sender } = data;

      if (!recipient_email || !recipient_email.includes('@')) {
        return send(res, 400, { success: false, error: 'Valid recipient email is required' });
      }
      if (!subject) return send(res, 400, { success: false, error: 'Subject is required' });
      if (!body) return send(res, 400, { success: false, error: 'Body is required' });

      const BREVO_KEY = process.env.BREVO_API_KEY;
      if (!BREVO_KEY) return send(res, 500, { success: false, error: 'Brevo API key not configured' });

      const SibApiV3Sdk = require('sib-api-v3-sdk');
      const brevoClient = SibApiV3Sdk.ApiClient.instance;
      brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
      const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();

      // Use personal sender name for outreach feel
      const senderMap = {
        'hello@perkfinity.net': { name: 'Nader | Perkfinity', email: 'hello@perkfinity.net' },
        'support@perkfinity.net': { name: 'Perkfinity Support', email: 'support@perkfinity.net' },
        'noreply@perkfinity.net': { name: 'Perkfinity', email: 'noreply@perkfinity.net' }
      };
      const senderObj = senderMap[sender] || senderMap['hello@perkfinity.net'];

      // Convert plain text to clean HTML paragraphs (no banner)
      const paragraphs = body.split(/\n\n+/).map(p => {
        const html = p.replace(/\n/g, '<br>');
        return `<p style="margin:0 0 16px 0;">${html}</p>`;
      }).join('');

      const html_body = `<div style="font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;max-width:600px;margin:0 auto;color:#333;line-height:1.7;font-size:15px;padding:24px 0;">${paragraphs}<p style="margin-top:32px;padding-top:16px;border-top:1px solid #eee;font-size:12px;color:#aaa;margin-bottom:0;">Perkfinity &middot; <a href="https://perkfinity.net" style="color:#5b3fa5;text-decoration:none;">perkfinity.net</a><br>If you'd prefer not to receive outreach from us, simply reply "Unsubscribe".</p></div>`;

      try {
        const sendSmtpEmail = new SibApiV3Sdk.SendSmtpEmail();
        sendSmtpEmail.sender = senderObj;
        sendSmtpEmail.to = [{ email: recipient_email }];
        sendSmtpEmail.subject = subject;
        sendSmtpEmail.htmlContent = html_body;
        await emailApi.sendTransacEmail(sendSmtpEmail);

        // Log to AnnouncementLog
        try {
          await sql`
            INSERT INTO "AnnouncementLog" (subject, sender, audience_type, filters, recipient_count, external_count, has_attachments, status, html_body)
            VALUES (
              ${subject},
              ${senderObj.email},
              'individual',
              ${JSON.stringify({ recipient: recipient_email })}::jsonb,
              1,
              1,
              false,
              'sent',
              ${html_body}
            )
          `;
        } catch (logErr) {
          console.error('AnnouncementLog insert error (individual):', logErr.message);
        }

        return send(res, 200, { success: true, data: { sent: 1, recipient: recipient_email } });
      } catch (sendErr) {
        console.error('Individual email send error:', sendErr.message || sendErr);
        return send(res, 500, { success: false, error: sendErr.message || 'Send failed' });
      }
    }

    // ── GET /api/v1/admin/announcement-history ────────────────────
    if (method === 'GET' && url.endsWith('/admin/announcement-history')) {
      // Auto-update scheduled entries whose time has passed → mark as sent
      try {
        await sql`
          UPDATE "AnnouncementLog"
          SET status = 'sent'
          WHERE status = 'scheduled' AND scheduled_at IS NOT NULL AND scheduled_at <= NOW()
        `;
      } catch (upErr) {
        console.error('Auto-update scheduled status error:', upErr.message);
      }

      const history = await sql`
        SELECT * FROM "AnnouncementLog"
        ORDER BY created_at DESC
        LIMIT 100
      `;
      return send(res, 200, { success: true, data: history });
    }

    // ── POST /api/v1/admin/access-codes — Generate a code
    if (method === 'POST' && url.endsWith('/admin/access-codes')) {
      if (!verifyAdminAuth(req)) {
        return send(res, 401, { success: false, error: 'Unauthorized' });
      }
      const data = req.body || {};
      const label = (data.label || '').trim() || null;
      const type = data.type === 'extended_trial' ? 'extended_trial' : (data.type === 'pouf' ? 'pouf' : 'free_for_life');

      let code;
      const chars = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
      if (type === 'extended_trial') {
        if (!data.custom_code || !data.member_limit || !data.expires_in_days) {
          return send(res, 400, { success: false, error: 'Missing required promo code fields' });
        }
        code = data.custom_code.trim().toUpperCase().replace(/\s+/g, '-');
      } else {
        const seg = (n) => Array.from({ length: n }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
        const prefix = type === 'pouf' ? 'POUF' : 'FREE';
        code = `${prefix}-${seg(4)}-${seg(4)}`;
      }

      const days = parseInt(data.expires_in_days) || 30;
      const expiresAt = new Date(Date.now() + days * 24 * 60 * 60 * 1000);
      const memberLimit = type === 'extended_trial' ? parseInt(data.member_limit) : null;

      const existing = await sql`SELECT id, expires_at FROM "AdminAccessCode" WHERE code = ${code}`;
      if (existing.length > 0) {
        const isExpired = new Date() > new Date(existing[0].expires_at);
        if (!isExpired) {
          return send(res, 400, { success: false, error: 'This promo code already exists and is still active. Please choose a different code name.' });
        }
        
        // Code is expired, so we can overwrite/regenerate it
        await sql`
          UPDATE "AdminAccessCode" 
          SET label = ${label}, 
              type = ${type}, 
              member_limit = ${memberLimit}, 
              expires_at = ${expiresAt},
              use_count = 0,
              used = false,
              created_at = NOW()
          WHERE id = ${existing[0].id}
        `;
        return send(res, 201, { success: true, data: { code, label, type, member_limit: memberLimit, expires_at: expiresAt } });
      }

      await sql`
        INSERT INTO "AdminAccessCode" (code, label, type, member_limit, expires_at)
        VALUES (${code}, ${label}, ${type}, ${memberLimit}, ${expiresAt})
      `;
      return send(res, 201, { success: true, data: { code, label, type, member_limit: memberLimit, expires_at: expiresAt } });
    }

    // ── GET /api/v1/admin/access-codes — List all codes
    if (method === 'GET' && url.endsWith('/admin/access-codes')) {
      if (!verifyAdminAuth(req)) {
        return send(res, 401, { success: false, error: 'Unauthorized' });
      }
      const now = new Date();
      const codes = await sql`
        SELECT ac.id, ac.code, ac.label, ac.type, ac.member_limit, ac.used, ac.used_by, ac.used_at, ac.expires_at, ac.created_at, ac.use_count,
               m.business_name as used_by_name
        FROM "AdminAccessCode" ac
        LEFT JOIN "Merchant" m ON m.id = ac.used_by
        ORDER BY ac.created_at DESC
      `;
      const enriched = codes.map(c => {
        let st = 'available';
        const expired = new Date(c.expires_at) < now;

        if (c.type === 'free_for_life' || c.type === 'pouf' || !c.type) {
          if (c.used) st = 'used';
          else if (expired) st = 'expired';
        } else {
          // extended_trial codes are infinite use until they expire
          if (expired) st = 'expired';
        }

        return { ...c, status: st };
      });
      return send(res, 200, { success: true, data: { codes: enriched } });
    }

    // ── GET /api/v1/admin/access-codes/validate — Public promo code check ──
    // Called from apply.html to give real-time feedback before submission.
    // No admin auth required — only looks up extended_trial codes (online brands only).
    if (method === 'GET' && url.startsWith('/api/v1/admin/access-codes/validate')) {
      const qs = (req.url || '').split('?')[1] || '';
      const code = new URLSearchParams(qs).get('code') || '';
      if (!code.trim()) return send(res, 400, { success: false, error: 'Code is required.' });

      const [ac] = await sql`
        SELECT id, code, type, member_limit, used, expires_at
        FROM "AdminAccessCode"
        WHERE UPPER(code) = UPPER(${code.trim()})
          AND (expires_at IS NULL OR expires_at > NOW())
        LIMIT 1
      `;
      if (!ac) return send(res, 404, { success: false, error: 'Invalid or expired promo code.' });
      // FFL codes are single-use — reject if already redeemed
      if (ac.type === 'free_for_life' && ac.used) {
        return send(res, 404, { success: false, error: 'This Free For Life code has already been used.' });
      }
      return send(res, 200, { success: true, data: { type: ac.type, member_limit: ac.member_limit, code: ac.code } });
    }

    // ── GET /api/v1/admin/access-codes/:id/merchants — Merchants who used a promo code
    const acMerchantsMatch = url.match(/\/api\/v1\/admin\/access-codes\/([^/]+)\/merchants$/);
    if (method === 'GET' && acMerchantsMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const codeId = acMerchantsMatch[1];
      const [codeRow] = await sql`SELECT code FROM "AdminAccessCode" WHERE id = ${codeId} LIMIT 1`;
      if (!codeRow) return send(res, 404, { success: false, error: 'Code not found' });
      const merchants = await sql`
        SELECT m.id, m.business_name, m.subscription_tier, m.billing_status, m.business_presence,
          m.created_at, mu.email as contact_email,
          ml.city as location_city, ml.postal_code as location_zip,
          (SELECT COUNT(*) FROM "MerchantMember" mm WHERE mm.merchant_id = m.id) as member_count,
          m.stripe_subscription_id, m.stripe_payment_method_id, m.member_limit, m.billing_cycle
        FROM "Merchant" m
        LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
        LEFT JOIN "MerchantLocation" ml ON ml.merchant_id = m.id AND ml.is_active = true
        WHERE m.promo_code = ${codeRow.code}
        ORDER BY m.created_at DESC
      `;
      return send(res, 200, { success: true, data: merchants });
    }

    // ── PUT /api/v1/admin/access-codes/:id/expire — Manually expire a code
    const expireCodeMatch = url.match(/\/api\/v1\/admin\/access-codes\/([^/]+)\/expire$/);
    if (method === 'PUT' && expireCodeMatch) {
      if (!verifyAdminAuth(req)) {
        return send(res, 401, { success: false, error: 'Unauthorized' });
      }
      const codeId = expireCodeMatch[1];
      const [code] = await sql`SELECT id, code FROM "AdminAccessCode" WHERE id = ${codeId} LIMIT 1`;
      if (!code) return send(res, 404, { success: false, error: 'Access code not found' });

      await sql`UPDATE "AdminAccessCode" SET expires_at = NOW() WHERE id = ${codeId}`;
      return send(res, 200, { success: true, message: `Code ${code.code} has been expired.` });
    }

    // ══════════════════════════════════════════════════════════════
    // ONLINE BRAND APPLICATION ADMIN ENDPOINTS
    // ══════════════════════════════════════════════════════════════

    // ── GET /api/v1/admin/online-applications ─────────────────────
    if (method === 'GET' && url.startsWith('/api/v1/admin/online-applications') && !url.endsWith('/history')) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const qs = (req.url || '').split('?')[1] || '';
      const statusFilter = new URLSearchParams(qs).get('status') || 'all';

      let applications;
      if (statusFilter === 'all') {
        applications = await sql`
          SELECT m.id, m.business_name, m.contact_name, m.phone, m.website, m.business_category,
                 m.business_presence, m.subscription_tier, m.application_status, m.application_notes,
                 m.billing_status, m.stripe_subscription_id,
                 m.billing_starts_at_member_count, m.stripe_customer_id, m.stripe_payment_method_id,
                 m.welcome_offer_text, m.welcome_promo_code, m.promo_code, m.logo_url, m.created_at,
                 m.member_limit, m.billing_cycle,
                 mu.email as contact_email,
                 (SELECT COUNT(*)::int FROM "MerchantMember" mm WHERE mm.merchant_id = m.id) as member_count
          FROM "Merchant" m
          LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
          WHERE m.business_presence IN ('online', 'hybrid') AND m.application_status IS NOT NULL
          ORDER BY m.created_at DESC
        `;
      } else {
        applications = await sql`
          SELECT m.id, m.business_name, m.contact_name, m.phone, m.website, m.business_category,
                 m.business_presence, m.subscription_tier, m.application_status, m.application_notes,
                 m.billing_status, m.stripe_subscription_id,
                 m.billing_starts_at_member_count, m.stripe_customer_id, m.stripe_payment_method_id,
                 m.welcome_offer_text, m.welcome_promo_code, m.promo_code, m.logo_url, m.created_at,
                 m.member_limit, m.billing_cycle,
                 mu.email as contact_email,
                 (SELECT COUNT(*)::int FROM "MerchantMember" mm WHERE mm.merchant_id = m.id) as member_count
          FROM "Merchant" m
          LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
          WHERE m.business_presence IN ('online', 'hybrid')
            AND m.application_status = ${statusFilter}
          ORDER BY m.created_at DESC
        `;
      }
      const pendingCount = applications.filter(a => a.application_status === 'pending').length;
      return send(res, 200, { success: true, data: applications, pending_count: pendingCount });
    }

    // ── PUT /api/v1/admin/online-applications/:id/approve ─────────
    const approveAppMatch = url.match(/\/api\/v1\/admin\/online-applications\/([a-zA-Z0-9_-]+)\/approve$/);
    if (method === 'PUT' && approveAppMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = approveAppMatch[1];
      const [merchant] = await sql`
        SELECT m.*, mu.email as contact_email
        FROM "Merchant" m
        LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
        WHERE m.id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchant.application_status === 'approved') return send(res, 409, { success: false, error: 'Already approved' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      let stripeSubscriptionId = null;
      let stripeCurrentPeriodEnd = null;
      let billingWarning = null;
      let isPouf = false;

      if (merchant.promo_code) {
        const [ac] = await sql`SELECT type FROM "AdminAccessCode" WHERE code = ${merchant.promo_code} LIMIT 1`;
        if (ac && ac.type === 'pouf') isPouf = true;
      }

      // Only create subscription/invoice immediately if no promo billing delay
      if (!merchant.billing_starts_at_member_count && merchant.subscription_tier !== 'free_for_life') {
        // Check prerequisites and surface a warning if anything is missing
        if (!merchant.stripe_customer_id || !merchant.stripe_payment_method_id) {
          billingWarning = !merchant.stripe_customer_id
            ? 'No Stripe customer ID on record. The merchant may not have completed payment setup.'
            : 'No payment method on record. The merchant did not complete their billing setup.';
        } else if (!STRIPE_KEY) {
          billingWarning = 'STRIPE_SECRET_KEY environment variable is not configured on the server.';
        } else {
          try {
            const stripeClient = Stripe(STRIPE_KEY);
            const priceId = getPriceId(merchant.subscription_tier, isPouf ? 'annual' : (merchant.billing_cycle || 'monthly'));
            if (!priceId) {
              billingWarning = `No Stripe price ID configured for tier '${merchant.subscription_tier}'. Check STRIPE_*_PRICE_ID env vars.`;
            } else {
              if (isPouf) {
                // POUF: One-time charge via Invoice, no subscription
                const priceObj = await stripeClient.prices.retrieve(priceId);
                await stripeClient.invoiceItems.create({
                  customer: merchant.stripe_customer_id,
                  amount: priceObj.unit_amount,
                  currency: priceObj.currency,
                  description: 'Lifetime Access - ' + merchant.subscription_tier
                });
                const invoice = await stripeClient.invoices.create({
                  customer: merchant.stripe_customer_id,
                  pending_invoice_items_behavior: 'include',
                  auto_advance: true,
                  metadata: { merchant_id: merchantId, trigger: 'admin_approval_pouf' }
                });
                await stripeClient.invoices.pay(invoice.id);
                // Mark promo code as used
                await sql`UPDATE "AdminAccessCode" SET used = true, used_by = ${merchantId}, used_at = NOW() WHERE code = ${merchant.promo_code}`;

                // Record the one-time POUF payment in the Invoice table
                await sql`
                  INSERT INTO "Invoice" (id, merchant_id, stripe_invoice_id, amount_cents, currency, status, period_start, period_end, paid_at, created_at, revenue_type)
                  VALUES (
                    gen_random_uuid()::text,
                    ${merchantId},
                    ${invoice.id},
                    ${priceObj.unit_amount},
                    ${priceObj.currency},
                    'paid',
                    NOW(),
                    NOW(),
                    NOW(),
                    NOW(),
                    'pouf'
                  )
                  ON CONFLICT (stripe_invoice_id) DO UPDATE SET status = 'paid', paid_at = NOW(), revenue_type = 'pouf'
                `;

                // Start commission attribution
                await sql`
                  UPDATE "ContractorMerchantAttribution"
                  SET commission_start_date = NOW(), updated_at = NOW()
                  WHERE merchant_id = ${merchantId} AND commission_start_date IS NULL
                `;
              } else {
                // Standard: Recurring Subscription
                const subscription = await stripeClient.subscriptions.create({
                  customer: merchant.stripe_customer_id,
                  items: [{ price: priceId }],
                  default_payment_method: merchant.stripe_payment_method_id,
                  metadata: { merchant_id: merchantId, trigger: 'admin_approval', billing_cycle: merchant.billing_cycle || 'monthly' }
                });
                stripeSubscriptionId = subscription.id;
                stripeCurrentPeriodEnd = subscription.current_period_end || subscription.items?.data?.[0]?.current_period_end;
              }
            }
          } catch (stripeErr) {
            console.error(`Stripe billing on approval failed for ${merchantId}:`, stripeErr.message);
            billingWarning = `Stripe error: ${stripeErr.message}`;
          }
        }
      }

      const finalBillingCycle = isPouf ? 'lifetime' : merchant.billing_cycle;

      await sql`
        UPDATE "Merchant"
        SET application_status = 'approved',
            onboarding_complete = true,
            billing_status = ${stripeSubscriptionId || isPouf ? 'active' : 'trial'},
            billing_cycle = COALESCE(${finalBillingCycle}, billing_cycle),
            stripe_subscription_id = ${stripeSubscriptionId},
            subscription_started_at = ${(stripeSubscriptionId || isPouf) ? new Date() : null},
            next_billing_date = ${stripeCurrentPeriodEnd ? new Date(stripeCurrentPeriodEnd * 1000) : (stripeSubscriptionId ? new Date(Date.now() + 30 * 24 * 60 * 60 * 1000) : null)},
            updated_at = NOW()
        WHERE id = ${merchantId}
      `;

      // Send approval email
      try {
        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY && merchant.contact_email) {
          const brevoClient = SibApiV3Sdk.ApiClient.instance;
          brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
          const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
          const emailObj = new SibApiV3Sdk.SendSmtpEmail();
          emailObj.sender = { name: 'Perkfinity', email: 'support@perkfinity.net' };
          emailObj.to = [{ email: merchant.contact_email }];
          emailObj.subject = `🎉 Welcome to Perkfinity, ${merchant.business_name}! Your application is approved.`;
          const billingNote = merchant.billing_starts_at_member_count
            ? `Your card will be charged once you reach <strong>${merchant.billing_starts_at_member_count} members</strong> via your promo arrangement.`
            : stripeSubscriptionId
              ? `Your subscription is now active and your card on file will be billed monthly.`
              : `Your account is now active. Our team will be in touch regarding your billing setup.`;
          emailObj.htmlContent = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;"><div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;"><div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div></div><div style="padding:28px 24px;"><div style="font-size:20px;font-weight:700;color:#5b3fa5;margin-bottom:16px;">🎉 You're approved, ${merchant.business_name}!</div><p style="font-size:15px;color:#555;line-height:1.6;">Your brand is now live on Perkfinity. Members can discover your offers at <a href="https://perkfinity.net/codes">perkfinity.net/codes</a>.</p><p style="font-size:14px;color:#555;">${billingNote}</p><p style="font-size:15px;color:#555;">Log in to your dashboard at <a href="https://perkfinity.net/login.html">perkfinity.net/login.html</a> to manage campaigns and view your member growth.</p></div></div>`;
          await emailApi.sendTransacEmail(emailObj);
        }
      } catch (emailErr) { console.error('Approval email failed:', emailErr.message); }

      // Record application history
      try {
        await sql`CREATE TABLE IF NOT EXISTS "OnlineApplicationHistory" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          merchant_id TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          changed_at TIMESTAMPTZ DEFAULT NOW()
        )`;
        await sql`INSERT INTO "OnlineApplicationHistory" (id, merchant_id, status, note, changed_at)
          VALUES (gen_random_uuid()::text, ${merchantId}, 'approved', ${billingWarning || null}, NOW())`;
      } catch (histErr) { console.error('History record failed (approve):', histErr.message); }

      return send(res, 200, {
        success: true,
        message: 'Application approved',
        stripe_subscription_id: stripeSubscriptionId,
        billing_warning: billingWarning || null,
        stripe_customer_id: merchant.stripe_customer_id || null
      });
    }

    // ── PUT /api/v1/admin/online-applications/:id/approve-demo ────
    const approveDemoMatch = url.match(/\/api\/v1\/admin\/online-applications\/([a-zA-Z0-9_-]+)\/approve-demo$/);
    if (method === 'PUT' && approveDemoMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = approveDemoMatch[1];
      
      const [merchant] = await sql`
        SELECT m.*, mu.email as contact_email
        FROM "Merchant" m
        LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
        WHERE m.id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchant.application_status === 'approved') return send(res, 409, { success: false, error: 'Already approved' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (STRIPE_KEY && merchant.stripe_payment_method_id) {
        try {
          const stripeClient = Stripe(STRIPE_KEY);
          await stripeClient.paymentMethods.detach(merchant.stripe_payment_method_id);
          console.log(`[Demo Setup] Detached payment method ${merchant.stripe_payment_method_id} for demo account ${merchantId}`);
        } catch (err) {
          console.error(`[Demo Setup] Failed to detach payment method for demo account ${merchantId}:`, err.message);
        }
      }

      await sql`
        UPDATE "Merchant"
        SET application_status = 'approved',
            onboarding_complete = true,
            billing_status = 'active',
            stripe_subscription_id = null,
            stripe_payment_method_id = null,
            billing_starts_at_member_count = null,
            member_limit = null,
            billing_cycle = 'monthly',
            updated_at = NOW()
        WHERE id = ${merchantId}
      `;

      try {
        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY && merchant.contact_email) {
          const brevoClient = SibApiV3Sdk.ApiClient.instance;
          brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
          const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
          const emailObj = new SibApiV3Sdk.SendSmtpEmail();
          emailObj.sender = { name: 'Perkfinity', email: 'support@perkfinity.net' };
          emailObj.to = [{ email: merchant.contact_email }];
          emailObj.subject = `🎉 Welcome to Perkfinity, ${merchant.business_name}! Your Demo Account is ready.`;
          emailObj.htmlContent = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;"><div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;"><div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div></div><div style="padding:28px 24px;"><div style="font-size:20px;font-weight:700;color:#5b3fa5;margin-bottom:16px;">🎉 Demo Account Activated, ${merchant.business_name}!</div><p style="font-size:15px;color:#555;line-height:1.6;">Your demo environment is now active. Your personal credit card has been detached and will never be charged.</p><p style="font-size:15px;color:#555;">Log in to your dashboard at <a href="https://perkfinity.net/login.html">perkfinity.net/login.html</a> to explore.</p></div></div>`;
          await emailApi.sendTransacEmail(emailObj);
        }
      } catch (emailErr) { console.error('Demo approval email failed:', emailErr.message); }

      return send(res, 200, { success: true, message: 'Demo account approved securely' });
    }

    // ── PUT /api/v1/admin/online-applications/:id/decline ─────────
    const declineAppMatch = url.match(/\/api\/v1\/admin\/online-applications\/([a-zA-Z0-9_-]+)\/decline$/);
    if (method === 'PUT' && declineAppMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = declineAppMatch[1];
      const { notes = '' } = req.body || {};
      const [merchant] = await sql`
        SELECT m.*, mu.email as contact_email
        FROM "Merchant" m
        LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
        WHERE m.id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (!['pending', 'followup'].includes(merchant.application_status)) {
        return send(res, 409, { success: false, error: `Cannot decline — application is already ${merchant.application_status}` });
      }

      // Cancel Stripe Setup Intent (best-effort)
      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (STRIPE_KEY && merchant.stripe_customer_id) {
        try {
          const stripeClient = Stripe(STRIPE_KEY);
          await stripeClient.customers.del(merchant.stripe_customer_id);
        } catch (e) { /* non-fatal */ }
      }

      // Ensure declined_at column exists (idempotent migration)
      await sql`ALTER TABLE "Merchant" ADD COLUMN IF NOT EXISTS declined_at TIMESTAMPTZ`;

      await sql`
        UPDATE "Merchant"
        SET application_status = 'declined',
            application_notes = ${notes},
            stripe_customer_id = NULL,
            stripe_payment_method_id = NULL,
            declined_at = NOW(),
            updated_at = NOW()
        WHERE id = ${merchantId}
      `;

      // Send decline email
      try {
        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY && merchant.contact_email) {
          const brevoClient = SibApiV3Sdk.ApiClient.instance;
          brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
          const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
          const emailObj = new SibApiV3Sdk.SendSmtpEmail();
          emailObj.sender = { name: 'Perkfinity', email: 'support@perkfinity.net' };
          emailObj.to = [{ email: merchant.contact_email }];
          emailObj.subject = `Update on your Perkfinity application — ${merchant.business_name}`;
          emailObj.htmlContent = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;"><div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;"><div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div></div><div style="padding:28px 24px;"><p style="font-size:16px;color:#333;line-height:1.6;">Hi ${merchant.business_name},</p><p style="font-size:15px;color:#555;line-height:1.6;">Thank you for your interest in joining Perkfinity. After reviewing your application, we weren't able to move forward at this time.${notes ? '</p><p style="font-size:15px;color:#555;line-height:1.6;"><strong>Reason:</strong> ' + notes : ''}</p><p style="font-size:15px;color:#555;line-height:1.6;">We want to work with as many great businesses as possible — if you'd like to discuss this decision or address any concerns, we'd genuinely love to hear from you. Reach out at <a href="mailto:support@perkfinity.net" style="color:#5b3fa5;font-weight:700;">support@perkfinity.net</a> and we can revisit your application together.</p><p style="font-size:15px;color:#555;line-height:1.6;">Your payment information has been fully removed and you will not be charged.</p><p style="font-size:15px;color:#555;line-height:1.6;">— The Perkfinity Team</p></div></div>`;
          await emailApi.sendTransacEmail(emailObj);
        }
      } catch (emailErr) { console.error('Decline email failed:', emailErr.message); }

      // Record application history
      try {
        await sql`CREATE TABLE IF NOT EXISTS "OnlineApplicationHistory" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          merchant_id TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          changed_at TIMESTAMPTZ DEFAULT NOW()
        )`;
        await sql`INSERT INTO "OnlineApplicationHistory" (id, merchant_id, status, note, changed_at)
          VALUES (gen_random_uuid()::text, ${merchantId}, 'declined', ${notes || null}, NOW())`;
      } catch (histErr) { console.error('History record failed (decline):', histErr.message); }

      return send(res, 200, { success: true, message: 'Application declined' });
    }

    // ── PUT /api/v1/admin/online-applications/:id/followup ─────────
    const followupAppMatch = url.match(/\/api\/v1\/admin\/online-applications\/([a-zA-Z0-9_-]+)\/followup$/);
    if (method === 'PUT' && followupAppMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = followupAppMatch[1];
      const { message = '' } = req.body || {};
      const [merchant] = await sql`
        SELECT m.*, mu.email as contact_email
        FROM "Merchant" m
        LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id AND mu.role = 'owner'
        WHERE m.id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (!['pending', 'followup'].includes(merchant.application_status)) {
        return send(res, 409, { success: false, error: `Cannot request follow-up — application is already ${merchant.application_status}` });
      }

      await sql`
        UPDATE "Merchant"
        SET application_status = 'followup', updated_at = NOW()
        WHERE id = ${merchantId}
      `;

      // Send follow-up email to applicant
      try {
        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY && merchant.contact_email) {
          const brevoClient = SibApiV3Sdk.ApiClient.instance;
          brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
          const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
          const emailObj = new SibApiV3Sdk.SendSmtpEmail();
          emailObj.sender = { name: 'Perkfinity', email: 'support@perkfinity.net' };
          emailObj.to = [{ email: merchant.contact_email }];
          emailObj.subject = `We'd love to learn more — ${merchant.business_name}`;
          emailObj.htmlContent = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto;">
            <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:28px 24px;text-align:center;">
              <div style="color:#fff;font-size:24px;font-weight:800;">Perkfinity</div>
            </div>
            <div style="padding:28px 24px;">
              <p style="font-size:16px;color:#333;line-height:1.6;">Hi ${merchant.business_name},</p>
              <p style="font-size:15px;color:#555;line-height:1.6;">Thank you for applying to join Perkfinity! We're reviewing your application and would love to connect before we finalize our decision.</p>
              ${message ? `<div style="background:#f5f3ff;border-left:4px solid #7c5cbf;border-radius:4px;padding:14px 16px;margin:16px 0;"><p style="font-size:14px;color:#4c1d95;margin:0;line-height:1.6;"><strong>Message from our team:</strong><br>${message}</p></div>` : ''}
              <p style="font-size:15px;color:#555;line-height:1.6;">Please reach out to us at <a href="mailto:support@perkfinity.net" style="color:#5b3fa5;font-weight:700;">support@perkfinity.net</a> at your earliest convenience — we're looking forward to speaking with you.</p>
              <p style="font-size:15px;color:#555;line-height:1.6;">— The Perkfinity Team</p>
            </div>
          </div>`;
          await emailApi.sendTransacEmail(emailObj);
        }
      } catch (emailErr) { console.error('Follow-up email failed:', emailErr.message); }

      // Record application history
      try {
        await sql`CREATE TABLE IF NOT EXISTS "OnlineApplicationHistory" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          merchant_id TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          changed_at TIMESTAMPTZ DEFAULT NOW()
        )`;
        await sql`INSERT INTO "OnlineApplicationHistory" (id, merchant_id, status, note, changed_at)
          VALUES (gen_random_uuid()::text, ${merchantId}, 'followup', ${message || null}, NOW())`;
      } catch (histErr) { console.error('History record failed (followup):', histErr.message); }

      return send(res, 200, { success: true, message: 'Follow-up email sent' });
    }

    // ── GET /api/v1/admin/online-applications/:id/history ──────────
    const appHistoryMatch = url.match(/\/api\/v1\/admin\/online-applications\/([a-zA-Z0-9_-]+)\/history$/);
    if (method === 'GET' && appHistoryMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const merchantId = appHistoryMatch[1];
      await sql`CREATE TABLE IF NOT EXISTS "OnlineApplicationHistory" (
        id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        merchant_id TEXT NOT NULL,
        status TEXT NOT NULL,
        note TEXT,
        changed_at TIMESTAMPTZ DEFAULT NOW()
      )`;
      const history = await sql`
        SELECT * FROM "OnlineApplicationHistory"
        WHERE merchant_id = ${merchantId}
        ORDER BY changed_at ASC
      `;
      return send(res, 200, { success: true, data: history });
    }

    // ── POST /api/v1/stripe/apply-setup-intent ────────────────────
    // Creates a bare Stripe Setup Intent (no customer) for apply.html Step 6
    if (method === 'POST' && url.endsWith('/stripe/apply-setup-intent')) {
      // Origin allowlist: only accept requests from our own domain or local dev
      const reqOrigin = req.headers.origin || req.headers.referer || '';
      const allowedOrigins = ['perkfinity.net', 'localhost', '127.0.0.1'];
      if (!allowedOrigins.some(h => reqOrigin.includes(h))) {
        return send(res, 403, { success: false, error: 'Forbidden' });
      }
      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);
      const setupIntent = await stripeClient.setupIntents.create({
        usage: 'off_session',
        metadata: { purpose: 'online_brand_application' }
      });
      return send(res, 200, { success: true, client_secret: setupIntent.client_secret });
    }

    // ══════════════════════════════════════════════════════════════
    // ADMIN EMAIL
    // ══════════════════════════════════════════════════════════════

    // ── POST /api/v1/admin/send-email — Admin bulk email via Brevo
    if (method === 'POST' && url.endsWith('/admin/send-email')) {
      if (!verifyAdminAuth(req)) {
        return send(res, 401, { success: false, error: 'Unauthorized' });
      }
      const data = req.body || {};
      if (!data.merchant_ids || !data.subject || !data.body) {
        return send(res, 400, { success: false, error: 'merchant_ids, subject, and body are required' });
      }
      const BREVO_KEY = process.env.BREVO_API_KEY;
      if (!BREVO_KEY) return send(res, 500, { success: false, error: 'Email not configured' });

      // Fetch emails for all specified merchant IDs
      const users = await sql`
        SELECT mu.email, m.business_name
        FROM "MerchantUser" mu
        JOIN "Merchant" m ON m.id = mu.merchant_id
        WHERE m.id = ANY(${data.merchant_ids})
          AND mu.role = 'owner'
      `;
      if (!users.length) return send(res, 404, { success: false, error: 'No merchants found' });

      const brevoClient = SibApiV3Sdk.ApiClient.instance;
      brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
      const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
      let sent = 0, failed = 0;
      for (const u of users) {
        try {
          const email = new SibApiV3Sdk.SendSmtpEmail();
          email.sender = { name: 'Perkfinity', email: 'noreply@perkfinity.net' };
          email.to = [{ email: u.email, name: u.business_name }];
          email.subject = data.subject;
          email.htmlContent = data.body;
          await emailApi.sendTransacEmail(email);
          sent++;
        } catch (e) {
          console.error(`Failed to send to ${u.email}:`, e.message);
          failed++;
        }
      }
      return send(res, 200, { success: true, data: { sent, failed, total: users.length } });
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

    // ── POST /api/v1/stripe/confirm-setup ─────────────────────────
    // Called immediately after confirmCardSetup() succeeds on the frontend.
    // For Trial: saves card only — billing starts when member limit is reached.
    // For Tier 1: saves card AND creates subscription immediately (replaces Stripe Checkout redirect).
    if (method === 'POST' && url.endsWith('/stripe/confirm-setup')) {
      const data = req.body || {};
      const { merchant_id, payment_method_id, tier, billing_cycle } = data;
      if (!merchant_id || !payment_method_id) {
        return send(res, 400, { success: false, error: 'merchant_id and payment_method_id are required' });
      }

      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchant_id) return send(res, 403, { success: false, error: 'Forbidden' });

      const finalCycle = billing_cycle === 'annual' ? 'annual' : 'monthly';

      if (tier === 'tier1' || (tier && tier.startsWith('online_'))) {
        // Paid plans: save card AND create subscription immediately
        const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
        const stripeClient = Stripe(STRIPE_KEY);

        const [merchant] = await sql`SELECT stripe_customer_id, promo_code FROM "Merchant" WHERE id = ${merchant_id} LIMIT 1`;
        if (!merchant?.stripe_customer_id) return send(res, 400, { success: false, error: 'No Stripe customer found' });

        // Check for POUF code
        let isPouf = false;
        if (merchant.promo_code) {
          const [ac] = await sql`SELECT type FROM "AdminAccessCode" WHERE code = ${merchant.promo_code} LIMIT 1`;
          if (ac && ac.type === 'pouf') {
            if (finalCycle !== 'annual') {
              return send(res, 400, { success: false, error: 'POUF Lifetime codes are only valid on Annual plans. Please select the Annual option to proceed.' });
            }
            isPouf = true;
          }
        }

        const priceId = getPriceId(tier, isPouf ? 'annual' : finalCycle);
        if (!priceId) return send(res, 500, { success: false, error: `Stripe price not configured for ${tier} (${finalCycle})` });

        // Set as default payment method on the customer
        await stripeClient.customers.update(merchant.stripe_customer_id, {
          invoice_settings: { default_payment_method: payment_method_id }
        });

        if (isPouf) {
          // POUF: One-time charge via Invoice, no subscription
          const priceObj = await stripeClient.prices.retrieve(priceId);
          await stripeClient.invoiceItems.create({
            customer: merchant.stripe_customer_id,
            amount: priceObj.unit_amount,
            currency: priceObj.currency,
            description: 'Lifetime Access - ' + tier
          });
          const invoice = await stripeClient.invoices.create({
            customer: merchant.stripe_customer_id,
            pending_invoice_items_behavior: 'include',
            auto_advance: true,
            metadata: { merchant_id, trigger: 'tier1_signup_pouf' }
          });
          await stripeClient.invoices.pay(invoice.id);
          
          await sql`UPDATE "AdminAccessCode" SET used = true, used_by = ${merchant_id}, used_at = NOW(), use_count = use_count + 1 WHERE code = ${merchant.promo_code} AND type = 'pouf'`;

          await sql`
            INSERT INTO "Invoice" (id, merchant_id, stripe_invoice_id, amount_cents, currency, status, period_start, period_end, paid_at, created_at, revenue_type)
            VALUES (gen_random_uuid()::text, ${merchant_id}, ${invoice.id}, ${priceObj.unit_amount}, ${priceObj.currency}, 'paid', NOW(), NOW(), NOW(), NOW(), 'pouf')
          `;

          await sql`
            UPDATE "Merchant"
            SET stripe_payment_method_id   = ${payment_method_id},
                billing_status             = 'active',
                subscription_tier          = ${tier},
                billing_cycle              = 'lifetime',
                subscription_started_at    = NOW(),
                updated_at                 = NOW()
            WHERE id = ${merchant_id}
          `;
        } else {
          // Create subscription — charges immediately for first month/year
          const subscription = await stripeClient.subscriptions.create({
            customer: merchant.stripe_customer_id,
            items: [{ price: priceId }],
            default_payment_method: payment_method_id,
            metadata: { merchant_id, trigger: 'tier1_signup' }
          });

          await sql`
            UPDATE "Merchant"
            SET stripe_payment_method_id   = ${payment_method_id},
                stripe_subscription_id     = ${subscription.id},
                billing_status             = 'active',
                subscription_tier          = ${tier},
                billing_cycle              = ${finalCycle},
                subscription_started_at    = NOW(),
                next_billing_date          = ${new Date((subscription.current_period_end || subscription.items?.data?.[0]?.current_period_end) * 1000)},
                updated_at                 = NOW()
            WHERE id = ${merchant_id}
          `;
        }
      } else {
        // Trial: save payment method only — billing starts when member limit is reached
        await sql`
          UPDATE "Merchant"
          SET stripe_payment_method_id = ${payment_method_id},
              billing_cycle = ${finalCycle},
              billing_status = COALESCE(billing_status, 'trial'),
              updated_at = NOW()
          WHERE id = ${merchant_id}
        `;
      }

      return send(res, 200, { success: true });
    }

    // ── POST /api/v1/stripe/create-checkout-session (Tier 1) ──────
    if (method === 'POST' && url.endsWith('/stripe/create-checkout-session')) {
      const data = req.body || {};
      const merchantId = data.merchant_id;
      if (!merchantId) return send(res, 400, { success: false, error: 'merchant_id is required' });

      // Accept billing_cycle from the frontend toggle ('monthly' | 'annual')
      const billingCycle = data.billing_cycle === 'annual' ? 'annual' : 'monthly';

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      const origin = data.origin || 'https://perkfinity.net';

      const [merchant] = await sql`SELECT id, business_name, stripe_customer_id, billing_status, stripe_subscription_id, subscription_tier, promo_code FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });

      // Guard: prevent double-subscription if merchant already has an active subscription
      if (merchant.billing_status === 'active' || merchant.stripe_subscription_id) {
        return send(res, 409, {
          success: false,
          error: 'An active subscription already exists for this account. Please contact support if you believe this is an error.'
        });
      }

      let checkoutMode = 'subscription';
      let isPouf = false;
      let sessionLineItems = [];
      let sessionInvoiceCreation = undefined;
      const tierLabel = merchant.subscription_tier === 'tier1' ? 'Connect' : 
                        merchant.subscription_tier === 'online_starter' ? 'Starter' :
                        merchant.subscription_tier === 'online_growth' ? 'Growth' :
                        merchant.subscription_tier === 'online_scale' ? 'Scale' : merchant.subscription_tier;

      if (merchant.promo_code) {
        const [ac] = await sql`SELECT type FROM "AdminAccessCode" WHERE code = ${merchant.promo_code} LIMIT 1`;
        if (ac && ac.type === 'pouf') {
          if (billingCycle !== 'annual') {
            return send(res, 400, { success: false, error: 'POUF Lifetime codes are only valid on Annual plans. Please select the Annual option to proceed.' });
          }
          isPouf = true;
          checkoutMode = 'payment';
          sessionInvoiceCreation = { enabled: true };
          
          const annualPriceId = getPriceId(merchant.subscription_tier || 'tier1', 'annual');
          if (!annualPriceId) return send(res, 500, { success: false, error: `Stripe annual price not configured for ${merchant.subscription_tier}` });
          
          const priceObj = await stripeClient.prices.retrieve(annualPriceId);
          sessionLineItems = [{
            price_data: {
              currency: priceObj.currency,
              unit_amount: priceObj.unit_amount,
              product_data: { name: 'Lifetime Access - ' + tierLabel }
            },
            quantity: 1
          }];
        }
      }

      if (!isPouf) {
        const checkoutPriceId = getPriceId(merchant.subscription_tier || 'tier1', billingCycle);
        if (!checkoutPriceId) return send(res, 500, { success: false, error: `Stripe price not configured for ${merchant.subscription_tier} (${billingCycle})` });
        sessionLineItems = [{ price: checkoutPriceId, quantity: 1 }];
      }

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

      const sessionPayload = {
        customer: customerId,
        payment_method_types: ['card'],
        line_items: sessionLineItems,
        mode: checkoutMode,
        success_url: `${origin}/signup.html?payment=success&merchant_id=${merchantId}&session_id={CHECKOUT_SESSION_ID}`,
        cancel_url: `${origin}/signup.html?payment=cancelled&merchant_id=${merchantId}`,
        metadata: { merchant_id: merchantId, billing_cycle: isPouf ? 'lifetime' : billingCycle }
      };
      if (sessionInvoiceCreation) sessionPayload.invoice_creation = sessionInvoiceCreation;

      const session = await stripeClient.checkout.sessions.create(sessionPayload);

      return send(res, 200, {
        success: true,
        data: {
          checkout_url: session.url,
          session_id: session.id
        }
      });
    }

    // ── POST /api/v1/stripe/confirm-checkout (Tier 1 return) ───────
    // Called immediately when Stripe redirects back with ?payment=success.
    // Verifies the checkout session server-side and writes billing state to DB.
    // This is the primary mechanism — webhook is a backup for any delay/failure.
    if (method === 'POST' && url.endsWith('/stripe/confirm-checkout')) {
      const data = req.body || {};
      const { merchant_id, session_id } = data;
      if (!merchant_id || !session_id) {
        return send(res, 400, { success: false, error: 'merchant_id and session_id are required' });
      }

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      // Retrieve session from Stripe — expand subscription for billing details
      let session;
      try {
        session = await stripeClient.checkout.sessions.retrieve(session_id, {
          expand: ['subscription', 'subscription.default_payment_method']
        });
      } catch (e) {
        return send(res, 400, { success: false, error: 'Could not retrieve Stripe session' });
      }

      if (session.payment_status !== 'paid') {
        return send(res, 402, { success: false, error: 'Payment not completed' });
      }

      const sub = session.subscription;
      const nextBilling = sub?.current_period_end
        ? new Date(sub.current_period_end * 1000)
        : null;
      const paymentMethodId = sub?.default_payment_method?.id
        || sub?.default_payment_method
        || null;

      // Read billing_cycle from Stripe session metadata (set when checkout was created)
      const confirmedBillingCycle = session.metadata?.billing_cycle || 'monthly';

      const [merch] = await sql`SELECT promo_code, subscription_tier FROM "Merchant" WHERE id = ${merchant_id} LIMIT 1`;

      if (confirmedBillingCycle === 'lifetime' && merch?.promo_code) {
        await sql`UPDATE "AdminAccessCode" SET used = true, used_by = ${merchant_id}, used_at = NOW(), use_count = use_count + 1 WHERE code = ${merch.promo_code} AND type = 'pouf'`;
      }

      await sql`
        UPDATE "Merchant"
        SET billing_status        = 'active',
            subscription_tier     = COALESCE(subscription_tier, 'tier1'),
            billing_cycle         = ${confirmedBillingCycle},
            stripe_customer_id    = ${session.customer},
            stripe_subscription_id = ${sub?.id || null},
            stripe_payment_method_id = ${paymentMethodId},
            next_billing_date     = ${nextBilling},
            subscription_started_at = COALESCE(subscription_started_at, NOW()),
            account_blocked       = false,
            updated_at            = NOW()
        WHERE id = ${merchant_id}
      `;

      return send(res, 200, { success: true });
    }

    // ── POST /api/v1/stripe/confirm-reactivation ───────────────────
    // Called from dashboard.html after Stripe redirects back with ?reactivated=true.
    // Verifies the checkout session and restores the merchant account,
    // preserving their existing subscription_tier (unlike confirm-checkout which hardcodes tier1).
    if (method === 'POST' && url.endsWith('/stripe/confirm-reactivation')) {
      const data = req.body || {};
      const { merchant_id, session_id } = data;
      if (!merchant_id || !session_id) {
        return send(res, 400, { success: false, error: 'merchant_id and session_id are required' });
      }

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      let session;
      try {
        session = await stripeClient.checkout.sessions.retrieve(session_id, {
          expand: ['subscription', 'subscription.default_payment_method']
        });
      } catch (e) {
        return send(res, 400, { success: false, error: 'Could not retrieve Stripe session' });
      }

      if (session.payment_status !== 'paid') {
        return send(res, 402, { success: false, error: 'Payment not completed' });
      }

      // Verify session belongs to this merchant
      if (session.metadata?.merchant_id !== merchant_id) {
        return send(res, 403, { success: false, error: 'Session does not match merchant' });
      }

      const sub = session.subscription;
      const nextBilling = sub?.current_period_end
        ? new Date(sub.current_period_end * 1000)
        : null;
      const paymentMethodId = sub?.default_payment_method?.id
        || sub?.default_payment_method
        || null;

      // Preserve existing subscription_tier — do NOT hardcode tier1
      await sql`
        UPDATE "Merchant"
        SET billing_status               = 'active',
            stripe_customer_id           = ${session.customer},
            stripe_subscription_id       = ${sub?.id || null},
            stripe_payment_method_id     = ${paymentMethodId},
            next_billing_date            = ${nextBilling},
            account_blocked              = false,
            cancelled_at                 = NULL,
            payment_failed_at            = NULL,
            payment_failure_reminder_count = NULL,
            subscription_started_at      = COALESCE(subscription_started_at, NOW()),
            updated_at                   = NOW()
        WHERE id = ${merchant_id}
      `;

      return send(res, 200, { success: true });
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

    // ── POST /api/v1/merchants/:id/cancel — Self-service cancel (Free For Life / no Stripe)
    const fflCancelMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/cancel$/);
    if (method === 'POST' && fflCancelMatch) {
      const merchantId = fflCancelMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const [merchant] = await sql`SELECT subscription_tier FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchant.subscription_tier !== 'free_for_life') {
        return send(res, 400, { success: false, error: 'This endpoint is for Free For Life accounts only. Use /billing/cancel for paid accounts.' });
      }

      await sql`
        UPDATE "Merchant"
        SET status = 'cancelled',
            billing_status = 'cancelled',
            account_blocked = true,
            cancelled_at = NOW(),
            updated_at = NOW()
        WHERE id = ${merchantId}
      `;
      await sql`UPDATE "Campaign" SET status = 'expired', updated_at = NOW() WHERE merchant_id = ${merchantId} AND status = 'active'`;
      return send(res, 200, { success: true, message: 'Your account has been cancelled. You can reactivate by contacting support.' });
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

      // Password verification — required to prevent unauthorized cancellations
      const cancelData = req.body || {};
      if (!cancelData.password) return send(res, 400, { success: false, error: 'Password is required to cancel your subscription.' });
      const [cancelUser] = await sql`SELECT id, password_hash FROM "MerchantUser" WHERE id = ${payload.userId} LIMIT 1`;
      if (!cancelUser || !(await bcrypt.compare(cancelData.password, cancelUser.password_hash))) {
        return send(res, 401, { success: false, error: 'Incorrect password. Please try again.' });
      }

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      const stripeClient = STRIPE_KEY ? Stripe(STRIPE_KEY) : null;

      const [merchant] = await sql`
        SELECT stripe_subscription_id, stripe_customer_id
        FROM "Merchant" WHERE id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });

      try {
        if (merchant.stripe_subscription_id) {
          // ── Active subscription: schedule cancellation at period end ──
          if (!stripeClient) return send(res, 500, { success: false, error: 'Stripe not configured' });
          await stripeClient.subscriptions.update(merchant.stripe_subscription_id, {
            cancel_at_period_end: true,
          });
          await sql`
            UPDATE "Merchant"
            SET billing_status = 'pending_cancellation'
            WHERE id = ${merchantId}
          `;
          return send(res, 200, { success: true, message: 'Subscription will cancel at period end' });
        } else {
          // ── Trial/promo: no active subscription — block immediately ──
          // Stripe customer and saved card are preserved for reactivation within 6 months.
          // They will be cleaned up when the account is permanently deleted.
          await sql`
            UPDATE "Merchant"
            SET account_blocked = true,
                billing_status  = 'cancelled',
                cancelled_at    = NOW(),
                updated_at      = NOW()
            WHERE id = ${merchantId}
          `;
          return send(res, 200, { success: true, message: 'Trial account blocked immediately' });
        }
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
               stripe_customer_id, stripe_subscription_id, stripe_sponsor_subscription_id, 
               stripe_web_sponsor_subscription_id, stripe_app_sponsor_subscription_id, stripe_bundle_sponsor_subscription_id,
               stripe_fullpage_sponsor_subscription_id, subscription_started_at,
               next_billing_date, member_limit, promo_code, created_at,
               payment_failed_at, payment_failure_reminder_count,
               billing_starts_at_member_count, application_status, business_presence,
               billing_cycle, is_web_sponsored, is_app_sponsored, web_sponsored_until, app_sponsored_until, promo_banner_url, cover_photo_url,
               is_fullpage_sponsored, fullpage_sponsored_until, rating_score, rating_count, rating_platform, promo_description
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
          member_limit: merchant.member_limit || 100,
          subscription_started_at: merchant.subscription_started_at,
          next_billing_date: merchant.next_billing_date,
          created_at: merchant.created_at,
          has_stripe: !!merchant.stripe_customer_id,
          has_subscription: !!merchant.stripe_subscription_id,
          has_sponsor_subscription: !!merchant.stripe_sponsor_subscription_id || !!merchant.stripe_bundle_sponsor_subscription_id || !!merchant.stripe_web_sponsor_subscription_id || !!merchant.stripe_app_sponsor_subscription_id || !!merchant.stripe_fullpage_sponsor_subscription_id,
          has_bundle_subscription: !!merchant.stripe_bundle_sponsor_subscription_id || (!!merchant.stripe_web_sponsor_subscription_id && merchant.stripe_web_sponsor_subscription_id === merchant.stripe_app_sponsor_subscription_id),
          has_web_subscription: !!merchant.stripe_web_sponsor_subscription_id,
          has_app_subscription: !!merchant.stripe_app_sponsor_subscription_id,
          has_fullpage_subscription: !!merchant.stripe_fullpage_sponsor_subscription_id,
          is_web_sponsored: merchant.is_web_sponsored && (!merchant.web_sponsored_until || new Date(merchant.web_sponsored_until) >= new Date()) ? true : false,
          is_app_sponsored: merchant.is_app_sponsored && (!merchant.app_sponsored_until || new Date(merchant.app_sponsored_until) >= new Date()) ? true : false,
          is_fullpage_sponsored: merchant.is_fullpage_sponsored && (!merchant.fullpage_sponsored_until || new Date(merchant.fullpage_sponsored_until) >= new Date()) ? true : false,
          web_sponsored_until: merchant.web_sponsored_until || null,
          app_sponsored_until: merchant.app_sponsored_until || null,
          fullpage_sponsored_until: merchant.fullpage_sponsored_until || null,
          cover_photo_url: merchant.cover_photo_url || null,
          promo_banner_url: merchant.promo_banner_url || null,
          rating_score: merchant.rating_score || null,
          rating_count: merchant.rating_count || null,
          rating_platform: merchant.rating_platform || null,
          promo_description: merchant.promo_description || null,
          payment_failed_at: merchant.payment_failed_at || null,
          billing_starts_at_member_count: merchant.billing_starts_at_member_count || null,
          application_status: merchant.application_status || null,
          business_presence: merchant.business_presence || 'physical',
          billing_cycle: merchant.billing_cycle || 'monthly',
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
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      const [merchant] = await sql`
        SELECT id, stripe_customer_id, stripe_subscription_id, account_blocked, subscription_tier, billing_cycle
        FROM "Merchant"
        WHERE id = ${merchantId}
        LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (!merchant.stripe_customer_id) {
        return send(res, 400, { success: false, error: 'No Stripe customer profile found.' });
      }
      if (merchant.stripe_subscription_id && merchant.account_blocked === false) {
        return send(res, 400, { success: false, error: 'You already have an active subscription.' });
      }

      // Resolve the correct price ID for this merchant's tier and billing cycle
      const reactivatePriceId = getPriceId(merchant.subscription_tier, merchant.billing_cycle || 'monthly') || process.env.STRIPE_TIER1_PRICE_ID;
      if (!reactivatePriceId) return send(res, 500, { success: false, error: `No Stripe price configured for tier '${merchant.subscription_tier}' (cycle: ${merchant.billing_cycle || 'monthly'})` });

      // ── Check if Stripe customer has a default payment method ──────
      let stripeCustomer;
      try {
        stripeCustomer = await stripeClient.customers.retrieve(merchant.stripe_customer_id);
      } catch (e) {
        return send(res, 400, { success: false, error: 'Could not retrieve Stripe customer profile.' });
      }
      const hasPaymentMethod =
        stripeCustomer.invoice_settings?.default_payment_method ||
        stripeCustomer.default_source;

      if (!hasPaymentMethod) {
        // No card on file — create a Checkout Session to capture a new one
        const origin = req.headers.origin || 'https://perkfinity.net';
        const session = await stripeClient.checkout.sessions.create({
          customer: merchant.stripe_customer_id,
          payment_method_types: ['card'],
          line_items: [{ price: reactivatePriceId, quantity: 1 }],
          mode: 'subscription',
          success_url: `${origin}/dashboard.html?reactivated=true&merchant_id=${merchantId}&session_id={CHECKOUT_SESSION_ID}`,
          cancel_url: `${origin}/dashboard.html?tab=billing`,
          metadata: { merchant_id: merchantId, trigger: 'reactivation', tier: merchant.subscription_tier, billing_cycle: merchant.billing_cycle || 'monthly' }
        });
        return send(res, 200, { success: false, needs_payment_method: true, checkout_url: session.url });
      }

      // ── Has payment method — create subscription directly ──────────
      try {
        const subscription = await stripeClient.subscriptions.create({
          customer: merchant.stripe_customer_id,
          items: [{ price: reactivatePriceId }],
          metadata: { merchant_id: merchantId, trigger: 'reactivation', tier: merchant.subscription_tier, billing_cycle: merchant.billing_cycle || 'monthly' }
        });

        await sql`
          UPDATE "Merchant"
          SET subscription_tier     = ${merchant.subscription_tier},
              stripe_subscription_id = ${subscription.id},
              billing_status        = 'active',
              account_blocked       = false,
              cancelled_at          = NULL,
              payment_failed_at     = NULL,
              payment_failure_reminder_count = NULL,
              subscription_started_at = NOW(),
              next_billing_date     = ${new Date((subscription.current_period_end || subscription.items?.data?.[0]?.current_period_end) * 1000)},
              updated_at            = NOW()
          WHERE id = ${merchantId}
        `;

        return send(res, 200, { success: true, message: 'Subscription reactivated successfully!' });
      } catch (stripeErr) {
        return send(res, 400, { success: false, error: `Reactivation failed: ${stripeErr.message}` });
      }
    }

    // ── POST /api/v1/merchants/:id/billing/sponsor ───────────────
    const sponsorMatch = url.match(/^\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing\/sponsor$/);
    if (method === 'POST' && sponsorMatch) {
      const merchantId = sponsorMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const origin = req.headers.origin || 'https://www.perkfinity.net';
      const { tier } = req.body || {};
      let priceId;
      if (tier === 'web') priceId = process.env.STRIPE_SPONSOR_WEB_PRICE_ID;
      else if (tier === 'app') priceId = process.env.STRIPE_SPONSOR_APP_PRICE_ID;
      else if (tier === 'bundle') priceId = process.env.STRIPE_SPONSOR_BUNDLE_PRICE_ID;
      else if (tier === 'fullpage') priceId = process.env.STRIPE_SPONSOR_FULLPAGE_PRICE_ID;
      else return send(res, 400, { success: false, error: 'Invalid tier specified. Must be web, app, bundle, or fullpage.' });

      if (!priceId) return send(res, 500, { success: false, error: 'Sponsorship Price ID not configured.' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      try {
        const merchants = await sql`SELECT stripe_customer_id, business_name FROM "Merchant" WHERE id = ${merchantId}`;
        const merchant = merchants[0];
        if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
        
        let stripeCustomerId = merchant.stripe_customer_id;
        if (!stripeCustomerId) {
          const customer = await stripeClient.customers.create({
            name: merchant.business_name,
            metadata: { merchant_id: merchantId }
          });
          stripeCustomerId = customer.id;
          await sql`UPDATE "Merchant" SET stripe_customer_id = ${stripeCustomerId} WHERE id = ${merchantId}`;
        }

        const session = await stripeClient.checkout.sessions.create({
          customer: stripeCustomerId,
          payment_method_types: ['card'],
          mode: 'subscription',
          line_items: [{ price: priceId, quantity: 1 }],
          success_url: `${origin}/dashboard.html?tab=billing&sponsor_success=true&session_id={CHECKOUT_SESSION_ID}`,
          cancel_url: `${origin}/dashboard.html?tab=billing`,
          metadata: {
            merchant_id: merchantId,
            sponsor_tier: tier,
            is_sponsor_purchase: 'true'
          }
        });

        return send(res, 200, { success: true, url: session.url });
      } catch (err) {
        console.error('Sponsor checkout error:', err);
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ── POST /api/v1/merchants/:id/billing/sponsor-verify ─────────
    const sponsorVerifyMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing\/sponsor-verify$/);
    if (method === 'POST' && sponsorVerifyMatch) {
      const merchantId = sponsorVerifyMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      try {
        const { session_id } = req.body || {};
        if (!session_id) return send(res, 400, { success: false, error: 'session_id required' });

        const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
        if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
        const stripeClient = Stripe(STRIPE_KEY);

        const session = await stripeClient.checkout.sessions.retrieve(session_id);
        if (session.payment_status !== 'paid' && session.status !== 'complete') {
          return send(res, 400, { success: false, error: 'Payment not complete' });
        }

        const tier = session.metadata?.sponsor_tier;
        const subId = typeof session.subscription === 'string' ? session.subscription : (session.subscription?.id || null);

        if (tier === 'fullpage') {
          await sql`
            UPDATE "Merchant"
            SET is_fullpage_sponsored = true,
                fullpage_sponsored_until = NOW() + INTERVAL '30 days',
                stripe_fullpage_sponsor_subscription_id = COALESCE(${subId}, stripe_fullpage_sponsor_subscription_id),
                updated_at = NOW()
            WHERE id = ${merchantId}
          `;
        } else if (tier === 'web') {
          await sql`
            UPDATE "Merchant"
            SET is_web_sponsored = true,
                web_sponsored_until = NOW() + INTERVAL '30 days',
                stripe_web_sponsor_subscription_id = COALESCE(${subId}, stripe_web_sponsor_subscription_id),
                updated_at = NOW()
            WHERE id = ${merchantId}
          `;
        } else if (tier === 'app') {
          await sql`
            UPDATE "Merchant"
            SET is_app_sponsored = true,
                app_sponsored_until = NOW() + INTERVAL '30 days',
                stripe_app_sponsor_subscription_id = COALESCE(${subId}, stripe_app_sponsor_subscription_id),
                updated_at = NOW()
            WHERE id = ${merchantId}
          `;
        } else if (tier === 'bundle') {
          await sql`
            UPDATE "Merchant"
            SET is_web_sponsored = true,
                is_app_sponsored = true,
                web_sponsored_until = NOW() + INTERVAL '30 days',
                app_sponsored_until = NOW() + INTERVAL '30 days',
                stripe_bundle_sponsor_subscription_id = COALESCE(${subId}, stripe_bundle_sponsor_subscription_id),
                updated_at = NOW()
            WHERE id = ${merchantId}
          `;
        }

        return send(res, 200, { success: true, tier });
      } catch (err) {
        console.error('Sponsor verify error:', err);
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ── POST /api/v1/merchants/:id/billing/sponsor-cancel ─────────
    const sponsorCancelMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing\/sponsor-cancel$/);
    if (method === 'POST' && sponsorCancelMatch) {
      const merchantId = sponsorCancelMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      try {
        const { tier } = req.body || {};

        const [merchant] = await sql`
          SELECT stripe_bundle_sponsor_subscription_id, stripe_app_sponsor_subscription_id, stripe_web_sponsor_subscription_id, stripe_fullpage_sponsor_subscription_id
          FROM "Merchant"
          WHERE id = ${merchantId}
          LIMIT 1
        `;

        if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
        
        let subId = null;
        if (tier === 'bundle') subId = merchant.stripe_bundle_sponsor_subscription_id;
        else if (tier === 'app') subId = merchant.stripe_app_sponsor_subscription_id;
        else if (tier === 'web') subId = merchant.stripe_web_sponsor_subscription_id;
        else if (tier === 'fullpage') subId = merchant.stripe_fullpage_sponsor_subscription_id;
        else return send(res, 400, { success: false, error: 'Invalid or missing tier' });

        if (!subId) return send(res, 400, { success: false, error: `No active ${tier} sponsorship` });

        // Cancel at period end
        await stripeClient.subscriptions.update(subId, {
          cancel_at_period_end: true
        });

        return send(res, 200, { success: true });
      } catch (err) {
        console.error('Sponsor cancel error:', err);
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ── POST /api/v1/merchants/:id/promo-banner ───────────────────
    const promoBannerMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/promo-banner$/);
    if (method === 'POST' && promoBannerMatch) {
      const merchantId = promoBannerMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      try {
        const { promo_banner_url } = req.body || {};
        const bannerValue = promo_banner_url ? promo_banner_url.trim() : null;

        await sql`UPDATE "Merchant" SET promo_banner_url = ${bannerValue} WHERE id = ${merchantId}`;
        return send(res, 200, { success: true, promo_banner_url: bannerValue });
      } catch (err) {
        console.error('Update promo banner error:', err);
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ── POST /api/v1/merchants/:id/promo-text ─────────────────────
    const promoTextMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/promo-text$/);
    if (method === 'POST' && promoTextMatch) {
      const merchantId = promoTextMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      try {
        const { promo_description } = req.body || {};
        // Strict 1000 character cap
        const textValue = promo_description ? promo_description.trim().slice(0, 1000) : null;

        await sql`UPDATE "Merchant" SET promo_description = ${textValue} WHERE id = ${merchantId}`;
        return send(res, 200, { success: true, promo_description: textValue });
      } catch (err) {
        console.error('Update promo text error:', err);
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ── POST /api/v1/merchants/:id/scrape-rating ─────────────────
    const scrapeRatingMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/scrape-rating$/);
    if (method === 'POST' && scrapeRatingMatch) {
      const merchantId = scrapeRatingMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      try {
        const { rating_score, rating_count, rating_platform } = req.body || {};
        
        let score = rating_score ? String(rating_score).trim() : null;
        let count = rating_count ? String(rating_count).trim() : null;
        let platform = rating_platform ? String(rating_platform).trim() : null;

        // If not manually provided, try auto-extracting from merchant's review_url
        if (!score) {
          const [m] = await sql`SELECT review_url FROM "Merchant" WHERE id = ${merchantId}`;
          if (m && m.review_url) {
            const rUrl = m.review_url.toLowerCase();
            if (rUrl.includes('google')) platform = 'Google';
            else if (rUrl.includes('yelp')) platform = 'Yelp';

            try {
              const fetchRes = await fetch(m.review_url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } });
              const htmlText = await fetchRes.text();
              const $ = cheerio.load(htmlText);
              
              // Extract star rating pattern (e.g. "4.9", "4.8 out of 5")
              const scoreMatch = htmlText.match(/([3-5]\.\d)\s*(?:out of 5|stars|★)?/i);
              if (scoreMatch) score = scoreMatch[1];

              // Extract review count pattern (e.g. "140 reviews", "46 ratings")
              const countMatch = htmlText.match(/(\d+[\d,]*\+?)\s*(?:reviews|ratings)/i);
              if (countMatch) count = countMatch[1] + '+ reviews';
            } catch (scrapeErr) {
              console.warn('Auto rating scrape fallback:', scrapeErr.message);
            }
          }
        }

        if (score || count || platform) {
          await sql`
            UPDATE "Merchant"
            SET rating_score = COALESCE(${score}, rating_score),
                rating_count = COALESCE(${count}, rating_count),
                rating_platform = COALESCE(${platform}, rating_platform)
            WHERE id = ${merchantId}
          `;
        }

        return send(res, 200, { success: true, rating_score: score, rating_count: count, rating_platform: platform });
      } catch (err) {
        console.error('Rating scrape error:', err);
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ── POST /api/v1/merchants/:id/billing/upgrade ─────────────────
    // Three cases:
    //   A) Physical/Mobile trial → tier1 (creates subscription immediately)
    //   B) Online/Hybrid promo-phase → any tier (creates subscription immediately, clears threshold)
    //   C) Online/Hybrid active subscription → next tier (updates price, proration:none)
    const upgradeMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing\/upgrade$/);
    if (method === 'POST' && upgradeMatch) {
      const merchantId = upgradeMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const { target_tier } = req.body || {};
      if (!target_tier) return send(res, 400, { success: false, error: 'target_tier is required' });

      const STRIPE_KEY = process.env.STRIPE_SECRET_KEY;
      if (!STRIPE_KEY) return send(res, 500, { success: false, error: 'Stripe not configured' });
      const stripeClient = Stripe(STRIPE_KEY);

      const tierPriceMap = {
        tier1: process.env.STRIPE_TIER1_PRICE_ID,
        online_starter: process.env.STRIPE_ONLINE_STARTER_PRICE_ID,
        online_growth: process.env.STRIPE_ONLINE_GROWTH_PRICE_ID,
        online_scale: process.env.STRIPE_ONLINE_SCALE_PRICE_ID,
      };
      const tierLimitMap = {
        tier1: 999999, online_starter: 500, online_growth: 2500, online_scale: 999999,
      };
      // Upgrade direction validation (no downgrades)
      const upgradeOrder = ['online_starter', 'online_growth', 'online_scale'];

      const targetPriceId = tierPriceMap[target_tier];
      if (!targetPriceId) return send(res, 400, { success: false, error: `No Stripe price configured for target tier '${target_tier}'` });
      const newLimit = tierLimitMap[target_tier];

      const [merchant] = await sql`
        SELECT id, subscription_tier, billing_status, account_blocked,
               stripe_customer_id, stripe_payment_method_id, stripe_subscription_id,
               billing_starts_at_member_count, billing_cycle
        FROM "Merchant" WHERE id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (merchant.account_blocked) return send(res, 400, { success: false, error: 'Account is blocked. Please reactivate first.' });

      const currentTier = merchant.subscription_tier;
      const hasActiveSubscription = !!merchant.stripe_subscription_id && merchant.billing_status === 'active';
      const isPromoPhase = !!merchant.billing_starts_at_member_count;
      const isTrialTier = ['trial', 'free', 'free_for_life'].includes(currentTier) || merchant.is_presetup;

      // Validate upgrade direction for online tiers
      if (!isTrialTier && !isPromoPhase && hasActiveSubscription) {
        const currentIdx = upgradeOrder.indexOf(currentTier);
        const targetIdx = upgradeOrder.indexOf(target_tier);
        if (currentIdx === -1 || targetIdx === -1 || targetIdx <= currentIdx) {
          return send(res, 400, { success: false, error: 'Invalid upgrade path. Only upward tier changes are allowed.' });
        }
      }

      try {
        // ── If no Stripe customer, no saved card, or no active subscription → open Stripe Checkout ──
        if (!merchant.stripe_customer_id || !merchant.stripe_payment_method_id || !merchant.stripe_subscription_id || merchant.billing_cycle === 'lifetime') {
          const origin = req.headers.origin || 'https://perkfinity.net';
          const sessionParams = {
            payment_method_types: ['card'],
            line_items: [{ price: targetPriceId, quantity: 1 }],
            mode: 'subscription',
            success_url: `${origin}/dashboard.html?tab=billing&upgrade_success=true&session_id={CHECKOUT_SESSION_ID}`,
            cancel_url: `${origin}/dashboard.html?tab=billing&upgrade_cancelled=true`,
            metadata: { merchant_id: merchantId, billing_cycle: 'monthly', trigger: 'manual_upgrade', to_tier: target_tier }
          };
          if (merchant.stripe_customer_id) {
            sessionParams.customer = merchant.stripe_customer_id;
          } else {
            sessionParams.customer_email = payload.email || undefined;
          }
          const session = await stripeClient.checkout.sessions.create(sessionParams);
          return send(res, 200, { success: true, checkout_url: session.url });
        }

        // ── Case A & B: Saved card on file but no active subscription → create one immediately ──
        if (isTrialTier || isPromoPhase || !hasActiveSubscription) {
          const subscription = await stripeClient.subscriptions.create({
            customer: merchant.stripe_customer_id,
            items: [{ price: targetPriceId }],
            default_payment_method: merchant.stripe_payment_method_id,
            metadata: { merchant_id: merchantId, trigger: 'manual_upgrade', from_tier: currentTier, to_tier: target_tier },
          });

          await sql`
            UPDATE "Merchant"
            SET subscription_tier              = ${target_tier},
                billing_status                 = 'active',
                member_limit                   = ${newLimit},
                member_cap_notified            = false,
                cap_block_count               = 0,
                stripe_subscription_id         = ${subscription.id},
                billing_starts_at_member_count = NULL,
                account_blocked                = false,
                subscription_started_at        = NOW(),
                next_billing_date              = ${new Date((subscription.current_period_end || subscription.items?.data?.[0]?.current_period_end) * 1000)},
                updated_at                     = NOW()
            WHERE id = ${merchantId}
          `;
          console.log(`[Upgrade] Merchant ${merchantId} upgraded from '${currentTier}' to '${target_tier}' — new subscription ${subscription.id}`);
          return send(res, 200, { success: true, message: `Upgraded to ${target_tier}! Billing starts today.` });
        }

        // ── Case C: Active subscription — update price, no proration ──
        const existingSub = await stripeClient.subscriptions.retrieve(merchant.stripe_subscription_id);
        if (!existingSub || !existingSub.items?.data?.length) {
          return send(res, 400, { success: false, error: 'Could not retrieve existing Stripe subscription items.' });
        }
        const subItemId = existingSub.items.data[0].id;

        await stripeClient.subscriptions.update(merchant.stripe_subscription_id, {
          items: [{ id: subItemId, price: targetPriceId }],
          proration_behavior: 'none',
          metadata: { merchant_id: merchantId, trigger: 'manual_upgrade', from_tier: currentTier, to_tier: target_tier },
        });

        await sql`
          UPDATE "Merchant"
          SET subscription_tier     = ${target_tier},
              member_limit          = ${newLimit},
              member_cap_notified   = false,
              cap_block_count      = 0,
              updated_at            = NOW()
          WHERE id = ${merchantId}
        `;
        console.log(`[Upgrade] Merchant ${merchantId} plan updated from '${currentTier}' to '${target_tier}' — effective at next billing`);
        return send(res, 200, { success: true, message: `Plan upgraded to ${target_tier}. New pricing takes effect at your next billing cycle.` });

      } catch (stripeErr) {
        console.error(`[Upgrade] Stripe error for merchant ${merchantId}:`, stripeErr.message);
        return send(res, 400, { success: false, error: `Upgrade failed: ${stripeErr.message}` });
      }
    }

    // ── POST /api/v1/merchants/:id/billing/cancel-promo ───────────
    // For promo-phase merchants (billing_starts_at_member_count set, no active subscription).
    // Blocks account immediately. No Stripe subscription to cancel.
    // Stripe customer + payment method retained for reactivation.
    const cancelPromoMatch = url.match(/\/api\/v1\/merchants\/([a-zA-Z0-9_-]+)\/billing\/cancel-promo$/);
    if (method === 'POST' && cancelPromoMatch) {
      const merchantId = cancelPromoMatch[1];
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });
      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }
      if (payload.merchantId !== merchantId) return send(res, 403, { success: false, error: 'Forbidden' });

      const [merchant] = await sql`
        SELECT id, subscription_tier, billing_starts_at_member_count, stripe_subscription_id
        FROM "Merchant" WHERE id = ${merchantId} LIMIT 1
      `;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });
      if (!merchant.billing_starts_at_member_count) {
        return send(res, 400, { success: false, error: 'No promo billing threshold found. Use /billing/cancel for active subscriptions.' });
      }
      if (merchant.stripe_subscription_id) {
        return send(res, 400, { success: false, error: 'Active subscription exists. Use /billing/cancel instead.' });
      }

      await sql`
        UPDATE "Merchant"
        SET billing_status                 = 'cancelled',
            account_blocked                = true,
            cancelled_at                   = NOW(),
            billing_starts_at_member_count = NULL,
            updated_at                     = NOW()
        WHERE id = ${merchantId}
      `;
      await sql`
        UPDATE "Campaign" SET status = 'expired', updated_at = NOW()
        WHERE merchant_id = ${merchantId} AND status = 'active'
      `;
      console.log(`[CancelPromo] Merchant ${merchantId} cancelled promo billing (no subscription existed).`);
      return send(res, 200, { success: true, message: 'Account cancelled. Your data is preserved and you can reactivate anytime.' });
    }

    // ── GET /api/v1/admin/stuck-payments ──────────────────────────
    // Returns merchants blocked due to auto-upgrade payment failure (not normal cancellation)
    if (method === 'GET' && url.endsWith('/admin/stuck-payments')) {
      const stuckMerchants = await sql`
        SELECT m.id, m.business_name, m.payment_failed_at, m.payment_failure_reminder_count,
               EXTRACT(DAY FROM NOW() - m.payment_failed_at)::int AS days_since_failure,
               mu.email
        FROM "Merchant" m
        LEFT JOIN "MerchantUser" mu ON mu.merchant_id = m.id
        WHERE m.billing_status = 'payment_failed'
          AND m.account_blocked = true
          AND m.payment_failed_at IS NOT NULL
        ORDER BY m.payment_failed_at ASC
      `;
      return send(res, 200, { success: true, data: stuckMerchants });
    }

    // ── DELETE /api/v1/merchants/account ──────────────────────────
    if (method === 'DELETE' && url.includes('/api/v1/merchants/account')) {
      const authHeader = req.headers.authorization;
      if (!authHeader) return send(res, 401, { success: false, error: 'Unauthorized' });

      let payload;
      try { payload = jwt.verify(authHeader.replace('Bearer ', ''), process.env.JWT_SECRET); }
      catch (err) { return send(res, 401, { success: false, error: 'Invalid token' }); }

      const merchantId = payload.merchantId;
      const data = req.body || {};
      if (!data.password) return send(res, 400, { success: false, error: 'Password is required' });

      // Password Check
      const [user] = await sql`SELECT id, password_hash FROM "MerchantUser" WHERE id = ${payload.userId} LIMIT 1`;
      if (!user || !(await bcrypt.compare(data.password, user.password_hash))) {
        return send(res, 401, { success: false, error: 'Incorrect password' });
      }

      // Check Billing Dependency Lock
      const [merchant] = await sql`SELECT billing_status, account_blocked, stripe_customer_id FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
      if (!merchant) return send(res, 404, { success: false, error: 'Merchant not found' });

      const st = merchant.billing_status;
      // Protect active Stripe subscriptions from getting ghosted in DB
      if (st === 'active' || st === 'payment_failed') {
        return send(res, 403, { success: false, error: 'Forbidden. You must cancel your active subscription first.' });
      }

      // ── Stripe customer cleanup — remove saved card so they can never be charged ──
      // Done BEFORE PII wipe so we still have the customer ID.
      if (merchant.stripe_customer_id) {
        try {
          const delStripeClient = process.env.STRIPE_SECRET_KEY ? Stripe(process.env.STRIPE_SECRET_KEY) : null;
          if (delStripeClient) await delStripeClient.customers.delete(merchant.stripe_customer_id);
        } catch (stripeErr) {
          // Non-fatal: log and continue — DB wipe must still complete
          console.error('[Delete] Stripe customer cleanup failed:', stripeErr.message);
        }
      }

      // Safe to Wipe PII
      // NOTE: MerchantUser.password_hash is NOT NULL in the schema,
      // so it must use a sentinel value instead of NULL to avoid constraint violations.
      const deletedEmail = 'deleted_' + payload.userId + '@deleted.invalid';
      await sql`UPDATE "MerchantUser" SET email = ${deletedEmail}, password_hash = 'DELETED' WHERE id = ${payload.userId}`;
      await sql`
        UPDATE "Merchant"
        SET contact_name = NULL,
            phone = NULL,
            website = NULL,
            logo_url = NULL,
            status = 'cancelled',
            billing_status = 'deleted',
            account_blocked = true,
            cancelled_at = NOW(),
            updated_at = NOW()
        WHERE id = ${merchantId}
      `;
      await sql`UPDATE "MerchantLocation" SET address = NULL, suite = NULL, city = NULL, state = NULL, postal_code = NULL WHERE merchant_id = ${merchantId}`;

      // ── Expire all active campaigns for this deleted merchant ──
      await sql`UPDATE "Campaign" SET status = 'expired', updated_at = NOW() WHERE merchant_id = ${merchantId} AND status = 'active'`;
      // ── Expire any unredeemed redemptions tied to those campaigns ──
      await sql`UPDATE "Redemption" SET status = 'expired' WHERE campaign_id IN (SELECT id FROM "Campaign" WHERE merchant_id = ${merchantId}) AND status = 'created' AND redeemed = false`;

      return send(res, 200, { success: true, message: 'Account wiped successfully' });
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

    // ── GET /api/v1/members/unsubscribe?token=&uid= ──────────────────
    if (method === 'GET' && url.startsWith('/api/v1/members/unsubscribe')) {
      const qs = (req.url || '').split('?')[1] || '';
      const params = new URLSearchParams(qs);
      const token = params.get('token');
      const uid = params.get('uid');

      const unsubPage = (success, message) => `<!DOCTYPE html><html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>${success ? 'Unsubscribed' : 'Invalid Link'} — Perkfinity</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', -apple-system, sans-serif; background: #f3f0fa; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; }
    .card { background: #fff; border-radius: 20px; padding: 48px 40px; max-width: 440px; width: 90%; text-align: center; box-shadow: 0 8px 32px rgba(91,63,165,0.10); }
    .icon { font-size: 52px; margin-bottom: 20px; }
    h1 { font-size: 22px; font-weight: 800; color: #1a1a2e; margin: 0 0 12px; }
    p { color: #666; font-size: 15px; line-height: 1.65; margin: 0 0 28px; }
    a { display: inline-block; color: #5B3FA5; text-decoration: none; font-weight: 700; font-size: 14px; border: 1.5px solid #5B3FA5; border-radius: 10px; padding: 10px 24px; transition: all 0.2s; }
    a:hover { background: #5B3FA5; color: #fff; }
  </style>
</head><body>
  <div class="card">
    <div class="icon">${success ? '\u2705' : '\u274C'}</div>
    <h1>${success ? "You've been unsubscribed" : 'Invalid or expired link'}</h1>
    <p>${message}</p>
    <a href="https://www.perkfinity.net">← Back to Perkfinity</a>
  </div>
</body></html>`;

      if (!token || !uid) {
        res.setHeader('Content-Type', 'text/html');
        return res.status(400).end(unsubPage(false, 'This unsubscribe link is missing required information. Please contact <strong>hello@perkfinity.net</strong> if you need help.'));
      }

      const secret = process.env.JWT_SECRET || 'perkfinity-secret';
      const expected = crypto.createHmac('sha256', secret).update(uid).digest('hex');

      if (token !== expected) {
        res.setHeader('Content-Type', 'text/html');
        return res.status(400).end(unsubPage(false, 'This unsubscribe link is invalid or has expired. Please contact <strong>hello@perkfinity.net</strong> if you need help.'));
      }

      try {
        await sql`UPDATE "User" SET email_unsubscribed = true WHERE id = ${uid}`;
        res.setHeader('Content-Type', 'text/html');
        return res.status(200).end(unsubPage(true, "You've been successfully removed from Perkfinity's Daily Digest emails. You won't receive any more marketing emails from us. You can still use the app and manage your account normally."));
      } catch (err) {
        console.error('Unsubscribe error:', err);
        res.setHeader('Content-Type', 'text/html');
        return res.status(500).end(unsubPage(false, 'Something went wrong. Please contact <strong>hello@perkfinity.net</strong> to unsubscribe manually and we\'ll take care of it right away.'));
      }
    }


    // ══════════════════════════════════════════════════════════════
    // ENTERPRISE INQUIRY ENDPOINTS
    // ══════════════════════════════════════════════════════════════

    // ── POST /api/v1/enterprise/inquiry ───────────────────────────
    if (method === 'POST' && url.endsWith('/enterprise/inquiry')) {
      // Ensure enterprise schema and tables exist
      await sql`CREATE SCHEMA IF NOT EXISTS enterprise`;
      await sql`
        CREATE TABLE IF NOT EXISTS enterprise."EnterpriseInquiry" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          company_legal_name TEXT NOT NULL,
          brand_name TEXT,
          industry TEXT NOT NULL,
          website TEXT NOT NULL,
          hq_street TEXT, hq_city TEXT, hq_state TEXT, hq_zip TEXT, hq_country TEXT DEFAULT 'USA',
          num_locations INTEGER,
          geographic_reach TEXT,
          location_types TEXT,
          same_brand_name BOOLEAN DEFAULT true,
          brand_note TEXT,
          operation_structure TEXT,
          operation_structure_note TEXT,
          decision_authority TEXT,
          decision_authority_note TEXT,
          pos_system TEXT,
          has_loyalty TEXT DEFAULT 'no',
          loyalty_name TEXT,
          loyalty_types TEXT,
          digital_deal TEXT,
          digital_deal_other TEXT,
          systems_integrated TEXT DEFAULT 'no',
          integration_note TEXT,
          member_db_size TEXT,
          member_capture TEXT,
          wants_import TEXT DEFAULT 'no',
          import_count TEXT,
          import_format TEXT,
          campaign_mgmt TEXT,
          data_visibility TEXT,
          notes TEXT,
          contact_name TEXT NOT NULL,
          contact_title TEXT,
          contact_email TEXT NOT NULL,
          contact_phone TEXT,
          logo_url TEXT,
          status TEXT NOT NULL DEFAULT 'new',
          admin_notes TEXT,
          submitted_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
        )`;

      const data = req.body || {};
      const d = data;
      // Handle logo upload (base64 → store as data URL; in production could upload to storage)
      const logoUrl = d.logo_base64 || null;

      const [inquiry] = await sql`
        INSERT INTO enterprise."EnterpriseInquiry" (
          company_legal_name, brand_name, industry, website,
          hq_street, hq_city, hq_state, hq_zip, hq_country,
          num_locations, geographic_reach, location_types, same_brand_name, brand_note,
          operation_structure, operation_structure_note, decision_authority, decision_authority_note,
          pos_system, has_loyalty, loyalty_name, loyalty_types,
          digital_deal, digital_deal_other, systems_integrated, integration_note, member_db_size,
          member_capture, wants_import, import_count, import_format,
          campaign_mgmt, data_visibility, notes,
          contact_name, contact_title, contact_email, contact_phone, logo_url
        ) VALUES (
          ${d.company_legal_name}, ${d.brand_name || null}, ${d.industry}, ${d.website},
          ${d.hq_street || null}, ${d.hq_city || null}, ${d.hq_state || null}, ${d.hq_zip || null}, ${d.hq_country || 'USA'},
          ${d.num_locations || null}, ${d.geographic_reach || null}, ${d.location_types || null},
          ${d.same_brand_name !== false}, ${d.brand_note || null},
          ${d.operation_structure || null}, ${d.operation_structure_note || null},
          ${d.decision_authority || null}, ${d.decision_authority_note || null},
          ${d.pos_system || null}, ${d.has_loyalty || 'no'}, ${d.loyalty_name || null}, ${d.loyalty_types || null},
          ${d.digital_deal || null}, ${d.digital_deal_other || null},
          ${d.systems_integrated || 'no'}, ${d.integration_note || null}, ${d.member_db_size || null},
          ${d.member_capture || null}, ${d.wants_import || 'no'}, ${d.import_count || null}, ${d.import_format || null},
          ${d.campaign_mgmt || null}, ${d.data_visibility || null}, ${d.notes || null},
          ${d.contact_name}, ${d.contact_title || null}, ${d.contact_email}, ${d.contact_phone || null}, ${logoUrl}
        ) RETURNING id, submitted_at`;

      // ── Admin notification email ──
      const adminHtml = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:580px;margin:0 auto">
        <div style="background:linear-gradient(135deg,#1e5c38,#2e7d4f);padding:28px 24px;text-align:center">
          <div style="color:#fff;font-size:22px;font-weight:800">🏢 New Enterprise Inquiry</div>
        </div>
        <div style="padding:28px 24px;background:#fff">
          <table style="width:100%;border-collapse:collapse;font-size:14px">
            <tr><td style="padding:8px 0;color:#666;width:40%">Company</td><td style="padding:8px 0;font-weight:700">${d.company_legal_name}${d.brand_name ? ' (' + d.brand_name + ')' : ''}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Industry</td><td style="padding:8px 0;font-weight:700">${d.industry}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Website</td><td style="padding:8px 0"><a href="${d.website}">${d.website}</a></td></tr>
            <tr><td style="padding:8px 0;color:#666">HQ</td><td style="padding:8px 0;font-weight:700">${d.hq_city || ''}, ${d.hq_state || ''}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Locations</td><td style="padding:8px 0;font-weight:700">${d.num_locations} · ${d.location_types} · ${d.geographic_reach}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Structure</td><td style="padding:8px 0;font-weight:700">${d.operation_structure}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Contact</td><td style="padding:8px 0;font-weight:700">${d.contact_name} — ${d.contact_title}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Email</td><td style="padding:8px 0"><a href="mailto:${d.contact_email}">${d.contact_email}</a></td></tr>
            <tr><td style="padding:8px 0;color:#666">Phone</td><td style="padding:8px 0;font-weight:700">${d.contact_phone}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Member Capture</td><td style="padding:8px 0;font-weight:700">${d.member_capture}</td></tr>
            <tr><td style="padding:8px 0;color:#666">Campaign Mgmt</td><td style="padding:8px 0;font-weight:700">${d.campaign_mgmt}</td></tr>
            ${d.notes ? `<tr><td style="padding:8px 0;color:#666;vertical-align:top">Notes</td><td style="padding:8px 0">${d.notes}</td></tr>` : ''}
          </table>
        </div>
      </div>`;

      // ── Applicant auto-reply email ──
      const applicantHtml = `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:520px;margin:0 auto">
        <div style="background:linear-gradient(135deg,#1e5c38,#2e7d4f);padding:28px 24px;text-align:center">
          <div style="color:#fff;font-size:24px;font-weight:800">Perkfinity</div>
        </div>
        <div style="padding:28px 24px">
          <p style="font-size:16px;color:#333;line-height:1.6">Hi ${d.contact_name},</p>
          <p style="font-size:15px;color:#555;line-height:1.6">Thank you for reaching out about Perkfinity Enterprise for <strong>${d.company_legal_name}</strong>. We've received your inquiry and our team will review the details you provided.</p>
          <p style="font-size:15px;color:#555;line-height:1.6">You can expect to hear back from us within <strong>3–5 business days</strong>. We'll reach you at <strong>${d.contact_email}</strong> or <strong>${d.contact_phone}</strong>.</p>
          <p style="font-size:15px;color:#555;line-height:1.6">In the meantime, if you have any questions feel free to contact us at <a href="mailto:hello@perkfinity.net" style="color:#1e5c38">hello@perkfinity.net</a>.</p>
          <p style="font-size:15px;color:#555;line-height:1.6">— The Perkfinity Team</p>
        </div>
      </div>`;

      const brevoKey = process.env.BREVO_API_KEY;
      if (brevoKey) {
        try {
          await fetch('https://api.brevo.com/v3/smtp/email', {
            method: 'POST',
            headers: { 'api-key': brevoKey, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              sender: { name: 'Perkfinity', email: 'no-reply@perkfinity.net' },
              to: [{ email: 'hello@perkfinity.net', name: 'Perkfinity Admin' }],
              subject: `🏢 New Enterprise Inquiry — ${d.company_legal_name}`,
              htmlContent: adminHtml
            })
          });
          await fetch('https://api.brevo.com/v3/smtp/email', {
            method: 'POST',
            headers: { 'api-key': brevoKey, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              sender: { name: 'Perkfinity', email: 'no-reply@perkfinity.net' },
              to: [{ email: d.contact_email, name: d.contact_name }],
              subject: 'We received your Perkfinity Enterprise inquiry',
              htmlContent: applicantHtml
            })
          });
        } catch (emailErr) {
          console.error('[enterprise] email error:', emailErr.message);
        }
      }

      // ── Record initial history entry ──
      await sql`
        CREATE TABLE IF NOT EXISTS enterprise."EnterpriseInquiryHistory" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          inquiry_id TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          changed_at TIMESTAMPTZ DEFAULT NOW()
        )`;
      await sql`
        INSERT INTO enterprise."EnterpriseInquiryHistory" (id, inquiry_id, status, note, changed_at)
        VALUES (gen_random_uuid()::text, ${inquiry.id}, 'new', 'Inquiry submitted', NOW())`;

      return send(res, 200, { success: true, data: { id: inquiry.id } });
    }

    // ── GET /api/v1/admin/enterprise-inquiries ────────────────────
    if (method === 'GET' && url.startsWith('/api/v1/admin/enterprise-inquiries') && !url.includes('/history')) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const qs = (req.url || '').split('?')[1] || '';
      const statusFilter = new URLSearchParams(qs).get('status') || 'all';
      let rows;
      if (statusFilter === 'all') {
        rows = await sql`SELECT * FROM enterprise."EnterpriseInquiry" ORDER BY submitted_at DESC`;
      } else {
        rows = await sql`SELECT * FROM enterprise."EnterpriseInquiry" WHERE status=${statusFilter} ORDER BY submitted_at DESC`;
      }
      return send(res, 200, { success: true, data: rows });
    }

    // ── PATCH /api/v1/admin/enterprise-inquiries/:id ──────────────
    // ── GET /api/v1/admin/enterprise-inquiries/:id/history ────────
    const entHistMatch = url.match(/\/api\/v1\/admin\/enterprise-inquiries\/([a-zA-Z0-9_-]+)\/history$/);
    if (method === 'GET' && entHistMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const inquiryId = entHistMatch[1];
      await sql`CREATE SCHEMA IF NOT EXISTS enterprise`;
      await sql`
        CREATE TABLE IF NOT EXISTS enterprise."EnterpriseInquiryHistory" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          inquiry_id TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          changed_at TIMESTAMPTZ DEFAULT NOW()
        )`;
      const rows = await sql`
        SELECT id, status, note, changed_at
        FROM enterprise."EnterpriseInquiryHistory"
        WHERE inquiry_id = ${inquiryId}
        ORDER BY changed_at ASC`;
      return send(res, 200, { success: true, data: rows });
    }

    // ── PATCH /api/v1/admin/enterprise-inquiries/:id ──────────────
    const entPatchMatch = url.match(/\/api\/v1\/admin\/enterprise-inquiries\/([a-zA-Z0-9_-]+)$/);
    if (method === 'PATCH' && entPatchMatch) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const entId = entPatchMatch[1];
      const data = req.body || {};
      const { status, admin_notes } = data;
      await sql`
        UPDATE enterprise."EnterpriseInquiry"
        SET status=COALESCE(${status || null},status),
            admin_notes=COALESCE(${admin_notes || null},admin_notes),
            updated_at=NOW()
        WHERE id=${entId}`;
      // Record history entry
      await sql`CREATE SCHEMA IF NOT EXISTS enterprise`;
      await sql`
        CREATE TABLE IF NOT EXISTS enterprise."EnterpriseInquiryHistory" (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          inquiry_id TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT,
          changed_at TIMESTAMPTZ DEFAULT NOW()
        )`;
      if (status) {
        await sql`
          INSERT INTO enterprise."EnterpriseInquiryHistory" (id, inquiry_id, status, note, changed_at)
          VALUES (gen_random_uuid()::text, ${entId}, ${status}, ${admin_notes || null}, NOW())`;
      }
      return send(res, 200, { success: true });
    }

    // ═══════════════════════════════════════════════════════════════════
    // CONTRACTOR MANAGEMENT — Task 3
    // ═══════════════════════════════════════════════════════════════════

    // ── GET /api/v1/admin/migrate-contractors ──────────────────────────
    // Creates all 8 contractor tables + seeds milestone config. Idempotent.
    if (method === 'GET' && url === '/api/v1/admin/migrate-contractors') {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      await sql`CREATE TABLE IF NOT EXISTS "Contractor" (
        id             TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        full_name      TEXT NOT NULL,
        legal_name     TEXT,
        email          TEXT UNIQUE NOT NULL,
        phone          TEXT,
        address        TEXT,
        referral_code  TEXT UNIQUE NOT NULL,
        status         TEXT NOT NULL DEFAULT 'inactive',
        ica_status     TEXT NOT NULL DEFAULT 'not_sent',
        stripe_account_id TEXT,
        stripe_onboarding_status TEXT NOT NULL DEFAULT 'pending',
        payment_method TEXT NOT NULL DEFAULT 'check',
        notes          TEXT,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`;
      await sql`CREATE TABLE IF NOT EXISTS "ContractorCompensationRule" (
        id                         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id              TEXT NOT NULL UNIQUE REFERENCES "Contractor"(id),
        commission_rate            NUMERIC(5,4) NOT NULL DEFAULT 0.25,
        commission_duration_months INT          NOT NULL DEFAULT 12,
        retainer_cents             INT          NOT NULL DEFAULT 0,
        created_at                 TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at                 TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      )`;
      await sql`CREATE TABLE IF NOT EXISTS "ContractorMerchantAttribution" (
        id                     TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id          TEXT NOT NULL REFERENCES "Contractor"(id),
        merchant_id            TEXT NOT NULL UNIQUE REFERENCES "Merchant"(id),
        commission_start_date  DATE,
        commission_end_date    DATE,
        retention_bonuses_paid INT  NOT NULL DEFAULT 0,
        source                 TEXT NOT NULL DEFAULT 'self',
        created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`;
      await sql`CREATE TABLE IF NOT EXISTS "ContractorPayout" (
        id                    TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id         TEXT NOT NULL REFERENCES "Contractor"(id),
        period_start          DATE NOT NULL,
        period_end            DATE NOT NULL,
        commission_cents      INT  NOT NULL DEFAULT 0,
        retainer_cents        INT  NOT NULL DEFAULT 0,
        milestone_bonus_cents INT  NOT NULL DEFAULT 0,
        retention_bonus_cents INT  NOT NULL DEFAULT 0,
        special_bonus_cents   INT  NOT NULL DEFAULT 0,
        total_cents           INT  NOT NULL DEFAULT 0,
        breakdown             JSONB,
        status                TEXT NOT NULL DEFAULT 'pending',
        approved_by           TEXT,
        approved_at           TIMESTAMPTZ,
        payment_method        TEXT,
        payment_reference     TEXT,
        paid_at               TIMESTAMPTZ,
        created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`;
      await sql`CREATE TABLE IF NOT EXISTS "ContractorEarningsSummary" (
        id                   TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id        TEXT NOT NULL REFERENCES "Contractor"(id),
        year                 INT  NOT NULL,
        commission_ytd_cents INT  NOT NULL DEFAULT 0,
        retainer_ytd_cents   INT  NOT NULL DEFAULT 0,
        bonus_ytd_cents      INT  NOT NULL DEFAULT 0,
        total_ytd_cents      INT  NOT NULL DEFAULT 0,
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(contractor_id, year)
      )`;
      await sql`CREATE TABLE IF NOT EXISTS "SystemMilestoneConfig" (
        id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        threshold   INT  NOT NULL UNIQUE,
        bonus_cents INT  NOT NULL,
        label       TEXT NOT NULL,
        is_active   BOOLEAN NOT NULL DEFAULT true,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`;
      await sql`CREATE TABLE IF NOT EXISTS "ContractorMilestoneRecord" (
        id            TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id TEXT NOT NULL REFERENCES "Contractor"(id),
        milestone_id  TEXT NOT NULL REFERENCES "SystemMilestoneConfig"(id),
        payout_id     TEXT REFERENCES "ContractorPayout"(id),
        earned_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(contractor_id, milestone_id)
      )`;
      await sql`CREATE TABLE IF NOT EXISTS "ContractorSpecialBonus" (
        id            TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id TEXT NOT NULL REFERENCES "Contractor"(id),
        label         TEXT NOT NULL,
        amount_cents  INT  NOT NULL CHECK (amount_cents > 0 AND amount_cents <= 200000),
        reason        TEXT,
        status        TEXT NOT NULL DEFAULT 'pending',
        payout_id     TEXT REFERENCES "ContractorPayout"(id),
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`;
      await sql`
        INSERT INTO "SystemMilestoneConfig" (id, threshold, bonus_cents, label)
        VALUES
          (gen_random_uuid()::text, 100,  25000, '100 Paying Subscribers'),
          (gen_random_uuid()::text, 250,  50000, '250 Paying Subscribers'),
          (gen_random_uuid()::text, 500, 100000, '500 Paying Subscribers')
        ON CONFLICT (threshold) DO NOTHING
      `;

      // Idempotent: rep portal password hash
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS password_hash TEXT`;
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS invite_token TEXT`;
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS invite_expires_at TIMESTAMP`;
      // Idempotent: entity type for 1099-NEC eligibility
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS entity_type TEXT`;
      // Idempotent: Dropbox Sign request ID for ICA webhook correlation
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS dropbox_sign_request_id TEXT`;
      // Idempotent: Stripe Connect fields
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS stripe_account_id TEXT`;
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS stripe_onboarding_status TEXT NOT NULL DEFAULT 'pending'`;
      // Idempotent: ICA Company Signatory
      await sql`ALTER TABLE "Contractor" ADD COLUMN IF NOT EXISTS ica_company_signatory TEXT`;
      // Territory table — one active territory per rep
      await sql`CREATE TABLE IF NOT EXISTS "ContractorTerritory" (
        id            TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id TEXT NOT NULL REFERENCES "Contractor"(id) ON DELETE CASCADE,
        label         TEXT NOT NULL,
        zip_codes     TEXT[] NOT NULL DEFAULT '{}',
        status        TEXT NOT NULL DEFAULT 'active',
        assigned_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`;
      await sql`CREATE UNIQUE INDEX IF NOT EXISTS idx_contractor_territory_active
        ON "ContractorTerritory"(contractor_id) WHERE status = 'active'`;
      // Quota period table — one-time 3-month qualification gate per rep
      await sql`CREATE TABLE IF NOT EXISTS "ContractorQuotaPeriod" (
        id            TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        contractor_id TEXT NOT NULL UNIQUE REFERENCES "Contractor"(id) ON DELETE CASCADE,
        period_start  DATE NOT NULL,
        period_end    DATE NOT NULL,
        quota_target  INT  NOT NULL DEFAULT 30,
        status        TEXT NOT NULL DEFAULT 'active',
        alert_sent    BOOLEAN NOT NULL DEFAULT false,
        locked_at     TIMESTAMPTZ,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`;
      return send(res, 200, { success: true, message: 'All 10 contractor tables up to date: password_hash, entity_type, ContractorTerritory, ContractorQuotaPeriod added.' });
    }

    // ── GET /api/v1/contractors/validate-code?code=REP-XXXXX ──────────
    // PUBLIC — validates a contractor referral code before signup
    if (method === 'GET' && url.startsWith('/api/v1/contractors/validate-code')) {
      const vcCode = ((new URL('http://x' + req.url)).searchParams.get('code') || '').toUpperCase().trim();
      if (!vcCode) return send(res, 400, { success: false, error: 'code is required.' });
      const [vcContractor] = await sql`
        SELECT id, full_name, referral_code
        FROM "Contractor"
        WHERE referral_code = ${vcCode} AND status = 'active'
        LIMIT 1
      `;
      if (!vcContractor) return send(res, 200, { valid: false });
      return send(res, 200, { valid: true, contractor_id: vcContractor.id, contractor_name: vcContractor.full_name, referral_code: vcContractor.referral_code });
    }

    // ── POST /api/v1/contractors/stripe/create-account ──────────────────
    if (method === 'POST' && url.endsWith('/contractors/stripe/create-account')) {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });

      try {
        const [rep] = await sql`SELECT id, email, full_name, stripe_account_id FROM "Contractor" WHERE id = ${repId}`;
        if (!rep) return send(res, 404, { success: false, error: 'Contractor not found' });
        
        if (rep.stripe_account_id) {
          return send(res, 200, { success: true, stripe_account_id: rep.stripe_account_id, message: 'Account already exists' });
        }

        const stripeClient = process.env.STRIPE_SECRET_KEY ? Stripe(process.env.STRIPE_SECRET_KEY) : null;
        if (!stripeClient) return send(res, 500, { success: false, error: 'Stripe not configured' });

        const account = await stripeClient.accounts.create({
          type: 'express',
          country: 'US',
          email: rep.email,
          capabilities: { 
            transfers: { requested: true },
            tax_reporting_us_1099_misc: { requested: true }
          },
          business_profile: {
            product_description: 'Independent Sales Contractor for Perkfinity'
          }
        });

        await sql`UPDATE "Contractor" SET stripe_account_id = ${account.id} WHERE id = ${repId}`;
        return send(res, 200, { success: true, stripe_account_id: account.id });
      } catch (e) {
        console.error('[Stripe] Create Account Error:', e.message);
        return send(res, 500, { success: false, error: e.message });
      }
    }

    // ── POST /api/v1/contractors/stripe/onboarding-link ───────────────
    if (method === 'POST' && url.endsWith('/contractors/stripe/onboarding-link')) {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      try {
        const [rep] = await sql`SELECT stripe_account_id FROM "Contractor" WHERE id = ${repId}`;
        if (!rep || !rep.stripe_account_id) return send(res, 400, { success: false, error: 'Stripe account not created' });
        
        const stripeClient = process.env.STRIPE_SECRET_KEY ? Stripe(process.env.STRIPE_SECRET_KEY) : null;
        if (!stripeClient) return send(res, 500, { success: false, error: 'Stripe not configured' });

        const origin = req.headers.origin || 'http://localhost:3000'; // fallback
        const accountLink = await stripeClient.accountLinks.create({
          account: rep.stripe_account_id,
          refresh_url: `${origin}/reps/index.html?onboarding=refresh`,
          return_url: `${origin}/reps/index.html?onboarding=return`,
          type: 'account_onboarding',
        });

        return send(res, 200, { success: true, url: accountLink.url });
      } catch (e) {
        console.error('[Stripe] Onboarding Link Error:', e.message);
        return send(res, 500, { success: false, error: e.message });
      }
    }

    // ── POST /api/v1/rep/stripe-sync ───────────────
    if (method === 'POST' && url.endsWith('/rep/stripe-sync')) {
      const repId = verifyRepAuth(req);
      if (!repId) return send(res, 401, { success: false, error: 'Unauthorized' });
      
      try {
        const [rep] = await sql`SELECT stripe_account_id, stripe_onboarding_status FROM "Contractor" WHERE id = ${repId}`;
        if (!rep || !rep.stripe_account_id) return send(res, 400, { success: false, error: 'Stripe account not created' });
        
        if (rep.stripe_onboarding_status === 'complete') {
          return send(res, 200, { success: true, status: 'complete' });
        }
        
        const stripeClient = process.env.STRIPE_SECRET_KEY ? Stripe(process.env.STRIPE_SECRET_KEY) : null;
        if (!stripeClient) return send(res, 500, { success: false, error: 'Stripe not configured' });

        const account = await stripeClient.accounts.retrieve(rep.stripe_account_id);
        const isComplete = account.charges_enabled && account.details_submitted;

        if (isComplete) {
          await sql`UPDATE "Contractor" SET stripe_onboarding_status = 'complete', updated_at = NOW() WHERE id = ${repId}`;
          return send(res, 200, { success: true, status: 'complete' });
        } else {
          return send(res, 200, { success: true, status: 'pending' });
        }
      } catch (e) {
        console.error('[Stripe] Sync Error:', e.message);
        return send(res, 500, { success: false, error: e.message });
      }
    }

    // ── PATCH /api/v1/merchants/referral-code ─────────────────────────
    // Merchant-auth required. Sets referral code for resume flow.
    // Idempotent: returns success if attribution already exists.
    if (method === 'PATCH' && url.endsWith('/merchants/referral-code')) {
      const rcAuthHeader = req.headers.authorization;
      if (!rcAuthHeader || !rcAuthHeader.startsWith('Bearer ')) return send(res, 401, { success: false, error: 'Unauthorized' });
      let rcDecoded;
      try { rcDecoded = jwt.verify(rcAuthHeader.slice(7), process.env.JWT_SECRET); }
      catch (e) { return send(res, 401, { success: false, error: 'Invalid or expired token' }); }
      const rcMerchantId = rcDecoded.merchantId;
      if (!rcMerchantId) return send(res, 401, { success: false, error: 'Invalid token claims' });
      const [rcExisting] = await sql`SELECT id FROM "ContractorMerchantAttribution" WHERE merchant_id = ${rcMerchantId} LIMIT 1`;
      if (rcExisting) return send(res, 200, { success: true, message: 'Referral attribution already recorded.' });
      const rcData = req.body || {};
      const rcCode = (rcData.contractor_code || '').toUpperCase().trim();
      if (rcCode) {
        const [rcContractor] = await sql`
          SELECT id FROM "Contractor" WHERE referral_code = ${rcCode} AND status = 'active' LIMIT 1
        `;
        if (!rcContractor) return send(res, 400, { success: false, error: 'Referral code not found or inactive.' });
        await sql`
          INSERT INTO "ContractorMerchantAttribution" (id, contractor_id, merchant_id, source, created_at, updated_at)
          VALUES (gen_random_uuid()::text, ${rcContractor.id}, ${rcMerchantId}, 'self', NOW(), NOW())
          ON CONFLICT (merchant_id) DO NOTHING
        `;
      }
      // If no code (= "I don't have one"), no attribution row — correct behaviour.
      return send(res, 200, { success: true, message: 'Referral preference recorded.' });
    }

    // ── POST /api/v1/admin/contractors ────────────────────────────────
    // Create a new contractor. Auto-generates referral_code.
    if (method === 'POST' && url === '/api/v1/admin/contractors') {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const ncData = req.body || {};
      if (!ncData.full_name || !ncData.email) return send(res, 400, { success: false, error: 'full_name and email are required.' });
      if (!ncData.password) return send(res, 400, { success: false, error: 'password is required to enable portal access.' });
      if (ncData.password.length < 8) return send(res, 400, { success: false, error: 'password must be at least 8 characters.' });
      const ncEmail = ncData.email.toLowerCase().trim();
      const [ncExisting] = await sql`SELECT id FROM "Contractor" WHERE email = ${ncEmail} LIMIT 1`;
      if (ncExisting) return send(res, 409, { success: false, error: 'A contractor with this email already exists.' });
      // Generate REF-{FIRSTNAME_UP_TO_6}{LAST_INITIAL}{4_DIGITS}
      const ncParts = ncData.full_name.trim().split(/\s+/);
      const ncFirst = (ncParts[0] || 'X').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 6);
      const ncLastInit = ncParts.length > 1 ? (ncParts[ncParts.length - 1][0] || '').toUpperCase().replace(/[^A-Z]/, '') : '';
      let ncRefCode; let ncAttempts = 0;
      do {
        const ncDigits = Math.floor(10000 + Math.random() * 90000);
        ncRefCode = `REF-${ncFirst}${ncDigits}`;
        const [ncClash] = await sql`SELECT id FROM "Contractor" WHERE referral_code = ${ncRefCode} LIMIT 1`;
        if (!ncClash) break;
        ncAttempts++;
      } while (ncAttempts < 10);
      // Hash password before storing
      const ncPasswordHash = await bcrypt.hash(ncData.password, 10);
      // Validate entity_type if provided
      const validEntityTypes = ['individual', 'sole_proprietor', 'llc_single', 'llc_partnership', 's_corporation', 'c_corporation', 'other'];
      const ncEntityType = validEntityTypes.includes(ncData.entity_type) ? ncData.entity_type : null;
      const [ncContractor] = await sql`
        INSERT INTO "Contractor" (id, full_name, legal_name, email, phone, address, referral_code, status, ica_status, stripe_onboarding_status, payment_method, notes, password_hash, entity_type, created_at, updated_at)
        VALUES (
          gen_random_uuid()::text,
          ${ncData.full_name.trim()},
          ${ncData.legal_name || null},
          ${ncEmail},
          ${ncData.phone || null},
          ${ncData.address || null},
          ${ncRefCode},
          ${ncData.status || 'inactive'},
          ${ncData.ica_status || 'not_sent'},
          ${ncData.stripe_onboarding_status || 'pending'},
          ${ncData.payment_method || 'check'},
          ${ncData.notes || null},
          ${ncPasswordHash},
          ${ncEntityType},
          NOW(), NOW()
        ) RETURNING id, full_name, legal_name, email, phone, address, referral_code, status, ica_status, stripe_onboarding_status, payment_method, notes, entity_type, created_at, updated_at
      `;
      if (ncData.commission_rate !== undefined || ncData.commission_duration_months !== undefined || ncData.retainer_cents !== undefined) {
        const ncRate = Math.min(Math.max(parseFloat(ncData.commission_rate) || 0.25, 0), 0.50);
        const ncDur = Math.min(Math.max(parseInt(ncData.commission_duration_months) || 12, 1), 24);
        const ncRet = Math.max(parseInt(ncData.retainer_cents) || 0, 0);
        await sql`
          INSERT INTO "ContractorCompensationRule" (id, contractor_id, commission_rate, commission_duration_months, retainer_cents, created_at, updated_at)
          VALUES (gen_random_uuid()::text, ${ncContractor.id}, ${ncRate}, ${ncDur}, ${ncRet}, NOW(), NOW())
          ON CONFLICT (contractor_id) DO UPDATE SET commission_rate=${ncRate}, commission_duration_months=${ncDur}, retainer_cents=${ncRet}, updated_at=NOW()
        `;
      }
      return send(res, 201, { success: true, data: ncContractor });
    }

      // ── GET /api/v1/admin/contractors ─────────────────────────────────
      if (method === 'GET' && url === '/api/v1/admin/contractors') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const lcList = await sql`
          SELECT c.*, r.commission_rate, r.commission_duration_months, r.retainer_cents,
            COUNT(DISTINCT me.id)::int AS attributed_merchants,
            COUNT(DISTINCT CASE WHEN a.commission_start_date IS NOT NULL AND me.id IS NOT NULL THEN a.merchant_id END)::int AS active_attributed,
            COALESCE(SUM(CASE WHEN p.status='paid' THEN p.total_cents ELSE 0 END)::int, 0) AS total_paid_cents
          FROM "Contractor" c
          LEFT JOIN "ContractorCompensationRule" r ON r.contractor_id = c.id
          LEFT JOIN "ContractorMerchantAttribution" a ON a.contractor_id = c.id
          LEFT JOIN "Merchant" me ON me.id = a.merchant_id AND me.business_name != '[Deleted]'
          LEFT JOIN "ContractorPayout" p ON p.contractor_id = c.id
          GROUP BY c.id, r.commission_rate, r.commission_duration_months, r.retainer_cents
          ORDER BY c.created_at DESC
        `;
        return send(res, 200, { success: true, data: lcList });
      }

      // ── GET /api/v1/admin/contractors/:id ─────────────────────────────
      {
        const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)$/);
        if (m && method === 'GET') {
          if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
          const [gcRow] = await sql`
            SELECT c.*, r.commission_rate, r.commission_duration_months, r.retainer_cents
            FROM "Contractor" c
            LEFT JOIN "ContractorCompensationRule" r ON r.contractor_id = c.id
            WHERE c.id = ${m[1]}
          `;
          if (!gcRow) return send(res, 404, { success: false, error: 'Contractor not found.' });
          // Fetch related data in parallel
          const [gcMerchants, gcBonuses, gcPayouts] = await Promise.all([
            sql`SELECT a.id AS attribution_id, a.commission_start_date, a.commission_end_date,
                       a.retention_bonuses_paid, a.source, a.created_at AS attributed_at,
                       me.id AS merchant_id, me.business_name, me.subscription_tier AS tier,
                       me.billing_status, me.contact_name, me.stripe_subscription_id, me.stripe_payment_method_id, me.member_limit, me.billing_cycle,
                       (SELECT COUNT(*) FROM "MerchantMember" WHERE merchant_id = me.id) AS member_count
                FROM "ContractorMerchantAttribution" a
                JOIN "Merchant" me ON me.id = a.merchant_id
                WHERE a.contractor_id = ${m[1]}
                ORDER BY a.created_at DESC`,
            sql`SELECT * FROM "ContractorSpecialBonus" WHERE contractor_id = ${m[1]} ORDER BY created_at DESC`,
            sql`SELECT * FROM "ContractorPayout" WHERE contractor_id = ${m[1]} ORDER BY created_at DESC`,
          ]);
          // Territory and quota (new tables — defensive)
          let gcTerritory = null; let gcQuota = null;
          try {
            const [t] = await sql`SELECT * FROM "ContractorTerritory" WHERE contractor_id = ${m[1]} AND status = 'active' LIMIT 1`;
            gcTerritory = t || null;
          } catch (_) {}
          try {
            const [q] = await sql`SELECT * FROM "ContractorQuotaPeriod" WHERE contractor_id = ${m[1]} LIMIT 1`;
            if (q) {
              const [qc] = await sql`SELECT COUNT(*)::int AS cnt FROM "ContractorMerchantAttribution" a JOIN "Merchant" me ON me.id = a.merchant_id WHERE a.contractor_id = ${m[1]} AND me.billing_status NOT IN ('cancelled', 'deleted')`;
              const qCount = qc?.cnt || 0;
              gcQuota = { ...q, current_count: qCount, percent: Math.min(Math.round((qCount / (q.quota_target || 30)) * 100), 100), days_remaining: Math.max(0, Math.ceil((new Date(q.period_end) - new Date()) / 86400000)) };
            }
          } catch (_) {}
          return send(res, 200, { success: true, data: { ...gcRow, attributed_merchants: gcMerchants, special_bonuses: gcBonuses, payouts: gcPayouts, territory: gcTerritory, quota: gcQuota } });
        }
      }

      // ── PATCH /api/v1/admin/contractors/:id ───────────────────────────
      {
        const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)$/);
        if (m && method === 'PATCH') {
          if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
          const ucData = req.body || {};
          const ucValidEntityTypes = ['individual', 'sole_proprietor', 'llc_single', 'llc_partnership', 's_corporation', 'c_corporation', 'other'];
          const ucEntityType = ucData.entity_type && ucValidEntityTypes.includes(ucData.entity_type) ? ucData.entity_type : (ucData.entity_type === null ? null : undefined);
          const [ucUpdated] = await sql`
            UPDATE "Contractor"
            SET full_name      = COALESCE(${ucData.full_name || null}, full_name),
                legal_name     = COALESCE(${ucData.legal_name != null ? ucData.legal_name : null}, legal_name),
                email          = COALESCE(${ucData.email ? ucData.email.toLowerCase().trim() : null}, email),
                phone          = COALESCE(${ucData.phone != null ? ucData.phone : null}, phone),
                address        = COALESCE(${ucData.address != null ? ucData.address : null}, address),
                status         = COALESCE(${ucData.status || null}, status),
                ica_status     = COALESCE(${ucData.ica_status || null}, ica_status),
                stripe_onboarding_status = COALESCE(${ucData.stripe_onboarding_status || null}, stripe_onboarding_status),
                payment_method = COALESCE(${ucData.payment_method || null}, payment_method),
                notes          = COALESCE(${ucData.notes != null ? ucData.notes : null}, notes),
                entity_type    = COALESCE(${ucEntityType !== undefined ? ucEntityType : null}, entity_type),
                updated_at     = NOW()
            WHERE id = ${m[1]}
            RETURNING id, full_name, legal_name, email, phone, address, referral_code, status, ica_status, stripe_onboarding_status, stripe_account_id, payment_method, notes, entity_type, created_at, updated_at
          `;
          if (!ucUpdated) return send(res, 404, { success: false, error: 'Contractor not found.' });
          return send(res, 200, { success: true, data: ucUpdated });
        }
      }

    // ── GET /api/v1/admin/contractors/1099-report ─────────────────────
    // Must appear BEFORE the generic /:id route to avoid ID collision.
    if (method === 'GET' && url.startsWith('/api/v1/admin/contractors/1099-report')) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const rptYear = parseInt((new URL('http://x' + url)).searchParams.get('year')) || new Date().getFullYear();
      const rptData = await sql`
        SELECT c.id, c.full_name, c.legal_name, c.email, c.address,
          COALESCE(SUM(p.total_cents)::int, 0) AS total_paid_cents,
          COALESCE(SUM(p.total_cents)::int, 0) >= 60000 AS requires_1099
        FROM "Contractor" c
        JOIN "ContractorPayout" p ON p.contractor_id = c.id
        WHERE p.status = 'paid' AND EXTRACT(YEAR FROM p.paid_at) = ${rptYear}
        GROUP BY c.id, c.full_name, c.legal_name, c.email, c.address
        ORDER BY total_paid_cents DESC
      `;
      return send(res, 200, { success: true, year: rptYear, data: rptData });
    }

    // ── GET /api/v1/admin/contractor-payouts ──────────────────────────
    if (method === 'GET' && url.startsWith('/api/v1/admin/contractor-payouts') && !url.match(/\/(approve|mark-paid|void)$/)) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const cpQp = new URL('http://x' + url).searchParams;
      const cpContId = cpQp.get('contractor_id') || null;
      const cpStatus = cpQp.get('status') || null;
      const cpList = await sql`
        SELECT p.*, c.full_name AS contractor_name, c.stripe_onboarding_status
        FROM "ContractorPayout" p
        JOIN "Contractor" c ON c.id = p.contractor_id
        WHERE (${cpContId}::text IS NULL OR p.contractor_id = ${cpContId})
          AND (${cpStatus}::text IS NULL OR p.status = ${cpStatus})
        ORDER BY p.created_at DESC
        LIMIT 200
      `;
      return send(res, 200, { success: true, data: cpList });
    }

    // ── PATCH /api/v1/admin/contractor-payouts/:id/approve ────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractor-payouts\/([^/]+)\/approve$/);
      if (m && method === 'PATCH') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const appPayout = await sql`
          UPDATE "ContractorPayout"
          SET status = 'approved', approved_at = NOW(), updated_at = NOW()
          WHERE id = ${m[1]} AND status = 'pending'
            AND EXISTS (
              SELECT 1 FROM "Contractor" c
              WHERE c.id = "ContractorPayout".contractor_id
                AND c.stripe_onboarding_status = 'complete'
            )
          RETURNING *
        `;
        if (!appPayout[0]) return send(res, 400, { success: false, error: 'Payout not pending or contractor Stripe KYC incomplete.' });
        return send(res, 200, { success: true, data: appPayout[0] });
      }
    }

    // ── PATCH /api/v1/admin/contractor-payouts/:id/mark-paid ──────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractor-payouts\/([^/]+)\/mark-paid$/);
      if (m && method === 'PATCH') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const mpData = req.body || {};
        const mpPayout = await sql`
          UPDATE "ContractorPayout"
          SET status = 'paid',
              payment_method = ${mpData.payment_method || null},
              payment_reference = ${mpData.payment_reference || null},
              paid_at = NOW(),
              updated_at = NOW()
          WHERE id = ${m[1]} AND status = 'approved'
            AND EXISTS (
              SELECT 1 FROM "Contractor" c
              WHERE c.id = "ContractorPayout".contractor_id
                AND c.stripe_onboarding_status = 'complete'
            )
          RETURNING *
        `;
        if (!mpPayout[0]) return send(res, 400, { success: false, error: 'Payout not approved or contractor Stripe KYC incomplete.' });
        const mp = mpPayout[0];
        const mpYear = new Date().getFullYear();
        const mpBonusCents = mp.milestone_bonus_cents + mp.retention_bonus_cents + mp.special_bonus_cents;
        await sql`
          INSERT INTO "ContractorEarningsSummary" (id, contractor_id, year, commission_ytd_cents, retainer_ytd_cents, bonus_ytd_cents, total_ytd_cents, updated_at)
          VALUES (gen_random_uuid()::text, ${mp.contractor_id}, ${mpYear}, ${mp.commission_cents}, ${mp.retainer_cents}, ${mpBonusCents}, ${mp.total_cents}, NOW())
          ON CONFLICT (contractor_id, year) DO UPDATE SET
            commission_ytd_cents = "ContractorEarningsSummary".commission_ytd_cents + ${mp.commission_cents},
            retainer_ytd_cents   = "ContractorEarningsSummary".retainer_ytd_cents   + ${mp.retainer_cents},
            bonus_ytd_cents      = "ContractorEarningsSummary".bonus_ytd_cents      + ${mpBonusCents},
            total_ytd_cents      = "ContractorEarningsSummary".total_ytd_cents      + ${mp.total_cents},
            updated_at           = NOW()
        `;
        return send(res, 200, { success: true, data: mp });
      }
    }

    // ── PATCH /api/v1/admin/contractor-payouts/:id/void ───────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractor-payouts\/([^/]+)\/void$/);
      if (m && method === 'PATCH') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const vpPayout = await sql`
          UPDATE "ContractorPayout"
          SET status = 'voided', updated_at = NOW()
          WHERE id = ${m[1]} AND status IN ('pending', 'approved')
          RETURNING *
        `;
        if (!vpPayout[0]) return send(res, 404, { success: false, error: 'Payout not found or already paid/voided.' });
        return send(res, 200, { success: true, data: vpPayout[0] });
      }
    }

    // ── GET /api/v1/admin/milestone-config ────────────────────────────
    if (method === 'GET' && url === '/api/v1/admin/milestone-config') {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const mcTiers = await sql`SELECT * FROM "SystemMilestoneConfig" ORDER BY threshold ASC`;
      return send(res, 200, { success: true, data: mcTiers });
    }

    // ── PATCH /api/v1/admin/milestone-config/:id ──────────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/milestone-config\/([^/]+)$/);
      if (m && method === 'PATCH') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const mcData = req.body || {};
        const mcUpdated = await sql`
          UPDATE "SystemMilestoneConfig"
          SET bonus_cents = COALESCE(${mcData.bonus_cents != null ? mcData.bonus_cents : null}, bonus_cents),
              label       = COALESCE(${mcData.label || null}, label),
              is_active   = COALESCE(${mcData.is_active != null ? mcData.is_active : null}, is_active),
              updated_at  = NOW()
          WHERE id = ${m[1]}
          RETURNING *
        `;
        if (!mcUpdated[0]) return send(res, 404, { success: false, error: 'Milestone config not found.' });
        return send(res, 200, { success: true, data: mcUpdated[0] });
      }
    }

    // ── POST /api/v1/admin/contractors/:id/compensation ───────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/compensation$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const compData = req.body || {};
        const compRate = Math.min(Math.max(parseFloat(compData.commission_rate) || 0.25, 0), 0.50);
        const compDur  = Math.min(Math.max(parseInt(compData.commission_duration_months) || 12, 1), 24);
        const compRet  = Math.max(parseInt(compData.retainer_cents) || 0, 0);
        const [compRule] = await sql`
          INSERT INTO "ContractorCompensationRule" (id, contractor_id, commission_rate, commission_duration_months, retainer_cents, created_at, updated_at)
          VALUES (gen_random_uuid()::text, ${m[1]}, ${compRate}, ${compDur}, ${compRet}, NOW(), NOW())
          ON CONFLICT (contractor_id) DO UPDATE SET
            commission_rate = ${compRate},
            commission_duration_months = ${compDur},
            retainer_cents = ${compRet},
            updated_at = NOW()
          RETURNING *
        `;
        return send(res, 200, { success: true, data: compRule });
      }
    }

    // ── GET /api/v1/admin/contractors/:id/merchants ───────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/merchants$/);
      if (m && method === 'GET') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const cmMerchants = await sql`
          SELECT a.id AS attribution_id, a.commission_start_date, a.commission_end_date,
                 a.retention_bonuses_paid, a.source, a.created_at AS attributed_at,
                 me.id AS merchant_id, me.business_name, me.subscription_tier,
                 me.billing_status, me.contact_name, mu.email AS merchant_email,
                 (SELECT COUNT(*) FROM "MerchantMember" WHERE merchant_id = me.id) AS member_count
          FROM "ContractorMerchantAttribution" a
          JOIN "Merchant" me ON me.id = a.merchant_id
          LEFT JOIN "MerchantUser" mu ON mu.merchant_id = me.id AND mu.role = 'owner'
          WHERE a.contractor_id = ${m[1]}
          ORDER BY a.created_at DESC
        `;
        return send(res, 200, { success: true, data: cmMerchants });
      }
    }

    // ── POST /api/v1/admin/contractors/attribute-merchant ─────────────
    if (method === 'POST' && url.endsWith('/admin/contractors/attribute-merchant')) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const amData = req.body || {};
      if (!amData.contractor_id || !amData.merchant_id) return send(res, 400, { success: false, error: 'contractor_id and merchant_id are required.' });
      const [amContr] = await sql`SELECT id FROM "Contractor" WHERE id = ${amData.contractor_id} LIMIT 1`;
      if (!amContr) return send(res, 404, { success: false, error: 'Contractor not found.' });
      const [amMerch] = await sql`SELECT id FROM "Merchant" WHERE id = ${amData.merchant_id} LIMIT 1`;
      if (!amMerch) return send(res, 404, { success: false, error: 'Merchant not found.' });
      const amAttr = await sql`
        INSERT INTO "ContractorMerchantAttribution" (id, contractor_id, merchant_id, source, created_at, updated_at)
        VALUES (gen_random_uuid()::text, ${amData.contractor_id}, ${amData.merchant_id}, 'manual', NOW(), NOW())
        ON CONFLICT (merchant_id) DO NOTHING
        RETURNING *
      `;
      if (!amAttr[0]) return send(res, 409, { success: false, error: 'This merchant is already attributed to a contractor.' });
      return send(res, 201, { success: true, data: amAttr[0] });
    }

    // ── DELETE /api/v1/admin/contractors/attribute-merchant/:merchant_id
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/attribute-merchant\/([^/]+)$/);
      if (m && method === 'DELETE') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const dmDeleted = await sql`
          DELETE FROM "ContractorMerchantAttribution"
          WHERE merchant_id = ${m[1]} AND commission_start_date IS NULL
          RETURNING id
        `;
      if (!dmDeleted[0]) return send(res, 400, { success: false, error: 'Attribution not found or commission has already started — cannot remove.' });
        return send(res, 200, { success: true, message: 'Attribution removed.' });
      }
    }

    // ── GET /api/v1/admin/merchants/:id/attribution ───────────────────
    // Returns the contractor/rep info attributed to a specific merchant.
    {
      const m = url.match(/^\/api\/v1\/admin\/merchants\/([^/]+)\/attribution$/);
      if (m && method === 'GET') {
        // if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const [attr] = await sql`
          SELECT
            a.id            AS attribution_id,
            a.contractor_id,
            a.merchant_id,
            a.source,
            a.commission_start_date,
            a.commission_end_date,
            a.retention_bonuses_paid,
            a.created_at    AS attributed_at,
            c.full_name     AS contractor_name,
            c.referral_code,
            c.email         AS contractor_email,
            c.phone         AS contractor_phone,
            c.status        AS contractor_status,
            c.created_at    AS contractor_start_date,
            r.commission_rate,
            r.commission_duration_months,
            r.retainer_cents
          FROM "ContractorMerchantAttribution" a
          JOIN "Contractor"                   c ON c.id = a.contractor_id
          LEFT JOIN "ContractorCompensationRule" r ON r.contractor_id = a.contractor_id
          WHERE a.merchant_id = ${m[1]}
          LIMIT 1
        `;
        if (!attr) return send(res, 200, { success: true, data: null }); // no attribution = unattributed
        return send(res, 200, { success: true, data: attr });
      }
    }

    // ── POST /api/v1/admin/contractors/:id/special-bonus ─────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/special-bonus$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const sbData = req.body || {};
        if (!sbData.label || !sbData.amount_cents) return send(res, 400, { success: false, error: 'label and amount_cents are required.' });
        const sbAmt = parseInt(sbData.amount_cents);
        if (sbAmt <= 0 || sbAmt > 200000) return send(res, 400, { success: false, error: 'amount_cents must be between 1 and 200000 (max $2,000).' });
        const [sbBonus] = await sql`
          INSERT INTO "ContractorSpecialBonus" (id, contractor_id, label, amount_cents, reason, status, created_at, updated_at)
          VALUES (gen_random_uuid()::text, ${m[1]}, ${sbData.label}, ${sbAmt}, ${sbData.reason || null}, 'pending', NOW(), NOW())
          RETURNING *
        `;
        return send(res, 201, { success: true, data: sbBonus });
      }
    }

    // ── GET /api/v1/admin/contractors/:id/special-bonuses ────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/special-bonuses$/);
      if (m && method === 'GET') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const sbList = await sql`
          SELECT * FROM "ContractorSpecialBonus"
          WHERE contractor_id = ${m[1]}
          ORDER BY created_at DESC
        `;
        return send(res, 200, { success: true, data: sbList });
      }
    }

    // ── PATCH /api/v1/admin/contractors/:id/special-bonuses/:bid ──────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/special-bonuses\/([^/]+)$/);
      if (m && method === 'PATCH') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const sbpData = req.body || {};
        const sbpUpdated = await sql`
          UPDATE "ContractorSpecialBonus"
          SET status     = COALESCE(${sbpData.status || null}, status),
              label      = COALESCE(${sbpData.label || null}, label),
              reason     = COALESCE(${sbpData.reason != null ? sbpData.reason : null}, reason),
              updated_at = NOW()
          WHERE id = ${m[2]} AND contractor_id = ${m[1]} AND status = 'pending'
          RETURNING *
        `;
        if (!sbpUpdated[0]) return send(res, 404, { success: false, error: 'Bonus not found or already processed.' });
        return send(res, 200, { success: true, data: sbpUpdated[0] });
      }
    }

    // ── POST /api/v1/admin/contractors/run-payouts ────────────────────
    // Manual payout calculation trigger (same algorithm as the monthly cron).
    if (method === 'POST' && url.endsWith('/admin/contractors/run-payouts')) {
      if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
      const rpInput = req.body || {};
      let rpStart, rpEnd;
      if (rpInput.period_start && rpInput.period_end) {
        rpStart = new Date(rpInput.period_start);
        rpEnd   = new Date(rpInput.period_end);
      } else {
        const rpNow = new Date();
        rpStart = new Date(rpNow.getFullYear(), rpNow.getMonth() - 1, 1);
        rpEnd   = new Date(rpNow.getFullYear(), rpNow.getMonth(), 0);
      }
      const rpPs = rpStart.toISOString().slice(0, 10);
      const rpPe = rpEnd.toISOString().slice(0, 10);
      const rpContractors = await sql`
        SELECT c.id, c.full_name, c.stripe_onboarding_status,
               r.commission_rate, r.commission_duration_months, r.retainer_cents
        FROM "Contractor" c
        JOIN "ContractorCompensationRule" r ON r.contractor_id = c.id
        WHERE c.status = 'active'
      `;
      const rpResults = [];
      for (const rc of rpContractors) {
        const rpAttrs = await sql`
          SELECT a.id, a.merchant_id, a.commission_start_date, a.commission_end_date, a.retention_bonuses_paid
          FROM "ContractorMerchantAttribution" a
          JOIN "Merchant" m ON m.id = a.merchant_id
          WHERE a.contractor_id = ${rc.id}
            AND a.commission_start_date IS NOT NULL
            AND a.commission_start_date::date <= ${rpPe}
            AND (a.commission_end_date IS NULL OR a.commission_end_date::date >= ${rpPs})
            AND m.subscription_tier NOT IN ('free_for_life', 'trial', 'free')
        `;
        let rpComm = 0; let rpRet = 0; const rpMb = [];
        for (const ra of rpAttrs) {
          const rpInvs = await sql`
            SELECT amount_cents FROM "Invoice"
            WHERE merchant_id = ${ra.merchant_id}
              AND paid_at >= ${rpStart} AND paid_at <= ${rpEnd}
              AND amount_cents > 0 AND status = 'paid'
          `;
          const rpInvTotal = rpInvs.reduce((s, i) => s + i.amount_cents, 0);
          const rpMerchComm = Math.round(rpInvTotal * parseFloat(rc.commission_rate));
          rpComm += rpMerchComm;
          const raStart = new Date(ra.commission_start_date);
          const raMos = Math.floor((rpEnd - raStart) / (30 * 24 * 60 * 60 * 1000));
          let raMerchRet = 0; let raNewRet = ra.retention_bonuses_paid;
          if (rpInvTotal > 0) {
            if (raMos >= 3 && raNewRet < 1) { raMerchRet += rpMerchComm; raNewRet = 1; }
            if (raMos >= 6 && raNewRet < 2) { raMerchRet += rpMerchComm; raNewRet = 2; }
          }
          rpRet += raMerchRet;
          if (raNewRet > ra.retention_bonuses_paid) {
            await sql`UPDATE "ContractorMerchantAttribution" SET retention_bonuses_paid = ${raNewRet}, updated_at = NOW() WHERE id = ${ra.id}`;
          }
          if (rpMerchComm > 0 || raMerchRet > 0) rpMb.push({ merchant_id: ra.merchant_id, invoice_total: rpInvTotal, commission: rpMerchComm, retention_bonus: raMerchRet, months_elapsed: raMos });
        }
        const [rpSubRow] = await sql`
          SELECT COUNT(DISTINCT a2.merchant_id)::int AS cnt
          FROM "ContractorMerchantAttribution" a2
          JOIN "Merchant" m2 ON m2.id = a2.merchant_id
          WHERE a2.contractor_id = ${rc.id}
            AND a2.commission_start_date IS NOT NULL
            AND m2.subscription_tier NOT IN ('free_for_life', 'trial', 'free')
            AND m2.billing_status NOT IN ('cancelled', 'deleted')
        `;
        const rpSubCount = rpSubRow?.cnt || 0;
        const rpMilestones = await sql`
          SELECT mc.id, mc.bonus_cents, mc.label
          FROM "SystemMilestoneConfig" mc
          WHERE mc.is_active = true AND mc.threshold <= ${rpSubCount}
            AND NOT EXISTS (SELECT 1 FROM "ContractorMilestoneRecord" mr WHERE mr.contractor_id = ${rc.id} AND mr.milestone_id = mc.id)
          ORDER BY mc.threshold ASC
        `;
        const rpMilCents = rpMilestones.reduce((s, mm) => s + mm.bonus_cents, 0);
        const rpSpecBonuses = await sql`
          SELECT id, amount_cents, label FROM "ContractorSpecialBonus"
          WHERE contractor_id = ${rc.id} AND status = 'pending'
        `;
        const rpSpecCents = rpSpecBonuses.reduce((s, b) => s + b.amount_cents, 0);
        const rpRetainer = parseInt(rc.retainer_cents) || 0;
        const rpTotal = rpComm + rpRetainer + rpMilCents + rpRet + rpSpecCents;
        if (rpTotal > 0 && rc.stripe_onboarding_status === 'complete') {
          const [rpPayout] = await sql`
            INSERT INTO "ContractorPayout" (
              id, contractor_id, period_start, period_end,
              commission_cents, retainer_cents, milestone_bonus_cents,
              retention_bonus_cents, special_bonus_cents, total_cents,
              breakdown, status, created_at, updated_at
            ) VALUES (
              gen_random_uuid()::text, ${rc.id}, ${rpPs}, ${rpPe},
              ${rpComm}, ${rpRetainer}, ${rpMilCents},
              ${rpRet}, ${rpSpecCents}, ${rpTotal},
              ${JSON.stringify({ active_subscribers: rpSubCount, merchant_breakdown: rpMb, milestones: rpMilestones.map(mm => mm.label), special_bonuses: rpSpecBonuses.map(b => b.label) })},
              'pending', NOW(), NOW()
            ) RETURNING id
          `;
          for (const rpMs of rpMilestones) {
            await sql`
              INSERT INTO "ContractorMilestoneRecord" (id, contractor_id, milestone_id, payout_id, earned_at)
              VALUES (gen_random_uuid()::text, ${rc.id}, ${rpMs.id}, ${rpPayout.id}, NOW())
              ON CONFLICT (contractor_id, milestone_id) DO NOTHING
            `;
          }
          if (rpSpecBonuses.length > 0) {
            const rpSpecIds = rpSpecBonuses.map(b => b.id);
            await sql`UPDATE "ContractorSpecialBonus" SET status = 'paid', payout_id = ${rpPayout.id}, updated_at = NOW() WHERE id = ANY(${rpSpecIds})`;
          }
          rpResults.push({ contractor_id: rc.id, name: rc.full_name, payout_id: rpPayout.id, total_cents: rpTotal });
        }
      }
      return send(res, 200, { success: true, data: { payouts_created: rpResults.length, period: `${rpPs} to ${rpPe}`, results: rpResults } });
    }
    // ── PATCH /api/v1/admin/contractors/:id/manual-kyc ─────────────────
    // Manually marks the Stripe KYC process as complete (fallback).
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/manual-kyc$/);
      if (m && method === 'PATCH') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const contractorId = m[1];
        try {
          const [ctr] = await sql`UPDATE "Contractor" SET stripe_onboarding_status = 'complete', updated_at = NOW() WHERE id = ${contractorId} RETURNING *`;
          if (!ctr) return send(res, 404, { success: false, error: 'Contractor not found.' });
          return send(res, 200, { success: true, data: ctr });
        } catch (err) {
          console.error(err);
          return send(res, 500, { success: false, error: 'Failed to update KYC status.' });
        }
      }
    }


    // ── POST /api/v1/admin/contractors/:id/send-invite ─────────────────
    // Generates a password reset / portal invite link and simulates sending an email
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/send-invite$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const contractorId = m[1];
        
        const inviteToken = require('crypto').randomBytes(32).toString('hex');
        
        await sql`
          UPDATE "Contractor"
          SET invite_token = ${inviteToken},
              invite_expires_at = NOW() + INTERVAL '48 hours'
          WHERE id = ${contractorId}
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
            
            // Get rep email and name
            const [repRecord] = await sql`SELECT email, full_name FROM "Contractor" WHERE id = ${contractorId}`;
            if (repRecord && repRecord.email) {
              sendSmtpEmail.to = [{ email: repRecord.email }];
              sendSmtpEmail.subject = 'Your Perkfinity Rep Portal Access';
              
              // We'll point this to the rep portal login using the current environment's origin
              const origin = req.headers.origin || 'https://perkfinity.net';
              const inviteLink = `${origin}/reps/index.html?token=${inviteToken}`;
              
              sendSmtpEmail.htmlContent = `
                <div style="font-family:'Helvetica Neue',Arial,sans-serif; max-width:520px; margin:0 auto; background:#ffffff; border-radius:16px; overflow:hidden; border:1px solid #eee;">
                  <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf); padding:28px 24px; text-align:center;">
                    <div style="color:#fff; font-size:24px; font-weight:800;">Perkfinity</div>
                  </div>
                  <div style="padding:28px 24px;">
                    <div style="font-size:20px; font-weight:700; color:#1a1a2e; margin-bottom:16px;">Welcome to the Perkfinity Rep Portal</div>
                    <p style="font-size:15px; color:#555; line-height:1.6; margin-bottom:24px;">
                      Hi ${repRecord.full_name},<br><br>
                      An invite or password reset link has been generated for your Sales Rep account. Click the button below to access your portal. This link expires in 48 hours.
                    </p>
                    <div style="text-align:center; margin-bottom:24px;">
                      <a href="${inviteLink}" style="display:inline-block; background:#5b3fa5; color:#fff; font-weight:600; text-decoration:none; padding:14px 28px; border-radius:10px;">Access Portal</a>
                    </div>
                  </div>
                </div>
              `;
              
              await emailApi.sendTransacEmail(sendSmtpEmail);
              console.log(`[Brevo] Portal invite sent to ${repRecord.email}`);
            }
          } catch (brevoErr) {
            console.error('Brevo rep invite email failed:', brevoErr.message || brevoErr);
          }
        } else {
          console.log(`[Email Simulator] Portal Invite Link for rep ${contractorId}:`);
          console.log(`http://localhost:8080/reps/index.html?token=${inviteToken}`);
        }
        
        return send(res, 200, { success: true, message: 'Portal invite sent successfully.' });
      }
    }

    // ── POST /api/v1/admin/contractors/:id/send-ica ────────────────────
    // Generates a personalised ICA PDF and sends it via Dropbox Sign for e-signature.
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/send-ica$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const contractorId = m[1];
        const [ctr] = await sql`
          SELECT c.*, r.commission_rate, r.commission_duration_months, r.retainer_cents
          FROM "Contractor" c
          LEFT JOIN "ContractorCompensationRule" r ON r.contractor_id = c.id
          WHERE c.id = ${contractorId}
          LIMIT 1
        `;
        if (!ctr) return send(res, 404, { success: false, error: 'Contractor not found.' });
        if (ctr.ica_status === 'signed') {
          return send(res, 400, { success: false, error: 'ICA already fully signed — cannot resend.' });
        }
        const data = req.body || {};
        const companySignatory = data.company_signatory || null;
        await sql`
          UPDATE "Contractor"
          SET ica_status = 'sent', updated_at = NOW(), ica_company_signatory = ${companySignatory}
          WHERE id = ${contractorId}
        `;

        const BREVO_KEY = process.env.BREVO_API_KEY;
        if (BREVO_KEY) {
          try {
            const SibApiV3Sdk = require('sib-api-v3-sdk');
            const brevoClient = SibApiV3Sdk.ApiClient.instance;
            brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
            const emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
            const sendSmtpEmail = new SibApiV3Sdk.SendSmtpEmail();
            sendSmtpEmail.sender = { name: 'Perkfinity', email: 'noreply@perkfinity.net' };
            sendSmtpEmail.to = [{ email: ctr.email }];
            sendSmtpEmail.subject = 'Action Required: Sign Your Independent Contractor Agreement';
            sendSmtpEmail.htmlContent = `
              <div style="font-family:sans-serif;color:#1e2035;max-width:600px;margin:0 auto;">
                <h2>Hello ${ctr.full_name},</h2>
                <p>Your Independent Contractor Agreement (ICA) has been generated and is ready for your signature.</p>
                <p>Please log in to your Perkfinity Rep Portal to review the terms and electronically sign the agreement.</p>
                <p>Your portal link: <a href="https://perkfinity.net/reps/" style="color:#5b3fa5;">https://perkfinity.net/reps/</a></p>
                <br>
                <p>Thank you,<br>The Perkfinity Team</p>
              </div>
            `;
            await emailApi.sendTransacEmail(sendSmtpEmail);
            console.log(`[send-ica] Notification email sent to ${ctr.email}`);
          } catch(e) {
            console.error('[send-ica] Brevo email error:', e);
          }
        }
        
        return send(res, 200, { success: true, message: `ICA sent. Contractor notified via email.` });
      }
    }

    // ══════════════════════════════════════════════════════════════════
    // TERRITORY ENDPOINTS
    // ══════════════════════════════════════════════════════════════════

    // ── GET /api/v1/admin/contractors/:id/ica-pdf ─────────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/ica-pdf$/);
      if (m && method === 'GET') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const contractorId = m[1];
        const [ctr] = await sql`
          SELECT c.*, r.commission_rate, r.commission_duration_months, r.retainer_cents
          FROM "Contractor" c
          LEFT JOIN "ContractorCompensationRule" r ON r.contractor_id = c.id
          WHERE c.id = ${contractorId} LIMIT 1
        `;
        if (!ctr) return send(res, 404, { success: false, error: 'Contractor not found.' });
        
        const [terr] = await sql`SELECT zip_codes FROM "ContractorTerritory" WHERE contractor_id = ${contractorId} AND status = 'active' LIMIT 1`;

        const { generateICAPdf } = require('./lib/generate-ica.js');
        const pdfBuffer = await generateICAPdf({
          contractorName: ctr.legal_name || ctr.full_name,
          contractorEmail: ctr.email,
          agreementDate: ctr.ica_status === 'signed' ? ctr.updated_at : new Date(),
          territoryZips: terr && terr.zip_codes ? terr.zip_codes : [], 
          commissionRate: ctr.commission_rate || 15,
          commissionDurationMonths: ctr.commission_duration_months || 12,
          retainerAmount: (ctr.retainer_cents || 0) / 100,
          isSigned: ctr.ica_status === 'signed',
          signatureName: ctr.full_name,
          companySignatory: ctr.ica_company_signatory,
          signedDate: ctr.updated_at
        });

        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `inline; filename="ICA_${ctr.full_name.replace(/\s+/g, '_')}.pdf"`);
        return res.end(pdfBuffer);
      }
    }

    // ── GET /api/v1/admin/contractors/:id/territory ───────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/territory$/);
      if (m && method === 'GET') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const [terr] = await sql`SELECT * FROM "ContractorTerritory" WHERE contractor_id = ${m[1]} AND status = 'active' LIMIT 1`;
        return send(res, 200, { success: true, data: terr || null });
      }
    }

    // ── POST /api/v1/admin/contractors/:id/territory ──────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/territory$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const { label, zip_codes } = req.body || {};
        if (!label || !Array.isArray(zip_codes) || zip_codes.length === 0)
          return send(res, 400, { success: false, error: 'label and zip_codes (non-empty array) are required.' });
        const cleanZips = zip_codes.map(z => String(z).trim()).filter(Boolean);
        // Check for overlaps with other active reps
        const terrOverlaps = await sql`
          SELECT t.label AS territory_label, t.zip_codes, c.full_name AS contractor_name
          FROM "ContractorTerritory" t
          JOIN "Contractor" c ON c.id = t.contractor_id
          WHERE t.status = 'active' AND t.contractor_id != ${m[1]} AND t.zip_codes && ${cleanZips}
        `;
        // Revoke existing active territory for this rep
        await sql`UPDATE "ContractorTerritory" SET status = 'revoked', updated_at = NOW() WHERE contractor_id = ${m[1]} AND status = 'active'`;
        // Insert new territory
        const [newTerr] = await sql`
          INSERT INTO "ContractorTerritory" (id, contractor_id, label, zip_codes, status, assigned_at, updated_at)
          VALUES (gen_random_uuid()::text, ${m[1]}, ${label}, ${cleanZips}, 'active', NOW(), NOW())
          RETURNING *
        `;
        return send(res, 201, { success: true, data: newTerr, overlap_warning: terrOverlaps.length > 0 ? terrOverlaps : null });
      }
    }

    // ── DELETE /api/v1/admin/contractors/:id/territory ────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/territory$/);
      if (m && method === 'DELETE') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        await sql`UPDATE "ContractorTerritory" SET status = 'revoked', updated_at = NOW() WHERE contractor_id = ${m[1]} AND status = 'active'`;
        return send(res, 200, { success: true, message: 'Territory revoked.' });
      }
    }

    // ══════════════════════════════════════════════════════════════════
    // QUOTA ENDPOINTS — ORDER MATTERS: more specific paths first
    // ══════════════════════════════════════════════════════════════════

    // ── POST /api/v1/admin/contractors/:id/quota/evaluate ─────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/quota\/evaluate$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const [qEval] = await sql`SELECT * FROM "ContractorQuotaPeriod" WHERE contractor_id = ${m[1]} LIMIT 1`;
        if (!qEval) return send(res, 404, { success: false, error: 'No quota period found for this rep.' });
        if (qEval.status !== 'active') return send(res, 400, { success: false, error: `Quota is already ${qEval.status}.` });
        const [qEvalCount] = await sql`SELECT COUNT(*)::int AS cnt FROM "ContractorMerchantAttribution" a JOIN "Merchant" me ON me.id = a.merchant_id WHERE a.contractor_id = ${m[1]} AND me.billing_status NOT IN ('cancelled', 'deleted')`;
        const qCount = qEvalCount?.cnt || 0;
        const qNow = new Date(); const qEnd = new Date(qEval.period_end);
        if (qCount >= qEval.quota_target) {
          // Ongoing Quota Logic: Roll forward 3 months and add dynamic target
          const [terr] = await sql`SELECT zip_codes FROM "ContractorTerritory" WHERE contractor_id = ${m[1]} AND status = 'active' LIMIT 1`;
          const numZips = terr && Array.isArray(terr.zip_codes) && terr.zip_codes.length > 0 ? terr.zip_codes.length : 1;
          const ongoingQuota = Math.max(10, numZips * 5);
          
          await sql`
            UPDATE "ContractorQuotaPeriod" 
            SET period_end = period_end + INTERVAL '3 months', 
                quota_target = quota_target + ${ongoingQuota},
                status = 'active',
                alert_sent = false,
                updated_at = NOW() 
            WHERE id = ${qEval.id}
          `;
          return send(res, 200, { success: true, result: 'rolled_forward', current_count: qCount, quota_target: qEval.quota_target + ongoingQuota });
        } else if (qNow > qEnd) {
          // Missed Quota: Revoke territory
          await sql`UPDATE "ContractorQuotaPeriod" SET status = 'missed', alert_sent = true, updated_at = NOW() WHERE id = ${qEval.id}`;
          await sql`DELETE FROM "ContractorTerritory" WHERE contractor_id = ${qEval.contractor_id}`;
          return send(res, 200, { success: true, result: 'missed', current_count: qCount, quota_target: qEval.quota_target });
        } else {
          return send(res, 200, { success: true, result: 'active', current_count: qCount, quota_target: qEval.quota_target, days_remaining: Math.ceil((qEnd - qNow) / 86400000) });
        }
      }
    }

    // ── POST /api/v1/admin/contractors/:id/quota/extend ───────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/quota\/extend$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const daysToAdd = Math.min(Math.max(parseInt((req.body || {}).days) || 30, 1), 180);
        const [qExt] = await sql`SELECT * FROM "ContractorQuotaPeriod" WHERE contractor_id = ${m[1]} LIMIT 1`;
        if (!qExt) return send(res, 404, { success: false, error: 'No quota period found.' });
        const [qExtUpdated] = await sql`
          UPDATE "ContractorQuotaPeriod"
          SET period_end = period_end + (${daysToAdd} || ' days')::INTERVAL,
              status = 'active', updated_at = NOW()
          WHERE id = ${qExt.id}
          RETURNING *
        `;
        return send(res, 200, { success: true, data: qExtUpdated });
      }
    }

    // ── POST /api/v1/admin/contractors/:id/quota/send-reminder ────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/quota\/send-reminder$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const [qRemRep] = await sql`SELECT full_name, email FROM "Contractor" WHERE id = ${m[1]} LIMIT 1`;
        if (!qRemRep) return send(res, 404, { success: false, error: 'Contractor not found.' });
        const [qRemQuota] = await sql`SELECT * FROM "ContractorQuotaPeriod" WHERE contractor_id = ${m[1]} LIMIT 1`;
        const [qRemCount] = await sql`SELECT COUNT(*)::int AS cnt FROM "ContractorMerchantAttribution" a JOIN "Merchant" me ON me.id = a.merchant_id WHERE a.contractor_id = ${m[1]} AND me.billing_status NOT IN ('cancelled', 'deleted')`;
        const qRCount = qRemCount?.cnt || 0; const qTarget = qRemQuota?.quota_target || 30;
        const qEnd = qRemQuota ? new Date(qRemQuota.period_end) : null;
        const qDaysLeft = qEnd ? Math.max(0, Math.ceil((qEnd - new Date()) / 86400000)) : null;
        console.log(`[quota-reminder] Reminder for ${qRemRep.full_name} (${qRemRep.email}): ${qRCount}/${qTarget} merchants, ${qDaysLeft} days left`);
        // Mark alert_sent so we don't double-remind automatically
        if (qRemQuota) await sql`UPDATE "ContractorQuotaPeriod" SET alert_sent = true, updated_at = NOW() WHERE id = ${qRemQuota.id}`;
        // TODO: Wire to email API (Brevo/SendGrid) — same pattern as W-9 request
        return send(res, 200, { success: true, message: `Reminder logged for ${qRemRep.email}. Email integration: coming soon.`, rep: { full_name: qRemRep.full_name, email: qRemRep.email, current_count: qRCount, quota_target: qTarget, days_remaining: qDaysLeft } });
      }
    }

    // ── GET /api/v1/admin/contractors/:id/quota ───────────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/quota$/);
      if (m && method === 'GET') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const [qGet] = await sql`SELECT * FROM "ContractorQuotaPeriod" WHERE contractor_id = ${m[1]} LIMIT 1`;
        if (!qGet) return send(res, 200, { success: true, data: null });
        const [qGetCount] = await sql`SELECT COUNT(*)::int AS cnt FROM "ContractorMerchantAttribution" a JOIN "Merchant" me ON me.id = a.merchant_id WHERE a.contractor_id = ${m[1]} AND me.billing_status NOT IN ('cancelled', 'deleted')`;
        const qGCount = qGetCount?.cnt || 0;
        const qGNow = new Date(); const qGEnd = new Date(qGet.period_end);
        // Auto-evaluate if period elapsed and still active
        let qGStatus = qGet.status;
        if (qGStatus === 'active' && qGNow > qGEnd) {
          if (qGCount >= qGet.quota_target) {
            await sql`UPDATE "ContractorQuotaPeriod" SET status = 'met', locked_at = NOW(), updated_at = NOW() WHERE id = ${qGet.id}`;
            qGStatus = 'met';
          } else {
            await sql`UPDATE "ContractorQuotaPeriod" SET status = 'missed', alert_sent = true, updated_at = NOW() WHERE id = ${qGet.id}`;
            qGStatus = 'missed';
          }
        }
        return send(res, 200, { success: true, data: { ...qGet, status: qGStatus, current_count: qGCount, percent: Math.min(Math.round((qGCount / (qGet.quota_target || 30)) * 100), 100), days_remaining: Math.max(0, Math.ceil((qGEnd - qGNow) / 86400000)) } });
      }
    }

    // ── POST /api/v1/admin/contractors/:id/quota ──────────────────────
    {
      const m = url.match(/^\/api\/v1\/admin\/contractors\/([^/]+)\/quota$/);
      if (m && method === 'POST') {
        if (!verifyAdminAuth(req)) return send(res, 401, { success: false, error: 'Unauthorized' });
        const [qExisting] = await sql`SELECT id FROM "ContractorQuotaPeriod" WHERE contractor_id = ${m[1]} LIMIT 1`;
        if (qExisting) return send(res, 409, { success: false, error: 'A quota period already exists. Use /extend or /evaluate to manage it.' });
        const [qRepCheck] = await sql`SELECT id FROM "Contractor" WHERE id = ${m[1]} LIMIT 1`;
        if (!qRepCheck) return send(res, 404, { success: false, error: 'Contractor not found.' });
        
        const [terr] = await sql`SELECT zip_codes FROM "ContractorTerritory" WHERE contractor_id = ${m[1]} AND status = 'active' LIMIT 1`;
        const numZips = terr && Array.isArray(terr.zip_codes) && terr.zip_codes.length > 0 ? terr.zip_codes.length : 1;
        const dynamicTarget = Math.max(20, numZips * 10);
        
        const qTarget = parseInt((req.body || {}).quota_target) || dynamicTarget;
        const [qNew] = await sql`
          INSERT INTO "ContractorQuotaPeriod" (id, contractor_id, period_start, period_end, quota_target, status, created_at, updated_at)
          VALUES (gen_random_uuid()::text, ${m[1]}, CURRENT_DATE, CURRENT_DATE + INTERVAL '3 months', ${qTarget}, 'active', NOW(), NOW())
          RETURNING *
        `;
        return send(res, 201, { success: true, data: qNew });
      }
    }





    return send(res, 404, { success: false, error: `No route: ${method} ${url}` });


  } catch (err) {
    // Detect PostgreSQL / Neon driver errors by SQLSTATE code or known message patterns.
    // Raw DB schema information (column names, table names, constraint details) must
    // never reach the client — it leaks internal structure and confuses users.
    const _rawMsg = err.message || '';
    const _isDbError = (
      (err.code && /^[0-9A-Z]{5}$/.test(String(err.code))) ||
      /column|relation|table|syntax error|null value|does not exist|violates|permission denied|ssl|tcp/i.test(_rawMsg)
    );
    // Always log the full details server-side for debugging.
    console.error('[perkfinity]', {
      message: _rawMsg,
      code: err.code || 'N/A',
      db: process.env.DATABASE_URL ? 'SET' : 'MISSING',
      jwt: process.env.JWT_SECRET ? 'SET' : 'MISSING',
    });
    return send(res, 500, {
      success: false,
      error: _isDbError
        ? 'An unexpected error occurred. Please try again shortly.'
        : (_rawMsg || 'An unexpected error occurred.'),
    });
  }
};
