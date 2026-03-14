/**
 * Perkfinity Backend — Vercel Serverless API Handler
 * Single CommonJS handler — no framework, no compilation needed.
 * Stack: @prisma/client (Neon PostgreSQL) + bcryptjs + jsonwebtoken
 */

const { PrismaClient } = require('@prisma/client');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

// Prisma singleton for connection reuse across warm lambda invocations
let _prisma;
function getPrisma() {
  if (!_prisma) {
    _prisma = new PrismaClient({
      datasources: { db: { url: process.env.DATABASE_URL } }
    });
  }
  return _prisma;
}

const ALLOWED_ORIGINS = [
  'https://perkfinity.net',
  'https://www.perkfinity.net',
  'https://dashboard.perkfinity.com',
];

function setCors(req, res) {
  const origin = req.headers.origin;
  const allowed = ALLOWED_ORIGINS.includes(origin);
  res.setHeader('Access-Control-Allow-Origin', allowed ? origin : 'https://perkfinity.net');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-idempotency-key');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Vary', 'Origin');
}

function json(res, status, data) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(data));
}

function getJwtSecret() {
  const secret = process.env.JWT_SECRET;
  if (!secret) throw new Error('JWT_SECRET environment variable is not set');
  return secret;
}

module.exports = async function handler(req, res) {
  setCors(req, res);

  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    res.end();
    return;
  }

  const url = (req.url || '/').split('?')[0];
  const method = req.method || 'GET';
  const prisma = getPrisma();

  try {

    // ── GET /health ──────────────────────────────────────────────
    if ((url === '/' || url === '/health' || url.endsWith('/health')) && method === 'GET') {
      await prisma.$queryRaw`SELECT 1`;
      return json(res, 200, { ok: true, status: 'healthy', timestamp: new Date().toISOString() });
    }

    // ── POST /api/v1/merchants/signup ────────────────────────────
    if (url.endsWith('/merchants/signup') && method === 'POST') {
      const data = req.body || {};

      if (!data.name || !data.email || !data.password) {
        return json(res, 400, { success: false, error: 'Missing required fields: name, email, password' });
      }

      // Hash password
      const password_hash = await bcrypt.hash(data.password, 12);

      // Create merchant + owner user in one transaction
      const merchant = await prisma.merchant.create({
        data: {
          business_name: data.name,
          subscription_tier: data.tier || 'trial',
          users: {
            create: { email: data.email.toLowerCase(), password_hash, role: 'owner' }
          }
        },
        include: { users: true }
      });

      const merchantUser = merchant.users[0];

      // Create primary location
      await prisma.merchantLocation.create({
        data: {
          merchant_id: merchant.id,
          address: `${data.address || ''}${data.suite ? ', ' + data.suite : ''}`.trim(),
          city: data.city || '',
          state: data.state || '',
          postal_code: data.zip || '',
          country: 'US',
        }
      });

      // Create welcome campaign
      const perkTitle = data.perk || 'Welcome Perk';
      await prisma.campaign.create({
        data: {
          merchant_id: merchant.id,
          title: perkTitle,
          discount_percentage: 10,
          status: 'active',
          start_at: new Date(),
          end_at: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000),
        }
      });

      // Generate QR code
      const public_code = crypto.randomBytes(9).toString('base64url');
      await prisma.qrCode.create({
        data: { merchant_id: merchant.id, public_code, status: 'active' }
      });

      // Sign JWT
      const accessToken = jwt.sign(
        { userId: merchantUser.id, merchantId: merchant.id, role: merchantUser.role },
        getJwtSecret(),
        { expiresIn: '8h' }
      );

      const { password_hash: _pw, ...safeUser } = merchantUser;

      return json(res, 201, {
        success: true,
        data: {
          merchant: {
            id: merchant.id,
            business_name: merchant.business_name,
            subscription_tier: merchant.subscription_tier,
          },
          merchantUser: safeUser,
          accessToken,
          qr_public_code: public_code,
          qr_url: `https://app.perkfinity.net/qr/${public_code}`,
        }
      });
    }

    // ── POST /api/v1/auth/login ──────────────────────────────────
    if ((url.endsWith('/auth/login') || url.endsWith('/merchants/login')) && method === 'POST') {
      const data = req.body || {};

      if (!data.email || !data.password) {
        return json(res, 400, { success: false, error: 'email and password are required' });
      }

      const user = await prisma.merchantUser.findUnique({
        where: { email: data.email.toLowerCase() },
        include: { merchant: { select: { id: true, business_name: true, subscription_tier: true, status: true } } }
      });

      if (!user || !(await bcrypt.compare(data.password, user.password_hash))) {
        return json(res, 401, { success: false, error: 'Invalid email or password' });
      }

      const accessToken = jwt.sign(
        { userId: user.id, merchantId: user.merchant_id, role: user.role },
        getJwtSecret(),
        { expiresIn: '8h' }
      );

      const { password_hash: _pw, ...safeUser } = user;
      return json(res, 200, { success: true, data: { merchantUser: safeUser, accessToken } });
    }

    // ── 404 ──────────────────────────────────────────────────────
    return json(res, 404, { success: false, error: `No route: ${method} ${url}` });

  } catch (err) {
    console.error('[perkfinity-api] Error:', err.message, err.stack);

    if (err.code === 'P2002') {
      return json(res, 400, { success: false, error: 'A merchant with this email already exists.' });
    }
    if (err.code === 'P2025') {
      return json(res, 404, { success: false, error: 'Record not found.' });
    }

    return json(res, 500, {
      success: false,
      error: err.message || 'Internal server error',
      // Include env check to help diagnose missing vars in Vercel
      _debug: {
        DATABASE_URL: process.env.DATABASE_URL ? '✓ SET' : '✗ MISSING',
        JWT_SECRET: process.env.JWT_SECRET ? '✓ SET' : '✗ MISSING',
        NODE_ENV: process.env.NODE_ENV,
      }
    });
  }
};
