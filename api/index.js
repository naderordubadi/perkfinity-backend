/* eslint-disable @typescript-eslint/no-var-requires */
// Plain CommonJS handler — no TypeScript compilation needed, works reliably on Vercel
const { PrismaClient } = require('@prisma/client');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

const prisma = new PrismaClient();

const ALLOWED_ORIGINS = [
  'https://perkfinity.net',
  'https://www.perkfinity.net',
  'https://dashboard.perkfinity.com',
];

function setCors(req, res) {
  const origin = req.headers.origin;
  if (!origin || ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
}

module.exports = async function handler(req, res) {
  setCors(req, res);

  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const url = (req.url || '').split('?')[0];

  try {
    // ── Health check ─────────────────────────────────────────────
    if (url === '/health' || url.endsWith('/health')) {
      return res.status(200).json({ ok: true, status: 'healthy', timestamp: new Date().toISOString() });
    }

    // ── Merchant Signup ──────────────────────────────────────────
    if (url.includes('/merchants/signup') && req.method === 'POST') {
      const data = req.body || {};

      if (!data.email || !data.password || !data.name) {
        return res.status(400).json({ success: false, error: 'Missing required fields: name, email, password' });
      }

      const password_hash = await bcrypt.hash(data.password, 12);

      const merchant = await prisma.merchant.create({
        data: {
          business_name: data.name,
          subscription_tier: data.tier || 'trial',
          users: {
            create: { email: data.email, password_hash, role: 'owner' }
          }
        },
        include: { users: true }
      });

      const merchantUser = merchant.users[0];

      await prisma.merchantLocation.create({
        data: {
          merchant_id: merchant.id,
          address: data.address || '',
          city: data.city || '',
          postal_code: data.zip || '',
          country: 'US',
        }
      });

      await prisma.campaign.create({
        data: {
          merchant_id: merchant.id,
          title: data.perk || 'Welcome Perk',
          discount_percentage: 10,
          status: 'active',
          start_at: new Date(),
          end_at: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000),
        }
      });

      const public_code = crypto.randomBytes(9).toString('base64url');
      await prisma.qrCode.create({
        data: { merchant_id: merchant.id, public_code, status: 'active' }
      });

      const jwtSecret = process.env.JWT_SECRET || 'fallback-secret-change-in-prod';
      const accessToken = jwt.sign(
        { userId: merchantUser.id, merchantId: merchant.id, role: merchantUser.role },
        jwtSecret,
        { expiresIn: '8h' }
      );

      const { password_hash: _pw, ...userWithoutPassword } = merchantUser;

      return res.status(201).json({
        success: true,
        data: {
          merchant: { id: merchant.id, business_name: merchant.business_name, subscription_tier: merchant.subscription_tier },
          merchantUser: userWithoutPassword,
          accessToken,
          qr_public_code: public_code,
          qr_url: `https://app.perkfinity.net/qr/${public_code}`,
        }
      });
    }

    // ── Merchant Login ───────────────────────────────────────────
    if (url.includes('/auth/login') && req.method === 'POST') {
      const data = req.body || {};

      const merchantUser = await prisma.merchantUser.findUnique({
        where: { email: data.email },
        include: { merchant: true }
      });

      if (!merchantUser) {
        return res.status(401).json({ success: false, error: 'Invalid email or password' });
      }

      const valid = await bcrypt.compare(data.password, merchantUser.password_hash);
      if (!valid) {
        return res.status(401).json({ success: false, error: 'Invalid email or password' });
      }

      const jwtSecret = process.env.JWT_SECRET || 'fallback-secret-change-in-prod';
      const accessToken = jwt.sign(
        { userId: merchantUser.id, merchantId: merchantUser.merchant_id, role: merchantUser.role },
        jwtSecret,
        { expiresIn: '8h' }
      );

      const { password_hash: _pw, ...userWithoutPassword } = merchantUser;
      return res.status(200).json({ success: true, data: { merchantUser: userWithoutPassword, accessToken } });
    }

    return res.status(404).json({ success: false, error: `Route not found: ${req.method} ${url}` });

  } catch (err) {
    console.error('Handler error:', err);
    if (err.code === 'P2002') {
      return res.status(400).json({ success: false, error: 'A merchant with this email already exists.' });
    }
    return res.status(500).json({
      success: false,
      error: err.message || 'Internal server error',
      env_check: {
        DATABASE_URL: process.env.DATABASE_URL ? 'SET' : 'MISSING',
        JWT_SECRET: process.env.JWT_SECRET ? 'SET' : 'MISSING',
      }
    });
  }
};
