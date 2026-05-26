'use strict';

/**
 * TaxBandits API helper
 * Handles:
 *  1. JWS generation (HMACSHA256 signed JWT for OAuth)
 *  2. Access token fetch + 50-minute in-memory cache
 *  3. sendW9Request() — calls POST /WhCertificate/RequestByEmail
 */

const crypto = require('crypto');

// ── Environment ────────────────────────────────────────────────────────────────
const CLIENT_ID     = process.env.TAXBANDITS_CLIENT_ID;
const CLIENT_SECRET = process.env.TAXBANDITS_CLIENT_SECRET;
const USER_TOKEN    = process.env.TAXBANDITS_USER_TOKEN;
const BUSINESS_ID   = process.env.TAXBANDITS_BUSINESS_ID || null; // optional — defaults to first business
const TB_ENV        = process.env.TAXBANDITS_ENV || 'sandbox';

const isSandbox = TB_ENV !== 'production';

const AUTH_URL = isSandbox
  ? 'https://testoauth.expressauth.net/v2/tbsauth'
  : 'https://oauth.expressauth.net/v2/tbsauth';

const API_BASE = isSandbox
  ? 'https://testapi.taxbandits.com/v1.7.3'
  : 'https://api.taxbandits.com/v1.7.3';

// ── Token cache ─────────────────────────────────────────────────────────────────
let _cachedToken   = null;
let _tokenExpireAt = 0; // Unix ms

// ── Helpers ─────────────────────────────────────────────────────────────────────

/** base64url encode a string or Buffer */
function b64url(input) {
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(input, 'utf8');
  return buf.toString('base64')
    .replace(/=/g,  '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

/**
 * Build a JWS token required by TaxBandits OAuth.
 * Header:    {"alg":"HS256","typ":"JWT"}
 * Payload:   {iss, sub, aud, iat}
 * Signature: HMACSHA256(header.payload, CLIENT_SECRET)
 */
function generateJWS() {
  const header  = b64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const payload = b64url(JSON.stringify({
    iss: CLIENT_ID,
    sub: CLIENT_ID,
    aud: USER_TOKEN,
    iat: Math.floor(Date.now() / 1000),
  }));
  const signingInput = `${header}.${payload}`;
  const sig = b64url(
    crypto.createHmac('sha256', CLIENT_SECRET).update(signingInput).digest()
  );
  return `${signingInput}.${sig}`;
}

/**
 * Fetch an OAuth access token from TaxBandits.
 * Tokens are cached for 50 minutes to avoid hammering the auth server.
 * TaxBandits tokens are valid for ~60 minutes; 50-min cache gives 10-min buffer.
 */
async function getAccessToken() {
  const now = Date.now();
  if (_cachedToken && now < _tokenExpireAt) {
    return _cachedToken;
  }

  const jws = generateJWS();

  const res = await fetch(AUTH_URL, {
    method:  'GET',
    headers: { Authentication: jws },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`TaxBandits auth failed (${res.status}): ${body}`);
  }

  const data = await res.json();
  const token = data.access_token || data.AccessToken || data.token;
  if (!token) {
    throw new Error(`TaxBandits auth response missing access_token: ${JSON.stringify(data)}`);
  }

  _cachedToken   = token;
  _tokenExpireAt = now + 50 * 60 * 1000; // 50 minutes
  return token;
}

/**
 * Send a W-9 collection request to a contractor via TaxBandits SmartCollect (WhCertificate/RequestByEmail).
 *
 * @param {string} email         - Contractor's email address
 * @param {string} name          - Contractor's legal name (as it should appear on the W-9)
 * @param {string} contractorId  - Our internal DB contractor ID (stored as PayeeRef for webhook mapping)
 * @returns {Promise<{submissionId: string, raw: object}>}
 */
async function sendW9Request(email, name, contractorId) {
  if (!CLIENT_ID || !CLIENT_SECRET || !USER_TOKEN) {
    throw new Error('TaxBandits credentials not configured. Check TAXBANDITS_CLIENT_ID, CLIENT_SECRET, USER_TOKEN env vars.');
  }

  const token = await getAccessToken();

  const payload = {
    Recipients: [
      {
        Email:    email,
        Name:     name,
        PayeeRef: contractorId, // echoed back in webhook — used to map event → contractor
      }
    ],
    IsTINMatching: true,
    Customizations: {
      ThemeColor: '#5b3fa5',
    },
  };

  // Only include BusinessId if explicitly configured (optional — defaults to first business)
  if (BUSINESS_ID) {
    payload.BusinessId = BUSINESS_ID;
  }

  const res = await fetch(`${API_BASE}/WhCertificate/RequestByEmail`, {
    method:  'POST',
    headers: {
      'Content-Type':  'application/json',
      'Authorization': `Bearer ${token}`,
    },
    body: JSON.stringify(payload),
  });

  const data = await res.json();

  if (!res.ok) {
    throw new Error(
      `TaxBandits W-9 request failed (${res.status}): ${JSON.stringify(data)}`
    );
  }

  // Extract the SubmissionId — used to correlate the webhook event back to this contractor
  const submissionId =
    data?.SubmissionId ||
    data?.submissionId ||
    data?.Data?.SubmissionId ||
    null;

  return { submissionId, raw: data };
}

module.exports = { sendW9Request, getAccessToken };
