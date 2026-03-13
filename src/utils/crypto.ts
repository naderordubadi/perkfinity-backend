import crypto from 'node:crypto';
import { env } from '../config/env.js';

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 12; // Recommended for GCM
const AUTH_TAG_LENGTH = 16;
let keyBuffer: Buffer;

function getKey(): Buffer {
  if (!keyBuffer) {
    keyBuffer = Buffer.from(env.PII_ENCRYPTION_KEY, 'base64');
    if (keyBuffer.length !== 32) {
      throw new Error('PII_ENCRYPTION_KEY must be exactly 32 bytes when decoded from base64');
    }
  }
  return keyBuffer;
}

export function encryptField(plaintext: string): string {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, getKey(), iv);
  
  let encrypted = cipher.update(plaintext, 'utf8', 'base64');
  encrypted += cipher.final('base64');
  
  const authTag = cipher.getAuthTag().toString('base64');
  
  // Format: iv:encrypted:authTag
  return `${iv.toString('base64')}:${encrypted}:${authTag}`;
}

export function decryptField(ciphertext: string): string {
  const parts = ciphertext.split(':');
  if (parts.length !== 3) {
    throw new Error('Invalid ciphertext format');
  }
  
  const [ivBase64, encryptedBase64, authTagBase64] = parts;
  
  const decipher = crypto.createDecipheriv(
    ALGORITHM,
    getKey(),
    Buffer.from(ivBase64, 'base64')
  );
  decipher.setAuthTag(Buffer.from(authTagBase64, 'base64'));
  
  let decrypted = decipher.update(encryptedBase64, 'base64', 'utf8');
  decrypted += decipher.final('utf8');
  
  return decrypted;
}

export function generateRedemptionToken(): string {
  return crypto.randomBytes(32).toString('base64url');
}

export function hashToken(token: string): string {
  return crypto.createHash('sha256').update(token).digest('hex');
}
