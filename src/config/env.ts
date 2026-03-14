import { z } from 'zod';
import dotenv from 'dotenv';

dotenv.config();

const envSchema = z.object({
  DATABASE_URL: z.string().url(),
  JWT_SECRET: z.string().min(32), // relaxed min for Vercel compat
  JWT_EXPIRY: z.string().default('15m'),
  JWT_REFRESH_EXPIRY: z.string().default('7d'),
  PII_ENCRYPTION_KEY: z.string().min(32),
  RATE_LIMIT_MAX: z.coerce.number().int().positive().default(120),
  RATE_LIMIT_WINDOW_MS: z.coerce.number().int().positive().default(60000),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.coerce.number().int().positive().default(3001),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
});

const _env = envSchema.safeParse(process.env);

if (!_env.success) {
  console.error('❌ Invalid environment variables:', JSON.stringify(_env.error.format()));
  // Do not throw — let handler return the error so it's visible in responses
}

export const env = _env.success ? _env.data : ({
  DATABASE_URL: process.env.DATABASE_URL || '',
  JWT_SECRET: process.env.JWT_SECRET || '',
  JWT_EXPIRY: '15m',
  JWT_REFRESH_EXPIRY: '7d',
  PII_ENCRYPTION_KEY: process.env.PII_ENCRYPTION_KEY || '',
  RATE_LIMIT_MAX: 120,
  RATE_LIMIT_WINDOW_MS: 60000,
  NODE_ENV: 'production',
  PORT: 3001,
  LOG_LEVEL: 'info',
} as any);
