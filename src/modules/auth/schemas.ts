import { z } from 'zod';

export const RegisterMerchantSchema = z.object({
  business_name: z.string().min(2),
  email: z.string().email(),
  password: z.string().min(8),
});

export const LoginMerchantSchema = z.object({
  email: z.string().email(),
  password: z.string(),
});

export const RefreshTokenSchema = z.object({
  refresh_token: z.string(),
});

export const AnonymousUserSchema = z.object({
  fingerprint: z.string().optional(),
});
