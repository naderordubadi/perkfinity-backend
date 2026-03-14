import { z } from 'zod';

export const SignupMerchantSchema = z.object({
  name: z.string().min(2),
  contactName: z.string().min(2),
  email: z.string().email(),
  phone: z.string().min(2), // allowing short strings for flexible test phone numbers
  address: z.string().min(2),
  city: z.string().min(2),
  zip: z.string().min(2),
  website: z.string().optional(),
  perk: z.string().min(2),
  tier: z.string(),
  password: z.string().min(8)
});
