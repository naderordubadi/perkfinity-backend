import { z } from 'zod';

export const CreateCampaignSchema = z.object({
  title: z.string().default('Instant Discount'),
  discount_percentage: z.number().int().min(1).max(90),
  terms: z.string().max(500).optional(),
  redemption_time_limit_minutes: z.number().int().min(5).max(60).default(15),
  start_at: z.string().datetime().optional(),
  end_at: z.string().datetime().optional(),
  location_id: z.string().uuid().optional(),
});

export const UpdateCampaignSchema = z.object({
  title: z.string().optional(),
  discount_percentage: z.number().int().min(1).max(90).optional(),
  terms: z.string().max(500).optional(),
  start_at: z.string().datetime().optional().nullable(),
  end_at: z.string().datetime().optional().nullable(),
});
