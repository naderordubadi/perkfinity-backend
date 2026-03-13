export function success<T>(data: T): { ok: true; data: T } {
  return { ok: true, data };
}

export type ErrorCode = 
  | 'AUTH_INVALID_CREDENTIALS'
  | 'AUTH_TOKEN_EXPIRED'
  | 'MERCHANT_NOT_FOUND'
  | 'CAMPAIGN_NOT_ACTIVE'
  | 'CAMPAIGN_NOT_FOUND'
  | 'TIER_LIMIT_REACHED'
  | 'TOKEN_NOT_FOUND'
  | 'TOKEN_EXPIRED'
  | 'TOKEN_ALREADY_REDEEMED'
  | 'QR_INACTIVE'
  | 'NO_ACTIVE_CAMPAIGN'
  | 'MERCHANT_MISMATCH'
  | 'RATE_LIMITED'
  | 'VALIDATION_ERROR'
  | 'INTERNAL_ERROR';

export function failure(code: ErrorCode, message: string, details?: unknown) {
  return {
    ok: false,
    error: {
      code,
      message,
      ...(details ? { details } : {})
    }
  };
}
