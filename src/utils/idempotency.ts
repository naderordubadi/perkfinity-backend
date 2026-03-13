interface IdempotencyRecord {
  result: unknown;
  expiresAt: number;
}

// In-memory store
const store = new Map<string, IdempotencyRecord>();

export function setIdempotencyResult(key: string, result: unknown, ttlMs: number): void {
  const expiresAt = Date.now() + ttlMs;
  store.set(key, { result, expiresAt });
  
  // Simple cleanup of this specific key after it expires
  setTimeout(() => {
    const record = store.get(key);
    if (record && Date.now() >= record.expiresAt) {
      store.delete(key);
    }
  }, ttlMs).unref();
}

export function getIdempotencyResult(key: string): unknown | null {
  const record = store.get(key);
  if (!record) return null;
  
  if (Date.now() > record.expiresAt) {
    store.delete(key);
    return null;
  }
  
  return record.result;
}
