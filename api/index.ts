import type { VercelRequest, VercelResponse } from '@vercel/node';

// Dynamic import so any crash in src/ is caught by try/catch
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const { buildApp } = await import('../src/app.js');
    const app = await buildApp();
    await app.ready();

    const response = await app.inject({
      method: req.method as any,
      url: req.url || '/',
      headers: req.headers as Record<string, string>,
      payload: req.body ? JSON.stringify(req.body) : undefined,
    });

    res.status(response.statusCode);
    Object.entries(response.headers).forEach(([key, value]) => {
      if (value !== undefined) res.setHeader(key, value as string);
    });
    return res.end(response.body);

  } catch (err: any) {
    res.status(500).json({
      error: err?.message || String(err),
      stack: err?.stack?.split('\n').slice(0, 8),
      env: {
        DATABASE_URL: process.env.DATABASE_URL ? 'SET' : 'MISSING',
        JWT_SECRET: process.env.JWT_SECRET ? 'SET' : 'MISSING',
        PII_ENCRYPTION_KEY: process.env.PII_ENCRYPTION_KEY ? 'SET' : 'MISSING',
        NODE_ENV: process.env.NODE_ENV,
      }
    });
  }
}
