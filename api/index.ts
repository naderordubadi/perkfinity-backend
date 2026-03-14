import { buildApp } from '../src/app.js';
import type { VercelRequest, VercelResponse } from '@vercel/node';

let appInstance: Awaited<ReturnType<typeof buildApp>> | null = null;
let startupError: string | null = null;

async function getApp() {
  if (startupError) throw new Error(startupError);
  if (!appInstance) {
    try {
      appInstance = await buildApp();
      await appInstance.ready();
    } catch (err: any) {
      startupError = err?.message || String(err);
      throw err;
    }
  }
  return appInstance;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const app = await getApp();

    const response = await app.inject({
      method: req.method as any,
      url: req.url || '/',
      headers: req.headers as Record<string, string>,
      payload: req.body ? JSON.stringify(req.body) : undefined,
    });

    res.status(response.statusCode);
    Object.entries(response.headers).forEach(([key, value]) => {
      if (value !== undefined) {
        res.setHeader(key, value as string);
      }
    });
    res.end(response.body);
  } catch (err: any) {
    res.status(500).json({
      error: 'STARTUP_ERROR',
      message: err?.message || String(err),
      env_keys_present: Object.keys(process.env).filter(k => !k.startsWith('npm_') && !k.startsWith('NVM_'))
    });
  }
}
