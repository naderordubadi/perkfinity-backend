import { buildApp } from '../src/app.js';
import type { VercelRequest, VercelResponse } from '@vercel/node';

let appInstance: Awaited<ReturnType<typeof buildApp>> | null = null;

async function getApp() {
  if (!appInstance) {
    appInstance = await buildApp();
    await appInstance.ready();
  }
  return appInstance;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const app = await getApp();

  // Convert Vercel request to something Fastify can handle
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
}
