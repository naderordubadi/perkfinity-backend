import pinoImport from 'pino';
import { env } from './env.js';

const pino = pinoImport as unknown as (options: Record<string, unknown>) => unknown;

export const logger = pino({
  level: env.LOG_LEVEL,
  transport:
    env.NODE_ENV === 'development'
      ? {
        target: 'pino-pretty',
        options: {
          translateTime: 'HH:MM:ss Z',
          ignore: 'pid,hostname',
        },
      }
      : undefined,
});