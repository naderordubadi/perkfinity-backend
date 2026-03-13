import cron from 'node-cron';
import { PrismaClient } from '@prisma/client';
import { cleanupExpiredRedemptions, detectSuspiciousActivity } from './cleanup.js';

export function startJobs(prisma: PrismaClient) {
  // Run nightly at 2am
  cron.schedule('0 2 * * *', async () => {
    console.log('Running nightly cleanup job...');
    try {
      await cleanupExpiredRedemptions(prisma);
    } catch (err) {
      console.error('Error running cleanup job:', err);
    }
  });

  // Run every 5 minutes
  cron.schedule('*/5 * * * *', async () => {
    try {
      await detectSuspiciousActivity(prisma);
    } catch (err) {
      console.error('Error running suspicious activity detection job:', err);
    }
  });

  console.log('⏳ Background jobs scheduled.');
}
