import cron from 'node-cron';
import { PrismaClient } from '@prisma/client';
import { cleanupExpiredRedemptions, detectSuspiciousActivity, purgeCancelledMerchants, sendPaymentFailureReminders } from './cleanup.js';

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

  // Run nightly at 3am - 6-month cancelled merchant PII purge
  cron.schedule('0 3 * * *', async () => {
    console.log('[PurgeCancelledMerchants] Running nightly purge check...');
    try {
      await purgeCancelledMerchants();
    } catch (err) {
      console.error('[PurgeCancelledMerchants] Job failed:', err);
    }
  });

  // Run nightly at 3:30am - payment failure reminder emails (Day 3 / 7 / 10)
  cron.schedule('30 3 * * *', async () => {
    console.log('[PaymentReminders] Running nightly reminder check...');
    try {
      await sendPaymentFailureReminders();
    } catch (err) {
      console.error('[PaymentReminders] Job failed:', err);
    }
  });

  console.log('⏳ Background jobs scheduled.');
}

