/**
 * Perkfinity Weekly Accounting Audit & FreshBooks Entry Generator
 * 
 * STRICT RULE: Always queries LIVE PRODUCTION Stripe (sk_live_...) and LIVE PRODUCTION Neon DB.
 * Usage: node backend/scripts/run-weekly-accounting.js [startDate] [endDate]
 * Example: node backend/scripts/run-weekly-accounting.js 2026-09-12 2026-09-18
 */

const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const Stripe = require('stripe');

// 1. Force load PRODUCTION environment
const prodEnvPath = path.resolve(__dirname, '../.env.production');
if (!fs.existsSync(prodEnvPath)) {
  console.error('❌ FATAL: backend/.env.production not found! Cannot run live accounting audit.');
  process.exit(1);
}

const env = dotenv.parse(fs.readFileSync(prodEnvPath));
const stripeKey = env.STRIPE_SECRET_KEY;

if (!stripeKey || !stripeKey.startsWith('sk_live_')) {
  console.error('❌ FATAL: STRIPE_SECRET_KEY in .env.production is NOT a live key (sk_live_...)! Aborting to prevent test data contamination.');
  process.exit(1);
}

const stripe = new Stripe(stripeKey);

async function runWeeklyAudit() {
  // Default to current week (last 7 days up to Friday night or specified dates)
  const args = process.argv.slice(2);
  let startDt, endDt;

  if (args.length >= 2) {
    startDt = new Date(args[0] + 'T00:00:00Z');
    endDt = new Date(args[1] + 'T23:59:59Z');
  } else {
    // Default to last completed Friday week
    const now = new Date();
    endDt = new Date(now);
    startDt = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  }

  const startUnix = Math.floor(startDt.getTime() / 1000);
  const endUnix = Math.floor(endDt.getTime() / 1000);

  console.log(`\n======================================================`);
  console.log(`🏛️ PERKFINITY LIVE PRODUCTION ACCOUNTING AUDIT`);
  console.log(`📅 Period: ${startDt.toISOString().slice(0,10)} to ${endDt.toISOString().slice(0,10)}`);
  console.log(`🔑 Stripe Key: ${stripeKey.slice(0, 14)}... (LIVE MODE CONFIRMED ✅)`);
  console.log(`======================================================\n`);

  // Fetch Balance Transactions in period
  const balanceTxs = await stripe.balanceTransactions.list({
    created: { gte: startUnix, lte: endUnix },
    limit: 100
  });

  // Fetch Invoices in period
  const invoices = await stripe.invoices.list({
    created: { gte: startUnix, lte: endUnix },
    limit: 100
  });

  // Fetch Payouts in period
  const payouts = await stripe.payouts.list({
    created: { gte: startUnix, lte: endUnix },
    limit: 100
  });

  let totalSubscriptionRev = 0;
  let totalCarouselRev = 0;
  let totalProcessingFees = 0;
  let totalUsageFees = 0;
  let totalRepCommissions = 0;
  let netStripeClearing = 0;

  const itemizedList = [];

  for (const inv of invoices.data) {
    if (inv.status !== 'paid') continue;
    const gross = inv.amount_paid / 100;
    const isSubscription = inv.lines?.data?.some(l => l.description?.includes('Starter') || l.description?.includes('Growth') || l.description?.includes('Scale') || l.description?.includes('Tier 1'));
    const isCarousel = inv.lines?.data?.some(l => l.description?.includes('Everywhere Bundle') || l.description?.includes('Carousel') || l.description?.includes('Sponsored'));

    // Rep commission check: Unique Kitchen and Baths (20%)
    let repComm = 0;
    const isUniqueKitchen = inv.customer_name?.includes('Unique Kitchen') || inv.customer_email?.includes('maxrpmconstruction');
    if (isUniqueKitchen && isSubscription) {
      repComm = Math.round(gross * 0.20 * 100) / 100;
      totalRepCommissions += repComm;
    }

    if (isSubscription) totalSubscriptionRev += gross;
    else if (isCarousel) totalCarouselRev += gross;
    else totalSubscriptionRev += gross;

    itemizedList.push({
      date: new Date(inv.created * 1000).toISOString().slice(0,10),
      customer: inv.customer_name || inv.customer_email,
      description: inv.lines?.data?.map(l => l.description).join(', '),
      amount: gross,
      repCommission: repComm,
      invoiceId: inv.id,
      chargeId: inv.charge
    });
  }

  // Fees calculation from balance transactions
  for (const tx of balanceTxs.data) {
    if (tx.type === 'charge') {
      totalProcessingFees += tx.fee / 100;
    } else if (tx.type === 'stripe_fee') {
      totalUsageFees += Math.abs(tx.amount) / 100;
    }
  }

  const totalGrossRev = totalSubscriptionRev + totalCarouselRev;
  const totalCogsFees = totalProcessingFees + totalUsageFees;
  netStripeClearing = totalGrossRev - totalCogsFees;

  console.log(`📊 REVENUE & EXPENSE TOTALS:`);
  console.log(`   - Subscription Revenue (4000-1): $${totalSubscriptionRev.toFixed(2)}`);
  console.log(`   - Carousel Ad Sponsorships (4000-8): $${totalCarouselRev.toFixed(2)}`);
  console.log(`   - Total Gross Revenue: $${totalGrossRev.toFixed(2)}`);
  console.log(`   - Stripe Processing Fees: $${totalProcessingFees.toFixed(2)}`);
  console.log(`   - Stripe Billing Usage Fees: $${totalUsageFees.toFixed(2)}`);
  console.log(`   - Total COGS Stripe Fees (5000-1): $${totalCogsFees.toFixed(2)}`);
  console.log(`   - Net Addition to Stripe-8118 Clearing (1000-38): $${netStripeClearing.toFixed(2)}`);
  console.log(`   - Accrued Sales Rep Commissions (2001-3 / 6003-6): $${totalRepCommissions.toFixed(2)}\n`);

  console.log(`📝 READY-TO-INPUT FRESHBOOKS JOURNAL ENTRY:`);
  console.log(`--------------------------------------------------------------------------------`);
  console.log(`Date: ${endDt.toISOString().slice(0,10)}`);
  console.log(`Name: Stripe Transactions & Rep Commission Accrual`);
  console.log(`Memo: Live Weekly Stripe Revenue ($${totalGrossRev.toFixed(2)}), Fees ($${totalCogsFees.toFixed(2)}), & Rep Commission ($${totalRepCommissions.toFixed(2)}) (${startDt.toISOString().slice(0,10)} - ${endDt.toISOString().slice(0,10)})\n`);

  console.log(`| Account Number | Account Name                                | Debit    | Credit   |`);
  console.log(`| :---:          | :---                                        | :---:    | :---:    |`);
  console.log(`| 1000-38        | Cash: Stripe-8118 Clearing                  | $${netStripeClearing.toFixed(2).padStart(7)} | —        |`);
  console.log(`| 5000-1         | Cost of Goods Sold: Stripe Processing Fees  | $${totalCogsFees.toFixed(2).padStart(7)} | —        |`);
  if (totalRepCommissions > 0) {
    console.log(`| 6003-6         | Contractors: Independent Sales Reps         | $${totalRepCommissions.toFixed(2).padStart(7)} | —        |`);
  }
  if (totalSubscriptionRev > 0) {
    console.log(`| 4000-1         | Revenue: Perkfinity Subscription Revenue    | —        | $${totalSubscriptionRev.toFixed(2).padStart(7)} |`);
  }
  if (totalCarouselRev > 0) {
    console.log(`| 4000-8         | Revenue: Carousel Ad Sponsorships           | —        | $${totalCarouselRev.toFixed(2).padStart(7)} |`);
  }
  if (totalRepCommissions > 0) {
    console.log(`| 2001-3         | Accrued Sales Commissions Payable           | —        | $${totalRepCommissions.toFixed(2).padStart(7)} |`);
  }
  const totalDebits = netStripeClearing + totalCogsFees + totalRepCommissions;
  const totalCredits = totalSubscriptionRev + totalCarouselRev + totalRepCommissions;
  console.log(`| TOTAL          |                                             | $${totalDebits.toFixed(2).padStart(7)} | $${totalCredits.toFixed(2).padStart(7)} |`);
  console.log(`--------------------------------------------------------------------------------\n`);

  console.log(`🔍 ITEMIZED INVOICE DETAILS:`);
  itemizedList.forEach((it, idx) => {
    console.log(`   ${idx + 1}. [${it.date}] ${it.customer} — $${it.amount.toFixed(2)} (${it.description}) [Inv: ${it.invoiceId}]`);
  });
  console.log(``);
}

runWeeklyAudit().catch(err => {
  console.error('❌ Audit script error:', err);
  process.exit(1);
});
