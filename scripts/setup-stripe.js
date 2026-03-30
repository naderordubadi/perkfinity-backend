/**
 * One-time script to create the Perkfinity Tier 1 product + price in Stripe.
 * Usage: STRIPE_SECRET_KEY=sk_test_... node scripts/setup-stripe.js
 */

const Stripe = require('stripe');

const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY;
if (!STRIPE_SECRET_KEY) {
  console.error('ERROR: Set STRIPE_SECRET_KEY environment variable');
  process.exit(1);
}

const stripe = Stripe(STRIPE_SECRET_KEY);

async function main() {
  console.log('Creating Perkfinity Tier 1 product in Stripe...\n');

  // 1. Create the Product
  const product = await stripe.products.create({
    name: 'Perkfinity Tier 1',
    description: 'Unlimited digital coupons & weekly newsletter inclusions for local merchants.',
    metadata: { tier: 'tier1' }
  });
  console.log('✅ Product created:', product.id);

  // 2. Create the Price ($29.99/month recurring)
  const price = await stripe.prices.create({
    product: product.id,
    unit_amount: 2999, // $29.99 in cents
    currency: 'usd',
    recurring: { interval: 'month' },
    metadata: { tier: 'tier1' }
  });
  console.log('✅ Price created:', price.id);

  console.log('\n════════════════════════════════════════════');
  console.log('  SAVE THIS — Add to Vercel env vars:');
  console.log('════════════════════════════════════════════');
  console.log(`  STRIPE_TIER1_PRICE_ID=${price.id}`);
  console.log('════════════════════════════════════════════\n');
}

main().catch(err => {
  console.error('Failed:', err.message);
  process.exit(1);
});
