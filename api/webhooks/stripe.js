/**
 * Perkfinity Stripe Webhook Handler
 * Listens for Stripe events and updates merchant billing status accordingly.
 * 
 * Deployed as a separate Vercel serverless function at /api/webhooks/stripe
 * 
 * Events handled:
 *   checkout.session.completed     — Tier 1 immediate signup charge succeeded
 *   setup_intent.succeeded         — Trial merchant saved card
 *   invoice.payment_succeeded      — Monthly recurring payment succeeded
 *   invoice.payment_failed         — Payment failed (retry will happen automatically)
 *   customer.subscription.updated  — Detects cancel_at_period_end (portal or app) → pending_cancellation
 *   customer.subscription.deleted  — Subscription cancelled → FULL BLOCK
 */

const Stripe = require('stripe');
const { neon } = require('@neondatabase/serverless');

module.exports = async (req, res) => {
  // Only accept POST
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Stripe-Signature');
    return res.status(204).end();
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY;
  const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;
  const DATABASE_URL = process.env.DATABASE_URL;

  if (!STRIPE_SECRET_KEY || !DATABASE_URL) {
    console.error('Missing STRIPE_SECRET_KEY or DATABASE_URL');
    return res.status(500).json({ error: 'Server misconfigured' });
  }

  const stripe = Stripe(STRIPE_SECRET_KEY);
  const sql = neon(DATABASE_URL);

  let event;

  // Read raw body from request stream (required with bodyParser: false on Vercel)
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
  }
  const rawBody = Buffer.concat(chunks);

  // Verify webhook signature if secret is set
  if (STRIPE_WEBHOOK_SECRET) {
    const sig = req.headers['stripe-signature'];
    try {
      event = stripe.webhooks.constructEvent(rawBody, sig, STRIPE_WEBHOOK_SECRET);
    } catch (err) {
      console.error('Webhook signature verification failed:', err.message);
      return res.status(400).json({ error: `Webhook Error: ${err.message}` });
    }
  } else {
    // In test mode without webhook secret, just parse the event
    event = JSON.parse(rawBody.toString());
    console.warn('⚠️ STRIPE_WEBHOOK_SECRET not set — skipping signature verification');
  }

  console.log(`[Stripe Webhook] Event received: ${event.type}`);

  try {
    switch (event.type) {

      // ═══════════════════════════════════════════════════════════
      // CHECKOUT COMPLETED — Tier 1 signup, or trial→tier1 auto-upgrade
      // ═══════════════════════════════════════════════════════════
      case 'checkout.session.completed': {
        const session = event.data.object;
        const merchantId = session.metadata?.merchant_id;

        if (!merchantId) {
          console.error('checkout.session.completed: No merchant_id in metadata');
          break;
        }

        // Retrieve the subscription from the session
        const subscriptionId = session.subscription;
        const customerId = session.customer;

        if (subscriptionId) {
          // Update merchant to active tier1
          await sql`
            UPDATE "Merchant"
            SET subscription_tier = 'tier1',
                stripe_customer_id = ${customerId},
                stripe_subscription_id = ${subscriptionId},
                billing_status = 'active',
                account_blocked = false,
                subscription_started_at = NOW(),
                next_billing_date = NOW() + INTERVAL '30 days',
                updated_at = NOW()
            WHERE id = ${merchantId}
          `;
          console.log(`[Stripe] Merchant ${merchantId} upgraded to tier1 via checkout`);
        }
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // SETUP INTENT SUCCEEDED — Trial merchant saved their card
      // ═══════════════════════════════════════════════════════════
      case 'setup_intent.succeeded': {
        const setupIntent = event.data.object;
        const merchantId = setupIntent.metadata?.merchant_id;
        const customerId = setupIntent.customer;
        const paymentMethodId = setupIntent.payment_method;

        if (!merchantId) {
          console.error('setup_intent.succeeded: No merchant_id in metadata');
          break;
        }

        // Save customer + payment method on the merchant
        await sql`
          UPDATE "Merchant"
          SET stripe_customer_id = ${customerId},
              stripe_payment_method_id = ${paymentMethodId},
              billing_status = 'trial',
              updated_at = NOW()
          WHERE id = ${merchantId}
        `;

        // Set this payment method as the default for the customer
        // so we can auto-charge later when they hit their limit
        try {
          await stripe.customers.update(customerId, {
            invoice_settings: { default_payment_method: paymentMethodId }
          });
        } catch (updateErr) {
          console.error('Failed to set default payment method:', updateErr.message);
        }

        console.log(`[Stripe] Trial merchant ${merchantId} saved payment method ${paymentMethodId}`);
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // INVOICE PAYMENT SUCCEEDED — Monthly renewal worked
      // ═══════════════════════════════════════════════════════════
      case 'invoice.payment_succeeded': {
        const invoice = event.data.object;
        const customerId = invoice.customer;
        const subscriptionId = invoice.subscription;

        // Find the merchant by stripe_customer_id
        const [merchant] = await sql`
          SELECT id, business_name, account_blocked, stripe_subscription_id FROM "Merchant"
          WHERE stripe_customer_id = ${customerId}
          LIMIT 1
        `;

        if (!merchant) {
          console.warn(`invoice.payment_succeeded: No merchant found for customer ${customerId}`);
          break;
        }

        // If merchant is fully blocked (subscription was deleted) and has no active subscription attached,
        // do not unblock them just because an old invoice cleared.
        if (merchant.account_blocked && !merchant.stripe_subscription_id) {
          console.log(`[Stripe] Late invoice cleared for permanently cancelled merchant ${merchant.id}. Keeping blocked status.`);
        } else {
          // Update billing status for active/past_due subscriptions
          await sql`
            UPDATE "Merchant"
            SET billing_status = 'active',
                account_blocked = false,
                next_billing_date = NOW() + INTERVAL '30 days',
                updated_at = NOW()
            WHERE id = ${merchant.id}
          `;
        }

        // Record in Invoice table
        await sql`
          INSERT INTO "Invoice" (id, merchant_id, stripe_invoice_id, amount_cents, currency, status, period_start, period_end, paid_at, created_at)
          VALUES (
            gen_random_uuid()::text,
            ${merchant.id},
            ${invoice.id},
            ${invoice.amount_paid || 2999},
            ${invoice.currency || 'usd'},
            'paid',
            ${invoice.period_start ? new Date(invoice.period_start * 1000) : null},
            ${invoice.period_end ? new Date(invoice.period_end * 1000) : null},
            NOW(),
            NOW()
          )
          ON CONFLICT (stripe_invoice_id) DO UPDATE SET status = 'paid', paid_at = NOW()
        `;

        console.log(`[Stripe] Payment succeeded for merchant ${merchant.id} (${merchant.business_name})`);
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // INVOICE PAYMENT FAILED — Card declined, will retry
      // ═══════════════════════════════════════════════════════════
      case 'invoice.payment_failed': {
        const invoice = event.data.object;
        const customerId = invoice.customer;

        const [merchant] = await sql`
          SELECT id, business_name FROM "Merchant"
          WHERE stripe_customer_id = ${customerId}
          LIMIT 1
        `;

        if (!merchant) break;

        // Mark billing as failed (but don't block yet — Stripe retries automatically)
        await sql`
          UPDATE "Merchant"
          SET billing_status = 'payment_failed',
              updated_at = NOW()
          WHERE id = ${merchant.id}
        `;

        console.warn(`[Stripe] Payment FAILED for merchant ${merchant.id} (${merchant.business_name})`);
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // SUBSCRIPTION UPDATED — Detects cancel_at_period_end changes
      // Fires when merchant cancels via Stripe portal or our app sets cancel_at_period_end
      // Also fires if merchant un-cancels from the portal
      // ═══════════════════════════════════════════════════════════
      case 'customer.subscription.updated': {
        const subscription = event.data.object;
        const customerId = subscription.customer;

        const [merchant] = await sql`
          SELECT id, business_name, billing_status FROM "Merchant"
          WHERE stripe_customer_id = ${customerId}
          LIMIT 1
        `;

        if (!merchant) {
          console.warn(`customer.subscription.updated: No merchant found for customer ${customerId}`);
          break;
        }

        const isCancelling = subscription.cancel_at_period_end === true || subscription.cancel_at !== null;

        if (isCancelling && merchant.billing_status !== 'pending_cancellation') {
          // Merchant initiated cancellation (via Stripe portal or our app)
          await sql`
            UPDATE "Merchant"
            SET billing_status = 'pending_cancellation',
                updated_at = NOW()
            WHERE id = ${merchant.id}
          `;
          console.log(`[Stripe] Merchant ${merchant.id} (${merchant.business_name}) — cancellation pending (cancel_at set)`);
        } else if (!isCancelling && merchant.billing_status === 'pending_cancellation') {
          // Merchant reversed cancellation (un-cancelled via Stripe portal)
          await sql`
            UPDATE "Merchant"
            SET billing_status = 'active',
                updated_at = NOW()
            WHERE id = ${merchant.id}
          `;
          console.log(`[Stripe] Merchant ${merchant.id} (${merchant.business_name}) — cancellation reversed, back to active`);
        }
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // SUBSCRIPTION DELETED — FULL ACCOUNT BLOCK
      // This fires when:
      //   1. Merchant voluntarily cancels (at period end)
      //   2. All payment retries exhausted by Stripe
      // ═══════════════════════════════════════════════════════════
      case 'customer.subscription.deleted': {
        const subscription = event.data.object;
        const customerId = subscription.customer;

        const [merchant] = await sql`
          SELECT id, business_name FROM "Merchant"
          WHERE stripe_customer_id = ${customerId}
          LIMIT 1
        `;

        if (!merchant) break;

        // ═══ FULL BLOCK ═══
        await sql`
          UPDATE "Merchant"
          SET billing_status = 'cancelled',
              account_blocked = true,
              stripe_subscription_id = NULL,
              updated_at = NOW()
          WHERE id = ${merchant.id}
        `;
        await sql`UPDATE "Campaign" SET status = 'inactive', updated_at = NOW() WHERE merchant_id = ${merchant.id} AND status = 'active'`;

        console.error(`[Stripe] 🚫 FULL BLOCK: Merchant ${merchant.id} (${merchant.business_name}) — subscription deleted, campaigns deactivated`);
        break;
      }

      default:
        console.log(`[Stripe] Unhandled event type: ${event.type}`);
    }

    return res.status(200).json({ received: true });

  } catch (err) {
    console.error('[Stripe Webhook] Processing error:', err);
    return res.status(500).json({ error: err.message });
  }
};

// Vercel needs raw body for webhook signature verification
module.exports.config = {
  api: {
    bodyParser: false
  }
};
