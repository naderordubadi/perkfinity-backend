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
 *   payment_method.detached        — Card removed from Stripe → clears PM in DB → triggers dashboard gate
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
  const STRIPE_CONNECT_WEBHOOK_SECRET = process.env.STRIPE_CONNECT_WEBHOOK_SECRET;
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
  if (STRIPE_WEBHOOK_SECRET || STRIPE_CONNECT_WEBHOOK_SECRET) {
    const sig = req.headers['stripe-signature'];
    let verified = false;
    let lastError = null;

    // Try standard secret first
    if (STRIPE_WEBHOOK_SECRET) {
      try {
        event = stripe.webhooks.constructEvent(rawBody, sig, STRIPE_WEBHOOK_SECRET);
        verified = true;
      } catch (err) {
        lastError = err;
      }
    }

    // Try connect secret if standard failed
    if (!verified && STRIPE_CONNECT_WEBHOOK_SECRET) {
      try {
        event = stripe.webhooks.constructEvent(rawBody, sig, STRIPE_CONNECT_WEBHOOK_SECRET);
        verified = true;
      } catch (err) {
        lastError = err;
      }
    }

    if (!verified) {
      console.error('Webhook signature verification failed:', lastError?.message);
      return res.status(400).json({ error: `Webhook Error: ${lastError?.message}` });
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
          const subscription = await stripe.subscriptions.retrieve(subscriptionId);
          const toTier = session.metadata?.to_tier || null;
          const toCycle = session.metadata?.billing_cycle || null;

          if (session.metadata?.is_sponsor_purchase === 'true') {
            const sponsorTier = session.metadata?.sponsor_tier;
            const cpe = new Date((subscription.current_period_end || subscription.items?.data?.[0]?.current_period_end) * 1000);
            
            let updateSql;
            if (sponsorTier === 'bundle') {
              updateSql = sql`UPDATE "Merchant" SET is_app_sponsored = true, app_sponsored_until = ${cpe}, is_web_sponsored = true, web_sponsored_until = ${cpe}, stripe_bundle_sponsor_subscription_id = ${subscriptionId} WHERE id = ${merchantId}`;
            } else if (sponsorTier === 'app') {
              updateSql = sql`UPDATE "Merchant" SET is_app_sponsored = true, app_sponsored_until = ${cpe}, stripe_app_sponsor_subscription_id = ${subscriptionId} WHERE id = ${merchantId}`;
            } else if (sponsorTier === 'web') {
              updateSql = sql`UPDATE "Merchant" SET is_web_sponsored = true, web_sponsored_until = ${cpe}, stripe_web_sponsor_subscription_id = ${subscriptionId} WHERE id = ${merchantId}`;
            }
            if (updateSql) await updateSql;
            console.log(`[Stripe] Merchant ${merchantId} purchased sponsorship via checkout`);
          } else {
            if (toCycle) {
              // Update merchant to active and change billing cycle
              await sql`
                UPDATE "Merchant"
                SET subscription_tier = COALESCE(${toTier}, subscription_tier, 'tier1'),
                    billing_cycle = ${toCycle},
                    stripe_customer_id = ${customerId},
                    stripe_subscription_id = ${subscriptionId},
                    billing_status = 'active',
                    account_blocked = false,
                    subscription_started_at = COALESCE(subscription_started_at, NOW()),
                    next_billing_date = ${new Date((subscription.current_period_end || subscription.items?.data?.[0]?.current_period_end) * 1000)},
                    updated_at = NOW()
                WHERE id = ${merchantId}
              `;
            } else {
              // Update merchant to active without changing billing cycle
              await sql`
                UPDATE "Merchant"
                SET subscription_tier = COALESCE(${toTier}, subscription_tier, 'tier1'),
                    stripe_customer_id = ${customerId},
                    stripe_subscription_id = ${subscriptionId},
                    billing_status = 'active',
                    account_blocked = false,
                    subscription_started_at = COALESCE(subscription_started_at, NOW()),
                    next_billing_date = ${new Date((subscription.current_period_end || subscription.items?.data?.[0]?.current_period_end) * 1000)},
                    updated_at = NOW()
                WHERE id = ${merchantId}
              `;
            }
            console.log(`[Stripe] Merchant ${merchantId} upgraded via checkout`);
          }
        } else if (session.mode === 'payment' && session.metadata?.billing_cycle === 'lifetime') {
          const [merch] = await sql`SELECT promo_code, subscription_tier FROM "Merchant" WHERE id = ${merchantId} LIMIT 1`;
          if (merch?.promo_code) {
            await sql`UPDATE "AdminAccessCode" SET used = true, used_by = ${merchantId}, used_at = NOW(), use_count = use_count + 1 WHERE code = ${merch.promo_code} AND type = 'pouf'`;
          }
          await sql`
            UPDATE "Merchant"
            SET billing_status = 'active',
                subscription_tier = COALESCE(${merch?.subscription_tier}, 'tier1'),
                billing_cycle = 'lifetime',
                stripe_customer_id = ${customerId},
                account_blocked = false,
                subscription_started_at = COALESCE(subscription_started_at, NOW()),
                updated_at = NOW()
            WHERE id = ${merchantId}
          `;

          // Record the one-time POUF payment in the Invoice table
          await sql`
            INSERT INTO "Invoice" (id, merchant_id, stripe_invoice_id, amount_cents, currency, status, period_start, period_end, paid_at, created_at)
            VALUES (
              gen_random_uuid()::text,
              ${merchantId},
              COALESCE(${session.invoice}, ${session.id}),
              ${session.amount_total ?? 0},
              ${session.currency || 'usd'},
              'paid',
              NOW(),
              NOW(),
              NOW(),
              NOW()
            )
            ON CONFLICT (stripe_invoice_id) DO UPDATE SET status = 'paid', paid_at = NOW()
          `;

          // Start commission attribution
          await sql`
            UPDATE "ContractorMerchantAttribution"
            SET commission_start_date = NOW(), updated_at = NOW()
            WHERE merchant_id = ${merchantId} AND commission_start_date IS NULL
          `;

          console.log(`[Stripe] Merchant ${merchantId} upgraded to lifetime via checkout. Invoice and attribution created.`);
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
          SELECT id, business_name, billing_status, account_blocked, stripe_subscription_id, billing_cycle, subscription_tier FROM "Merchant"
          WHERE stripe_customer_id = ${customerId}
          LIMIT 1
        `;

        if (!merchant) {
          console.warn(`invoice.payment_succeeded: No merchant found for customer ${customerId}`);
          break;
        }

        if (merchant.billing_status === 'deleted') {
          console.log(`[Stripe Webhook] Merchant ${merchant.id} is deleted. Ignoring invoice.payment_succeeded.`);
          break;
        }

        // If merchant is fully blocked (subscription was deleted) and has no active subscription attached,
        // do not unblock them just because an old invoice cleared.
        if (merchant.account_blocked && !merchant.stripe_subscription_id) {
          console.log(`[Stripe] Late invoice cleared for permanently cancelled merchant ${merchant.id}. Keeping blocked status.`);
        } else {
          let currentPeriodEnd = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);
          let isSponsorship = false;
          let sponsorTier = null;

          if (subscriptionId) {
            try {
              const sub = await stripe.subscriptions.retrieve(subscriptionId);
              const cpe = sub.current_period_end || sub.items?.data?.[0]?.current_period_end;
              if (cpe) {
                currentPeriodEnd = new Date(cpe * 1000);
              }

              // Check if this subscription is for sponsorship
              for (const item of sub.items?.data || []) {
                const pId = item.price.id;
                if (pId === process.env.STRIPE_SPONSOR_WEB_PRICE_ID) { isSponsorship = true; sponsorTier = 'web'; break; }
                if (pId === process.env.STRIPE_SPONSOR_APP_PRICE_ID) { isSponsorship = true; sponsorTier = 'app'; break; }
                if (pId === process.env.STRIPE_SPONSOR_BUNDLE_PRICE_ID) { isSponsorship = true; sponsorTier = 'bundle'; break; }
              }
            } catch (e) {
              console.error('Failed to fetch subscription for webhook:', e);
            }
          }
          if (isSponsorship) {
            let updateSql;
            if (sponsorTier === 'bundle') {
              updateSql = sql`UPDATE "Merchant" SET is_app_sponsored = true, app_sponsored_until = ${currentPeriodEnd}, is_web_sponsored = true, web_sponsored_until = ${currentPeriodEnd}, stripe_bundle_sponsor_subscription_id = COALESCE(stripe_bundle_sponsor_subscription_id, ${subscriptionId}) WHERE id = ${merchant.id}`;
            } else if (sponsorTier === 'app') {
              updateSql = sql`UPDATE "Merchant" SET is_app_sponsored = true, app_sponsored_until = ${currentPeriodEnd}, stripe_app_sponsor_subscription_id = COALESCE(stripe_app_sponsor_subscription_id, ${subscriptionId}) WHERE id = ${merchant.id}`;
            } else if (sponsorTier === 'web') {
              updateSql = sql`UPDATE "Merchant" SET is_web_sponsored = true, web_sponsored_until = ${currentPeriodEnd}, stripe_web_sponsor_subscription_id = COALESCE(stripe_web_sponsor_subscription_id, ${subscriptionId}) WHERE id = ${merchant.id}`;
            }
            if (updateSql) await updateSql;
            console.log(`[Stripe] Sponsorship invoice paid for merchant ${merchant.id}, tier: ${sponsorTier}`);
          } else {
            if (merchant.billing_cycle !== 'lifetime') {
              await sql`
                UPDATE "Merchant"
                SET billing_status = 'active',
                    account_blocked = false,
                    cancelled_at = NULL,
                    next_billing_date = ${currentPeriodEnd},
                    updated_at = NOW()
                WHERE id = ${merchant.id}
              `;
            }
            console.log(`[Stripe] Merchant ${merchant.id} invoice succeeded. Active until ${currentPeriodEnd}`);
          }
        }

        const revenueType = isSponsorship ? 'sponsorship' : 'platform';
        // Record in Invoice table
        await sql`
          INSERT INTO "Invoice" (id, merchant_id, stripe_invoice_id, amount_cents, currency, status, period_start, period_end, paid_at, created_at, revenue_type)
          VALUES (
            gen_random_uuid()::text,
            ${merchant.id},
            ${invoice.id},
            ${invoice.amount_paid ?? 0},
            ${invoice.currency || 'usd'},
            'paid',
            ${invoice.period_start ? new Date(invoice.period_start * 1000) : null},
            ${invoice.period_end ? new Date(invoice.period_end * 1000) : null},
            NOW(),
            NOW(),
            ${revenueType}
          )
          ON CONFLICT (stripe_invoice_id) DO UPDATE SET status = 'paid', paid_at = NOW(), revenue_type = ${revenueType}
        `;

        // Start commission if there's an active attribution waiting for the first payment
        await sql`
          UPDATE "ContractorMerchantAttribution"
          SET commission_start_date = NOW(), updated_at = NOW()
          WHERE merchant_id = ${merchant.id} AND commission_start_date IS NULL
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
          SELECT id, business_name, billing_status FROM "Merchant"
          WHERE stripe_customer_id = ${customerId}
          LIMIT 1
        `;

        if (!merchant) break;

        if (merchant.billing_status === 'deleted') {
          console.log(`[Stripe Webhook] Merchant ${merchant.id} is deleted. Ignoring invoice.payment_failed.`);
          break;
        }

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

        if (merchant.billing_status === 'deleted') {
          console.log(`[Stripe Webhook] Merchant ${merchant.id} is deleted. Ignoring customer.subscription.updated.`);
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
                cancelled_at = NULL,
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
        const subscriptionId = subscription.id;

        const [merchant] = await sql`
          SELECT id, business_name, billing_status, stripe_subscription_id, stripe_bundle_sponsor_subscription_id, stripe_app_sponsor_subscription_id, stripe_web_sponsor_subscription_id FROM "Merchant"
          WHERE stripe_customer_id = ${customerId}
          LIMIT 1
        `;

        if (!merchant) break;

        if (merchant.billing_status === 'deleted') {
          console.log(`[Stripe Webhook] Merchant ${merchant.id} is deleted. Ignoring customer.subscription.deleted.`);
          break;
        }

        if (merchant.stripe_bundle_sponsor_subscription_id === subscriptionId) {
          // It's the bundle sponsor subscription being deleted
          await sql`
            UPDATE "Merchant"
            SET is_app_sponsored = false,
                is_web_sponsored = false,
                app_sponsored_until = NULL,
                web_sponsored_until = NULL,
                stripe_bundle_sponsor_subscription_id = NULL,
                updated_at = NOW()
            WHERE id = ${merchant.id}
          `;
          console.log(`[Stripe] Merchant ${merchant.id} (${merchant.business_name}) — bundle sponsor subscription deleted`);
        } else if (merchant.stripe_app_sponsor_subscription_id === subscriptionId) {
          // It's the app sponsor subscription being deleted
          await sql`
            UPDATE "Merchant"
            SET is_app_sponsored = false,
                app_sponsored_until = NULL,
                stripe_app_sponsor_subscription_id = NULL,
                updated_at = NOW()
            WHERE id = ${merchant.id}
          `;
          console.log(`[Stripe] Merchant ${merchant.id} (${merchant.business_name}) — app sponsor subscription deleted`);
        } else if (merchant.stripe_web_sponsor_subscription_id === subscriptionId) {
          // It's the web sponsor subscription being deleted
          await sql`
            UPDATE "Merchant"
            SET is_web_sponsored = false,
                web_sponsored_until = NULL,
                stripe_web_sponsor_subscription_id = NULL,
                updated_at = NOW()
            WHERE id = ${merchant.id}
          `;
          console.log(`[Stripe] Merchant ${merchant.id} (${merchant.business_name}) — web sponsor subscription deleted`);
        } else if (merchant.stripe_subscription_id === subscriptionId || !merchant.stripe_subscription_id) {
          // ═══ FULL BLOCK ═══
          await sql`
            UPDATE "Merchant"
            SET billing_status = 'cancelled',
                account_blocked = true,
                cancelled_at = NOW(),
                stripe_subscription_id = NULL,
                updated_at = NOW()
            WHERE id = ${merchant.id}
          `;
          await sql`UPDATE "Campaign" SET status = 'expired', updated_at = NOW() WHERE merchant_id = ${merchant.id} AND status = 'active'`;

          console.error(`[Stripe] 🚫 FULL BLOCK: Merchant ${merchant.id} (${merchant.business_name}) — main subscription deleted, campaigns deactivated`);
        } else {
          console.log(`[Stripe] Merchant ${merchant.id} — unknown subscription ${subscriptionId} deleted, ignoring.`);
        }
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // PAYMENT METHOD DETACHED — Card removed from Stripe
      // Fires when a merchant deletes their card via the Stripe portal
      // or any API call. Clears our DB field so the login/dashboard
      // gate detects the missing PM and redirects to add-card flow.
      // ═══════════════════════════════════════════════════════════
      case 'payment_method.detached': {
        const paymentMethod = event.data.object;
        const paymentMethodId = paymentMethod.id;

        // Find the merchant who had this payment method on file
        const [merchant] = await sql`
          SELECT id, business_name, billing_status, subscription_tier FROM "Merchant"
          WHERE stripe_payment_method_id = ${paymentMethodId}
          LIMIT 1
        `;

        if (!merchant) {
          // PM may have been attached to a customer but not the primary one on file — safe to ignore
          console.log(`[Stripe] payment_method.detached: ${paymentMethodId} — no merchant match, ignoring`);
          break;
        }

        if (merchant.billing_status === 'deleted') {
          console.log(`[Stripe Webhook] Merchant ${merchant.id} is deleted. Ignoring payment_method.detached.`);
          break;
        }

        // Clear the payment method from our DB
        await sql`
          UPDATE "Merchant"
          SET stripe_payment_method_id = NULL,
              updated_at = NOW()
          WHERE id = ${merchant.id}
        `;

        console.warn(`[Stripe] ⚠️ Payment method ${paymentMethodId} detached for merchant ${merchant.id} (${merchant.business_name}, tier: ${merchant.subscription_tier}). PM cleared from DB — next login will require re-adding card.`);
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // ACCOUNT UPDATED — Stripe Connect Express KYC Status
      // ═══════════════════════════════════════════════════════════
      case 'account.updated': {
        const account = event.data.object;
        const [rep] = await sql`
          SELECT id, full_name FROM "Contractor"
          WHERE stripe_account_id = ${account.id}
          LIMIT 1
        `;

        if (rep) {
          if (account.charges_enabled && account.details_submitted) {
            await sql`
              UPDATE "Contractor"
              SET stripe_onboarding_status = 'complete',
                  updated_at = NOW()
              WHERE id = ${rep.id}
            `;
            console.log(`[Stripe Connect] Rep ${rep.id} (${rep.full_name}) onboarding complete.`);
          } else {
            await sql`
              UPDATE "Contractor"
              SET stripe_onboarding_status = 'pending',
                  updated_at = NOW()
              WHERE id = ${rep.id}
            `;
            console.log(`[Stripe Connect] Rep ${rep.id} (${rep.full_name}) onboarding pending (KYC incomplete).`);
          }
        }
        break;
      }

      // ═══════════════════════════════════════════════════════════
      // PAYOUT PAID / FAILED — Connect Account Payouts
      // ═══════════════════════════════════════════════════════════
      case 'payout.paid':
      case 'payout.failed': {
        const stripeAccountId = event.account;
        if (!stripeAccountId) break;

        const [rep] = await sql`
          SELECT id FROM "Contractor"
          WHERE stripe_account_id = ${stripeAccountId}
          LIMIT 1
        `;

        if (!rep) break;

        if (event.type === 'payout.paid') {
          const processingPayouts = await sql`
            UPDATE "ContractorPayout"
            SET status = 'paid',
                paid_at = NOW(),
                updated_at = NOW()
            WHERE contractor_id = ${rep.id} AND status IN ('processing', 'failed')
            RETURNING *
          `;
          
          const mpYear = new Date().getFullYear();
          for (const p of processingPayouts) {
            const mpBonusCents = p.milestone_bonus_cents + p.retention_bonus_cents + p.special_bonus_cents;
            await sql`
              INSERT INTO "ContractorEarningsSummary" (id, contractor_id, year, commission_ytd_cents, retainer_ytd_cents, bonus_ytd_cents, total_ytd_cents, updated_at)
              VALUES (gen_random_uuid()::text, ${p.contractor_id}, ${mpYear}, ${p.commission_cents}, ${p.retainer_cents}, ${mpBonusCents}, ${p.total_cents}, NOW())
              ON CONFLICT (contractor_id, year) DO UPDATE SET
                commission_ytd_cents = "ContractorEarningsSummary".commission_ytd_cents + ${p.commission_cents},
                retainer_ytd_cents   = "ContractorEarningsSummary".retainer_ytd_cents   + ${p.retainer_cents},
                bonus_ytd_cents      = "ContractorEarningsSummary".bonus_ytd_cents      + ${mpBonusCents},
                total_ytd_cents      = "ContractorEarningsSummary".total_ytd_cents      + ${p.total_cents},
                updated_at           = NOW()
            `;
          }
          console.log(`[Stripe Connect] Payouts paid for rep ${rep.id}`);
        } else if (event.type === 'payout.failed') {
          await sql`
            UPDATE "ContractorPayout"
            SET status = 'failed',
                updated_at = NOW()
            WHERE contractor_id = ${rep.id} AND status IN ('processing', 'failed')
          `;
          console.error(`[Stripe Connect] Payout FAILED for rep ${rep.id}`);
        }
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
