const Stripe = require('stripe');
require('dotenv').config({ path: __dirname + '/../.env' });

const stripe = Stripe(process.env.STRIPE_SECRET_KEY);

async function run() {
  console.log('--- Triggering Live Stripe Test Charge ---');
  
  const customerId = 'cus_UGKiUidSeSuVSS';
  
  try {
    // 1. Create an Invoice Item (a charge pending to be invoiced)
    await stripe.invoiceItems.create({
      customer: customerId,
      amount: 14900, // $149.00
      currency: 'usd',
      description: 'Phase 4 Test Sale',
    });

    // 2. Create and pay the Invoice
    const invoice = await stripe.invoices.create({
      customer: customerId,
      collection_method: 'charge_automatically',
      auto_advance: true
    });

    const paidInvoice = await stripe.invoices.pay(invoice.id);
    console.log(`✅ Success! Created and paid test invoice for $149.00.`);
    console.log(`Invoice ID: ${paidInvoice.id}`);
    console.log(`\nCheck your 'stripe listen' terminal tab! You should see the 'invoice.payment_succeeded' webhook arrive momentarily.`);
  } catch (err) {
    console.error('❌ Failed to charge customer:', err.message);
    if (err.message.includes('No such customer')) {
        console.log('It looks like this customer was deleted from your Stripe account.');
    } else if (err.message.includes('payment method')) {
        console.log('This customer does not have a test card attached in Stripe to charge.');
    }
  }
}

run();
