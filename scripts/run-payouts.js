require('dotenv').config({ path: __dirname + '/../.env' });
const handler = require('../api/cron/process-contractor-payouts.js');

const req = {
  method: 'GET',
  headers: {}
};

const res = {
  status: (code) => {
    console.log('Status:', code);
    return res;
  },
  json: (data) => {
    console.log('Response:', JSON.stringify(data, null, 2));
    process.exit(0);
  }
};

console.log('--- Triggering Stripe Payout Processor Locally ---');
handler(req, res).catch(err => {
  console.error('Error running payout processor:', err);
  process.exit(1);
});
