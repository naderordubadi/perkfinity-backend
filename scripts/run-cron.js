require('dotenv').config({ path: __dirname + '/../.env' });
const handler = require('../api/cron/generate-contractor-payouts.js');

const req = {
  method: 'GET',
  headers: {},
  query: { year: 2026, month: 6 }
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

console.log('--- Triggering Cron Job Locally ---');
handler(req, res).catch(err => {
  console.error('Error running cron:', err);
  process.exit(1);
});
