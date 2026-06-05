const http = require('http');

const data = JSON.stringify({ email: 'naderordubadi@yahoo.com', password: 'Test1234!' });

const options = {
  hostname: 'localhost',
  port: 3001,
  path: '/api/v1/rep/login',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Content-Length': data.length,
  },
};

const req = http.request(options, (res) => {
  let body = '';
  res.on('data', (chunk) => { body += chunk; });
  res.on('end', () => {
    console.log(`Status: ${res.statusCode}`);
    console.log(`Body: ${body}`);
  });
});

req.on('error', (e) => { console.error(`Problem with request: ${e.message}`); });
req.write(data);
req.end();
