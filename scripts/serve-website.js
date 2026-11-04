const http = require('http');
const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '../../Website-Perkfinity');
const PORT = process.env.WEB_PORT || 3000;

const mimeTypes = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
};

const server = http.createServer((req, res) => {
  // Enable CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  let reqPath = req.url.split('?')[0];

  // Dynamic SSR routes for local development
  const storeMatch = reqPath.match(/^\/(?:deals\/)?store\/([^/?#]+)$/);
  if (storeMatch) {
    const storeHandler = require(path.join(root, 'api/store-page.js'));
    req.query = { slug: storeMatch[1] };
    res.status = (code) => ({ send: (body) => { res.writeHead(code); res.end(body); } });
    return storeHandler(req, res);
  }
  const dealsMatch = reqPath.match(/^\/deals\/([^/?#]+)$/);
  if (dealsMatch) {
    const dealsHandler = require(path.join(root, 'api/deals-page.js'));
    req.query = { location: dealsMatch[1] };
    res.status = (code) => ({ send: (body) => { res.writeHead(code); res.end(body); } });
    return dealsHandler(req, res);
  }
  if (reqPath === '/sitemap.xml' || reqPath === '/sitemap-live.xml') {
    const sitemapHandler = require(path.join(root, 'api/sitemap.js'));
    res.status = (code) => ({ send: (body) => { res.writeHead(code); res.end(body); } });
    return sitemapHandler(req, res);
  }

  if (reqPath === '/' || reqPath.endsWith('/')) reqPath += 'index.html';
  const filePath = path.resolve(root, '.' + reqPath);

  if (!filePath.startsWith(root)) {
    console.error('Forbidden path:', { reqPath, filePath, root });
    res.statusCode = 403;
    return res.end('Forbidden');
  }

  fs.stat(filePath, (err, stats) => {
    if (err) {
      res.statusCode = 404;
      return res.end('Not Found: ' + reqPath);
    }
    if (stats.isDirectory()) {
      const indexFile = path.join(filePath, 'index.html');
      fs.stat(indexFile, (err2, stats2) => {
        if (err2 || !stats2.isFile()) {
          res.statusCode = 404;
          return res.end('Directory Index Not Found');
        }
        res.writeHead(200, { 'Content-Type': 'text/html' });
        fs.createReadStream(indexFile).pipe(res);
      });
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    const contentType = mimeTypes[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': contentType });
    fs.createReadStream(filePath).pipe(res);
  });
});

server.listen(PORT, () => {
  console.log(`\n🌐 Website-Perkfinity local server running at http://localhost:${PORT}`);
  console.log(`   Admin Portal: http://localhost:${PORT}/admin/index.html\n`);
});

server.on('error', (err) => {
  console.error('Server error:', err.message);
  process.exit(1);
});
