const { neon } = require('@neondatabase/serverless');
const cheerio = require('cheerio');

async function fetchOpenGraphImage(targetUrl) {
  if (!targetUrl) return null;
  try {
    const urlStr = targetUrl.startsWith('http') ? targetUrl : 'https://' + targetUrl;
    const res = await fetch(urlStr, {
      headers: { 'User-Agent': 'PerkfinityBot/1.0 (+https://perkfinity.net)' }
    });
    if (!res.ok) return null;
    const html = await res.text();
    const $ = cheerio.load(html);
    const ogImage = $('meta[property="og:image"]').attr('content');
    if (!ogImage) return null;
    
    // Blacklist of known website builder default template images
    const blacklist = [
      'ui.nuxt.com', 
      'webflow.com', 
      'wix.com/placeholder', 
      'squarespace-cdn.com/default'
    ];
    
    for (const phrase of blacklist) {
      if (ogImage.includes(phrase)) {
        console.log(`Skipping blacklisted OG image: ${ogImage}`);
        return null;
      }
    }
    
    return ogImage;
  } catch (e) {
    console.error('Error fetching OG image:', e.message);
    return null;
  }
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET' && req.method !== 'POST') {
    res.statusCode = 405;
    return res.end('Method Not Allowed');
  }

  const authHeader = req.headers.authorization;
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    res.statusCode = 401;
    return res.end('Unauthorized');
  }

  const sql = neon(process.env.DATABASE_URL);

  try {
    // Fetch cover photos ONLY for active merchants who do NOT have any cover photo set
    const merchants = await sql`
      SELECT id, website 
      FROM "Merchant" 
      WHERE website IS NOT NULL 
        AND TRIM(website) != ''
        AND (cover_photo_url IS NULL OR TRIM(cover_photo_url) = '')
        AND account_blocked = false
        AND (billing_status IS NULL OR billing_status NOT IN ('deleted', 'cancelled'))
    `;

    let updatedCount = 0;

    for (const m of merchants) {
      let newOgImage = await fetchOpenGraphImage(m.website);
      if (newOgImage) {
        if (newOgImage.startsWith('http://')) {
          newOgImage = newOgImage.replace(/^http:\/\//i, 'https://');
        }
        await sql`
          UPDATE "Merchant" 
          SET cover_photo_url = ${newOgImage}, updated_at = NOW() 
          WHERE id = ${m.id} AND (cover_photo_url IS NULL OR TRIM(cover_photo_url) = '')
        `;
        updatedCount++;
      }
    }

    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({ success: true, updatedCount, totalChecked: merchants.length }));
  } catch (error) {
    console.error('Cron fetch-og-images error:', error);
    res.statusCode = 500;
    return res.end(JSON.stringify({ success: false, error: error.message }));
  }
};
