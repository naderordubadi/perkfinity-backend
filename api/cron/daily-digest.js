/**
 * Perkfinity Daily Digest Cron
 * Runs daily at 9 AM PST (17:00 UTC) via Vercel Cron.
 * Batches all queued notifications per user into one email + one push.
 */

const { neon } = require('@neondatabase/serverless');
const SibApiV3Sdk = require('sib-api-v3-sdk');
const admin = require('firebase-admin');

// ── Firebase Admin Init ──────────────────────────────────────────
let firebaseInitialized = false;
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT && !admin.apps.length) {
    let raw = process.env.FIREBASE_SERVICE_ACCOUNT;
    // Fix: Vercel sometimes double-escapes \\n in private_key — normalize before parsing
    raw = raw.replace(/\\\\n/g, '\\n');
    const cert = JSON.parse(raw);
    // Ensure private_key newlines are real newlines
    if (cert.private_key) cert.private_key = cert.private_key.replace(/\\n/g, '\n');
    admin.initializeApp({ credential: admin.credential.cert(cert) });
    firebaseInitialized = true;
  } else if (admin.apps.length) {
    firebaseInitialized = true;
  }
} catch (err) {
  console.error('Firebase Admin init error:', err);
}

async function sendPushNotification(token, title, body) {
  if (!firebaseInitialized || !token) return;
  try {
    await admin.messaging().send({
      token,
      notification: { title, body },
      apns: {
        headers: { 'apns-priority': '10' },
        payload: {
          aps: {
            alert: { title, body },
            sound: 'default',
            badge: 1
          }
        }
      }
    });
    return true;
  } catch (err) {
    console.error('Firebase push error:', err);
    return false;
  }
}

module.exports = async (req, res) => {
  // ── Security: only Vercel Cron or manual trigger with secret ──
  const cronSecret = process.env.CRON_SECRET;
  const authHeader = req.headers.authorization;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }

  const DATABASE_URL = process.env.DATABASE_URL;
  if (!DATABASE_URL) {
    return res.status(500).json({ success: false, error: 'DATABASE_URL not configured' });
  }
  const sql = neon(DATABASE_URL);

  try {
    // ── Fetch all unsent queue items ────────────────────────────
    const pendingItems = await sql`
      SELECT nq.*, u.email, u.push_token, u.full_name
      FROM "NotificationQueue" nq
      JOIN "User" u ON u.id = nq.user_id
      WHERE nq.sent = false
      ORDER BY nq.user_id, nq.created_at ASC
    `;

    if (pendingItems.length === 0) {
      return res.status(200).json({ success: true, message: 'No pending notifications', stats: { users: 0, emails: 0, pushes: 0 } });
    }

    // ── Group by user_id ──────────────────────────────────────
    const byUser = {};
    for (const item of pendingItems) {
      if (!byUser[item.user_id]) byUser[item.user_id] = [];
      byUser[item.user_id].push(item);
    }

    const BREVO_KEY = process.env.BREVO_API_KEY;
    let emailApi = null;
    if (BREVO_KEY) {
      const brevoClient = SibApiV3Sdk.ApiClient.instance;
      brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
      emailApi = new SibApiV3Sdk.TransactionalEmailsApi();
    }

    let totalEmailSent = 0;
    let totalPushSent = 0;
    let totalUsersProcessed = 0;
    const processedIds = [];

    // ── Process each user's batch ─────────────────────────────
    for (const [userId, items] of Object.entries(byUser)) {
      const userEmail = items[0].email;
      const pushToken = items[0].push_token;
      const userName = items[0].full_name || 'Perkfinity Member';

      // Determine what channels to send
      const shouldEmail = items.some(i => i.channels === 'email' || i.channels === 'both');
      const shouldPush = items.some(i => i.channels === 'push' || i.channels === 'both');

      // ── Build Digest Email ──────────────────────────────────
      if (shouldEmail && emailApi && userEmail) {
        const offerCount = items.length;
        const subject = offerCount === 1
          ? `🎉 New perk from ${items[0].store_name}`
          : `🎉 You have ${offerCount} new perks from your local stores`;

        // Build offer cards HTML
        const offerCards = items.map(item => `
          <div style="display:flex; align-items:center; gap:14px; padding:14px 16px; background:#f9fafb; border-radius:12px; margin-bottom:10px; border:1px solid #f0f0f0;">
            ${item.logo_url ? `<img src="${item.logo_url}" alt="${item.store_name}" style="width:40px;height:40px;border-radius:50%;object-fit:contain;background:#fff;border:1px solid #eee;flex-shrink:0;"/>` : `<div style="width:40px;height:40px;border-radius:50%;background:linear-gradient(135deg,#8B5CF6,#6BC17A);display:flex;align-items:center;justify-content:center;flex-shrink:0;color:#fff;font-weight:800;font-size:16px;">${item.store_name.charAt(0)}</div>`}
            <div style="flex:1;min-width:0;">
              <div style="font-size:14px;font-weight:700;color:#1a1a2e;margin-bottom:2px;">${item.store_name}</div>
              <div style="font-size:14px;color:#5B3FA5;font-weight:600;">${item.title}</div>
              ${item.body && item.body !== item.title ? `<div style="font-size:12px;color:#888;margin-top:2px;">${item.body}</div>` : ''}
              ${item.store_address ? `<div style="font-size:11px;color:#aaa;margin-top:3px;">📍 ${item.store_address}</div>` : ''}
              ${item.offer_expires_at ? `<div style="font-size:11px;color:#B45309;margin-top:3px;font-weight:600;">Expires: ${new Date(item.offer_expires_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>` : ''}
              ${item.disclaimer ? `<div style="font-size:10px;color:#aaa;margin-top:3px;font-style:italic;">${item.disclaimer}</div>` : ''}
            </div>
          </div>
        `).join('');

        const emailHtml = `
          <div style="font-family:'Helvetica Neue',Arial,sans-serif; max-width:520px; margin:0 auto; background:#ffffff; border-radius:16px; overflow:hidden; border:1px solid #eee;">
            <div style="background:linear-gradient(135deg,#5B3FA5,#6BC17A); padding:24px; text-align:center;">
              <img src="https://perkfinity.net/assets/Perkfinity-Logo.png" alt="Perkfinity" width="200" height="40" style="display:inline-block; width:200px; max-width:100%; height:auto; max-height:40px; object-fit:contain; opacity:0.95; margin-bottom:8px;"/>
              <div style="color:#fff; font-size:20px; font-weight:800;">Your Neighborhood Perks, Just for You</div>
              <div style="color:rgba(255,255,255,0.85); font-size:13px; margin-top:4px;">${offerCount} new perk${offerCount > 1 ? 's' : ''} from your favorite stores</div>
            </div>
            <div style="padding:20px 24px;">
              <div style="font-size:14px; color:#666; margin-bottom:16px;">Hi ${userName} 👋, here's what's new today:</div>
              ${offerCards}
              <div style="background:rgba(107,193,122,0.12); border:1px solid rgba(107,193,122,0.3); border-radius:10px; padding:12px 18px; text-align:center; margin-top:16px;">
                <p style="font-size:13px; color:#1a6b2b; margin:0; font-weight:500;">Scan the Perkfinity QR code in-store to activate your perks!</p>
              </div>
            </div>
            <div style="padding:16px 24px; border-top:1px solid #f0f0f0; text-align:center;">
              <div style="font-size:11px; color:#bbb;">Powered by <strong style="color:#5B3FA5;">Perkfinity</strong></div>
            </div>
          </div>
        `;

        try {
          const sendSmtpEmail = new SibApiV3Sdk.SendSmtpEmail();
          sendSmtpEmail.sender = { name: 'Perkfinity', email: 'noreply@perkfinity.net' };
          sendSmtpEmail.to = [{ email: userEmail }];
          sendSmtpEmail.subject = subject;
          sendSmtpEmail.htmlContent = emailHtml;
          await emailApi.sendTransacEmail(sendSmtpEmail);
          totalEmailSent++;
        } catch (emailErr) {
          console.error(`Digest email failed for user ${userId}:`, emailErr.message || emailErr);
        }
      }

      // ── Build Digest Push ────────────────────────────────────
      if (shouldPush && pushToken) {
        const offerCount = items.length;
        const pushTitle = offerCount === 1
          ? `🎉 New perk from ${items[0].store_name}`
          : `🎉 ${offerCount} new perks from your local stores`;
        let pushBody = offerCount === 1
          ? `${items[0].title}${items[0].store_address ? ' — 📍 ' + items[0].store_address : ''}`
          : items.map(i => `${i.store_name}: ${i.title}`).slice(0, 3).join(' • ') + (offerCount > 3 ? ` +${offerCount - 3} more` : '');

        // Append disclaimer to push body if present
        const disclaimers = [...new Set(items.map(i => i.disclaimer).filter(Boolean))];
        if (disclaimers.length > 0) {
          pushBody += '\n' + disclaimers.join(' | ');
        }

        const pushResult = await sendPushNotification(pushToken, pushTitle, pushBody);
        if (pushResult) totalPushSent++;
      }

      // Collect processed IDs
      for (const item of items) {
        processedIds.push(item.id);
      }

      // ── Persist to NotificationHistory for in-app viewing ────────
      const offerCount = items.length;
      const histTitle = offerCount === 1
        ? `New perk from ${items[0].store_name}`
        : `${offerCount} new perks from your local stores`;
      const histBody = offerCount === 1
        ? items[0].title
        : items.map(i => `${i.store_name}: ${i.title}`).join(' • ');
      const histPayload = JSON.stringify(items.map(i => ({
        store_name: i.store_name,
        logo_url: i.logo_url || null,
        title: i.title,
        body: i.body || null,
        store_address: i.store_address || null,
        campaign_id: i.campaign_id,
        merchant_id: i.merchant_id,
        offer_expires_at: i.offer_expires_at || null,
        disclaimer: i.disclaimer || null,
      })));
      try {
        await sql`
          INSERT INTO "NotificationHistory" (user_id, title, body, type, payload)
          VALUES (${userId}, ${histTitle}, ${histBody}, ${offerCount === 1 ? 'single' : 'digest'}, ${histPayload}::jsonb)
        `;
      } catch (histErr) {
        console.error(`Failed to persist NotificationHistory for user ${userId}:`, histErr.message || histErr);
      }

      totalUsersProcessed++;
    }

    // ── Mark all processed items as sent ────────────────────────
    if (processedIds.length > 0) {
      await sql`UPDATE "NotificationQueue" SET sent = true WHERE id = ANY(${processedIds})`;
    }

    return res.status(200).json({
      success: true,
      message: `Daily digest sent to ${totalUsersProcessed} user(s)`,
      stats: {
        users: totalUsersProcessed,
        emails: totalEmailSent,
        pushes: totalPushSent,
        items_processed: processedIds.length
      }
    });
  } catch (err) {
    console.error('Daily digest cron error:', err);
    return res.status(500).json({ success: false, error: err.message || 'Internal server error' });
  }
};
