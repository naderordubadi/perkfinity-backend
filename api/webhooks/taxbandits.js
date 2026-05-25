'use strict';

/**
 * Perkfinity — TaxBandits Webhook Handler
 *
 * Endpoint: POST /api/webhooks/taxbandits
 *
 * TaxBandits fires this when a contractor completes (or otherwise changes)
 * their W-9 via the SmartCollect (WhCertificate) flow.
 *
 * What we do:
 *   1. Parse the WhCertificate status-change payload
 *   2. If Status === 'COMPLETED' → look up contractor by taxbandits_submission_id
 *      and set w9_status = 'pending' (admin still needs to review and click "Mark Verified")
 *   3. Always return HTTP 200 — TaxBandits will retry on non-200
 *
 * NOTE: TaxBandits sandbox does not send a signature header.
 *       We validate by matching the SubmissionId in our DB — only a valid
 *       prior sendW9Request() will have stored that ID, so random POSTs do nothing.
 */

const { neon } = require('@neondatabase/serverless');

module.exports = async (req, res) => {
  // ── CORS preflight ───────────────────────────────────────────────────────────
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    return res.status(204).end();
  }

  // Always respond 200 first — TaxBandits retries on non-200
  // We process asynchronously below but return 200 immediately if needed.
  // For simplicity (and since DB ops are fast), we process synchronously then return 200.

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const sql = neon(process.env.DATABASE_URL);

  try {
    const body = req.body || {};

    // TaxBandits sends the event under a "WhCertificate" key
    const event = body.WhCertificate || body.FormW9 || null;

    if (!event) {
      console.log('[taxbandits-webhook] Received payload with no WhCertificate or FormW9 key — ignoring.', JSON.stringify(body).slice(0, 500));
      return res.status(200).json({ received: true });
    }

    const submissionId = event.SubmissionId || event.submissionId || null;
    const status       = (event.Status || event.W9Status || '').toUpperCase();
    const email        = event.Email || null;
    const pdfUrl       = event.PdfUrl || event.pdfUrl || null;

    console.log(`[taxbandits-webhook] SubmissionId=${submissionId} Status=${status} Email=${email}`);

    if (!submissionId) {
      console.warn('[taxbandits-webhook] No SubmissionId in payload — cannot map to contractor.');
      return res.status(200).json({ received: true });
    }

    // ── COMPLETED: contractor submitted the W-9 ─────────────────────────────
    if (status === 'COMPLETED') {
      // Look up contractor by the SubmissionId we stored when we sent the request
      const [contractor] = await sql`
        SELECT id, full_name, w9_status
        FROM "Contractor"
        WHERE taxbandits_submission_id = ${submissionId}
        LIMIT 1
      `;

      if (!contractor) {
        console.warn(`[taxbandits-webhook] No contractor found for SubmissionId=${submissionId}`);
        return res.status(200).json({ received: true });
      }

      // Only update if not already verified — don't downgrade a verified status
      if (contractor.w9_status !== 'verified') {
        // Map TaxBandits TaxClassification to our internal entity_type values
        const TAX_CLASS_MAP = {
          'Individual':       'individual',
          'SoleProprietor':   'sole_proprietor',
          'SingleMemberLLC':  'llc_single',
          'LLCPartnership':   'llc_partnership',
          'CCorporation':     'c_corporation',
          'SCorporation':     's_corporation',
        };
        const formData    = event.FormData || event.formData || {};
        const taxClass    = formData.TaxClassification || formData.taxClassification || null;
        const entityType  = taxClass ? (TAX_CLASS_MAP[taxClass] || 'other') : null;
        await sql`
          UPDATE "Contractor"
          SET
            w9_status   = 'pending',
            entity_type = COALESCE(${entityType}, entity_type),
            updated_at  = NOW()
          WHERE id = ${contractor.id}
        `;
        console.log(`[taxbandits-webhook] ✅ Contractor ${contractor.full_name} (${contractor.id}) W-9 status → pending, entity_type=${entityType || 'unchanged'}. PDF: ${pdfUrl || 'N/A'}`);
      } else {
        console.log(`[taxbandits-webhook] Contractor ${contractor.full_name} already verified — no update.`);
      }

      return res.status(200).json({ received: true });
    }

    // ── Other statuses (errors, expirations, etc.) ──────────────────────────
    // Log but take no action — admin will handle edge cases manually
    console.log(`[taxbandits-webhook] Unhandled status="${status}" for SubmissionId=${submissionId} — no DB update.`);
    return res.status(200).json({ received: true });

  } catch (err) {
    // Still return 200 so TaxBandits doesn't retry — log for debugging
    console.error('[taxbandits-webhook] Error processing payload:', err.message, err.stack);
    return res.status(200).json({ received: true });
  }
};
