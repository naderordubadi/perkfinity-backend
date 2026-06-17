/**
 * Perkfinity — Monthly Contractor Payout Cron
 * Task 3: Sales Contractor Management System
 *
 * Runs on the 1st of every month at 09:00 UTC (schedule: 0 9 1 * * in vercel.json).
 * Calculates commissions, retainers, milestone bonuses, retention bonuses,
 * and special bonuses for all active contractors whose W-9 is verified.
 * Creates ContractorPayout records in 'pending' status for admin review.
 *
 * Period: month prior to last (Net-45 EOM structure).
 *
 * Commission rules:
 *   Monthly merchants:
 *     - Regular commission = rate × invoice amount per period
 *     - Retention bonus fires at exactly 12 and 24 *calendar* months elapsed
 *       (measured accurately via year/month components — not a 30-day approximation)
 *
 *   Annual merchants:
 *     - Regular commission = rate × annual invoice amount
 *     - Commission is capped by invoice count, not just date:
 *         12-month plan → pay commission on invoice #1 only (year 1)
 *         24-month plan → pay commission on invoices #1 and #2 (years 1 and 2)
 *       Prevents double-paying when a renewal falls within a still-open date window.
 *     - Retention bonus fires at 1st annual renewal (2nd paid invoice)
 *       and 2nd annual renewal (3rd paid invoice, 24-month plan only)
 *     - Retention bonus amount = rate × (annual_invoice / 12)  [monthly equivalent]
 *
 * commission_end_date:
 *   Set by the Stripe webhook at first payment (= start + commission_duration_months).
 *   If NULL for any reason (pre-fix attributions), calculated dynamically here and
 *   used as a soft filter — the merchant is still included but the dynamic bound is
 *   applied to monthly window checks.
 *
 * Paying subscriber milestone count:
 *   Excludes merchants with billing_status IN ('cancelled', 'deleted', 'payment_failed')
 *   and subscription_tier IN ('free_for_life', 'trial', 'free').
 *   payment_failed merchants are excluded because their revenue is at risk and
 *   they should not count toward a rep's volume milestone until billing is restored.
 *
 * Stripe KYC gate:
 *   If a contractor's Stripe onboarding is not complete, no payout record is created.
 *   An admin notification email is sent listing all skipped contractors.
 *
 * Deployed at: /api/cron/generate-contractor-payouts
 */

const { neon }       = require('@neondatabase/serverless');
const SibApiV3Sdk    = require('sib-api-v3-sdk');

// ── Calendar-accurate month difference ──────────────────────────────────────
// Returns the number of whole calendar months between two dates using year/month
// components. This avoids the drift of a fixed 30-day approximation.
// Example: Jan 15 → Jan 31 (next year) = 12 months  ✓
//          Jan 15 → Dec 31 (same year)  = 11 months  ✓
function calendarMonthsElapsed(startDate, endDate) {
  return (endDate.getFullYear() - startDate.getFullYear()) * 12
       + (endDate.getMonth()    - startDate.getMonth());
}

// ── Admin notification email ─────────────────────────────────────────────────
// Sends a plain admin alert email via Brevo. Non-fatal — errors are logged only.
async function sendAdminEmail(subject, htmlContent) {
  const BREVO_KEY = process.env.BREVO_API_KEY;
  if (!BREVO_KEY) {
    console.warn('[Cron:ContractorPayouts] BREVO_API_KEY not set — admin email skipped.');
    return;
  }
  try {
    const brevoClient = SibApiV3Sdk.ApiClient.instance;
    brevoClient.authentications['api-key'].apiKey = BREVO_KEY;
    const emailApi  = new SibApiV3Sdk.TransactionalEmailsApi();
    const emailObj  = new SibApiV3Sdk.SendSmtpEmail();
    emailObj.sender      = { name: 'Perkfinity System', email: 'support@perkfinity.net' };
    emailObj.to          = [{ email: process.env.ADMIN_EMAIL || 'admin@perkfinity.net' }];
    emailObj.subject     = subject;
    emailObj.htmlContent = htmlContent;
    await emailApi.sendTransacEmail(emailObj);
  } catch (emailErr) {
    console.error('[Cron:ContractorPayouts] Admin email send failed:', emailErr.message);
  }
}

module.exports = async (req, res) => {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && req.headers['authorization'] !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const sql = neon(process.env.DATABASE_URL);

  const testYear = req.query?.year ? parseInt(req.query.year) : null;
  const testMonth = req.query?.month ? parseInt(req.query.month) : null;

  let periodStart, periodEnd;
  if (testYear && testMonth !== null) {
    periodStart = new Date(testYear, testMonth - 1, 1);
    periodEnd   = new Date(testYear, testMonth, 0);
  } else {
    // Period = month prior to last (Net-45 EOM)
    const now         = new Date();
    periodStart = new Date(now.getFullYear(), now.getMonth() - 2, 1);
    periodEnd   = new Date(now.getFullYear(), now.getMonth() - 1, 0);
  }
  
  const psDate      = periodStart.toISOString().slice(0, 10);
  const peDate      = periodEnd.toISOString().slice(0, 10);

  console.log(`[Cron:ContractorPayouts] Running for period ${psDate} → ${peDate}`);

  try {
    // Fetch all active contractors with their compensation rules.
    // Stripe onboarding status is checked later — we calculate first, then gate payout creation.
    const contractors = await sql`
      SELECT c.id, c.full_name, c.stripe_onboarding_status,
             r.commission_rate, r.commission_duration_months, r.retainer_cents
      FROM "Contractor" c
      JOIN "ContractorCompensationRule" r ON r.contractor_id = c.id
      WHERE c.status = 'active'
    `;

    const results        = [];
    const stripeSkippedReps = [];   // collect reps blocked by KYC gate for admin notification

    for (const rc of contractors) {
      const durationMonths = parseInt(rc.commission_duration_months) || 12;

      // Max annual invoices that earn commission:  12-month plan → 1,  24-month plan → 2
      const maxAnnualCommPayments = Math.floor(durationMonths / 12);

      // Max retention bonuses — one per 12-month block (same denominator)
      const maxRetentionBonuses   = Math.floor(durationMonths / 12);

      // ── Get attributed merchants whose commission window is open ───────────
      // Includes merchants where commission_end_date IS NULL (webhook pre-fix)
      // — these are handled via the dynamic effectiveEndDate calculation below.
      const attrs = await sql`
        SELECT a.id,
               a.merchant_id,
               a.commission_start_date,
               a.commission_end_date,
               a.retention_bonuses_paid,
               m.billing_cycle
        FROM "ContractorMerchantAttribution" a
        JOIN "Merchant" m ON m.id = a.merchant_id
        WHERE a.contractor_id = ${rc.id}
          AND a.commission_start_date IS NOT NULL
          AND a.commission_start_date::date <= ${peDate}
          AND (
            a.commission_end_date IS NULL
            OR a.commission_end_date::date >= ${psDate}
          )
          AND m.subscription_tier NOT IN ('free_for_life', 'trial', 'free')
      `;

      let commCents    = 0;
      let retBonusCents = 0;
      const mBreakdown = [];

      for (const ra of attrs) {
        const billingCycle = ra.billing_cycle || 'monthly';
        const msStart      = new Date(ra.commission_start_date);

        // Effective commission end date — use DB value if set, otherwise calculate
        // dynamically from commission_start_date + duration for pre-fix attributions.
        const effectiveEndDate = ra.commission_end_date
          ? new Date(ra.commission_end_date)
          : (() => {
              const d = new Date(msStart);
              d.setMonth(d.getMonth() + durationMonths);
              return d;
            })();

        // ── Get invoices paid in this period ───────────────────────────────
        const allInvs = await sql`
          SELECT id, amount_cents, paid_at FROM "Invoice"
          WHERE merchant_id = ${ra.merchant_id}
            AND paid_at >= ${periodStart}
            AND paid_at <= ${periodEnd}
            AND amount_cents > 0
            AND status = 'paid'
          ORDER BY paid_at ASC
        `;
        
        // Deduplicate invoices by amount_cents to catch Stripe double-charge errors
        // while still allowing genuine plan upgrades (which have different amounts).
        const invs = [];
        const seenAmounts = new Set();
        for (const inv of allInvs) {
          if (!seenAmounts.has(inv.amount_cents)) {
            invs.push(inv);
            seenAmounts.add(inv.amount_cents);
          }
        }
        
        // Math is now driven strictly by the deduplicated valid invoices
        const validInvTotal = invs.reduce((s, i) => s + i.amount_cents, 0);
        // invTotal is kept ONLY as a flag (invTotal > 0) for backward compatibility
        const invTotal = allInvs.reduce((s, i) => s + i.amount_cents, 0);

        // ── For annual merchants: count total paid invoices since commission start ──
        // Fetched only when the merchant has a paid invoice this period because:
        //   (a) annual merchants pay once per year, so invTotal > 0 is the signal
        //       that a relevant billing event occurred this month;
        //   (b) the count is used only for commission cap and retention checks,
        //       both of which also require invTotal > 0 to fire.
        // This means if no annual invoice arrived this period, totalAnnualInvoicesPaid
        // stays 0 and all commission/retention checks are safely skipped via the
        // `if (invTotal > 0)` guard below.
        let totalAnnualInvoicesPaid = 0;
        if ((billingCycle === 'annual' || billingCycle === 'lifetime') && invTotal > 0) {
          const [annRow] = await sql`
            SELECT COUNT(DISTINCT DATE_TRUNC('month', paid_at))::int AS cnt FROM "Invoice"
            WHERE merchant_id = ${ra.merchant_id}
              AND paid_at::date >= ${ra.commission_start_date}
              AND amount_cents > 0
              AND status = 'paid'
          `;
          totalAnnualInvoicesPaid = annRow?.cnt || 0;
        }

        // ── Regular commission ─────────────────────────────────────────────
        let merchantComm = 0;
        let invoiceTotalForBreakdown = 0;

        if (invs.length > 0) {
          if (billingCycle === 'annual' || billingCycle === 'lifetime') {
            // Annual/Lifetime: only pay commission for invoices within the agreed renewal count.
            // Subtract the 1 event from this period to get the milestone count BEFORE this period
            let pastAnnualInvoicesCount = totalAnnualInvoicesPaid - 1;
            if (pastAnnualInvoicesCount < 0) pastAnnualInvoicesCount = 0;
            
            if (pastAnnualInvoicesCount < maxAnnualCommPayments) {
              for (const inv of invs) {
                inv._calcComm = Math.round(inv.amount_cents * parseFloat(rc.commission_rate));
                merchantComm += inv._calcComm;
                invoiceTotalForBreakdown += inv.amount_cents;
              }
            }
          } else {
            // Monthly: standard commission on any invoice within the commission window.
            // The effectiveEndDate guards merchants whose window closed this period.
            if (periodEnd <= effectiveEndDate) {
              for (const inv of invs) {
                inv._calcComm = Math.round(inv.amount_cents * parseFloat(rc.commission_rate));
                merchantComm += inv._calcComm;
                invoiceTotalForBreakdown += inv.amount_cents;
              }
            }
          }
        }

        commCents += merchantComm;

        // ── Retention bonus ────────────────────────────────────────────────
        let merchantRetBonus = 0;
        let newRetPaid       = parseInt(ra.retention_bonuses_paid) || 0;

        if (billingCycle === 'annual') {
          // Annual retention bonus fires at each annual renewal (invoice count milestone).
          // Only check when there is an invoice in this period (the renewal invoice itself).
          if (invs.length > 0) {
            const baseAnnualInvoice = validInvTotal;
            const mosElapsed = calendarMonthsElapsed(msStart, periodEnd);
            // 1st annual renewal = 2nd paid invoice (year 2 payment)
            // Time-gated to >= 11 months to prevent duplicate payments triggering it
            if (totalAnnualInvoicesPaid >= 2 && mosElapsed >= 11 && newRetPaid < 1 && maxRetentionBonuses >= 1) {
              // Retention amount = 1 month's equivalent (rate × annual_invoice / 12)
              merchantRetBonus += Math.round((baseAnnualInvoice / 12) * parseFloat(rc.commission_rate));
              newRetPaid = 1;
            }
            // 2nd annual renewal = 3rd paid invoice (year 3 payment) — 24-month plan only
            // Time-gated to >= 23 months to prevent duplicate payments triggering it
            if (totalAnnualInvoicesPaid >= 3 && mosElapsed >= 23 && newRetPaid < 2 && maxRetentionBonuses >= 2) {
              merchantRetBonus += Math.round((baseAnnualInvoice / 12) * parseFloat(rc.commission_rate));
              newRetPaid = 2;
            }
          }
        } else if (billingCycle === 'monthly') {
          // Monthly: retention bonus fires at exactly 12 and 24 *calendar* months.
          // Uses year/month component math — not a 30-day approximation.
          const mosElapsed = calendarMonthsElapsed(msStart, periodEnd);

          if (invs.length > 0) {
            // 1st retention bonus — at 12 calendar months
            if (mosElapsed >= 12 && newRetPaid < 1 && maxRetentionBonuses >= 1) {
              merchantRetBonus += Math.round(validInvTotal * parseFloat(rc.commission_rate));
              newRetPaid = 1;
            }
            // 2nd retention bonus — at 24 calendar months (24-month plan only)
            if (mosElapsed >= 24 && newRetPaid < 2 && maxRetentionBonuses >= 2) {
              merchantRetBonus += Math.round(validInvTotal * parseFloat(rc.commission_rate));
              newRetPaid = 2;
            }
          }
        }

        retBonusCents += merchantRetBonus;

        // Persist updated retention_bonuses_paid if it changed
        if (newRetPaid > parseInt(ra.retention_bonuses_paid)) {
          await sql`
            UPDATE "ContractorMerchantAttribution"
            SET retention_bonuses_paid = ${newRetPaid}, updated_at = NOW()
            WHERE id = ${ra.id}
          `;
        }

        if (invs.length > 0 && (merchantComm > 0 || merchantRetBonus > 0)) {
          for (let i = 0; i < invs.length; i++) {
            const inv = invs[i];
            const hasComm = inv._calcComm && inv._calcComm > 0;
            const hasRet  = i === 0 && merchantRetBonus > 0;
            if (hasComm || hasRet) {
              mBreakdown.push({
                merchant_id:      ra.merchant_id,
                invoice_id:       inv.id,
                billing_cycle:    billingCycle,
                invoice_total:    inv.amount_cents,
                commission:       inv._calcComm || 0,
                retention_bonus:  hasRet ? merchantRetBonus : 0,
              });
            }
          }
        } else if (merchantRetBonus > 0) {
          mBreakdown.push({
            merchant_id:      ra.merchant_id,
            invoice_id:       null,
            billing_cycle:    billingCycle,
            invoice_total:    0,
            commission:       0,
            retention_bonus:  merchantRetBonus,
          });
        }
      }

      // ── Count active paying subscribers for milestone check ──────────────
      // Excludes: FFL, trial, free (tier), cancelled, deleted, AND payment_failed (billing_status).
      // payment_failed merchants are excluded because their revenue is at risk —
      // they should not count toward a rep's milestone until billing is restored.
      const [subRow] = await sql`
        SELECT COUNT(DISTINCT a2.merchant_id)::int AS cnt
        FROM "ContractorMerchantAttribution" a2
        JOIN "Merchant" m2 ON m2.id = a2.merchant_id
        WHERE a2.contractor_id = ${rc.id}
          AND a2.commission_start_date IS NOT NULL
          AND m2.subscription_tier NOT IN ('free_for_life', 'trial', 'free')
          AND m2.billing_status NOT IN ('cancelled', 'deleted', 'payment_failed')
      `;
      const subCount = subRow?.cnt || 0;

      // ── Find newly unlocked milestones not yet awarded ────────────────────
      const milestones = await sql`
        SELECT mc.id, mc.bonus_cents, mc.label
        FROM "SystemMilestoneConfig" mc
        WHERE mc.is_active = true
          AND mc.threshold <= ${subCount}
          AND NOT EXISTS (
            SELECT 1 FROM "ContractorMilestoneRecord" mr
            WHERE mr.contractor_id = ${rc.id} AND mr.milestone_id = mc.id
          )
        ORDER BY mc.threshold ASC
      `;
      const milCents = milestones.reduce((s, mm) => s + mm.bonus_cents, 0);

      // ── Collect pending special bonuses ────────────────────────────────────
      const specBonuses = await sql`
        SELECT id, amount_cents, label FROM "ContractorSpecialBonus"
        WHERE contractor_id = ${rc.id} AND status = 'pending'
      `;
      const specCents = specBonuses.reduce((s, b) => s + b.amount_cents, 0);

      const retainerCents = parseInt(rc.retainer_cents) || 0;
      const totalCents    = commCents + retainerCents + milCents + retBonusCents + specCents;

      // ── Create payout record — only if something is owed AND Stripe KYC is complete ─
      if (totalCents > 0 && rc.stripe_onboarding_status === 'complete') {
        const [payout] = await sql`
          INSERT INTO "ContractorPayout" (
            id, contractor_id, period_start, period_end,
            commission_cents, retainer_cents, milestone_bonus_cents,
            retention_bonus_cents, special_bonus_cents, total_cents,
            breakdown, status, created_at, updated_at
          ) VALUES (
            gen_random_uuid()::text, ${rc.id}, ${psDate}, ${peDate},
            ${commCents}, ${retainerCents}, ${milCents},
            ${retBonusCents}, ${specCents}, ${totalCents},
            ${JSON.stringify({
              active_subscribers: subCount,
              merchant_breakdown: mBreakdown,
              milestones:     milestones.map(mm => mm.label),
              special_bonuses: specBonuses.map(b => b.label)
            })},
            'pending', NOW(), NOW()
          ) RETURNING id
        `;

        for (const ms of milestones) {
          await sql`
            INSERT INTO "ContractorMilestoneRecord" (id, contractor_id, milestone_id, payout_id, earned_at)
            VALUES (gen_random_uuid()::text, ${rc.id}, ${ms.id}, ${payout.id}, NOW())
            ON CONFLICT (contractor_id, milestone_id) DO NOTHING
          `;
        }

        if (specBonuses.length > 0) {
          const specIds = specBonuses.map(b => b.id);
          await sql`
            UPDATE "ContractorSpecialBonus"
            SET status = 'paid', payout_id = ${payout.id}, updated_at = NOW()
            WHERE id = ANY(${specIds})
          `;
        }

        results.push({ contractor_id: rc.id, name: rc.full_name, payout_id: payout.id, total_cents: totalCents });
        console.log(`[Cron:ContractorPayouts] Created payout for ${rc.full_name}: $${(totalCents / 100).toFixed(2)}`);

      } else if (totalCents > 0 && rc.stripe_onboarding_status !== 'complete') {
        // Stripe gate: log the block and collect for admin email
        console.error(`[Cron:ContractorPayouts] KYC BLOCK — ${rc.full_name} (${rc.id}): Stripe KYC not complete. Would have paid $${(totalCents / 100).toFixed(2)}. Admin notified.`);
        stripeSkippedReps.push({ name: rc.full_name, id: rc.id, amount: (totalCents / 100).toFixed(2) });
      }
    }

    // ── Send admin notification if any reps were blocked by Stripe gate ────────
    if (stripeSkippedReps.length > 0) {
      const repRows = stripeSkippedReps
        .map(r => `<tr><td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;">${r.name}</td><td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;font-family:monospace;font-size:12px;">${r.id}</td><td style="padding:8px 12px;border-bottom:1px solid #f0f0f0;color:#dc2626;font-weight:600;">$${r.amount}</td></tr>`)
        .join('');

      await sendAdminEmail(
        `⚠️ Perkfinity: ${stripeSkippedReps.length} Contractor Payout(s) Blocked — Stripe KYC Not Complete`,
        `<div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:600px;margin:0 auto;">
          <div style="background:linear-gradient(135deg,#5b3fa5,#7c5cbf);padding:24px;text-align:center;">
            <div style="color:#fff;font-size:22px;font-weight:800;">Perkfinity Admin Alert</div>
          </div>
          <div style="padding:24px;">
            <div style="font-size:18px;font-weight:700;color:#dc2626;margin-bottom:12px;">⚠️ Contractor Payouts Blocked — Stripe KYC Missing</div>
            <p style="font-size:14px;color:#555;line-height:1.6;">
              The monthly payout cron ran for period <strong>${psDate} → ${peDate}</strong>.<br>
              The following contractor(s) had earnings calculated but <strong>no payout was created</strong>
              because their Stripe Connect onboarding is not yet complete. They will be skipped again next month until complete.
            </p>
            <table style="width:100%;border-collapse:collapse;margin-top:16px;font-size:14px;">
              <thead>
                <tr style="background:#f5f3ff;">
                  <th style="padding:8px 12px;text-align:left;color:#5b3fa5;">Contractor</th>
                  <th style="padding:8px 12px;text-align:left;color:#5b3fa5;">ID</th>
                  <th style="padding:8px 12px;text-align:left;color:#5b3fa5;">Amount Blocked</th>
                </tr>
              </thead>
              <tbody>${repRows}</tbody>
            </table>
            <p style="font-size:13px;color:#888;margin-top:20px;">
              Action required: Remind the rep to log into their dashboard and complete Stripe onboarding.
            </p>
          </div>
        </div>`
      );
    }

    console.log(`[Cron:ContractorPayouts] Done. ${results.length} payout(s) created for ${psDate} → ${peDate}.`);
    return res.status(200).json({
      success: true,
      payouts_created: results.length,
      kyc_blocked: stripeSkippedReps.length,
      period: `${psDate} to ${peDate}`,
      results,
      kyc_skipped: stripeSkippedReps,
    });

  } catch (err) {
    console.error('[Cron:ContractorPayouts] Error:', err);
    return res.status(500).json({ success: false, error: err.message });
  }
};
