'use strict';

/**
 * dropbox-sign.js
 * Perkfinity — Dropbox Sign (HelloSign) Integration
 *
 * Responsibilities:
 *  - generateICAPdf(data)  : Build the ICA PDF with pdfkit, embedding DS text tags
 *  - sendICA(data)         : POST the PDF to Dropbox Sign, return signature_request_id
 *
 * Dropbox Sign file-based approach (not template-based) is used so that
 * all contract variables ([[Contractor Full Name]], [[Commission Rate]], etc.)
 * are filled in by the backend before the document reaches the signer.
 * Signature placement uses Dropbox Sign "text tags" embedded in the PDF.
 *
 * Text tags format:  [sig|req|signer1]  [date|req|signer1]  etc.
 * Signers order:  1 = Contractor  /  2 = Company (Nader — countersigns)
 */

const https    = require('https');
const PDFDoc   = require('pdfkit');
const FormData = require('form-data');

// ─── helpers ────────────────────────────────────────────────────────────────

/** Format a JS Date or ISO string as "Month D, YYYY" */
function fmtDate(d) {
  const date = d instanceof Date ? d : new Date(d);
  return date.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
}

/** Add 3 calendar months to a Date */
function addThreeMonths(d) {
  const date = d instanceof Date ? new Date(d) : new Date(d);
  date.setMonth(date.getMonth() + 3);
  return date;
}

// ─── PDF generation ─────────────────────────────────────────────────────────

/**
 * generateICAPdf
 * @param {object} p
 * @param {string} p.contractorName          Full legal name
 * @param {string} p.contractorEmail
 * @param {string} p.agreementDate           ISO date string or Date
 * @param {string[]} p.territoryZips         Array of ZIP strings  (may be empty)
 * @param {number} p.commissionRate          e.g. 25
 * @param {number} p.commissionDurationMonths e.g. 12
 * @param {number} p.retainerAmount          e.g. 0
 * @returns {Promise<Buffer>}
 */
function generateICAPdf(p) {
  return new Promise((resolve, reject) => {
    const doc    = new PDFDoc({ margin: 72, size: 'LETTER', bufferPages: true });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    doc.on('end',  () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);

    const aggDate    = p.agreementDate ? new Date(p.agreementDate) : new Date();
    const quotaEnd   = addThreeMonths(aggDate);
    const zips       = Array.isArray(p.territoryZips) && p.territoryZips.length
                         ? p.territoryZips.join(', ')
                         : '[To be assigned by Company]';
    const retainer   = Number(p.retainerAmount || 0).toFixed(2);

    // ── typography helpers ──────────────────────────────────────────────────
    const W          = doc.page.width  - 144; // text width (both margins)
    const BODY_SIZE  = 10;
    const H2_SIZE    = 11;
    const LINE_GAP   = 4;

    function h1(text) {
      doc.font('Helvetica-Bold').fontSize(14).text(text, { align: 'center' });
      doc.moveDown(0.4);
    }

    function h2(text) {
      doc.moveDown(0.6);
      doc.font('Helvetica-Bold').fontSize(H2_SIZE).text(text.toUpperCase());
      doc.moveDown(0.3);
    }

    function body(text) {
      doc.font('Helvetica').fontSize(BODY_SIZE).lineGap(LINE_GAP).text(text, { lineBreak: true });
      doc.moveDown(0.5);
    }

    function bullet(items) {
      items.forEach(item => {
        doc.font('Helvetica').fontSize(BODY_SIZE).lineGap(LINE_GAP)
           .text(`• ${item}`, { indent: 20, lineBreak: true });
      });
      doc.moveDown(0.4);
    }

    function divider() {
      doc.moveDown(0.4);
      doc.moveTo(72, doc.y).lineTo(72 + W, doc.y).stroke('#cccccc');
      doc.moveDown(0.6);
    }

    // ── TITLE ───────────────────────────────────────────────────────────────
    h1('INDEPENDENT CONTRACTOR AGREEMENT');
    doc.font('Helvetica-Oblique').fontSize(11).text('Perkfinity Sales Representative Program', { align: 'center' });
    divider();

    // ── PARTIES ─────────────────────────────────────────────────────────────
    body(`This Independent Contractor Agreement ("Agreement") is entered into as of ${fmtDate(aggDate)} ("Effective Date") by and between:`);

    doc.font('Helvetica-Bold').fontSize(BODY_SIZE).text('Company:');
    body('Safe Box Financial Technologies and Education LLC, a California limited liability company, doing business as Perkfinity ("Company")');

    doc.font('Helvetica-Bold').fontSize(BODY_SIZE).text('Contractor:');
    body(`${p.contractorName}, an individual ("Contractor")`);

    body('(Each a "Party" and collectively the "Parties")');
    divider();

    // ── 1. INDEPENDENT CONTRACTOR RELATIONSHIP ──────────────────────────────
    h2('1. Independent Contractor Relationship');
    body('1.1 The Contractor is engaged as an independent contractor and not as an employee, partner, joint venturer, or agent of the Company. Nothing in this Agreement shall be construed to create an employment relationship.');
    body('1.2 The Contractor has full control over the manner and means by which the services are performed, subject to the results required by this Agreement. The Company shall not control the Contractor\'s work hours, schedule, methods, or tools.');
    body('1.3 The Contractor is responsible for all federal, state, and local taxes on compensation received under this Agreement, including self-employment taxes. The Company will not withhold any taxes on behalf of the Contractor. Tax reporting, including the issuance of Form 1099-NEC for compensation of $600 or more in a calendar year, is handled securely via a third-party payment processor (currently Stripe).');
    body('1.4 The Contractor is not entitled to any employee benefits, including but not limited to health insurance, retirement plans, paid time off, workers\' compensation, or unemployment insurance.');
    divider();

    // ── 2. SERVICES ─────────────────────────────────────────────────────────
    h2('2. Services');
    body('2.1 The Contractor agrees to provide the following services ("Services"):');
    bullet([
      'Identify, solicit, and onboard local businesses ("Merchants") to the Perkfinity platform within the Contractor\'s assigned Territory (as defined in Section 4)',
      'Represent the Perkfinity brand professionally and accurately in all communications with prospective Merchants',
      'Provide prospective Merchants with accurate information about Perkfinity\'s plans, pricing, and features',
      'Use only the referral code and marketing materials provided by the Company',
    ]);
    body('2.2 The Contractor shall not make any representations, warranties, or commitments on behalf of the Company beyond what is set forth in the Company\'s official materials.');
    body('2.3 The Contractor shall not collect payment from Merchants on behalf of the Company. All billing is handled directly by the Company.');
    divider();

    // ── 3. TERM ─────────────────────────────────────────────────────────────
    h2('3. Term');
    body('3.1 This Agreement begins on the Effective Date and continues until terminated in accordance with Section 10.');
    divider();

    // ── 4. TERRITORY ────────────────────────────────────────────────────────
    h2('4. Territory');
    body(`4.1 Assignment. Subject to this Section, the Company assigns to the Contractor the exclusive sales territory consisting of the following ZIP codes ("Territory"): ${zips}.`);
    body('4.2 Initial Exclusivity. From the Effective Date through the end of the Quota Period (Section 5), the Territory is assigned exclusively to the Contractor. No other contractor will be assigned to the same ZIP codes during this period, provided the Contractor is actively performing Services.');
    body('4.3 Ongoing Exclusivity Upon Initial Quota Met. If the Contractor meets the Initial Quota (Section 5.1) by the end of the Initial Quota Period, the Territory remains exclusive to the Contractor subject to the Ongoing Maintenance Quota (Section 5.2). The Company will not assign another contractor to the same Territory as long as the Contractor meets all ongoing performance requirements.');
    body('4.4 Quota Failure — Company Rights. If the Contractor fails to meet any Quota by the end of the respective Quota Period, the Territory\'s exclusive status expires automatically. The Company may, at its sole discretion: (a) Add one or more additional contractors to the same Territory; (b) Revoke the Contractor\'s Territory assignment entirely and assign it to a new contractor; or (c) Extend the Quota Period by a defined grace period and notify the Contractor in writing. The Company will notify the Contractor of its election in writing. The Contractor\'s right to commission on merchants already attributed prior to the quota deadline is not affected by this election.');
    body('4.5 No Property Right. The Territory is a business management designation only. The Contractor acknowledges that the Territory does not constitute property, and the Company\'s rights under Section 4.4 are absolute upon quota failure.');
    body('4.6 Attribution. Merchant attribution is determined by the referral code used during signup, not by Territory. If a Merchant in the Contractor\'s Territory signs up using another contractor\'s referral code, attribution follows the referral code.');
    divider();

    // ── 5. QUOTA ────────────────────────────────────────────────────────────
    h2('5. Quota');
    body(`5.1 Initial Quota Requirement. To earn ongoing exclusive rights to the Territory, the Contractor must onboard a minimum of 30 active Merchants to the Perkfinity platform within the first 3 months following the Effective Date ("Initial Quota Period"). The Initial Quota Period begins on the Effective Date (${fmtDate(aggDate)}) and ends 3 calendar months later (${fmtDate(quotaEnd)}).`);
    body(`5.2 Ongoing Maintenance Quota. Following the successful completion of the Initial Quota Period, the Contractor must maintain territory exclusivity by achieving a net increase of 10 active Merchants in their portfolio every subsequent 3-month calendar period ("Ongoing Quota Period"). For example, if the Contractor finishes the Initial Quota Period with 30 active Merchants, they must reach 40 active Merchants by the end of month 6, 50 by month 9, and so forth.`);
    body('5.3 "Active Merchant" means a Merchant that has completed signup on the Perkfinity platform, passed any applicable review, and holds an active, paying subscription as of the last day of the respective Quota Period.');
    body('5.4 Quota Tracking. The Company will provide the Contractor with access to a dashboard showing their attributed Merchant count, the current target, and days remaining in the current Quota Period at any time.');
    body('5.5 Success — Rolling Lock-In. If the Contractor reaches the active Merchant target before or on the last day of any Quota Period, the Territory remains exclusive to them, and the target automatically rolls forward to the next 3-month milestone (+10 Merchants).');
    body('5.6 Failure — Revocation of Exclusivity. If the Contractor fails to meet either the Initial Quota or any subsequent Ongoing Maintenance Quota by the end of the respective Quota Period, the Territory\'s exclusive status expires automatically and immediately. The Company may, at its sole discretion, revoke the Territory assignment entirely or assign additional contractors to the Territory. The Contractor will be notified in writing of the failure.');
    body('5.7 Commissions Unaffected. Quota failure does not affect the Contractor\'s right to commission on Merchants attributed prior to the deadline. Commission continues to be paid per Section 6 for all attributed Merchants during their respective Commission Periods, even if territory exclusivity is revoked.');
    divider();

    // ── 6. COMPENSATION ─────────────────────────────────────────────────────
    h2('6. Compensation');
    
    const rateVal = Number(p.commissionRate) || 0;
    const displayRate = (rateVal <= 1 ? rateVal * 100 : rateVal).toFixed(2).replace(/\.00$/, '');
    body(`6.1 Commission. The Company will pay the Contractor a commission of ${displayRate}% of the net subscription revenue collected from each attributed Merchant for a period of ${p.commissionDurationMonths} months from the date of the Merchant's first successful payment ("Commission Period").`);
    
    let sec6Num = 2;
    if (Number(retainer) > 0) {
      body(`6.${sec6Num++} Monthly Retainer. The Company will pay the Contractor a monthly retainer of $${retainer} ("Retainer"), payable on or around the 1st of each calendar month, subject to the Contractor being in good standing under this Agreement.`);
    }
    
    body(`6.${sec6Num++} Payment Schedule. Commissions are paid on a Net-45 End of Month (EOM) schedule. For example, commissions earned on revenue collected in January will be processed and paid on or around March 15th.`);
    body(`6.${sec6Num++} KYC and Tax Compliance. No commission or retainer payment will be issued until the Contractor has completed identity verification and submitted tax information (W-9 equivalent). All KYC compliance, W-9 collection, payouts, and 1099 tax reporting are handled exclusively by a secure 3rd-party payment processor (currently Stripe).`);
    body(`6.${sec6Num++} Data Security. The Company does not collect, store, or handle the Contractor's sensitive financial information, such as Social Security Numbers (SSN), Individual Taxpayer Identification Numbers (ITIN), or bank account routing details. All such data is transmitted directly to and managed by the 3rd-party processor.`);
    body(`6.${sec6Num++} Compliance Gate. If the Contractor's Stripe onboarding and tax verification are not fully complete, earned commissions will be held and released only upon verified completion. Held commissions do not accrue interest.`);
    body(`6.${sec6Num++} No Guarantee. The Contractor acknowledges that compensation is performance-based. The Company does not guarantee any minimum earnings beyond the Retainer (if applicable).`);
    body(`6.${sec6Num++} Expense Reimbursement. The Company does not reimburse the Contractor for any expenses, including travel, marketing materials, or equipment, unless agreed to in writing in advance.`);
    divider();

    // ── 7. CONFIDENTIALITY ──────────────────────────────────────────────────
    h2('7. Confidentiality');
    body('7.1 The Contractor will receive access to confidential and proprietary information about the Company, including but not limited to: Merchant data, pricing structures, commission rates, internal systems, business strategies, and customer lists ("Confidential Information").');
    body('7.2 The Contractor agrees to:');
    bullet([
      'Keep all Confidential Information strictly confidential',
      'Not disclose Confidential Information to any third party without prior written consent from the Company',
      'Use Confidential Information solely for the purpose of performing Services under this Agreement',
      'Return or destroy all Confidential Information upon termination of this Agreement',
    ]);
    body('7.3 This confidentiality obligation survives termination of this Agreement indefinitely.');
    divider();

    // ── 8. NON-SOLICITATION ─────────────────────────────────────────────────
    h2('8. Non-Solicitation');
    body('8.1 During the term of this Agreement and for 12 months following termination for any reason, the Contractor agrees not to:');
    bullet([
      'Solicit, recruit, or encourage any Merchant attributed to the Contractor under this Agreement to cancel their Perkfinity subscription or to subscribe to a competing loyalty or merchant rewards platform',
      'Solicit, recruit, or induce any other Perkfinity contractor, employee, or partner to terminate their relationship with the Company',
    ]);
    body('8.2 This Section does not prevent the Contractor from being employed by or contracting with a competitor, so long as the Contractor does not engage in the specific prohibited activities above.');
    divider();

    // ── 9. INTELLECTUAL PROPERTY ────────────────────────────────────────────
    h2('9. Intellectual Property');
    body('9.1 All work product, materials, and deliverables created by the Contractor in the course of performing Services under this Agreement — including but not limited to marketing content, prospect lists, and outreach templates — are the exclusive property of the Company.');
    body('9.2 The Contractor retains no rights to any Perkfinity trademarks, branding, or intellectual property.');
    divider();

    // ── 10. TERMINATION ─────────────────────────────────────────────────────
    h2('10. Termination');
    body('10.1 By Either Party. Either Party may terminate this Agreement at any time, with or without cause, upon 14 days\' written notice to the other Party.');
    body('10.2 Immediate Termination by Company. The Company may terminate this Agreement immediately, without notice, if the Contractor:');
    bullet([
      'Misrepresents Perkfinity\'s products, pricing, or terms to any Merchant or prospective Merchant',
      'Breaches the Confidentiality (Section 7) or Non-Solicitation (Section 8) obligations',
      'Engages in fraud, dishonesty, or conduct that damages the Company\'s reputation',
      'Fails to submit a valid W-9 within 30 days of the Effective Date',
    ]);
    body('10.3 Effect of Termination. Upon termination:');
    bullet([
      'The Contractor will cease performing Services and representing Perkfinity immediately',
      'Commissions earned prior to termination on existing attributed Merchants will continue to be paid for the remainder of each Merchant\'s active Commission Period, provided the Contractor has a verified W-9 on file',
      'The Contractor\'s Territory assignment ends immediately',
      'All Confidential Information must be returned or destroyed',
    ]);
    body('10.4 Survival. Sections 7 (Confidentiality), 8 (Non-Solicitation), 9 (Intellectual Property), and 14 (Governing Law) survive termination.');
    divider();

    // ── 11. REPRESENTATIONS AND WARRANTIES ──────────────────────────────────
    h2('11. Representations and Warranties');
    body('11.1 The Contractor represents and warrants that:');
    bullet([
      'They have the legal right and authority to enter into this Agreement',
      'Performing Services under this Agreement does not violate any other agreement they are party to',
      'They will perform Services in a professional and lawful manner',
      'All information provided to the Company (including W-9 tax information) is accurate and complete',
    ]);
    divider();

    // ── 12. LIMITATION OF LIABILITY ─────────────────────────────────────────
    h2('12. Limitation of Liability');
    body('12.1 In no event shall either Party be liable to the other for any indirect, incidental, consequential, punitive, or special damages arising out of or related to this Agreement.');
    body('12.2 The Company\'s total liability to the Contractor under this Agreement shall not exceed the total compensation paid to the Contractor in the 3 months preceding the event giving rise to the claim.');
    divider();

    // ── 13. DISPUTE RESOLUTION ──────────────────────────────────────────────
    h2('13. Dispute Resolution');
    body('13.1 The Parties agree to attempt to resolve any dispute arising out of or related to this Agreement through good-faith negotiation before pursuing formal legal action.');
    body('13.2 If the Parties cannot resolve a dispute through negotiation within 30 days, the dispute shall be submitted to binding arbitration in Los Angeles County, California, under the rules of the American Arbitration Association (AAA).');
    body('13.3 The prevailing Party in any arbitration or legal proceeding shall be entitled to recover reasonable attorneys\' fees and costs.');
    divider();

    // ── 14. GOVERNING LAW ───────────────────────────────────────────────────
    h2('14. Governing Law');
    body('14.1 This Agreement is governed by and construed in accordance with the laws of the State of California, without regard to its conflict of law provisions.');
    divider();

    // ── 15. ENTIRE AGREEMENT ────────────────────────────────────────────────
    h2('15. Entire Agreement');
    body('15.1 This Agreement constitutes the entire agreement between the Parties with respect to its subject matter and supersedes all prior agreements, representations, and understandings.');
    body('15.2 This Agreement may only be modified by a written amendment signed by both Parties.');
    body('15.3 If any provision of this Agreement is found to be unenforceable, the remaining provisions remain in full force.');
    divider();

    // ── 16. NOTICES ─────────────────────────────────────────────────────────
    h2('16. Notices');
    body('All notices under this Agreement shall be in writing and sent by email to:');
    body(`Company: support@perkfinity.net\nContractor: ${p.contractorEmail}`);
    divider();

    // ── SIGNATURES ──────────────────────────────────────────────────────────

    doc.addPage();
    h1('SIGNATURES');
    body('By signing below, the Parties agree to be bound by the terms of this Agreement.');
    doc.moveDown(1);

    // Left column — Company (signer 2)
    const sigY = doc.y;
    doc.font('Helvetica-Bold').fontSize(BODY_SIZE).text('Safe Box Financial Technologies and Education LLC\n(d/b/a Perkfinity)', 72, sigY, { width: W / 2 - 20 });
    doc.moveDown(2.5);
    doc.moveTo(72, doc.y).lineTo(72 + W / 2 - 30, doc.y).stroke('#333333');
    
    // Company Signature
    const companyTagY = doc.y - 14;
    const companyName = p.companySignatory || 'Nader Ordubadi';
    const companyTitle = p.companySignatory ? 'Authorized Representative' : 'Founder & CEO';

    if (p.isSigned || p.companySignatory) {
      doc.font('Helvetica-Oblique').fontSize(14).text(companyName, 72, companyTagY);
    }
    
    doc.moveDown(0.4);
    doc.font('Helvetica').fontSize(BODY_SIZE).text('Signature', { indent: 0 });
    doc.moveDown(0.6);
    doc.font('Helvetica').fontSize(BODY_SIZE).text(`Name: ${companyName}`);
    doc.font('Helvetica').fontSize(BODY_SIZE).text(`Title: ${companyTitle}`);
    doc.font('Helvetica').fontSize(BODY_SIZE).text(`Date: ${fmtDate(p.signedDate || aggDate)}`);

    // Right column — Contractor (signer 1)
    doc.font('Helvetica-Bold').fontSize(BODY_SIZE).text('Contractor', 72 + W / 2 + 10, sigY, { width: W / 2 - 10 });
    const contractorSigY = sigY + doc.currentLineHeight() * 4;
    doc.moveTo(72 + W / 2 + 10, contractorSigY).lineTo(72 + W, contractorSigY).stroke('#333333');
    
    // Contractor Signature
    if (p.isSigned && p.signatureName) {
      doc.font('Helvetica-Oblique').fontSize(14).text(p.signatureName, 72 + W / 2 + 10, contractorSigY - 14);
    }

    doc.font('Helvetica').fontSize(BODY_SIZE).text('Signature', 72 + W / 2 + 10, contractorSigY + 6);
    doc.moveDown(0.6);
    doc.font('Helvetica').fontSize(BODY_SIZE).text(`Name: ${p.contractorName}`, 72 + W / 2 + 10);
    doc.font('Helvetica').fontSize(BODY_SIZE).text(`Date: ${fmtDate(p.signedDate || aggDate)}`, 72 + W / 2 + 10);

    doc.moveDown(3);
    divider();
    
    if (p.isSigned) {
      doc.font('Helvetica-Oblique').fontSize(8.5).fillColor('#666666')
         .text(`This Agreement was electronically signed by ${p.signatureName} on ${fmtDate(p.signedDate || aggDate)} via the Perkfinity Rep Portal. Electronic signatures have the same legal effect as handwritten signatures under the Electronic Signatures in Global and National Commerce Act (E-SIGN) and California Civil Code § 1633.1 et seq.`, { align: 'center' });
    }

    doc.end();
  });
}

module.exports = { generateICAPdf };
