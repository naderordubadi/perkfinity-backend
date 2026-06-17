require('dotenv').config();
const postgres = require('postgres');
const bcrypt = require('bcryptjs');

const sql = postgres(process.env.DATABASE_URL);

async function createTestRep() {
  try {
    const email = 'testrep@example.com';
    const password = 'password123';
    const passwordHash = await bcrypt.hash(password, 10);

    // Check if exists
    const [existing] = await sql`SELECT id FROM "Contractor" WHERE email = ${email}`;
    let repId;

    if (existing) {
      console.log('Test rep already exists. Updating...');
      repId = existing.id;
      await sql`
        UPDATE "Contractor"
        SET status = 'active',
            ica_status = 'not_sent',
            stripe_onboarding_status = 'complete',
            password_hash = ${passwordHash}
        WHERE id = ${repId}
      `;
    } else {
      console.log('Creating new test rep...');
      const [newRep] = await sql`
        INSERT INTO "Contractor" (id, full_name, email, phone, address, status, ica_status, stripe_onboarding_status, created_at, updated_at, password_hash, referral_code)
        VALUES (gen_random_uuid()::text, 'Test Dynamic Rep', ${email}, '555-0199', '123 Test St, NY', 'active', 'not_sent', 'complete', NOW(), NOW(), ${passwordHash}, 'TESTREP123')
        RETURNING id
      `;
      repId = newRep.id;
    }

    // Add 5 ZIP codes to ContractorTerritory
    await sql`DELETE FROM "ContractorTerritory" WHERE contractor_id = ${repId}`;
    await sql`
      INSERT INTO "ContractorTerritory" (id, contractor_id, label, zip_codes, status, assigned_at, updated_at)
      VALUES (gen_random_uuid()::text, ${repId}, 'Test 5 Zips', ARRAY['10001', '10002', '10003', '10004', '10005']::text[], 'active', NOW(), NOW())
    `;

    // Ensure no quota exists yet so we can test the auto-start
    await sql`DELETE FROM "ContractorQuotaPeriod" WHERE contractor_id = ${repId}`;

    console.log('Successfully created/updated Test Rep.');
    console.log('Email:', email);
    console.log('Password:', password);
    console.log('Stripe Onboarding:', 'complete');
    console.log('Assigned ZIP codes: 5 (10001 - 10005)');
    console.log('Go to Admin Dashboard to send ICA, or Rep Dashboard to sign it.');

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await sql.end();
  }
}

createTestRep();
