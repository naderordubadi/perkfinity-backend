require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');

async function deleteRep() {
  const sql = neon(process.env.DATABASE_URL);
  const email = 'naderordubadi@yahoo.com';
  try {
      const rows = await sql`SELECT id FROM "Contractor" WHERE email = ${email}`;
      if (rows.length === 0) {
        console.log("Rep not found.");
        return;
      }
      const repId = rows[0].id;
      console.log(`Deleting rep ${repId}...`);
      
      // Delete compensation rules
      await sql`DELETE FROM "ContractorCompensationRule" WHERE contractor_id = ${repId}`;
      // Delete payouts
      await sql`DELETE FROM "ContractorPayout" WHERE contractor_id = ${repId}`;
      // Delete merchants (or nullify them, but let's just delete or set contractor_id = null)
      await sql`DELETE FROM "ContractorMerchantAttribution" WHERE contractor_id = ${repId}`;
      
      // Finally, delete the contractor
      await sql`DELETE FROM "Contractor" WHERE id = ${repId}`;
      console.log(`Successfully deleted rep with email: ${email}`);
  } catch (err) {
      console.error('Delete error:', err);
  }
}
deleteRep();
