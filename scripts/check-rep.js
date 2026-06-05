require('dotenv').config({ path: __dirname + '/../.env' });
const { neon } = require('@neondatabase/serverless');
const bcrypt = require('bcryptjs');

async function check() {
  const sql = neon(process.env.DATABASE_URL);
  const email = 'naderordubadi@yahoo.com';
  const rows = await sql`SELECT * FROM "Contractor" WHERE email = ${email}`;
  if (rows.length === 0) {
    console.log("Rep not found in DB.");
    return;
  }
  const rep = rows[0];
  console.log("Rep found:", rep.id, rep.full_name, rep.email);
  
  const pw = 'Test1234!';
  if (rep.password_hash) {
    const match = await bcrypt.compare(pw, rep.password_hash);
    console.log("Does Test1234! match?", match);
    if (!match) {
        const newHash = await bcrypt.hash(pw, 10);
        await sql`UPDATE "Contractor" SET password_hash = ${newHash} WHERE id = ${rep.id}`;
        console.log("Password updated to Test1234!");
    }
  } else {
    console.log("No password hash found for this rep.");
    const newHash = await bcrypt.hash(pw, 10);
    await sql`UPDATE "Contractor" SET password_hash = ${newHash} WHERE id = ${rep.id}`;
    console.log("Password updated to Test1234!");
  }
}
check().catch(console.error);
