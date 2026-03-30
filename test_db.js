const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

async function main() {
  const email = 'naderordubadi@gmail.com';
  
  // 1. Find user
  const user = await prisma.user.findUnique({
    where: { email },
    include: {
      memberOf: {
        include: {
          merchant: true
        }
      }
    }
  });

  if (!user) {
    console.log('User not found!');
    return;
  }

  console.log('User ID:', user.id);
  console.log('Member of merchants:');
  user.memberOf.forEach(m => {
    console.log(`- ${m.merchant.businessName} (ID: ${m.merchantId})`);
  });

  // 2. Find campaigns for these merchants
  const merchantIds = user.memberOf.map(m => m.merchantId);
  const campaigns = await prisma.campaign.findMany({
    where: {
      merchantId: { in: merchantIds },
      status: 'active'
    },
    include: {
      merchant: true
    }
  });

  console.log('\nActive Campaigns for these merchants:');
  campaigns.forEach(c => {
    console.log(`- ${c.merchant.businessName}: ${c.title} (ID: ${c.id})`);
  });

  // 3. Find notification queue entries for this user
  const queue = await prisma.notificationQueue.findMany({
    where: {
      userId: user.id
    },
    include: {
      campaign: {
        include: { merchant: true }
      }
    }
  });

  console.log('\nTotal queue entries for this user:', queue.length);
  
  const pending = queue.filter(q => q.status === 'pending');
  console.log(`Pending queue entries: ${pending.length}`);
  if (pending.length > 0) {
    pending.forEach(p => {
      console.log(`  - Pending: ${p.campaign.merchant.businessName} (${p.channel})`);
    });
  }

  const processed = queue.filter(q => q.status === 'processed' || q.status === 'sent');
  console.log(`Processed/Sent queue entries (Latest 20): ${processed.length}`);
  if (processed.length > 0) {
    processed.sort((a,b) => b.updatedAt - a.updatedAt).slice(0, 20).forEach(p => {
      console.log(`  - Sent: ${p.campaign.merchant.businessName} (Channel: ${p.channel}, Updated: ${p.updatedAt})`);
    });
  }
}

main()
  .catch(e => console.error(e))
  .finally(async () => await prisma.$disconnect());
