import { PrismaClient } from '@prisma/client';

export class AnalyticsService {
  constructor(private prisma: PrismaClient) { }

  async getAnalyticsSummary(merchant_id: string, period: '7d' | '30d') {
    const days = period === '7d' ? 7 : 30;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    const events = await this.prisma.event.findMany({
      where: {
        merchant_id,
        created_at: { gte: startDate }
      },
      select: {
        event_name: true,
        user_id: true,
        campaign_id: true,
      }
    });

    const scans = events.filter(e => e.event_name === 'QR_SCANNED').length;
    const activations = events.filter(e => e.event_name === 'CAMPAIGN_ACTIVATED').length;
    const redemptions = events.filter(e => e.event_name === 'REDEMPTION_VALIDATED_SUCCESS').length;

    const activatedUsers = events.filter(e => e.event_name === 'CAMPAIGN_ACTIVATED' && e.user_id);
    const uniqueUserIds = new Set(activatedUsers.map(e => e.user_id));

    // Calculate repeat users
    const userCounts = activatedUsers.reduce((acc, e) => {
      if (e.user_id) acc[e.user_id] = (acc[e.user_id] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);
    const repeat_users = Object.values(userCounts).filter((count: number) => count >= 2).length;
    const scan_to_activation_rate = scans > 0 ? Math.round((activations / scans) * 100) : 0;
    const activation_to_redemption_rate = activations > 0 ? Math.round((redemptions / activations) * 100) : 0;

    // By campaign
    const campaigns = await this.prisma.campaign.findMany({
      where: { merchant_id },
      select: { id: true, title: true, discount_percentage: true }
    });

    const by_campaign = campaigns.map(c => {
      const cEvents = events.filter(e => e.campaign_id === c.id);
      return {
        campaign_id: c.id,
        title: c.title,
        discount_percentage: c.discount_percentage,
        scans: cEvents.filter(e => e.event_name === 'QR_SCANNED').length,
        activations: cEvents.filter(e => e.event_name === 'CAMPAIGN_ACTIVATED').length,
        redemptions: cEvents.filter(e => e.event_name === 'REDEMPTION_VALIDATED_SUCCESS').length,
      };
    }).filter(c => c.scans > 0 || c.activations > 0 || c.redemptions > 0);

    return {
      period,
      scans,
      activations,
      redemptions,
      unique_users: uniqueUserIds.size,
      repeat_users,
      scan_to_activation_rate,
      activation_to_redemption_rate,
      by_campaign,
    };
  }

  async getEventsList(merchant_id: string, page: number = 1, limit: number = 50) {
    const offset = (page - 1) * limit;

    const [events, total] = await Promise.all([
      this.prisma.event.findMany({
        where: { merchant_id },
        orderBy: { created_at: 'desc' },
        skip: offset,
        take: limit,
        include: {
          campaign: { select: { title: true } }
        }
      }),
      this.prisma.event.count({ where: { merchant_id } })
    ]);

    return { events, total, page, limit };
  }
}
