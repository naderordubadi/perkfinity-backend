import { PrismaClient } from '@prisma/client';
import { assertTierAllowsNewCampaign, countActiveCampaigns } from '../tiers/service.js';
import { logAudit } from '../audit/service.js';

export class CampaignService {
  constructor(private prisma: PrismaClient) {}

  async createCampaign(merchant_user_id: string, merchant_id: string, tier: string, data: any) {
    if (process.env.NODE_ENV !== 'development') {
      const activeCount = await countActiveCampaigns(this.prisma, merchant_id);
      assertTierAllowsNewCampaign(tier, activeCount);
    }

    const campaign = await this.prisma.campaign.create({
      data: {
        merchant_id,
        title: data.title,
        discount_percentage: data.discount_percentage,
        terms: data.terms,
        redemption_time_limit_minutes: data.redemption_time_limit_minutes,
        start_at: data.start_at,
        end_at: data.end_at,
        location_id: data.location_id,
        status: 'active',
      }
    });

    await logAudit(this.prisma, {
      actor_type: 'merchant_user',
      actor_id: merchant_user_id,
      merchant_id,
      action: 'CAMPAIGN_CREATED',
      target_type: 'campaign',
      target_id: campaign.id,
    });

    await this.prisma.event.create({
      data: {
        event_name: 'CAMPAIGN_CREATED',
        merchant_id,
        campaign_id: campaign.id,
      }
    });

    return campaign;
  }

  async updateCampaign(merchant_user_id: string, merchant_id: string, campaignId: string, data: any) {
    const campaign = await this.prisma.campaign.findFirst({
      where: { id: campaignId, merchant_id }
    });

    if (!campaign) throw new Error('CAMPAIGN_NOT_FOUND');

    const updated = await this.prisma.campaign.update({
      where: { id: campaignId },
      data,
    });

    await logAudit(this.prisma, {
      actor_type: 'merchant_user',
      actor_id: merchant_user_id,
      merchant_id,
      action: 'CAMPAIGN_UPDATED',
      target_type: 'campaign',
      target_id: campaignId,
    });

    return updated;
  }

  async pauseCampaign(merchant_user_id: string, merchant_id: string, campaignId: string) {
    return this.updateStatus(merchant_user_id, merchant_id, campaignId, 'paused');
  }

  async activateCampaign(merchant_user_id: string, merchant_id: string, tier: string, campaignId: string) {
    if (process.env.NODE_ENV !== 'development') {
      const activeCount = await countActiveCampaigns(this.prisma, merchant_id);
      assertTierAllowsNewCampaign(tier, activeCount);
    }
    return this.updateStatus(merchant_user_id, merchant_id, campaignId, 'active');
  }

  async deleteCampaign(merchant_user_id: string, merchant_id: string, campaignId: string) {
    return this.updateStatus(merchant_user_id, merchant_id, campaignId, 'deleted');
  }

  private async updateStatus(merchant_user_id: string, merchant_id: string, campaignId: string, status: string) {
    const campaign = await this.prisma.campaign.findFirst({
      where: { id: campaignId, merchant_id }
    });

    if (!campaign) throw new Error('CAMPAIGN_NOT_FOUND');

    const updated = await this.prisma.campaign.update({
      where: { id: campaignId },
      data: { status },
    });

    await logAudit(this.prisma, {
      actor_type: 'merchant_user',
      actor_id: merchant_user_id,
      merchant_id,
      action: `CAMPAIGN_${status.toUpperCase()}`,
      target_type: 'campaign',
      target_id: campaignId,
    });

    return updated;
  }

  async getCampaign(merchant_id: string, campaignId: string) {
    return this.prisma.campaign.findFirst({
      where: { id: campaignId, merchant_id, status: { not: 'deleted' } }
    });
  }

  async listCampaigns(merchant_id: string) {
    return this.prisma.campaign.findMany({
      where: { merchant_id, status: { not: 'deleted' } },
      orderBy: { created_at: 'desc' }
    });
  }
}
