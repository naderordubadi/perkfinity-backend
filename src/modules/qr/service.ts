import { PrismaClient } from '@prisma/client';
import crypto from 'node:crypto';
import { logAudit } from '../audit/service.js';

export class QrService {
  constructor(private prisma: PrismaClient) {}

  async resolveQrCode(public_code: string) {
    const qr = await this.prisma.qrCode.findUnique({
      where: { public_code },
      include: {
        merchant: { select: { id: true, business_name: true } },
        location: { select: { id: true, address: true, city: true, state: true } },
      }
    });

    if (!qr || qr.status !== 'active') {
      return null; // Signals QR_INACTIVE
    }

    const now = new Date();
    
    // Find active campaign
    const campaign = await this.prisma.campaign.findFirst({
      where: {
        merchant_id: qr.merchant_id,
        status: 'active',
        OR: [{ start_at: null }, { start_at: { lte: now } }],
        AND: [
          { OR: [{ end_at: null }, { end_at: { gt: now } }] },
        ]
      },
      select: {
        id: true,
        title: true,
        discount_percentage: true,
        terms: true,
        redemption_time_limit_minutes: true,
        status: true,
        start_at: true,
        end_at: true,
      }
    });

    if (!campaign) {
      return { qr, campaign: null }; // Signals NO_ACTIVE_CAMPAIGN
    }

    await this.prisma.event.create({
      data: {
        event_name: 'QR_SCANNED',
        merchant_id: qr.merchant_id,
        location_id: qr.location_id,
        campaign_id: campaign.id,
      }
    });

    return {
      merchant: qr.merchant,
      location: qr.location,
      campaign,
    };
  }

  async generateQrCode(merchant_user_id: string, merchant_id: string, location_id?: string) {
    const public_code = crypto.randomBytes(9).toString('base64url'); // 12 chars
    
    const qr = await this.prisma.qrCode.create({
      data: {
        merchant_id,
        location_id,
        public_code,
      }
    });

    await logAudit(this.prisma, {
      actor_type: 'merchant_user',
      actor_id: merchant_user_id,
      merchant_id,
      action: 'QR_GENERATED',
      target_type: 'qr_code',
      target_id: qr.id,
    });

    return {
      qr,
      url: `https://app.perkfinity.net/qr/${public_code}`
    };
  }
  async generateMerchantQrImage(public_code: string): Promise<string> {
    const qrcode = await import('qrcode');
    const url = `https://app.perkfinity.net/qr/${public_code}`;
    
    // Generate raw QR code options (high error correction for center logo)
    const qrDataUrl = await qrcode.default.toDataURL(url, {
      errorCorrectionLevel: 'H',
      type: 'image/jpeg',
      quality: 0.9,
      margin: 2,
      scale: 10,
      color: {
        dark: '#000000',
        light: '#FFFFFF'
      }
    });

    // In a full production implementation, we would use the 'canvas' package to draw the
    // 'perkfinity-logo.png' over the exact center of this data URL. For now, returning the raw
    // high-res QR initialized with H-level error correction which safely allows the frontend
    // or print scripts to safely overlay the logo themselves without breaking the code.
    return qrDataUrl;
  }
}
