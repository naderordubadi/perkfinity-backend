import { PrismaClient } from '@prisma/client';
import bcrypt from 'bcryptjs';
import { logAudit } from '../audit/service.js';
import { env } from '../../config/env.js';

export class AuthService {
  constructor(private prisma: PrismaClient, private jwt: any) { }

  async registerMerchant(data: any) {
    const password_hash = await bcrypt.hash(data.password, 12);
    const merchant = await this.prisma.merchant.create({
      data: {
        business_name: data.business_name,
        users: {
          create: {
            email: data.email,
            password_hash,
            role: 'owner',
          }
        }
      },
      include: { users: true }
    });

    const merchantUser = merchant.users[0];
    const { accessToken, refreshToken } = this.generateMerchantTokens(merchantUser.id, merchant.id, merchantUser.role);

    await logAudit(this.prisma, {
      actor_type: 'merchant_user',
      actor_id: merchantUser.id,
      merchant_id: merchant.id,
      action: 'MERCHANT_REGISTERED',
      target_type: 'merchant',
      target_id: merchant.id,
    });

    // Remove password hash before returning
    const { password_hash: _ignored, ...userWithoutPassword } = merchantUser;

    return { merchant, merchantUser: userWithoutPassword, accessToken, refreshToken };
  }

  async loginMerchant(data: any) {
    const merchantUser = await this.prisma.merchantUser.findUnique({
      where: { email: data.email },
      include: { merchant: true }
    });

    if (!merchantUser) {
      return null;
    }

    const valid = await bcrypt.compare(data.password, merchantUser.password_hash);
    if (!valid) {
      return null;
    }

    const { accessToken, refreshToken } = this.generateMerchantTokens(merchantUser.id, merchantUser.merchant_id, merchantUser.role);
    const { password_hash: _ignored, merchant, ...userWithoutPassword } = merchantUser;

    return { accessToken, refreshToken, merchant, role: merchantUser.role };
  }

  async getOrCreateAnonymousUser() {
    const user = await this.prisma.user.create({ data: {} });
    const accessToken = await this.jwt.sign(
      { sub: user.id, type: 'user' },
      { expiresIn: '30d' }
    );
    return { user, accessToken };
  }

  async refreshAccessToken(refreshToken: string) {
    const decoded: any = await this.jwt.verify(refreshToken);
    if (decoded.type === 'merchant') {
      const accessToken = await this.jwt.sign(
        { sub: decoded.sub, merchant_id: decoded.merchant_id, role: decoded.role, type: 'merchant' },
        { expiresIn: env.JWT_EXPIRY }
      );
      return { accessToken };
    }
    throw new Error('Invalid token type');
  }

  private generateMerchantTokens(userId: string, merchantId: string, role: string) {
    const payload = { sub: userId, merchant_id: merchantId, role, type: 'merchant' };
    const accessToken = this.jwt.sign(payload, { expiresIn: env.JWT_EXPIRY });
    const refreshToken = this.jwt.sign(payload, { expiresIn: env.JWT_REFRESH_EXPIRY });
    return { accessToken, refreshToken };
  }
}
