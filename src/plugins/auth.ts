import { FastifyRequest, FastifyReply } from 'fastify';
import { failure } from '../utils/response.js';

export async function requireMerchantAuth(request: FastifyRequest, reply: FastifyReply) {
  try {
    console.log('[DEBUG] Auth Header:', request.headers.authorization);
    await request.jwtVerify();
    const user = request.user as any;
    if (user.type !== 'merchant') {
      return reply.code(403).send(failure('AUTH_INVALID_CREDENTIALS', 'Merchant auth required'));
    }
  } catch (err) {
    return reply.code(401).send(failure('AUTH_INVALID_CREDENTIALS', 'Invalid or missing token'));
  }
}

export async function requireUserAuth(request: FastifyRequest, reply: FastifyReply) {
  try {
    await request.jwtVerify();
    const user = request.user as any;
    if (user.type !== 'user') {
      return reply.code(403).send(failure('AUTH_INVALID_CREDENTIALS', 'User auth required'));
    }
  } catch (err) {
    return reply.code(401).send(failure('AUTH_INVALID_CREDENTIALS', 'Invalid or missing token'));
  }
}

export function requireRole(...roles: string[]) {
  return async (request: FastifyRequest, reply: FastifyReply) => {
    const user = request.user as any;
    if (!user || user.type !== 'merchant' || !roles.includes(user.role)) {
      if (user.role === 'admin') return; // admins can bypass role checks
      return reply.code(403).send(failure('AUTH_INVALID_CREDENTIALS', 'Insufficient permissions'));
    }
  };
}

export async function optionalAuth(request: FastifyRequest, reply: FastifyReply) {
  try {
    if (request.headers.authorization) {
      await request.jwtVerify();
    }
  } catch (err) {
    // Ignore invalid token
  }
}
