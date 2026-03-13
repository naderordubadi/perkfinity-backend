import { FastifyPluginAsync } from 'fastify';
import { PrismaClient } from '@prisma/client';
import fp from 'fastify-plugin';

const prismaPluginAsync: FastifyPluginAsync = async (fastify, options) => {
  const prisma = new PrismaClient();
  await prisma.$connect();
  
  fastify.decorate('prisma', prisma);
  
  fastify.addHook('onClose', async (server) => {
    await server.prisma.$disconnect();
  });
};

export const prismaPlugin = fp(prismaPluginAsync, { name: 'prismaPlugin' });

declare module 'fastify' {
  interface FastifyInstance {
    prisma: PrismaClient;
  }
}
