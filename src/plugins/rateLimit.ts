import fp from 'fastify-plugin';

export const rateLimitConfig = {
  public: { max: 120, timeWindow: 60000 },
  activation: { max: 30, timeWindow: 3600000 },
  merchantValidation: { max: 300, timeWindow: 3600000 },
  auth: { max: 10, timeWindow: 60000 },
  default: { max: 200, timeWindow: 60000 },
};

export const rateLimitErrorHandlerPlugin = fp(async (fastify, options) => {
  fastify.setErrorHandler(function (error, request, reply) {
    if (error.statusCode === 429) {
      reply.header('Retry-After', (error as { headers?: Record<string, string | number> }).headers?.['retry-after'] || 60);
      return reply.code(429).send({
        ok: false,
        error: { code: 'RATE_LIMITED', message: 'Rate limit exceeded' }
      });
    }

    if (error.validation) {
      return reply.code(400).send({
        ok: false,
        error: { code: 'VALIDATION_ERROR', message: 'Validation failed', details: error.validation }
      });
    }

    request.log.error(error);
    reply.code(error.statusCode || 500).send(error);
  });
});
