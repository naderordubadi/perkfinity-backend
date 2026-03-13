import { buildApp } from './app.js';
import { env } from './config/env.js';
import { startJobs } from './jobs/runner.js';

async function startServer() {
  try {
    const app = await buildApp();

    const signals = ['SIGINT', 'SIGTERM'];
    for (const signal of signals) {
      process.on(signal, async () => {
        app.log.info(`Received ${signal}, shutting down gracefully...`);
        await app.close();
        process.exit(0);
      });
    }

    await app.listen({ port: env.PORT, host: '0.0.0.0' });
    
    // Start background jobs
    startJobs(app.prisma);

    // Pino logger handles the log automatically but we can be explicit
    app.log.info(`Server listening on port ${env.PORT}`);
  } catch (err) {
    console.error('❌ Error starting server:', err);
    process.exit(1);
  }
}

startServer();
