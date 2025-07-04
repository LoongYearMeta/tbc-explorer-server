import dotenv from "dotenv";

import { connectDB, disconnectDB } from "../config/db.js";
import { connectRedis, disconnectRedis } from "../config/redis.js";
import serviceManager from "../services/ServiceManager.js";
import ZeroMQService from "../services/zeromq.js";
import servicesConfig from "../config/services.js";
import logger from "../config/logger.js";

dotenv.config();

class ZeroMQWorker {
  constructor() {
    this.zeromqService = null;
    this.isRunning = false;
  }

  async initialize() {
    try {
      logger.info("ZeroMQWorker: Initializing...");
      await connectDB();
      await connectRedis();
      await serviceManager.initialize();
      this.zeromqService = new ZeroMQService(servicesConfig.zeromq);
      this.zeromqService.setServiceManager(serviceManager);
      logger.info("ZeroMQWorker: Initialization completed");
      await this.start();
      
    } catch (error) {
      logger.error("ZeroMQWorker: Initialization failed", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  async start() {
    if (this.isRunning) {
      logger.warn("ZeroMQWorker: Already running");
      return;
    }

    this.isRunning = true;
    logger.info("ZeroMQWorker: Starting ZeroMQ worker");

    try {
      await this.zeromqService.start();
      logger.info("ZeroMQWorker: ZeroMQ worker started successfully");
      this.keepAlive();
    } catch (error) {
      logger.error("ZeroMQWorker: Failed to start ZeroMQ worker", {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  keepAlive() {
    process.on('SIGTERM', async () => {
      await this.stop();
    });

    process.on('SIGINT', async () => {
      await this.stop();
    });
  }

  async stop() {
    if (!this.isRunning) {
      logger.info("ZeroMQWorker: Already stopped");
      return;
    }

    logger.info("ZeroMQWorker: Stopping ZeroMQ worker");
    this.isRunning = false;

    try {
      if (this.zeromqService) {
        await this.zeromqService.stop();
      }
      
      logger.info("ZeroMQWorker: ZeroMQ worker stopped successfully");
    } catch (error) {
      logger.error("ZeroMQWorker: Error stopping ZeroMQ worker", {
        error: error.message,
        stack: error.stack
      });
    }
  }

  async cleanup() {
    logger.info('ZeroMQWorker: Starting cleanup process...');

    try {
      await this.stop();
      
      if (serviceManager) {
        await serviceManager.shutdown();
      }
      
      await disconnectRedis();
      await disconnectDB();

      logger.info('ZeroMQWorker: Cleanup completed successfully');
    } catch (error) {
      logger.error('ZeroMQWorker: Error during cleanup', {
        error: error.message,
        stack: error.stack
      });
    }
  }
}

const worker = new ZeroMQWorker();

process.on('SIGTERM', async () => {
  logger.info('ZeroMQWorker: Received SIGTERM signal');
  await worker.cleanup();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('ZeroMQWorker: Received SIGINT signal');
  await worker.cleanup();
  process.exit(0);
});

process.on('uncaughtException', async (error) => {
  logger.error('ZeroMQWorker: Uncaught exception', {
    error: error.message,
    stack: error.stack
  });
  await worker.cleanup();
  process.exit(1);
});

process.on('unhandledRejection', async (reason, promise) => {
  logger.error('ZeroMQWorker: Unhandled promise rejection', {
    reason: reason,
    promise: promise
  });
  await worker.cleanup();
  process.exit(1);
});

worker.initialize().catch(async (error) => {
  logger.error('ZeroMQWorker: Failed to initialize', {
    error: error.message,
    stack: error.stack
  });
  await worker.cleanup();
  process.exit(1);
}); 