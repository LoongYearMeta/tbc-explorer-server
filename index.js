import dotenv from "dotenv";
import createError from "http-errors";
import express from "express";
import mongoose from "mongoose";
import { spawn } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import cluster from "cluster";
import os from "os";

import logger from "./config/logger.js";
import { connectDB, disconnectDB } from "./config/db.js";
import { connectRedis, disconnectRedis, getRedisStats } from "./config/redis.js";
import {
  requestLogger,
  errorLogger,
} from "./middleware/requestLogger.js";
import { globalRateLimit } from "./middleware/rateLimiter.js";
import serviceManager from "./services/ServiceManager.js";
import transactionAggregator from "./services/TransactionAggregator.js";
import generalRpcAggregator from "./services/GeneralRpcAggregator.js";
import addressRoutes from "./routes/address.js";
import blockRoutes from "./routes/block.js";
import transactionRoutes from "./routes/transaction.js";
import chaininfoRoutes from "./routes/chaininfo.js";
import mempoolRoutes from "./routes/mempool.js";
import networkRoutes from "./routes/network.js";
import adminRoutes from "./routes/admin.js";
import { getRealClientIP } from "./lib/util.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 3000;
const CLUSTER_WORKERS = process.env.CLUSTER_WORKERS || os.cpus().length;

if (cluster.isPrimary) {
  logger.info(`Master process ${process.pid} is running`);
  logger.info(`Starting ${CLUSTER_WORKERS} cluster workers for HTTP requests`);

  for (let i = 0; i < CLUSTER_WORKERS; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    logger.warn(`Cluster worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
    if (!process.shuttingDown) {
      logger.info('Starting a new cluster worker');
      cluster.fork();
    }
  });

  cluster.on('online', (worker) => {
    logger.info(`Cluster worker ${worker.process.pid} is online`);
  });

  setTimeout(() => {
    logger.info("Starting dedicated worker processes...");
    startWorkerProcesses();
  }, 2000);

  process.on('SIGTERM', () => gracefulShutdownMaster('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdownMaster('SIGINT'));

} else {
  logger.info(`Cluster worker ${process.pid} starting`);
  startHttpServer();
}

async function startHttpServer() {
  const app = express();

  app.set('trust proxy', true);
  app.use(requestLogger);
  app.use(express.json());

  app.get("/health", async (req, res) => {
    const serviceStatus = serviceManager.getServiceStatus();

    const dbState = mongoose.connection.readyState;
    const dbConnected = dbState === 1;

    const redisStats = getRedisStats();
    const redisConnected = redisStats.status === 'ready';

    const overallHealthy = serviceStatus.initialized && dbConnected && redisConnected;

    logger.info("Health check request", {
      worker: process.pid,
      ip: getRealClientIP(req),
      healthy: overallHealthy
    });

    res.status(overallHealthy ? 200 : 503).json({
      healthy: overallHealthy,
      timestamp: new Date().toISOString(),
      services: {
        database: dbConnected,
        redis: redisConnected,
        serviceManager: serviceStatus.initialized
      },
      worker: process.pid
    });
  });

  app.use(globalRateLimit);

  app.use("/address", addressRoutes);
  app.use("/block", blockRoutes);
  app.use("/transaction", transactionRoutes);
  app.use("/chain", chaininfoRoutes);
  app.use("/mempool", mempoolRoutes);
  app.use("/network", networkRoutes);
  app.use("/admin", adminRoutes);

  app.use(function (_req, _res, next) {
    next(createError(404));
  });

  app.use(errorLogger);
  app.use((error, req, res, next) => {
    const status = error.status || 500;
    const message = error.message || "Internal Server Error";

    if (status !== 404) {
      logger.error(`Error ${status}: ${message}`, {
        worker: process.pid,
        url: req.originalUrl,
        method: req.method,
        ip: getRealClientIP(req),
        userAgent: req.get("User-Agent"),
        stack: error.stack,
      });
    }

    res.status(status).json({
      code: status,
      message: message,
    });
  });

  try {
    logger.info("Starting database connection...");
    await connectDB();

    logger.info("Starting Redis connection...");
    await connectRedis();

    logger.info("Starting external service connections initialization...");
    await serviceManager.initialize();

    app.listen(PORT, () => {
      logger.info(`Cluster worker ${process.pid} is running on port ${PORT}`);
    });
    process.on('SIGTERM', () => gracefulShutdownWorker('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdownWorker('SIGINT'));

  } catch (error) {
    logger.error("Cluster worker startup failed", {
      worker: process.pid,
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

let zeromqWorkerStarting = false;
function startZeroMQWorker() {
  if (zeromqWorkerStarting) {
    logger.warn("ZeroMQ worker is already starting, skipping duplicate start request");
    return;
  }
  if (process.zeromqWorkerProcess) {
    try {
      process.kill(process.zeromqWorkerProcess.pid, 0);
      logger.warn("ZeroMQ worker process already exists and running, skipping start");
      return;
    } catch (error) {
      logger.warn(`ZeroMQ worker process reference exists but process ${process.zeromqWorkerProcess.pid} is dead, cleaning up and restarting`);
      process.zeromqWorkerProcess = null;
    }
  }
  zeromqWorkerStarting = true;
  logger.info("Starting ZeroMQ worker process...");

  const zeromqWorkerPath = path.join(__dirname, 'workers', 'zeromqWorker.js');
  const zeromqWorkerProcess = spawn('node', [zeromqWorkerPath], {
    stdio: 'inherit',
    env: { ...process.env, WORKER_TYPE: 'zeromq' }
  });
  process.zeromqWorkerProcess = zeromqWorkerProcess;

  zeromqWorkerProcess.on('error', (error) => {
    logger.error("ZeroMQ worker process error", {
      error: error.message,
      stack: error.stack
    });
    if (process.zeromqWorkerProcess === zeromqWorkerProcess) {
      process.zeromqWorkerProcess = null;
    }
    zeromqWorkerStarting = false;
  });

  zeromqWorkerProcess.on('exit', (code, signal) => {
    logger.warn("ZeroMQ worker process exited", {
      code,
      signal
    });
    if (process.zeromqWorkerProcess === zeromqWorkerProcess) {
      process.zeromqWorkerProcess = null;
    }
    zeromqWorkerStarting = false;
    if (code !== 0 && signal !== 'SIGTERM' && signal !== 'SIGINT' && !process.shuttingDown) {
      logger.info("ZeroMQ worker process will be restarted in 5 seconds...");
      setTimeout(() => {
        if (!process.shuttingDown && !process.zeromqWorkerProcess) {
          startZeroMQWorker();
        }
      }, 5000);
    }
  });

  logger.info("ZeroMQ worker process started successfully", {
    pid: zeromqWorkerProcess.pid
  });
  zeromqWorkerStarting = false;
}

function startWorkerProcesses() {
  logger.info("Starting block preprocessor worker process...");
  const blockWorkerPath = path.join(__dirname, 'workers', 'blockPreprocessorWorker.js');
  const blockWorkerProcess = spawn('node', [blockWorkerPath], {
    stdio: 'inherit',
    env: { ...process.env, WORKER_TYPE: 'block' }
  });

  blockWorkerProcess.on('error', (error) => {
    logger.error("Block preprocessor worker process error", {
      error: error.message,
      stack: error.stack
    });
  });

  blockWorkerProcess.on('exit', (code, signal) => {
    if (code === 0) {
      logger.info("Block preprocessor worker process completed successfully", {
        code,
        signal
      });
    } else {
      logger.error("Block preprocessor worker process exited with error", {
        code,
        signal
      });
    }
    process.blockWorkerProcess = null;
  });

  process.blockWorkerProcess = blockWorkerProcess;

  logger.info("Starting Redis cache worker process (one-time execution)...");
  const cacheWorkerPath = path.join(__dirname, 'workers', 'redisCacheWorker.js');
  const cacheWorkerProcess = spawn('node', [cacheWorkerPath], {
    stdio: 'inherit',
    env: { ...process.env, WORKER_TYPE: 'cache' }
  });

  cacheWorkerProcess.on('error', (error) => {
    logger.error("Redis cache worker process error", {
      error: error.message,
      stack: error.stack
    });
  });

  cacheWorkerProcess.on('exit', (code, signal) => {
    if (code === 0) {
      logger.info("Redis cache worker process completed successfully", {
        code,
        signal
      });
    } else {
      logger.error("Redis cache worker process exited with error", {
        code,
        signal
      });
    }
    process.cacheWorkerProcess = null;
  });

  process.cacheWorkerProcess = cacheWorkerProcess;

  startZeroMQWorker();
}

async function gracefulShutdownMaster(signal) {
  logger.info(`Master process received ${signal} signal`);

  process.shuttingDown = true;
  zeromqWorkerStarting = false;

  logger.info('Stopping all cluster workers...');
  for (const id in cluster.workers) {
    cluster.workers[id].kill('SIGTERM');
  }

  await new Promise((resolve) => {
    let remainingWorkers = Object.keys(cluster.workers).length;
    if (remainingWorkers === 0) {
      resolve();
      return;
    }

    const timeout = setTimeout(() => {
      logger.warn('Some cluster workers did not exit gracefully, force killing...');
      for (const id in cluster.workers) {
        cluster.workers[id].kill('SIGKILL');
      }
      resolve();
    }, 10000);

    cluster.on('exit', () => {
      remainingWorkers--;
      if (remainingWorkers === 0) {
        clearTimeout(timeout);
        resolve();
      }
    });
  });

  if (process.zeromqWorkerProcess) {
    logger.info('Terminating ZeroMQ worker process...');
    process.zeromqWorkerProcess.kill('SIGTERM');

    await new Promise((resolve) => {
      let timeout = setTimeout(() => {
        logger.warn('ZeroMQ worker process did not exit gracefully, force killing...');
        if (process.zeromqWorkerProcess) {
          process.zeromqWorkerProcess.kill('SIGKILL');
        }
        resolve();
      }, 10000);

      if (process.zeromqWorkerProcess) {
        process.zeromqWorkerProcess.on('exit', () => {
          clearTimeout(timeout);
          resolve();
        });
      } else {
        clearTimeout(timeout);
        resolve();
      }
    });
  }

  if (process.blockWorkerProcess) {
    logger.info('Terminating block preprocessor worker process...');
    process.blockWorkerProcess.kill('SIGTERM');

    await new Promise((resolve) => {
      let timeout = setTimeout(() => {
        logger.warn('Block preprocessor worker process did not exit gracefully, force killing...');
        if (process.blockWorkerProcess) {
          process.blockWorkerProcess.kill('SIGKILL');
        }
        resolve();
      }, 10000);

      if (process.blockWorkerProcess) {
        process.blockWorkerProcess.on('exit', () => {
          clearTimeout(timeout);
          resolve();
        });
      } else {
        clearTimeout(timeout);
        resolve();
      }
    });
  }

  if (process.cacheWorkerProcess) {
    logger.info('Terminating Redis cache worker process...');
    process.cacheWorkerProcess.kill('SIGTERM');

    await new Promise((resolve) => {
      let timeout = setTimeout(() => {
        logger.warn('Redis cache worker process did not exit gracefully, force killing...');
        if (process.cacheWorkerProcess) {
          process.cacheWorkerProcess.kill('SIGKILL');
        }
        resolve();
      }, 5000);

      if (process.cacheWorkerProcess) {
        process.cacheWorkerProcess.on('exit', () => {
          clearTimeout(timeout);
          resolve();
        });
      } else {
        clearTimeout(timeout);
        resolve();
      }
    });
  }

  logger.info('Master process shutting down...');
  process.exit(0);
}

async function gracefulShutdownWorker(signal) {
  logger.info(`Cluster worker ${process.pid} received ${signal} signal`);

  logger.info('Disconnecting Redis...');
  await disconnectRedis();

  logger.info('Shutting down TransactionAggregator...');
  await transactionAggregator.shutdown();

  logger.info('Shutting down GeneralRpcAggregator...');
  await generalRpcAggregator.shutdown();

  logger.info('Shutting down ServiceManager...');
  await serviceManager.shutdown();

  logger.info('Disconnecting MongoDB...');
  await disconnectDB();

  logger.info(`Cluster worker ${process.pid} shutting down...`);
  process.exit(0);
}
