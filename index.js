import dotenv from "dotenv";
import createError from "http-errors";
import express from "express";
import mongoose from "mongoose";
import { spawn } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

import logger from "./config/logger.js";
import { connectDB, disconnectDB } from "./config/db.js";
import { connectRedis, disconnectRedis, redis } from "./config/redis.js";
import {
  requestLogger,
  errorLogger,
} from "./middleware/requestLogger.js";
import serviceManager from "./services/ServiceManager.js";
import transactionAggregator from "./services/TransactionAggregator.js";
import generalRpcAggregator from "./services/GeneralRpcAggregator.js";
import addressRoutes from "./routes/address.js";
import blockRoutes from "./routes/block.js";
import transactionRoutes from "./routes/transaction.js";
import chaininfoRoutes from "./routes/chaininfo.js";
import mempoolRoutes from "./routes/mempool.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(requestLogger);
app.use(express.json());

app.get("/health", (req, res) => {
  const serviceStatus = serviceManager.getServiceStatus();
  const dbConnectionStates = {
    0: 'disconnected',
    1: 'connected',
    2: 'connecting',
    3: 'disconnecting'
  };

  const dbState = mongoose.connection.readyState;
  const dbStatus = {
    state: dbConnectionStates[dbState] || 'unknown',
    connected: dbState === 1,
    host: mongoose.connection.host,
    name: mongoose.connection.name,
    readyState: dbState
  };

  const redisStatus = {
    connected: redis.status === 'ready',
    state: redis.status,
    host: redis.options.host,
    port: redis.options.port,
    keyPrefix: redis.options.keyPrefix
  };

  const workersStatus = {
    blockPreprocessorWorker: {
      running: !!process.blockWorkerProcess,
      pid: process.blockWorkerProcess ? process.blockWorkerProcess.pid : null
    },
    redisCacheWorker: {
      running: !!process.cacheWorkerProcess,
      pid: process.cacheWorkerProcess ? process.cacheWorkerProcess.pid : null
    },
    zeromqWorker: {
      running: !!process.zeromqWorkerProcess,
      pid: process.zeromqWorkerProcess ? process.zeromqWorkerProcess.pid : null
    }
  };

  const aggregatorStatus = transactionAggregator.getStats();

  const overallHealthy = serviceStatus.initialized && dbStatus.connected && redisStatus.connected;

  logger.info("Health check request", {
    ip: req.ip,
    serviceStatus: serviceStatus.initialized,
    dbStatus: dbStatus.connected,
    redisStatus: redisStatus.connected,
    workersStatus,
    overallHealthy
  });

  res.status(overallHealthy ? 200 : 503).json({
    message: overallHealthy ? "Service is running" : "Service is unhealthy",
    healthy: overallHealthy,
    timestamp: new Date().toISOString(),
    services: serviceStatus,
    database: dbStatus,
    redis: redisStatus,
    workers: workersStatus,
    aggregator: {
      transaction: aggregatorStatus
    },
  });
});

app.use("/address", addressRoutes);
app.use("/block", blockRoutes);
app.use("/transaction", transactionRoutes);
app.use("/chain", chaininfoRoutes);
app.use("/mempool", mempoolRoutes);

app.use(function (_req, _res, next) {
  next(createError(404));
});

app.use(errorLogger);
app.use((error, req, res, next) => {
  const status = error.status || 500;
  const message = error.message || "Internal Server Error";

  if (status !== 404) {
    logger.error(`Error ${status}: ${message}`, {
      url: req.originalUrl,
      method: req.method,
      ip: req.ip,
      userAgent: req.get("User-Agent"),
      stack: error.stack,
    });
  }

  res.status(status).json({
    code: status,
    message: message,
  });
});

const PORT = process.env.PORT || 3000;

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
    env: process.env
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

async function startServer() {
  try {
    logger.info("Starting database connection...");
    await connectDB();

    logger.info("Starting Redis connection...");
    await connectRedis();

    logger.info("Starting external service connections initialization...");
    await serviceManager.initialize();

    app.listen(PORT, () => {
      logger.info(`Server is running on port ${PORT}`);
      logger.info("Application started successfully");
      logger.info("All services are ready (database + Redis + external services)");

      logger.info("Available API endpoints:");
      logger.info("  GET  /health                              - Health check (includes aggregator stats)");
      logger.info("  GET  /address/:address                - Get address info");
      logger.info("  GET  /block/height/:height            - Get block by height");
      logger.info("  GET  /block/hash/:hash                - Get block by hash");
      logger.info("  GET  /block/latest                    - Get latest 10 blocks");
      logger.info("  POST /block/heights                   - Get multiple blocks");
      logger.info("  GET  /transaction/:txid               - Get transaction (with aggregator optimization)");
      logger.info("  GET  /transaction/:txid/raw           - Get raw transaction (hex)");
      logger.info("  POST /transaction/batch/raw           - Get multiple raw transactions (max 500)");
      logger.info("  GET  /chain                           - Get blockchain info");
      logger.info("  GET  /chain/txstats/:count            - Get transaction stats");
      logger.info("  GET  /mempool                         - Get raw mempool");
      logger.info("  GET  /mempool/info                    - Get mempool info");

      logger.info("Worker processes:");
      logger.info("  Block preprocessor worker             - Process historical blocks");
      logger.info("  Redis cache worker                    - Cache recent blocks and mempool (one-time)");
      logger.info("  ZeroMQ worker                         - Real-time block and transaction notifications");
      setTimeout(() => {
        logger.info("Starting worker processes...");
        startWorkerProcesses();
      }, 100);
    });

  } catch (error) {
    logger.error("Application startup failed (database, Redis, or external services)", {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

function startWorkerProcesses() {
  logger.info("Starting block preprocessor worker process...");
  const blockWorkerPath = path.join(__dirname, 'workers', 'blockPreprocessorWorker.js');
  const blockWorkerProcess = spawn('node', [blockWorkerPath], {
    stdio: 'inherit',
    env: process.env
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
    env: process.env
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

startServer();

async function gracefulShutdown(signal) {
  logger.info(`Received ${signal} signal`);

  process.shuttingDown = true;
  zeromqWorkerStarting = false;

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

  logger.info('Main process shutting down...');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
