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

  const overallHealthy = serviceStatus.initialized && dbStatus.connected && redisStatus.connected;

  logger.info("Health check request", {
    ip: req.ip,
    serviceStatus: serviceStatus.initialized,
    dbStatus: dbStatus.connected,
    redisStatus: redisStatus.connected,
    overallHealthy
  });

  res.status(overallHealthy ? 200 : 503).json({
    message: overallHealthy ? "Service is running" : "Service is unhealthy",
    healthy: overallHealthy,
    timestamp: new Date().toISOString(),
    services: serviceStatus,
    database: dbStatus,
    redis: redisStatus,
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
      logger.info("  GET  /health                              - Health check");
      logger.info("  GET  /address/:address                - Get address info");
      logger.info("  GET  /address/:address/balance        - Get address balance");
      logger.info("  GET  /address/:address/txids          - Get address transaction IDs");
      logger.info("  GET  /block/height/:height            - Get block by height");
      logger.info("  GET  /block/hash/:hash                - Get block by hash");
      logger.info("  GET  /block/latest                    - Get latest 10 blocks");
      logger.info("  POST /block/heights                   - Get multiple blocks");
      logger.info("  GET  /transaction/:txid               - Get transaction");
      logger.info("  GET  /transaction/:txid/raw           - Get raw transaction (hex)");
      logger.info("  POST /transaction/batch/raw           - Get multiple raw transactions (max 500)");
      logger.info("  GET  /chain                           - Get blockchain info");
      logger.info("  GET  /chain/mining                    - Get mining info");
      logger.info("  GET  /chain/txstats/:count            - Get transaction stats");
      logger.info("  GET  /chain/status                    - Get chain status");
      logger.info("  GET  /mempool                         - Get raw mempool");
      logger.info("  GET  /mempool/info                    - Get mempool info");
      logger.info("  GET  /mempool/count                   - Get mempool count");
      
      logger.info("ZeroMQ subscriptions:");
      logger.info("  tcp://0.0.0.0:28332                   - Block hash notifications");
      logger.info("  tcp://0.0.0.0:28333                   - Transaction hash notifications");
    });

    logger.info("Starting block preprocessor worker process...");
    const workerPath = path.join(__dirname, 'workers', 'blockPreprocessorWorker.js');
    const workerProcess = spawn('node', [workerPath], {
      stdio: 'inherit',
      env: process.env
    });

    workerProcess.on('error', (error) => {
      logger.error("Block preprocessor worker process error", {
        error: error.message,
        stack: error.stack
      });
    });

    workerProcess.on('exit', (code, signal) => {
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
      process.workerProcess = null;
    });

    process.workerProcess = workerProcess;
    
  } catch (error) {
    logger.error("Application startup failed (database, Redis, or external services)", {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

startServer();

async function gracefulShutdown(signal) {
  logger.info(`Received ${signal} signal`);
  
  if (process.workerProcess) {
    logger.info('Terminating worker process...');
    process.workerProcess.kill('SIGTERM');
    
    await new Promise((resolve) => {
      let timeout = setTimeout(() => {
        logger.warn('Worker process did not exit gracefully, force killing...');
        if (process.workerProcess) {
          process.workerProcess.kill('SIGKILL');
        }
        resolve();
      }, 10000);
      
      if (process.workerProcess) {
        process.workerProcess.on('exit', () => {
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
  
  logger.info('Shutting down ServiceManager...');
  await serviceManager.shutdown();
  
  logger.info('Disconnecting MongoDB...');
  await disconnectDB();
  
  logger.info('Main process shutting down...');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
