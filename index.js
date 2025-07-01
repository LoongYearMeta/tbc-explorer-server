import dotenv from "dotenv";
import createError from "http-errors";
import express from "express";

import logger from "./config/logger.js";
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
const app = express();

app.use(requestLogger);

app.use(express.json());

app.get("/health", (req, res) => {
  const serviceStatus = serviceManager.getServiceStatus();

  logger.info("Health check request", {
    ip: req.ip,
    serviceStatus: serviceStatus.initialized,
  });

  res.status(200).json({
    message: "Service is running",
    timestamp: new Date().toISOString(),
    services: serviceStatus,
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
    logger.info("Starting external service connections initialization...");
    await serviceManager.initialize();

    app.listen(PORT, () => {
      logger.info(`Server is running on port ${PORT}`);
      logger.info("Application started successfully");
      logger.info("All services are ready");
      
      logger.info("Available API endpoints:");
      logger.info("  GET  /health                              - Health check");
      logger.info("  GET  /api/address/:address                - Get address info");
      logger.info("  GET  /api/address/:address/balance        - Get address balance");
      logger.info("  GET  /api/address/:address/txids          - Get address transaction IDs");
      logger.info("  GET  /api/block/height/:height            - Get block by height");
      logger.info("  GET  /api/block/hash/:hash                - Get block by hash");
      logger.info("  POST /api/block/heights                   - Get multiple blocks");
      logger.info("  GET  /api/transaction/:txid               - Get transaction");
      logger.info("  POST /api/transaction/batch               - Get multiple transactions");
      logger.info("  GET  /api/chain/                          - Get blockchain info");
      logger.info("  GET  /api/chain/mining                    - Get mining info");
      logger.info("  GET  /api/chain/txstats/:count            - Get transaction stats");
      logger.info("  GET  /api/chain/status                    - Get chain status");
      logger.info("  GET  /api/mempool/                        - Get raw mempool");
      logger.info("  GET  /api/mempool/info                    - Get mempool info");
      logger.info("  GET  /api/mempool/count                   - Get mempool count");
    });
  } catch (error) {
    logger.error("Application startup failed", {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  }
}

startServer();
