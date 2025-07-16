import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";
import { addressRateLimit } from "../middleware/rateLimiter.js";

const router = express.Router();

router.get("/:address", addressRateLimit, async (req, res, next) => {
  try {
    const { address } = req.params;
    
    logger.info("Comprehensive address info request", {
      address,
      ip: getRealClientIP(req),
    });

    const [balance, txInfo] = await Promise.all([
      serviceManager.getAddressBalance(address),
      serviceManager.getAddressTransactionIds(address)
    ]);
    
    res.status(200).json({
      address,
      balance: balance.confirmed || 0,
      unconfirmed: balance.unconfirmed || 0,
      txIds: txInfo.txIds,
      totalTransactions: txInfo.totalTransactions,
      scriptHash: txInfo.scriptHash,
    });
  } catch (error) {
    next(error);
  }
});

export default router; 