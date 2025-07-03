import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";

const router = express.Router();

router.get("/:address/balance", async (req, res, next) => {
  try {
    const { address } = req.params;
    
    logger.info("Address balance request", {
      address,
      ip: req.ip,
    });

    const balance = await serviceManager.getAddressBalance(address);
    
    res.status(200).json({
      address,
      balance,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/:address/txids", async (req, res, next) => {
  try {
    const { address } = req.params;
    
    logger.info("Address transaction IDs request", {
      address,
      ip: req.ip,
    });

    const result = await serviceManager.getAddressTransactionIds(address);
    
    res.status(200).json({
      ...result,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/:address", async (req, res, next) => {
  try {
    const { address } = req.params;
    
    logger.info("Comprehensive address info request", {
      address,
      ip: req.ip,
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
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

export default router; 