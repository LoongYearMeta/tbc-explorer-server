import express from "express";
import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";

const router = express.Router();

router.get("/:txid", async (req, res, next) => {
  try {
    const { txid } = req.params;
    
    if (!/^[a-fA-F0-9]{64}$/.test(txid)) {
      return res.status(400).json({
        error: "Invalid transaction ID format",
        timestamp: new Date().toISOString()
      });
    }
    
    logger.info("Transaction request", {
      txid,
      ip: req.ip,
    });

    const transaction = await serviceManager.getRawTransaction(txid);
    
    if (!transaction) {
      return res.status(404).json({
        error: "Transaction not found",
        txid,
        timestamp: new Date().toISOString()
      });
    }
    
    res.json({
      transaction,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.post("/batch", async (req, res, next) => {
  try {
    const { txids } = req.body;
    
    if (!Array.isArray(txids) || txids.length === 0) {
      return res.status(400).json({
        error: "txids array is required and cannot be empty",
        timestamp: new Date().toISOString()
      });
    }

    const invalidTxids = txids.filter(txid => !/^[a-fA-F0-9]{64}$/.test(txid));
    if (invalidTxids.length > 0) {
      return res.status(400).json({
        error: "Invalid transaction ID format",
        invalidTxids,
        timestamp: new Date().toISOString()
      });
    }

    logger.info("Multiple transactions request", {
      count: txids.length,
      ip: req.ip,
    });

    const transactions = await serviceManager.getRawTransactions(txids);
    
    const result = txids.map((txid, index) => ({
      txid,
      transaction: transactions[index] || null,
      found: !!transactions[index]
    }));
    
    res.json({
      results: result,
      total: txids.length,
      found: result.filter(r => r.found).length,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

export default router;
