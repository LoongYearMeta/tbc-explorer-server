import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";
import { Transaction } from "../models/transaction.js";

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

    res.status(200).json({
      transaction,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/:txid/raw", async (req, res, next) => {
  try {
    const { txid } = req.params;

    if (!/^[a-fA-F0-9]{64}$/.test(txid)) {
      return res.status(400).json({
        error: "Invalid transaction ID format",
        timestamp: new Date().toISOString()
      });
    }

    logger.info("Raw transaction request", {
      txid,
      ip: req.ip,
    });

    let rawTransaction = "";
    
    const dbTransaction = await Transaction.findOne({ txid }).lean();
    if (dbTransaction) {
      rawTransaction = dbTransaction.raw;
      logger.debug(`Raw transaction ${txid} found in database`);
    } else {
      logger.debug(`Raw transaction ${txid} not found in database, fetching from RPC`);
      rawTransaction = await serviceManager.getRawTransactionHex(txid);
    }

    if (!rawTransaction) {
      return res.status(404).json({
        error: "Transaction not found",
        txid,
        timestamp: new Date().toISOString()
      });
    }

    res.status(200).json({
      txid,
      rawTransaction
    });
  } catch (error) {
    next(error);
  }
});

router.post("/batch/raw", async (req, res, next) => {
  try {
    const { txids } = req.body;

    if (!Array.isArray(txids) || txids.length === 0) {
      return res.status(400).json({
        error: "txids array is required and cannot be empty",
        timestamp: new Date().toISOString()
      });
    }

    if (txids.length > 500) {
      return res.status(400).json({
        error: "Maximum 500 transaction IDs allowed per request",
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

    logger.info("Multiple raw transactions request", {
      count: txids.length,
      ip: req.ip,
    });

    const dbTransactions = await Transaction.find({ txid: { $in: txids } }).lean();
    const dbTxMap = new Map(dbTransactions.map(tx => [tx.txid, tx.raw]));
    
    const missingTxids = txids.filter(txid => !dbTxMap.has(txid));
    
    logger.debug(`Found ${dbTransactions.length} transactions in database, ${missingTxids.length} missing`);

    let rpcTransactions = [];
    if (missingTxids.length > 0) {
      rpcTransactions = await serviceManager.getRawTransactionsHex(missingTxids);
    }

    const rawTransactions = txids.map((txid, index) => {
      if (dbTxMap.has(txid)) {
        return dbTxMap.get(txid);
      } else {
        const rpcIndex = missingTxids.indexOf(txid);
        return (rpcIndex !== -1) ? rpcTransactions[rpcIndex] : null;
      }
    });

    const result = txids.map((txid, index) => ({
      txid,
      rawTransaction: rawTransactions[index] || null
    }));

    res.status(200).json({
      results: result,
      total: txids.length
    });
  } catch (error) {
    next(error);
  }
});

export default router;
