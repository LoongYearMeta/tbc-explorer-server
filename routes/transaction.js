import express from "express";

import serviceManager from "../services/ServiceManager.js";
import transactionAggregator from "../services/TransactionAggregator.js";
import redisService from "../services/RedisService.js";
import logger from "../config/logger.js";
import { Transaction } from "../models/transaction.js";
import { getRealClientIP } from "../lib/util.js";
import { transactionRateLimit, rawTransactionRateLimit, batchTransactionRateLimit } from "../middleware/rateLimiter.js";

const router = express.Router();

router.get("/:txid", transactionRateLimit, async (req, res, next) => {
  try {
    const { txid } = req.params;

    if (!/^[a-fA-F0-9]{64}$/.test(txid)) {
      return res.status(400).json({
        error: "Invalid transaction ID format"
      });
    }

    logger.info("Transaction request", {
      txid,
      ip: getRealClientIP(req),
    });
    
    const transaction = await transactionAggregator.getRawTransaction(txid);

    if (!transaction) {
      return res.status(404).json({
        error: "Transaction not found",
        txid
      });
    }

    res.status(200).json({
      transaction
    });
  } catch (error) {
    next(error);
  }
});

router.get("/:txid/raw", rawTransactionRateLimit, async (req, res, next) => {
  try {
    const { txid } = req.params;

    if (!/^[a-fA-F0-9]{64}$/.test(txid)) {
      return res.status(400).json({
        error: "Invalid transaction ID format"
      });
    }

    logger.info("Raw transaction request", {
      txid,
      ip: getRealClientIP(req),
    });

    let rawTransaction = "";

    try {
      const cacheKey = `mempool:tx:${txid}`;
      const cachedTx = await redisService.getJSON(cacheKey);
      if (cachedTx && cachedTx.raw) {
        rawTransaction = cachedTx.raw;
        logger.debug(`Raw transaction ${txid} found in Redis cache`);
      }
    } catch (error) {
      logger.warn(`Redis lookup failed for transaction ${txid}`, { error: error.message });
    }

    if (!rawTransaction) {
      const dbTransaction = await Transaction.findOne({ txid }).lean();
      if (dbTransaction) {
        rawTransaction = dbTransaction.raw;
        logger.debug(`Raw transaction ${txid} found in database`);
      } else {
        logger.debug(`Raw transaction ${txid} not found in database, fetching from RPC`);
        rawTransaction = await serviceManager.getRawTransactionHex(txid);
        if (rawTransaction) {
          logger.debug(`Raw transaction ${txid} fetched from RPC`);
        }
      }
    }

    if (!rawTransaction) {
      return res.status(404).json({
        error: "Transaction not found",
        txid,
      });
    }

    res.status(200).json({
      txid,
      rawTransaction,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/batch/raw", batchTransactionRateLimit, async (req, res, next) => {
  try {
    const { txids } = req.body;

    if (!Array.isArray(txids) || txids.length === 0) {
      return res.status(400).json({
        error: "txids array is required and cannot be empty"
      });
    }

    if (txids.length > 500) {
      return res.status(400).json({
        error: "Maximum 500 transaction IDs allowed per request"
      });
    }

    const invalidTxids = txids.filter(txid => !/^[a-fA-F0-9]{64}$/.test(txid));
    if (invalidTxids.length > 0) {
      return res.status(400).json({
        error: "Invalid transaction ID format",
        invalidTxids
      });
    }

    logger.info("Multiple raw transactions request", {
      count: txids.length,
      ip: getRealClientIP(req),
    });

    const redisTxMap = new Map();
    try {
      const redisPromises = txids.map(txid =>
        redisService.getJSON(`mempool:tx:${txid}`).catch(err => {
          logger.debug(`Redis lookup failed for ${txid}:`, err.message);
          return null;
        })
      );
      const redisResults = await Promise.all(redisPromises);

      redisResults.forEach((cachedTx, index) => {
        if (cachedTx && cachedTx.raw) {
          redisTxMap.set(txids[index], cachedTx.raw);
        }
      });

      logger.debug(`Found ${redisTxMap.size} transactions in Redis cache`);
    } catch (error) {
      logger.warn('Redis batch lookup failed', { error: error.message });
    }

    const redisNotFoundTxids = txids.filter(txid => !redisTxMap.has(txid));
    let dbTxMap = new Map();

    if (redisNotFoundTxids.length > 0) {
      const dbTransactions = await Transaction.find({ txid: { $in: redisNotFoundTxids } }).lean();
      dbTxMap = new Map(dbTransactions.map(tx => [tx.txid, tx.raw]));
      logger.debug(`Found ${dbTransactions.length} transactions in database`);
    }

    const dbNotFoundTxids = redisNotFoundTxids.filter(txid => !dbTxMap.has(txid));
    let rpcTxMap = new Map();

    if (dbNotFoundTxids.length > 0) {
      const rpcTransactions = await serviceManager.getRawTransactionsHex(dbNotFoundTxids);
      rpcTransactions.forEach((rawTx, index) => {
        if (rawTx) {
          rpcTxMap.set(dbNotFoundTxids[index], rawTx);
        }
      });
      logger.debug(`Found ${rpcTxMap.size} transactions from RPC`);
    }

    const rawTransactions = txids.map(txid => {
      return redisTxMap.get(txid) || dbTxMap.get(txid) || rpcTxMap.get(txid) || null;
    });

    const result = txids.map((txid, index) => ({
      txid,
      rawTransaction: rawTransactions[index] || null
    }));

    logger.info("Batch transaction lookup completed", {
      total: txids.length,
      redisHits: redisTxMap.size,
      dbHits: dbTxMap.size,
      rpcHits: rpcTxMap.size
    });

    res.status(200).json({
      results: result,
      total: txids.length
    });
  } catch (error) {
    next(error);
  }
});

export default router;
