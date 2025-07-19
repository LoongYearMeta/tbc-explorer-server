import express from "express";

import generalRpcAggregator from "../services/GeneralRpcAggregator.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";

const router = express.Router();

router.get("/info", async (req, res, next) => {
  try {
    logger.info("Mempool info request", {
      ip: getRealClientIP(req),
    });

    const mempoolInfo = await generalRpcAggregator.callRpc('getMempoolInfo');

    res.status(200).json({
      mempoolInfo
    });
  } catch (error) {
    next(error);
  }
});

router.get("/", async (req, res, next) => {
  try {
    logger.info("Mempool transaction IDs request", {
      ip: getRealClientIP(req),
    });

    const rawMempool = await generalRpcAggregator.callRpc('getRawMempool');

    res.status(200).json({
      txids: rawMempool || [],
      count: Array.isArray(rawMempool) ? rawMempool.length : 0,
    });
  } catch (error) {
    next(error);
  }
});

router.get("/stats", async (req, res, next) => {
  try {
    logger.info("Mempool fee stats request", {
      ip: getRealClientIP(req),
    });

    const feeStats = await generalRpcAggregator.callRpc('getMempoolFeeStats');

    res.status(200).json(feeStats);
  } catch (error) {
    next(error);
  }
});

export default router;
