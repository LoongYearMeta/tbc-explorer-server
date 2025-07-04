import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";

const router = express.Router();

router.get("/info", async (req, res, next) => {
  try {
    logger.info("Mempool info request", {
      ip: req.ip,
    });

    const mempoolInfo = await serviceManager.getMempoolInfo();

    res.status(200).json({
      mempoolInfo
    });
  } catch (error) {
    next(error);
  }
});

router.get("/", async (req, res, next) => {
  try {
    logger.info("Raw mempool request", {
      ip: req.ip,
    });

    const rawMempool = await serviceManager.getRawMempool();

    res.status(200).json({
      txids: rawMempool || [],
      count: Array.isArray(rawMempool) ? rawMempool.length : 0,
    });
  } catch (error) {
    next(error);
  }
});

export default router;
