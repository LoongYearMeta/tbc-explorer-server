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
    
    res.json({
      mempoolInfo,
      timestamp: new Date().toISOString()
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
    
    res.json({
      mempool: rawMempool,
      count: Object.keys(rawMempool || {}).length,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/count", async (req, res, next) => {
  try {
    logger.info("Mempool count request", {
      ip: req.ip,
    });

    const mempoolInfo = await serviceManager.getMempoolInfo();
    
    res.json({
      count: mempoolInfo?.size || 0,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

export default router;
