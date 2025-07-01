import express from "express";
import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";

const router = express.Router();

router.get("/", async (req, res, next) => {
  try {
    logger.info("Blockchain info request", {
      ip: req.ip,
    });

    const blockchainInfo = await serviceManager.getBlockchainInfo();
    
    res.json({
      blockchainInfo,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/mining", async (req, res, next) => {
  try {
    logger.info("Mining info request", {
      ip: req.ip,
    });

    const miningInfo = await serviceManager.getMiningInfo();
    
    res.json({
      miningInfo,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/txstats/:blockCount?", async (req, res, next) => {
  try {
    const blockCount = req.params.blockCount ? parseInt(req.params.blockCount) : undefined;
    
    if (blockCount !== undefined && (isNaN(blockCount) || blockCount <= 0)) {
      return res.status(400).json({
        error: "Block count must be a positive integer",
        timestamp: new Date().toISOString()
      });
    }
    
    logger.info("Chain transaction stats request", {
      blockCount,
      ip: req.ip,
    });

    const txStats = await serviceManager.getChainTxStats(blockCount);
    
    res.json({
      txStats,
      blockCount: blockCount || "default",
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/status", async (req, res, next) => {
  try {
    logger.info("Chain status request", {
      ip: req.ip,
    });

    const [blockchainInfo, miningInfo] = await Promise.all([
      serviceManager.getBlockchainInfo(),
      serviceManager.getMiningInfo()
    ]);
    
    res.json({
      blockchain: blockchainInfo,
      mining: miningInfo,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

export default router;
