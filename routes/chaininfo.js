import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";

const router = express.Router();

router.get("/", async (req, res, next) => {
  try {
    logger.info("Chain status request", {
      ip: req.ip,
    });

    const [blockchainInfo, miningInfo] = await Promise.all([
      serviceManager.getBlockchainInfo(),
      serviceManager.getMiningInfo()
    ]);

    res.status(200).json({
      blockchain: blockchainInfo,
      mining: miningInfo,
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
        error: "Block count must be a positive integer"
      });
    }

    logger.info("Chain transaction stats request", {
      blockCount,
      ip: req.ip,
    });

    const txStats = await serviceManager.getChainTxStats(blockCount);

    res.status(200).json({
      txStats,
      blockCount: blockCount || "default"
    });
  } catch (error) {
    next(error);
  }
});

export default router;
