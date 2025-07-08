import express from "express";

import generalRpcAggregator from "../services/GeneralRpcAggregator.js";
import logger from "../config/logger.js";

const router = express.Router();

// Get network information
router.get("/info", async (req, res, next) => {
  try {
    logger.info("Network info request", {
      ip: req.ip,
    });

    const networkInfo = await generalRpcAggregator.callRpc('getNetworkInfo');

    res.status(200).json({
      networkInfo,
    });
  } catch (error) {
    next(error);
  }
});

// Get network totals
router.get("/totals", async (req, res, next) => {
  try {
    logger.info("Network totals request", {
      ip: req.ip,
    });

    const netTotals = await generalRpcAggregator.callRpc('getNetTotals');

    res.status(200).json({
      netTotals,
    });
  } catch (error) {
    next(error);
  }
});

// Get uptime
router.get("/uptime", async (req, res, next) => {
  try {
    logger.info("Network uptime request", {
      ip: req.ip,
    });

    const uptimeSeconds = await generalRpcAggregator.callRpc('getUptimeSeconds');

    res.status(200).json({
      uptimeSeconds,
    });
  } catch (error) {
    next(error);
  }
});

// Get peer information
router.get("/peers", async (req, res, next) => {
  try {
    logger.info("Network peers request", {
      ip: req.ip,
    });

    const peerInfo = await generalRpcAggregator.callRpc('getPeerInfo');

    res.status(200).json({
      peerInfo,
    });
  } catch (error) {
    next(error);
  }
});

export default router; 