import express from "express";

import generalRpcAggregator from "../services/GeneralRpcAggregator.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";

const router = express.Router();

router.get("/info", async (req, res, next) => {
  try {
    logger.info("Network info request", {
      ip: getRealClientIP(req),
    });

    const networkInfo = await generalRpcAggregator.callRpc('getNetworkInfo');

    res.status(200).json({
      networkInfo,
    });
  } catch (error) {
    next(error);
  }
});

export default router; 