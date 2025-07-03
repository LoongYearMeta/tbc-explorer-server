import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";

const router = express.Router();

router.get("/status", async (req, res) => {
  try {
    const zmqService = serviceManager.getZeroMQService();
    const status = zmqService.getStatus();
    
    logger.info("ZeroMQ status request", {
      ip: req.ip,
      status: status.running
    });

    res.status(200).json({
      success: true,
      data: status,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error("Error getting ZeroMQ status", {
      error: error.message,
      stack: error.stack
    });
    
    res.status(500).json({
      success: false,
      error: "Failed to get ZeroMQ status",
      timestamp: new Date().toISOString()
    });
  }
});

export default router; 