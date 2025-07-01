import express from "express";
import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";

const router = express.Router();

router.get("/height/:height", async (req, res, next) => {
  try {
    const height = parseInt(req.params.height);
    
    if (isNaN(height) || height < 0) {
      return res.status(400).json({
        error: "Invalid block height",
        timestamp: new Date().toISOString()
      });
    }
    
    logger.info("Block by height request", {
      height,
      ip: req.ip,
    });

    const block = await serviceManager.getBlockByHeight(height);
    
    if (!block) {
      return res.status(404).json({
        error: "Block not found",
        height,
        timestamp: new Date().toISOString()
      });
    }
    
    res.json({
      block,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/hash/:hash", async (req, res, next) => {
  try {
    const { hash } = req.params;
    
    logger.info("Block by hash request", {
      hash,
      ip: req.ip,
    });

    const block = await serviceManager.getBlockByHash(hash);
    
    if (!block) {
      return res.status(404).json({
        error: "Block not found",
        hash,
        timestamp: new Date().toISOString()
      });
    }
    
    res.json({
      block,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.post("/heights", async (req, res, next) => {
  try {
    const { heights } = req.body;
    
    if (!Array.isArray(heights) || heights.length === 0) {
      return res.status(400).json({
        error: "heights array is required and cannot be empty",
        timestamp: new Date().toISOString()
      });
    }

    const validHeights = heights.filter(h => Number.isInteger(h) && h >= 0);
    if (validHeights.length !== heights.length) {
      return res.status(400).json({
        error: "All heights must be non-negative integers",
        timestamp: new Date().toISOString()
      });
    }

    logger.info("Multiple blocks by heights request", {
      count: heights.length,
      ip: req.ip,
    });

    const blocks = await serviceManager.getBlocksByHeight(heights);
    
    res.json({
      blocks,
      total: blocks.length,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

export default router;
