import express from "express";
import rateLimiter from "../middleware/rateLimiter.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";
import { getRedisStats } from "../config/redis.js";
import mongoose from "mongoose";

const router = express.Router();

// 本地访问限制中间件
const localOnlyMiddleware = (req, res, next) => {
  const ip = getRealClientIP(req);
  const isLocal = ip === '127.0.0.1' || ip === '::1' || ip === '::ffff:127.0.0.1' || ip === 'localhost';
  
  if (!isLocal) {
    logger.warn('Admin access denied for non-local IP', { ip, url: req.originalUrl });
    return res.status(403).json({
      error: 'Forbidden',
      message: 'Admin endpoints are only accessible from localhost'
    });
  }
  next();
};

// 应用本地访问限制到所有admin路由
router.use(localOnlyMiddleware);

// 获取详细统计数据
router.get("/stats", async (req, res) => {
  try {
    logger.info("Admin stats request", { ip: getRealClientIP(req) });

    // MongoDB连接状态
    async function getMongoPoolStats(type) {
      try {
        if (mongoose.connection.readyState !== 1) {
          return 'disconnected';
        }
        const db = mongoose.connection.db;
        if (!db) return 'no-db';
        
        const serverStatus = await db.admin().serverStatus();
        const connections = serverStatus.connections;
        
        if (type === 'current') return connections.current || 0;
        if (type === 'available') return connections.available || 0;
        return 'unknown';
      } catch (error) {
        logger.debug('Failed to get MongoDB pool stats', { error: error.message });
        return 'error';
      }
    }

    const dbConnectionStates = {
      0: 'disconnected',
      1: 'connected', 
      2: 'connecting',
      3: 'disconnecting'
    };

    const dbState = mongoose.connection.readyState;
    const dbStatus = {
      state: dbConnectionStates[dbState] || 'unknown',
      connected: dbState === 1,
      host: mongoose.connection.host,
      name: mongoose.connection.name,
      readyState: dbState,
      poolInfo: {
        maxPoolSize: parseInt(process.env.MONGO_MAX_POOL_SIZE) || 1500,
        minPoolSize: parseInt(process.env.MONGO_MIN_POOL_SIZE) || 50,
        maxIdleTimeMS: parseInt(process.env.MONGO_MAX_IDLE_TIME) || 30000,
        serverSelectionTimeoutMS: parseInt(process.env.MONGO_SERVER_SELECTION_TIMEOUT) || 5000,
        heartbeatFrequencyMS: parseInt(process.env.MONGO_HEARTBEAT_FREQUENCY) || 10000,
        currentConnections: await getMongoPoolStats('current'),
        availableConnections: await getMongoPoolStats('available')
      }
    };

    // Redis状态
    const redisStats = getRedisStats();
    
    // 内存使用情况
    const memoryUsage = process.memoryUsage();
    const systemInfo = {
      pid: process.pid,
      uptime: process.uptime(),
      nodeVersion: process.version,
      memory: {
        rss: Math.round(memoryUsage.rss / 1024 / 1024) + 'MB',
        heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024) + 'MB',
        heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024) + 'MB',
        external: Math.round(memoryUsage.external / 1024 / 1024) + 'MB',
        heapUsagePercent: Math.round((memoryUsage.heapUsed / memoryUsage.heapTotal) * 100)
      },
      cpuUsage: process.cpuUsage()
    };

    res.json({
      timestamp: new Date().toISOString(),
      database: dbStatus,
      redis: redisStats,
      system: systemInfo,
      rateLimiter: {
        algorithms: {
          global: 'sliding-window',
          address: 'sliding-window',
          transaction: 'sliding-window',
          rawTransaction: 'sliding-window',
          batchTransaction: 'sliding-window'
        },
        configs: rateLimiter.config
      }
    });
  } catch (error) {
    logger.error('Admin stats error', { error: error.message, ip: getRealClientIP(req) });
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// 获取特定IP的限流状态
router.get("/ratelimit/:ip", async (req, res) => {
  try {
    const { ip } = req.params;
    const { endpoint = 'global' } = req.query;
    
    logger.info("Rate limit status request", {
      targetIp: ip,
      endpoint,
      adminIp: getRealClientIP(req),
    });

    const status = await rateLimiter.getStatus(ip, endpoint);
    
    if (!status) {
      return res.status(404).json({
        error: "Rate limit status not found",
        ip,
        endpoint
      });
    }

    res.json({
      ip,
      endpoint,
      status,
      config: rateLimiter.config[endpoint] || rateLimiter.config.global
    });
  } catch (error) {
    logger.error('Get rate limit status error', { error: error.message, ip: req.params.ip });
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// 获取多个端点的限流状态
router.get("/ratelimit/:ip/all", async (req, res) => {
  try {
    const { ip } = req.params;
    const endpoints = ['global', 'address', 'transaction', 'rawTransaction', 'batchTransaction'];
    
    logger.info("Rate limit all status request", {
      targetIp: ip,
      adminIp: getRealClientIP(req),
    });

    const rateLimitStatus = {};
    
    for (const endpoint of endpoints) {
      try {
        const status = await rateLimiter.getStatus(ip, endpoint);
        rateLimitStatus[endpoint] = {
          ...status,
          config: rateLimiter.config[endpoint] || rateLimiter.config.global
        };
      } catch (error) {
        rateLimitStatus[endpoint] = {
          error: error.message,
          config: rateLimiter.config[endpoint] || rateLimiter.config.global
        };
      }
    }

    res.json({
      ip,
      status: rateLimitStatus,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Get all rate limit status error', { error: error.message, ip: req.params.ip });
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// 清除特定IP的限流记录
router.delete("/ratelimit/:ip", async (req, res) => {
  try {
    const { ip } = req.params;
    const { endpoint = 'global' } = req.query;
    
    logger.info("Rate limit clear request", {
      targetIp: ip,
      endpoint,
      adminIp: getRealClientIP(req),
    });

    const success = await rateLimiter.clearLimits(ip, endpoint);
    
    if (!success) {
      return res.status(500).json({
        error: "Failed to clear rate limit",
        ip,
        endpoint
      });
    }

    res.json({
      message: "Rate limit cleared successfully",
      ip,
      endpoint,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    logger.error('Clear rate limit error', { error: error.message, ip: req.params.ip });
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// 获取所有限流配置
router.get("/ratelimit/config", async (req, res) => {
  try {
    logger.info("Rate limit config request", {
      adminIp: getRealClientIP(req),
    });

    res.json({
      configs: rateLimiter.config,
      endpoints: Object.keys(rateLimiter.config),
      algorithms: {
        global: 'sliding-window',
        address: 'sliding-window',
        transaction: 'sliding-window',
        rawTransaction: 'sliding-window',
        batchTransaction: 'sliding-window'
      }
    });
  } catch (error) {
    logger.error('Get rate limit config error', { error: error.message });
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

export default router; 