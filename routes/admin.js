import express from "express";
import mongoose from "mongoose";

import rateLimiter from "../middleware/rateLimiter.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";
import { getRedisStats } from "../config/redis.js";
import serviceManager from "../services/ServiceManager.js";

const router = express.Router();

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

// router.use(localOnlyMiddleware);

router.get("/stats", async (req, res) => {
    try {
        logger.info("Admin stats request", { ip: getRealClientIP(req) });

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
            connected: dbState === 1,
            state: dbConnectionStates[dbState] || 'unknown',
            database: mongoose.connection.name,
            currentConnections: await getMongoPoolStats('current'),
            availableConnections: await getMongoPoolStats('available'),
            healthy: dbState === 1
        };

        const redisStats = getRedisStats();
        const serviceStatus = serviceManager.getServiceStatus();

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
            redis: {
                status: redisStats.status,
                activeConnections: redisStats.activeConnections,
                totalConnects: redisStats.connects,
                totalDisconnects: redisStats.disconnects,
                errors: redisStats.errors,
                lastConnectTime: redisStats.lastConnectTime,
                lastErrorTime: redisStats.lastErrorTime,
                healthy: redisStats.status === 'ready' && redisStats.errors < 10
            },
            services: {
                initialized: serviceStatus.initialized,
                rpcServices: serviceStatus.rpcServices,
                circuitBreaker: serviceStatus.circuitBreaker,
                electrumxPool: serviceStatus.electrumxPool
            },
            system: systemInfo
        });
    } catch (error) {
        logger.error('Admin stats error', { error: error.message, ip: getRealClientIP(req) });
        res.status(500).json({
            error: 'Internal server error',
            message: error.message
        });
    }
});

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
                    limit: `${rateLimiter.config[endpoint].max}/${rateLimiter.config[endpoint].windowMs / 1000}s`
                };
            } catch (error) {
                rateLimitStatus[endpoint] = {
                    error: error.message,
                    limit: `${rateLimiter.config[endpoint].max}/${rateLimiter.config[endpoint].windowMs / 1000}s`
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

router.delete("/ratelimit/:ip", async (req, res) => {
    try {
        const { ip } = req.params;
        const endpoints = ['global', 'address', 'transaction', 'rawTransaction', 'batchTransaction'];

        logger.info("Rate limit clear all request", {
            targetIp: ip,
            adminIp: getRealClientIP(req),
        });

        const results = {};
        let hasError = false;

        for (const endpoint of endpoints) {
            try {
                const success = await rateLimiter.clearLimits(ip, endpoint);
                results[endpoint] = success ? 'cleared' : 'failed';
                if (!success) hasError = true;
            } catch (error) {
                results[endpoint] = 'error';
                hasError = true;
                logger.error('Clear rate limit error for endpoint', {
                    ip,
                    endpoint,
                    error: error.message
                });
            }
        }

        if (hasError) {
            return res.status(500).json({
                error: "Failed to clear some rate limits",
                ip,
                results,
                timestamp: new Date().toISOString()
            });
        }

        res.json({
            message: "All rate limits cleared successfully",
            ip,
            results,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        logger.error('Clear all rate limits error', { error: error.message, ip: req.params.ip });
        res.status(500).json({
            error: 'Internal server error',
            message: error.message
        });
    }
});

export default router; 