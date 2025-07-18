import dotenv from "dotenv";

import logger from "../config/logger.js";
import serviceManager from "./ServiceManager.js";
import redisService from "./RedisService.js";

dotenv.config();

class Cache {
    constructor() {
        this.cache = new Map();
        this.config = {
            mempoolCacheTTL: parseInt(process.env.MEMPOOL_CACHE_TTL) || 1000,
            mempoolInfoCacheTTL: parseInt(process.env.MEMPOOL_INFO_CACHE_TTL) || 1000,
            blockchainInfoCacheTTL: parseInt(process.env.BLOCKCHAIN_INFO_CACHE_TTL) || 5000,
            miningInfoCacheTTL: parseInt(process.env.MINING_INFO_CACHE_TTL) || 10000,
            chainTxStatsCacheTTL: parseInt(process.env.CHAIN_TX_STATS_CACHE_TTL) || 30000,
            networkInfoCacheTTL: parseInt(process.env.NETWORK_INFO_CACHE_TTL) || 10000,
            maxCacheSize: parseInt(process.env.RPC_MAX_CACHE_SIZE) || 100,
        };


        this.cleanupInterval = setInterval(() => {
            this.cleanup();
        }, 5000);

        logger.info('RpcCache initialized', {
            config: this.config
        });
    }

    generateCacheKey(method, params = []) {
        return `${method}:${JSON.stringify(params)}`;
    }

    getTTLForMethod(method) {
        switch (method) {
            case 'getRawMempool':
                return this.config.mempoolCacheTTL;
            case 'getMempoolInfo':
                return this.config.mempoolInfoCacheTTL;
            case 'getBlockchainInfo':
                return this.config.blockchainInfoCacheTTL;
            case 'getMiningInfo':
                return this.config.miningInfoCacheTTL;
            case 'getChainTxStats':
                return this.config.chainTxStatsCacheTTL;
            case 'getNetworkInfo':
                return this.config.networkInfoCacheTTL;
            default:
                return this.config.mempoolCacheTTL;
        }
    }

    get(method, params = []) {
        const key = this.generateCacheKey(method, params);
        const cached = this.cache.get(key);

        if (!cached) {
            return null;
        }

        const ttl = this.getTTLForMethod(method);
        const isExpired = Date.now() - cached.timestamp > ttl;

        if (isExpired) {
            this.cache.delete(key);
            return null;
        }

        cached.accessCount++;
        cached.lastAccess = Date.now();

        logger.debug(`RpcCache: Cache hit for ${method}`, {
            cacheKey: key,
            age: Date.now() - cached.timestamp,
            accessCount: cached.accessCount
        });

        return cached.data;
    }

    set(method, params = [], data) {
        const key = this.generateCacheKey(method, params);

        if (this.cache.size >= this.config.maxCacheSize) {
            this.evictOldest();
        }

        this.cache.set(key, {
            data,
            timestamp: Date.now(),
            lastAccess: Date.now(),
            accessCount: 1,
            method,
            params
        });

        logger.debug(`RpcCache: Cached result for ${method}`, {
            cacheKey: key,
            cacheSize: this.cache.size
        });
    }

    evictOldest() {
        let oldestKey = null;
        let oldestTime = Date.now();

        for (const [key, entry] of this.cache.entries()) {
            if (entry.lastAccess < oldestTime) {
                oldestTime = entry.lastAccess;
                oldestKey = key;
            }
        }

        if (oldestKey) {
            this.cache.delete(oldestKey);
            logger.debug('RpcCache: Evicted oldest cache entry', {
                evictedKey: oldestKey
            });
        }
    }

    cleanup() {
        const before = this.cache.size;
        const now = Date.now();

        for (const [key, entry] of this.cache.entries()) {
            const ttl = this.getTTLForMethod(entry.method);
            const isExpired = now - entry.timestamp > ttl;

            if (isExpired) {
                this.cache.delete(key);
            }
        }

        const after = this.cache.size;
        if (before !== after) {
            logger.debug('RpcCache: Cleanup completed', {
                before,
                after,
                removed: before - after
            });
        }
    }

    async getCachedOrFetch(method, params = []) {
        const cached = this.get(method, params);
        if (cached !== null) {
            return cached;
        }

        logger.debug(`RpcCache: Cache miss, fetching from service`, {
            method,
            params
        });

        let result;
        switch (method) {
            case 'getRawMempool':
                result = await this.getRawMempoolFromRedis();
                if (result === null) {
                    result = await serviceManager.getRawMempool();
                }
                break;
            case 'getMempoolFeeStats':
                result = await this.getMempoolFeeStatsFromRedis();
                break;
            case 'getMempoolInfo':
                result = await serviceManager.getMempoolInfo();
                break;
            case 'getBlockchainInfo':
                result = await serviceManager.getBlockchainInfo();
                break;
            case 'getMiningInfo':
                result = await serviceManager.getMiningInfo();
                break;
            case 'getChainTxStats':
                result = await serviceManager.getChainTxStats(params[0]);
                break;
            case 'getNetworkInfo':
                result = await serviceManager.getNetworkInfo();
                break;
            default:
                throw new Error(`Unsupported cache method: ${method}`);
        }

        this.set(method, params, result);

        return result;
    }

    async getRawMempoolFromRedis() {
        try {
            const txIds = await redisService.getJSON('mempool:tx:list');
            if (txIds && Array.isArray(txIds)) {
                logger.debug(`Cache: Retrieved ${txIds.length} mempool transaction IDs from Redis`);
                return txIds;
            }
            return null;
        } catch (error) {
            logger.error('Cache: Error getting raw mempool from Redis', {
                error: error.message
            });
            return null;
        }
    }

    async getMempoolFeeStatsFromRedis() {
        try {
            const feeStats = await redisService.getJSON('mempool:fee:stats');
            
            if (!feeStats) {
                return {
                    feeRanges: {
                        "0-0.0001": { count: 0, totalFee: 0 },
                        "0.0001-0.001": { count: 0, totalFee: 0 },
                        "0.001-0.01": { count: 0, totalFee: 0 },
                        "0.01-0.1": { count: 0, totalFee: 0 },
                        "0.1-1": { count: 0, totalFee: 0 },
                        ">1": { count: 0, totalFee: 0 }
                    },
                    totalTransactions: 0,
                    totalFees: 0
                };
            }

            const totalTransactions = Object.values(feeStats).reduce((sum, range) => sum + range.count, 0);
            const totalFees = Object.values(feeStats).reduce((sum, range) => sum + range.totalFee, 0);

            logger.debug(`Cache: Retrieved mempool fee statistics from Redis (${totalTransactions} transactions)`);

            return {
                feeRanges: feeStats,
                totalTransactions,
                totalFees
            };
        } catch (error) {
            logger.error('Cache: Error getting mempool fee stats from Redis', {
                error: error.message
            });
            return {
                feeRanges: {
                    "0-0.0001": { count: 0, totalFee: 0 },
                    "0.0001-0.001": { count: 0, totalFee: 0 },
                    "0.001-0.01": { count: 0, totalFee: 0 },
                    "0.01-0.1": { count: 0, totalFee: 0 },
                    "0.1-1": { count: 0, totalFee: 0 },
                    ">1": { count: 0, totalFee: 0 }
                },
                totalTransactions: 0,
                totalFees: 0
            };
        }
    }

    clear() {
        const size = this.cache.size;
        this.cache.clear();
        logger.info(`RpcCache: Cleared all cache entries`, {
            clearedCount: size
        });
    }

    shutdown() {
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
        }
        this.clear();
        logger.info('RpcCache: Shutdown completed');
    }
}

const cache = new Cache();

export default cache; 