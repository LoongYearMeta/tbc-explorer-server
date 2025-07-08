import dotenv from "dotenv";

import logger from "../config/logger.js";
import serviceManager from "./ServiceManager.js";

dotenv.config();

class MempoolCache {
    constructor() {
        this.cache = new Map();
        this.config = {
            mempoolCacheTTL: parseInt(process.env.MEMPOOL_CACHE_TTL) || 1000,
            mempoolInfoCacheTTL: parseInt(process.env.MEMPOOL_INFO_CACHE_TTL) || 1000,
            blockchainInfoCacheTTL: parseInt(process.env.BLOCKCHAIN_INFO_CACHE_TTL) || 5000,
            miningInfoCacheTTL: parseInt(process.env.MINING_INFO_CACHE_TTL) || 10000,
            chainTxStatsCacheTTL: parseInt(process.env.CHAIN_TX_STATS_CACHE_TTL) || 30000,
            networkInfoCacheTTL: parseInt(process.env.NETWORK_INFO_CACHE_TTL) || 10000,
            netTotalsCacheTTL: parseInt(process.env.NET_TOTALS_CACHE_TTL) || 5000,
            uptimeCacheTTL: parseInt(process.env.UPTIME_CACHE_TTL) || 30000,
            peerInfoCacheTTL: parseInt(process.env.PEER_INFO_CACHE_TTL) || 10000,
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
            case 'getNetTotals':
                return this.config.netTotalsCacheTTL;
            case 'getUptimeSeconds':
                return this.config.uptimeCacheTTL;
            case 'getPeerInfo':
                return this.config.peerInfoCacheTTL;
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
                result = await serviceManager.getRawMempool();
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
            case 'getNetTotals':
                result = await serviceManager.getNetTotals();
                break;
            case 'getUptimeSeconds':
                result = await serviceManager.getUptimeSeconds();
                break;
            case 'getPeerInfo':
                result = await serviceManager.getPeerInfo();
                break;
            default:
                throw new Error(`Unsupported cache method: ${method}`);
        }

        this.set(method, params, result);

        return result;
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

const mempoolCache = new MempoolCache();

export default mempoolCache; 