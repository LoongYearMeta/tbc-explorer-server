import dotenv from "dotenv";

import { connectDB, disconnectDB } from "../config/db.js";
import { connectRedis, disconnectRedis } from "../config/redis.js";
import serviceManager from "../services/ServiceManager.js";
import redisService from "../services/RedisService.js";
import logger from "../config/logger.js";
import { Block } from "../models/block.js";

dotenv.config();

class RedisCacheWorker {
    constructor() {
        this.maxRecentBlocks = 11;
    }

    async initialize() {
        try {
            logger.info("RedisCacheWorker: Initializing...");
            await connectDB();
            await connectRedis();
            await serviceManager.initialize();

            logger.info("RedisCacheWorker: Initialization completed");
            await this.start();
        } catch (error) {
            logger.error("RedisCacheWorker: Initialization failed", {
                error: error.message,
                stack: error.stack,
            });
            process.exit(1);
        }
    }

    async start() {
        logger.info("RedisCacheWorker: Starting cache worker");

        try {
            await this.clearAllCache();

            const [blocksCached, mempoolTxCached] = await Promise.all([
                this.cacheRecentBlocks(),
                this.cacheMempoolTransactions()
            ]);

            logger.info("RedisCacheWorker: Cache worker completed successfully", {
                blocksCached,
                mempoolTxCached,
                totalCached: blocksCached + mempoolTxCached
            });

        } catch (error) {
            logger.error("RedisCacheWorker: Cache worker failed", {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }

        setTimeout(() => {
            process.exit(0);
        }, 2000);
    }

    async clearAllCache() {
        try {
            logger.info("RedisCacheWorker: Clearing all existing cache");

            const blockPattern = 'tbc-explorer:blocks:recent:*';
            const blockKeys = await redisService.exec('KEYS', blockPattern);
            if (blockKeys && blockKeys.length > 0) {
                await redisService.exec('DEL', ...blockKeys);
                logger.debug(`RedisCacheWorker: Cleared ${blockKeys.length} block cache entries`);
            }
            const mempoolPattern = 'tbc-explorer:mempool:tx:*';
            const mempoolKeys = await redisService.exec('KEYS', mempoolPattern);
            if (mempoolKeys && mempoolKeys.length > 0) {
                await redisService.exec('DEL', ...mempoolKeys);
                logger.debug(`RedisCacheWorker: Cleared ${mempoolKeys.length} mempool transaction cache entries`);
            }
            await redisService.del('blocks:recent:queue');
            
            await redisService.del('mempool:fee:stats');
            logger.debug(`RedisCacheWorker: Cleared mempool fee statistics`);

            await redisService.del('mempool:tx:list');
            logger.debug(`RedisCacheWorker: Cleared mempool transaction IDs list`);

            logger.info("RedisCacheWorker: Cache clearing completed");

        } catch (error) {
            logger.error("RedisCacheWorker: Error clearing cache", {
                error: error.message,
                stack: error.stack
            });
        }
    }

    async cacheRecentBlocks() {
        logger.info("RedisCacheWorker: Caching recent blocks");
        const blockchainInfo = await serviceManager.getBlockchainInfo();
        const latestHeight = blockchainInfo.blocks;
        const startHeight = Math.max(0, latestHeight - this.maxRecentBlocks + 1);
        const heights = [];
        for (let h = startHeight; h <= latestHeight; h++) {
            heights.push(h);
        }

        logger.debug(`RedisCacheWorker: Caching blocks from ${startHeight} to ${latestHeight}`);

        const blocks = await Block.find({ height: { $in: heights } }).lean();
        const blockMap = new Map(blocks.map(b => [b.height, b]));

        const dbMissingHeights = heights.filter(h => !blockMap.has(h));
        if (dbMissingHeights.length > 0) {
            logger.debug(`RedisCacheWorker: Fetching ${dbMissingHeights.length} blocks from RPC`);
            const rpcBlocks = await serviceManager.getBlocksByHeight(dbMissingHeights);

            for (const block of rpcBlocks) {
                if (block) {
                    blockMap.set(block.height, block);
                }
            }
        }

        const sortedHeights = heights.sort((a, b) => a - b);
        let cachedCount = 0;

        for (const height of sortedHeights) {
            const block = blockMap.get(height);
            if (block) {
                const cacheKey = `blocks:recent:${height}`;
                await redisService.setJSON(cacheKey, block);
                await redisService.rpush('blocks:recent:queue', height);
                cachedCount++;
            }
        }

        await this.trimBlockQueue();

        logger.debug(`RedisCacheWorker: Cached ${cachedCount} blocks to Redis`);
        return cachedCount;
    }

    async cacheMempoolTransactions() {
        logger.info("RedisCacheWorker: Caching mempool transactions");
        const mempoolTxIds = await serviceManager.getRawMempool();

        if (!mempoolTxIds || mempoolTxIds.length === 0) {
            logger.debug("RedisCacheWorker: No transactions in mempool");
            await this.clearMempoolFeeStats();
            return 0;
        }

        logger.debug(`RedisCacheWorker: Found ${mempoolTxIds.length} transactions in mempool`);

        const BATCH_SIZE = 100;
        let cachedCount = 0;
        const feeStats = {
            "0-0.0001": { count: 0, totalFee: 0 },
            "0.0001-0.001": { count: 0, totalFee: 0 },
            "0.001-0.01": { count: 0, totalFee: 0 },
            "0.01-0.1": { count: 0, totalFee: 0 },
            "0.1-1": { count: 0, totalFee: 0 },
            ">1": { count: 0, totalFee: 0 }
        };

        for (let i = 0; i < mempoolTxIds.length; i += BATCH_SIZE) {
            const batch = mempoolTxIds.slice(i, i + BATCH_SIZE);

            try {
                const [rawTxs, mempoolEntries] = await Promise.all([
                    serviceManager.getRawTransactionsHex(batch),
                    serviceManager.getMempoolEntries(batch)
                ]);

                for (let j = 0; j < batch.length; j++) {
                    const txid = batch[j];
                    const raw = rawTxs[j];
                    const mempoolEntry = mempoolEntries[j];

                    if (raw && mempoolEntry) {
                        const cacheKey = `mempool:tx:${txid}`;
                        const txData = {
                            txid: txid,
                            raw: raw,
                            fee: mempoolEntry.fee,
                            size: mempoolEntry.size,
                            time: mempoolEntry.time,
                            height: mempoolEntry.height,
                            descendantcount: mempoolEntry.descendantcount,
                            descendantsize: mempoolEntry.descendantsize,
                            descendantfees: mempoolEntry.descendantfees,
                            ancestorcount: mempoolEntry.ancestorcount,
                            ancestorsize: mempoolEntry.ancestorsize,
                            ancestorfees: mempoolEntry.ancestorfees
                        };
                        await redisService.setJSON(cacheKey, txData);
                        cachedCount++;

                        this.updateFeeStats(feeStats, mempoolEntry.fee);
                    } else {
                        if (!raw) {
                            logger.warn(`RedisCacheWorker: Failed to get raw transaction for txid: ${txid}`);
                        }
                        if (!mempoolEntry) {
                            logger.warn(`RedisCacheWorker: Failed to get mempool entry for txid: ${txid}`);
                        }
                    }
                }

                logger.debug(`RedisCacheWorker: Cached batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} transactions)`);

            } catch (error) {
                logger.error(`RedisCacheWorker: Error caching mempool transaction batch`, {
                    error: error.message,
                    batchStart: i,
                    batchSize: batch.length
                });
            }
        }

        await this.updateMempoolFeeStats(feeStats);

        await this.updateMempoolTxIds(mempoolTxIds);

        logger.debug(`RedisCacheWorker: Cached ${cachedCount} mempool transactions to Redis`);
        return cachedCount;
    }

    updateFeeStats(feeStats, fee) {
        if (fee < 0.0001) {
            feeStats["0-0.0001"].count++;
            feeStats["0-0.0001"].totalFee += fee;
        } else if (fee < 0.001) {
            feeStats["0.0001-0.001"].count++;
            feeStats["0.0001-0.001"].totalFee += fee;
        } else if (fee < 0.01) {
            feeStats["0.001-0.01"].count++;
            feeStats["0.001-0.01"].totalFee += fee;
        } else if (fee < 0.1) {
            feeStats["0.01-0.1"].count++;
            feeStats["0.01-0.1"].totalFee += fee;
        } else if (fee < 1) {
            feeStats["0.1-1"].count++;
            feeStats["0.1-1"].totalFee += fee;
        } else {
            feeStats[">1"].count++;
            feeStats[">1"].totalFee += fee;
        }
    }

    async updateMempoolFeeStats(feeStats) {
        try {
            const cacheKey = 'mempool:fee:stats';
            await redisService.setJSON(cacheKey, feeStats);
            logger.debug("RedisCacheWorker: Updated mempool fee statistics in Redis");
        } catch (error) {
            logger.error("RedisCacheWorker: Error updating mempool fee statistics", {
                error: error.message,
                stack: error.stack
            });
        }
    }

    async clearMempoolFeeStats() {
        try {
            const cacheKey = 'mempool:fee:stats';
            await redisService.del(cacheKey);
            logger.debug("RedisCacheWorker: Cleared mempool fee statistics from Redis");
            
            const txListKey = 'mempool:tx:list';
            await redisService.del(txListKey);
            logger.debug("RedisCacheWorker: Cleared mempool transaction IDs list from Redis");
        } catch (error) {
            logger.error("RedisCacheWorker: Error clearing mempool fee statistics", {
                error: error.message,
                stack: error.stack
            });
        }
    }

    async trimBlockQueue() {
        try {
            const queueLength = await redisService.llen('blocks:recent:queue');

            if (queueLength > this.maxRecentBlocks) {
                const toRemove = queueLength - this.maxRecentBlocks;
                for (let i = 0; i < toRemove; i++) {
                    const oldHeight = await redisService.lpop('blocks:recent:queue');
                    if (oldHeight) {
                        const oldCacheKey = `blocks:recent:${oldHeight}`;
                        await redisService.del(oldCacheKey);
                        logger.debug(`RedisCacheWorker: Removed old block ${oldHeight} from queue and cache`);
                    }
                }

                logger.debug(`RedisCacheWorker: Trimmed ${toRemove} old blocks from queue`);
            }
        } catch (error) {
            logger.error("RedisCacheWorker: Error trimming block queue", {
                error: error.message,
                stack: error.stack
            });
        }
    }

    async updateMempoolTxIds(txIds) {
        try {
            const cacheKey = 'mempool:tx:list';
            await redisService.setJSON(cacheKey, txIds || []);
            logger.debug(`RedisCacheWorker: Updated mempool transaction IDs list (${txIds ? txIds.length : 0} transactions)`);
        } catch (error) {
            logger.error("RedisCacheWorker: Error updating mempool transaction IDs list", {
                error: error.message,
                stack: error.stack
            });
        }
    }

    async cleanup() {
        logger.info('RedisCacheWorker: Starting cleanup process...');

        try {
            if (serviceManager) {
                await serviceManager.shutdown();
            }

            await disconnectRedis();
            await disconnectDB();

            logger.info('RedisCacheWorker: Cleanup completed successfully');
        } catch (error) {
            logger.error('RedisCacheWorker: Error during cleanup', {
                error: error.message,
                stack: error.stack
            });
        }
    }
}

const worker = new RedisCacheWorker();

process.on('SIGTERM', async () => {
    logger.info('RedisCacheWorker: Received SIGTERM signal');
    await worker.cleanup();
    process.exit(0);
});

process.on('SIGINT', async () => {
    logger.info('RedisCacheWorker: Received SIGINT signal');
    await worker.cleanup();
    process.exit(0);
});

process.on('uncaughtException', async (error) => {
    logger.error('RedisCacheWorker: Uncaught exception', {
        error: error.message,
        stack: error.stack
    });
    await worker.cleanup();
    process.exit(1);
});

process.on('unhandledRejection', async (reason, promise) => {
    logger.error('RedisCacheWorker: Unhandled promise rejection', {
        reason: reason,
        promise: promise
    });
    await worker.cleanup();
    process.exit(1);
});

worker.initialize().catch(async (error) => {
    logger.error('RedisCacheWorker: Failed to initialize', {
        error: error.message,
        stack: error.stack
    });
    await worker.cleanup();
    process.exit(1);
}); 