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
        this.maxRecentBlocks = 10;
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
            // 清空所有相关缓存
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

            // 清空区块缓存（必须使用完整前缀）
            const blockPattern = 'tbc-explorer:blocks:recent:*';
            const blockKeys = await redisService.exec('KEYS', blockPattern);
            if (blockKeys && blockKeys.length > 0) {
                await redisService.exec('DEL', ...blockKeys);
                logger.debug(`RedisCacheWorker: Cleared ${blockKeys.length} block cache entries`);
            }

            // 清空内存池交易缓存（必须使用完整前缀）
            const mempoolPattern = 'tbc-explorer:mempool:tx:*';
            const mempoolKeys = await redisService.exec('KEYS', mempoolPattern);
            if (mempoolKeys && mempoolKeys.length > 0) {
                await redisService.exec('DEL', ...mempoolKeys);
                logger.debug(`RedisCacheWorker: Cleared ${mempoolKeys.length} mempool transaction cache entries`);
            }

            // 清空区块队列
            await redisService.del('blocks:recent:queue');

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

        // 缓存区块到Redis（按高度排序，从低到高）
        const sortedHeights = heights.sort((a, b) => a - b);
        let cachedCount = 0;

        for (const height of sortedHeights) {
            const block = blockMap.get(height);
            if (block) {
                const cacheKey = `blocks:recent:${height}`;
                await redisService.setJSON(cacheKey, block);

                // 添加到队列（队列保持高度顺序）
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
            return 0;
        }

        logger.debug(`RedisCacheWorker: Found ${mempoolTxIds.length} transactions in mempool`);

        const BATCH_SIZE = 100;
        let cachedCount = 0;

        for (let i = 0; i < mempoolTxIds.length; i += BATCH_SIZE) {
            const batch = mempoolTxIds.slice(i, i + BATCH_SIZE);

            try {
                const rawTxs = await serviceManager.getRawTransactionsHex(batch);

                for (let j = 0; j < batch.length; j++) {
                    const txid = batch[j];
                    const raw = rawTxs[j];

                    if (raw) {
                        const cacheKey = `mempool:tx:${txid}`;
                        const txData = {
                            txid: txid,
                            raw: raw
                        };
                        await redisService.setJSON(cacheKey, txData);
                        cachedCount++;
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

        logger.debug(`RedisCacheWorker: Cached ${cachedCount} mempool transactions to Redis`);
        return cachedCount;
    }

    async trimBlockQueue() {
        try {
            const queueLength = await redisService.llen('blocks:recent:queue');

            if (queueLength > this.maxRecentBlocks) {
                // 计算需要移除的区块数量
                const toRemove = queueLength - this.maxRecentBlocks;

                // 从队列前端移除老区块的高度，并删除对应的缓存
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