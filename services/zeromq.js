import zmq from "zeromq";

import logger from "../config/logger.js";
import { Block } from "../models/block.js";
import { Transaction } from "../models/transaction.js";
import redisService from "./RedisService.js";

class ZeroMQService {
  constructor(config) {
    this.config = config;
    this.subscribers = new Map();
    this.isRunning = false;
    this.reconnectTimeouts = new Map();
    this.messageHandlers = new Map();

    this.serviceManager = null;
    this.setupDefaultHandlers();
  }

  setServiceManager(serviceManager) {
    this.serviceManager = serviceManager;
  }

  setupDefaultHandlers() {
    this.messageHandlers.set('hashblock', (topic, message) => {
      const blockHash = message.toString('hex');
      logger.info(`[ZMQ] New block hash: ${blockHash}`);
      this.onNewBlockHash(blockHash).catch(error => {
        logger.error(`[ZMQ] Error in onNewBlockHash handler: ${error.message}`);
      });
    });

    this.messageHandlers.set('hashtx', (topic, message) => {
      const txHash = message.toString('hex');
      logger.info(`[ZMQ] New transaction hash: ${txHash}`);
      this.onNewTransactionHash(txHash).catch(error => {
        logger.error(`[ZMQ] Error in onNewTransactionHash handler: ${error.message}`);
      });
    });
  }

  async start() {
    if (!this.config.enabled) {
      logger.info('[ZMQ] ZeroMQ service is disabled');
      return;
    }

    if (this.isRunning) {
      logger.warn('[ZMQ] ZeroMQ service is already running');
      return;
    }

    this.isRunning = true;
    logger.info('[ZMQ] Starting ZeroMQ service...');

    await this.clearRedisCache();

    const subscriptionPromises = [];
    for (const [subscriptionName, subscription] of Object.entries(this.config.subscriptions)) {
      if (subscription.enabled) {
        subscriptionPromises.push(this.createSubscription(subscriptionName, subscription));
      }
    }

    await Promise.allSettled(subscriptionPromises);

    const connectedCount = Array.from(this.subscribers.values()).filter(sub => sub.connected).length;
    const totalCount = this.subscribers.size;

    logger.info('[ZMQ] ZeroMQ service started');
    logger.info(`[ZMQ] Subscriptions: ${connectedCount}/${totalCount} connected`);

    if (connectedCount === 0) {
      logger.warn('[ZMQ] No ZeroMQ subscriptions connected - will attempt to reconnect');
    }
  }

  async createSubscription(name, subscription) {
    const address = `tcp://${this.config.host}:${subscription.port}`;

    try {
      const subscriber = new zmq.Subscriber();
      subscriber.subscribe(subscription.topic);
      subscriber.connect(address);

      this.subscribers.set(name, {
        subscriber,
        subscription,
        address,
        connected: false,
        lastError: null
      });

      this.setupMessageHandler(subscriber, name, subscription.topic);

      logger.info(`[ZMQ] Attempting to connect to ${name} at ${address}`);
    } catch (error) {
      logger.error(`[ZMQ] Failed to create subscription for ${name}`, {
        error: error.message,
        address
      });

      this.subscribers.set(name, {
        subscriber: null,
        subscription,
        address,
        connected: false,
        lastError: error
      });
      this.scheduleReconnect(name);
    }
  }

  setupMessageHandler(subscriber, name, topic) {
    const messageHandler = this.messageHandlers.get(name);

    if (!messageHandler) {
      logger.warn(`[ZMQ] No message handler found for ${name}`);
      return;
    }

    (async () => {
      try {
        const subscription = this.subscribers.get(name);
        if (subscription) {
          subscription.connected = true;
        }

        for await (const [topicBuffer, message] of subscriber) {
          const receivedTopic = topicBuffer.toString();
          if (receivedTopic === topic) {
            if (subscription && !subscription.connected) {
              logger.info(`[ZMQ] Successfully connected to ${name}`);
              subscription.connected = true;
              subscription.lastError = null;
            }

            messageHandler(receivedTopic, message);
          }
        }
      } catch (error) {
        logger.error(`[ZMQ] Message handler error for ${name}`, {
          error: error.message
        });
        const subscription = this.subscribers.get(name);
        if (subscription) {
          subscription.connected = false;
          subscription.lastError = error;
        }

        this.handleSubscriptionError(name, error);
      } finally {
        logger.warn(`[ZMQ] Message handler loop ended for ${name}`);
        const subscription = this.subscribers.get(name);
        if (subscription) {
          subscription.connected = false;
        }
      }
    })();
  }

  handleSubscriptionError(name, error) {
    const subscription = this.subscribers.get(name);
    if (subscription) {
      subscription.lastError = error;
      subscription.connected = false;
      this.scheduleReconnect(name);
    }
  }

  scheduleReconnect(name) {
    if (this.reconnectTimeouts.has(name)) {
      return;
    }

    const timeout = setTimeout(() => {
      this.reconnectTimeouts.delete(name);
      if (this.isRunning) {
        this.reconnectSubscription(name);
      }
    }, this.config.reconnectInterval);

    this.reconnectTimeouts.set(name, timeout);
    logger.info(`[ZMQ] Scheduled reconnect for ${name} in ${this.config.reconnectInterval}ms`);
  }


  async reconnectSubscription(name) {
    try {
      const subscriptionData = this.subscribers.get(name);
      if (!subscriptionData) {
        return;
      }

      logger.info(`[ZMQ] Attempting to reconnect ${name}...`);

      if (subscriptionData.subscriber) {
        try {
          await subscriptionData.subscriber.close();
        } catch (closeError) {
          logger.debug(`[ZMQ] Error closing existing connection for ${name}:`, closeError.message);
        }
      }

      await this.createSubscription(name, subscriptionData.subscription);

    } catch (error) {
      logger.error(`[ZMQ] Failed to reconnect ${name}`, {
        error: error.message
      });
      this.scheduleReconnect(name);
    }
  }

  async onNewBlockHash(blockHash) {
    try {
      logger.info(`[ZMQ] Processing new block hash: ${blockHash}`);

      const existingBlock = await Block.findOne({ hash: blockHash });
      if (existingBlock) {
        logger.debug(`[ZMQ] Block ${blockHash} already exists in database`);
        return;
      }

      if (!this.serviceManager) {
        logger.warn(`[ZMQ] ServiceManager not set, cannot fetch block details for ${blockHash}`);
        return;
      }

      logger.debug(`[ZMQ] Fetching block details for ${blockHash}`);
      const blockDetails = await this.serviceManager.getBlockByHash(blockHash);

      if (!blockDetails) {
        logger.error(`[ZMQ] Failed to fetch block details for ${blockHash}`);
        return;
      }

      const blockDoc = new Block({
        hash: blockDetails.hash,
        height: blockDetails.height,
        confirmations: blockDetails.confirmations,
        size: blockDetails.size,
        version: blockDetails.version,
        versionHex: blockDetails.versionHex,
        merkleroot: blockDetails.merkleroot,
        num_tx: blockDetails.num_tx,
        time: blockDetails.time,
        mediantime: blockDetails.mediantime,
        nonce: blockDetails.nonce,
        bits: blockDetails.bits,
        difficulty: blockDetails.difficulty,
        chainwork: blockDetails.chainwork,
        previousblockhash: blockDetails.previousblockhash,
        nextblockhash: blockDetails.nextblockhash,
        tx: blockDetails.tx,
        coinbaseTx: blockDetails.coinbaseTx,
        totalFees: blockDetails.totalFees,
        miner: blockDetails.miner
      });

      await blockDoc.save();
      logger.info(`[ZMQ] Successfully saved block ${blockHash} (height: ${blockDetails.height}) to database`);
      await this.processBlockTransactions(blockDetails);
      await this.updateRedisBlockCache(blockDetails);
      this.scheduleUpdateMempoolCache(blockDetails.tx, blockDetails.height);
    } catch (error) {
      if (error.code === 11000) {
        logger.debug(`[ZMQ] Block ${blockHash} already exists (concurrent write), skipping`);
      } else {
        logger.error(`[ZMQ] Error processing new block hash ${blockHash}`, {
          error: error.message,
          stack: error.stack
        });
      }
    }
  }

  async processBlockTransactions(blockDetails) {
    try {
      const { height, tx: txIds } = blockDetails;
      const MAX_BLOCKS = 10000;
      const TARGET_BLOCKS = 9000;
      const currentBlockCount = await Transaction.getDistinctBlockCount();

      if (currentBlockCount >= MAX_BLOCKS) {
        const sortedHeights = await Transaction.getDistinctBlockHeights(-1);
        const blocksToKeep = sortedHeights.slice(0, TARGET_BLOCKS);
        const minHeightToKeep = Math.min(...blocksToKeep);

        logger.info(`[ZMQ] Block count (${currentBlockCount}) reached ${MAX_BLOCKS}, cleaning up transactions below height ${minHeightToKeep}`);

        const deleteResult = await Transaction.deleteBeforeHeight(minHeightToKeep);

        logger.info(`[ZMQ] Cleaned up ${deleteResult.deletedCount} transactions from old blocks, maintaining ${TARGET_BLOCKS} newest blocks`);
      }
      logger.info(`[ZMQ] Processing ${txIds.length} transactions from block ${height}`);
      const existingTransactions = await Transaction.find({ txid: { $in: txIds } }).select('txid');
      const existingTxIds = new Set(existingTransactions.map(tx => tx.txid));
      const missingTxIds = txIds.filter(txid => !existingTxIds.has(txid));

      if (missingTxIds.length === 0) {
        logger.info(`[ZMQ] All transactions from block ${height} already exist in database`);
        return;
      }

      const BATCH_SIZE = 500;
      const batches = [];
      for (let i = 0; i < missingTxIds.length; i += BATCH_SIZE) {
        batches.push(missingTxIds.slice(i, i + BATCH_SIZE));
      }

      let totalSaved = 0;
      let totalErrors = 0;

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        try {
          const rawTxs = await this.serviceManager.getRawTransactionsHex(batch);

          const txDocs = [];
          for (let j = 0; j < batch.length; j++) {
            const txid = batch[j];
            const rawTx = rawTxs[j];

            if (rawTx) {
              txDocs.push({
                txid: txid,
                raw: rawTx,
                blockHeight: height
              });
            } else {
              logger.warn(`[ZMQ] Failed to get raw transaction for txid: ${txid}`);
              totalErrors++;
            }
          }

          if (txDocs.length > 0) {
            try {
              const result = await Transaction.insertMany(txDocs, {
                ordered: false,
                rawResult: true
              });
              const savedCount = result.insertedCount || txDocs.length;
              totalSaved += savedCount;
              logger.debug(`[ZMQ] Saved ${savedCount} transactions from block ${height} batch ${i + 1}`);
            } catch (error) {
              if (error.code === 11000 && error.writeErrors) {
                const successCount = txDocs.length - error.writeErrors.length;
                totalSaved += successCount;
                logger.debug(`[ZMQ] Batch insert completed with ${error.writeErrors.length} duplicates, ${successCount} succeeded`);
              } else {
                throw error;
              }
            }
          }
        } catch (error) {
          logger.error(`[ZMQ] Error processing transaction batch from block ${height}`, {
            error: error.message,
            batchIndex: i,
            batchSize: batch.length
          });
          totalErrors += batch.length;
        }
      }

      logger.info(`[ZMQ] Completed processing transactions from block ${height}`, {
        totalProcessed: missingTxIds.length,
        totalSaved,
        totalErrors,
        successRate: `${((totalSaved / missingTxIds.length) * 100).toFixed(1)}%`
      });

    } catch (error) {
      logger.error(`[ZMQ] Error in processBlockTransactions`, {
        error: error.message,
        stack: error.stack,
        blockHeight: blockDetails.height
      });
    }
  }

  async onNewTransactionHash(txHash) {
    try {
      logger.info(`[ZMQ] Processing new transaction hash: ${txHash}`);
      const cacheKey = `mempool:tx:${txHash}`;
      const existingTx = await redisService.exists(cacheKey);

      if (existingTx) {
        logger.debug(`[ZMQ] Transaction ${txHash} already exists in Redis cache`);
        return;
      }

      if (!this.serviceManager) {
        logger.warn(`[ZMQ] ServiceManager not set, cannot fetch transaction details for ${txHash}`);
        return;
      }

      const rawTx = await this.serviceManager.getRawTransactionHex(txHash);
      if (!rawTx) {
        logger.warn(`[ZMQ] Failed to get raw transaction for ${txHash}`);
        return;
      }

      const txData = {
        txid: txHash,
        raw: rawTx
      };

      await redisService.setJSON(cacheKey, txData);
      logger.debug(`[ZMQ] Cached new transaction ${txHash} to Redis`);

    } catch (error) {
      logger.error(`[ZMQ] Error processing new transaction hash ${txHash}`, {
        error: error.message,
        stack: error.stack
      });
    }
  }

  async stop() {
    if (!this.isRunning) {
      logger.info('[ZMQ] ZeroMQ service is not running');
      return;
    }
    logger.info('[ZMQ] Stopping ZeroMQ service...');
    this.isRunning = false;
    for (const [name, timeout] of this.reconnectTimeouts.entries()) {
      clearTimeout(timeout);
      this.reconnectTimeouts.delete(name);
      logger.debug(`[ZMQ] Cleared reconnect timeout for ${name}`);
    }
    const closePromises = [];
    for (const [name, subscriptionData] of this.subscribers.entries()) {
      if (subscriptionData.subscriber) {
        try {
          const closePromise = subscriptionData.subscriber.close();
          if (closePromise && typeof closePromise.catch === 'function') {
            closePromises.push(
              closePromise.catch(error => {
                logger.debug(`[ZMQ] Error closing subscription ${name}:`, error.message);
              })
            );
          }
        } catch (error) {
          logger.debug(`[ZMQ] Error calling close() for subscription ${name}:`, error.message);
        }
        subscriptionData.connected = false;
      }
    }

    if (closePromises.length > 0) {
      await Promise.allSettled(closePromises);
    }

    this.subscribers.clear();
    logger.info('[ZMQ] ZeroMQ service stopped successfully');
  }

  async updateRedisBlockCache(blockDetails) {
    try {
      logger.debug(`[ZMQ] Updating Redis block cache for block ${blockDetails.height}`);

      const maxRecentBlocks = 10;
      const cacheKey = `blocks:recent:${blockDetails.height}`;
      await redisService.setJSON(cacheKey, blockDetails);
      await redisService.rpush('blocks:recent:queue', blockDetails.height);
      const queueLength = await redisService.llen('blocks:recent:queue');
      if (queueLength > maxRecentBlocks) {
        const oldHeight = await redisService.lpop('blocks:recent:queue');
        if (oldHeight) {
          const oldCacheKey = `blocks:recent:${oldHeight}`;
          await redisService.del(oldCacheKey);
          logger.debug(`[ZMQ] Removed old block ${oldHeight} from queue and cache`);
        }
      }

      logger.debug(`[ZMQ] Successfully updated Redis block cache for block ${blockDetails.height}`);

    } catch (error) {
      logger.error(`[ZMQ] Error updating Redis block cache`, {
        error: error.message,
        stack: error.stack,
        blockHeight: blockDetails.height
      });
    }
  }

  async clearRedisCache() {
    try {
      logger.info('[ZMQ] Clearing Redis cache on startup...');
      const mempoolPattern = 'tbc-explorer:mempool:tx:*';
      const mempoolKeys = await redisService.exec('KEYS', mempoolPattern);
      if (mempoolKeys && mempoolKeys.length > 0) {
        await redisService.exec('DEL', ...mempoolKeys);
        logger.info(`[ZMQ] Cleared ${mempoolKeys.length} mempool transaction cache entries`);
      }
      const blockPattern = 'tbc-explorer:blocks:recent:*';
      const blockKeys = await redisService.exec('KEYS', blockPattern);
      if (blockKeys && blockKeys.length > 0) {
        await redisService.exec('DEL', ...blockKeys);
        logger.info(`[ZMQ] Cleared ${blockKeys.length} recent blocks cache entries`);
      }
      
      const queueExists = await redisService.exists(`blocks:recent:queue`);
      if (queueExists) {
        await redisService.del(`blocks:recent:queue`);
        logger.info(`[ZMQ] Cleared blocks recent queue`);
      }
      
      logger.info('[ZMQ] Redis cache cleared successfully');
    } catch (error) {
      logger.error('[ZMQ] Error clearing Redis cache on startup', {
        error: error.message,
        stack: error.stack
      });
    }
  }

  async updateRedisMempoolCache(excludeBlockTxIds = []) {
    try {
      logger.debug(`[ZMQ] Updating Redis mempool cache`);
      const currentMempoolTxIds = await this.serviceManager.getRawMempool();
      const pattern = 'tbc-explorer:mempool:tx:*';
      const cachedKeys = await redisService.exec('KEYS', pattern);
      const cachedTxIds = cachedKeys.map(key => {
        const parts = key.split(':');
        return parts[parts.length - 1]; 
      });

      let filteredMempoolTxIds = currentMempoolTxIds;
      if (excludeBlockTxIds && excludeBlockTxIds.length > 0) {
        const excludeSet = new Set(excludeBlockTxIds);
        filteredMempoolTxIds = currentMempoolTxIds.filter(txid => !excludeSet.has(txid));
        logger.debug(`[ZMQ] Excluded ${excludeBlockTxIds.length} block transactions from mempool cache update`);
      }

      if (!filteredMempoolTxIds || filteredMempoolTxIds.length === 0) {
        if (cachedKeys.length > 0) {
          await redisService.exec('DEL', ...cachedKeys);
          logger.debug(`[ZMQ] Mempool is empty, cleared ${cachedKeys.length} cached transactions`);
        }
        return;
      }

      const currentTxSet = new Set(filteredMempoolTxIds);
      const cachedTxSet = new Set(cachedTxIds);

      const toRemove = cachedTxIds.filter(txid => !currentTxSet.has(txid));
      const toAdd = filteredMempoolTxIds.filter(txid => !cachedTxSet.has(txid));
      if (toRemove.length > 0) {
        const keysToRemove = toRemove.map(txid => `tbc-explorer:mempool:tx:${txid}`);
        await redisService.exec('DEL', ...keysToRemove);
        logger.debug(`[ZMQ] Removed ${toRemove.length} stale transactions from Redis cache`);
      }
      if (toAdd.length > 0) {
        logger.debug(`[ZMQ] Adding ${toAdd.length} new transactions to Redis cache`);

        const BATCH_SIZE = 100;
        let cachedCount = 0;

        for (let i = 0; i < toAdd.length; i += BATCH_SIZE) {
          const batch = toAdd.slice(i, i + BATCH_SIZE);

          try {
            const rawTxs = await this.serviceManager.getRawTransactionsHex(batch);
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

            logger.debug(`[ZMQ] Cached batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} transactions)`);

          } catch (error) {
            logger.error(`[ZMQ] Error caching mempool transaction batch`, {
              error: error.message,
              batchStart: i,
              batchSize: batch.length
            });
          }
        }

        logger.debug(`[ZMQ] Added ${cachedCount} new transactions to Redis cache`);
      }

      logger.debug(`[ZMQ] Successfully updated Redis mempool cache`, {
        removed: toRemove.length,
        added: toAdd.length,
        total: filteredMempoolTxIds.length,
        excluded: excludeBlockTxIds.length
      });

    } catch (error) {
      logger.error(`[ZMQ] Error updating Redis mempool cache`, {
        error: error.message,
        stack: error.stack
      });
    }
  }

  scheduleUpdateMempoolCache(blockTxIds, blockHeight, attempt = 1, maxAttempts = 3) {
    const delay = attempt * 2000;

    setTimeout(async () => {
      try {
        logger.debug(`[ZMQ] Attempting mempool cache update for block ${blockHeight}, attempt ${attempt}/${maxAttempts}`);
        const currentMempoolTxIds = await this.serviceManager.getRawMempool();
        const blockTxSet = new Set(blockTxIds);
        const stillInMempool = currentMempoolTxIds.filter(txid => blockTxSet.has(txid));
        
        if (stillInMempool.length > 0 && attempt < maxAttempts) {
          logger.debug(`[ZMQ] ${stillInMempool.length} block transactions still in mempool, will retry in ${(attempt + 1) * 2}s`);
          this.scheduleUpdateMempoolCache(blockTxIds, blockHeight, attempt + 1, maxAttempts);
          return;
        }

        if (stillInMempool.length > 0) {
          logger.warn(`[ZMQ] ${stillInMempool.length} block transactions still in mempool after ${maxAttempts} attempts, updating cache anyway`);
        }

        await this.updateRedisMempoolCache(blockTxIds);
        logger.debug(`[ZMQ] Successfully updated mempool cache for block ${blockHeight} on attempt ${attempt}`);
        
      } catch (error) {
        logger.error(`[ZMQ] Error in mempool cache update attempt ${attempt} for block ${blockHeight}`, {
          error: error.message,
          stack: error.stack
        });
        
        if (attempt < maxAttempts) {
          logger.debug(`[ZMQ] Will retry mempool cache update for block ${blockHeight}`);
          this.scheduleUpdateMempoolCache(blockTxIds, blockHeight, attempt + 1, maxAttempts);
        }
      }
    }, delay);
  }
}

export default ZeroMQService;
