import zmq from "zeromq";

import logger from "../config/logger.js";
import { Block } from "../models/block.js";
import { Transaction } from "../models/transaction.js";

class ZeroMQService {
  constructor(config) {
    this.config = config;
    this.subscribers = new Map();
    this.isRunning = false;
    this.reconnectTimeouts = new Map();
    this.messageHandlers = new Map();
    this.stats = {
      hashblock: { received: 0, errors: 0, lastReceived: null },
      hashtx: { received: 0, errors: 0, lastReceived: null }
    };
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
      this.stats.hashblock.received++;
      this.stats.hashblock.lastReceived = new Date();
      this.onNewBlockHash(blockHash).catch(error => {
        logger.error(`[ZMQ] Error in onNewBlockHash handler: ${error.message}`);
      });
    });

    this.messageHandlers.set('hashtx', (topic, message) => {
      const txHash = message.toString('hex');
      logger.info(`[ZMQ] New transaction hash: ${txHash}`);
      this.stats.hashtx.received++;
      this.stats.hashtx.lastReceived = new Date();
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
        this.stats[name].errors++;
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

  getStatus() {
    const subscriptions = {};
    
    for (const [name, subscriptionData] of this.subscribers.entries()) {
      subscriptions[name] = {
        connected: subscriptionData.connected,
        address: subscriptionData.address,
        lastError: subscriptionData.lastError?.message || null,
        stats: this.stats[name]
      };
    }

    return {
      enabled: this.config.enabled,
      running: this.isRunning,
      subscriptions,
      totalSubscriptions: this.subscribers.size
    };
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

      // Process transactions from the new block
      await this.processBlockTransactions(blockDetails);
      
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
      
      // First, check if we need to clean up old transactions
      const MAX_BLOCKS = 10000;  // When reaching this, trigger cleanup
      const TARGET_BLOCKS = 9000; // Keep this many blocks after cleanup
      
      // Get the actual count of distinct block heights in the database
      const currentBlockCount = await Transaction.getDistinctBlockCount();
      
      if (currentBlockCount >= MAX_BLOCKS) {
        // When we reach 10000 blocks, keep only the newest 9000 blocks
        const sortedHeights = await Transaction.getDistinctBlockHeights(-1); // Sort descending
        const blocksToKeep = sortedHeights.slice(0, TARGET_BLOCKS);
        const minHeightToKeep = Math.min(...blocksToKeep);
        
        logger.info(`[ZMQ] Block count (${currentBlockCount}) reached ${MAX_BLOCKS}, cleaning up transactions below height ${minHeightToKeep}`);
        
        const deleteResult = await Transaction.deleteBeforeHeight(minHeightToKeep);
        
        logger.info(`[ZMQ] Cleaned up ${deleteResult.deletedCount} transactions from old blocks, maintaining ${TARGET_BLOCKS} newest blocks`);
      }

      // Process new transactions
      logger.info(`[ZMQ] Processing ${txIds.length} transactions from block ${height}`);

      // Get existing transactions to avoid duplicates
      const existingTransactions = await Transaction.find({ txid: { $in: txIds } }).select('txid');
      const existingTxIds = new Set(existingTransactions.map(tx => tx.txid));
      const missingTxIds = txIds.filter(txid => !existingTxIds.has(txid));

      if (missingTxIds.length === 0) {
        logger.info(`[ZMQ] All transactions from block ${height} already exist in database`);
        return;
      }

      // Process transactions in batches
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
      
      // TODO: 实现交易哈希处理逻辑
      // 可以在这里添加：
      // 1. 检查交易是否已存在
      // 2. 获取交易详情
      // 3. 保存到数据库
      // 4. 触发相关业务逻辑
      
      logger.debug(`[ZMQ] Transaction hash processing not implemented yet: ${txHash}`);
      
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
          // Only add to promises if close() returns a Promise
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
}

export default ZeroMQService;
