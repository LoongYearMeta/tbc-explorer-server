import dotenv from "dotenv";

import logger from "../config/logger.js";
import serviceManager from "./ServiceManager.js";

dotenv.config();

class TransactionAggregator {
  constructor() {
    this.pendingRequests = new Map(); 
    this.batchTimer = null;
    this.config = {
      batchDelay: parseInt(process.env.TX_BATCH_DELAY) || 200, 
      maxBatchSize: parseInt(process.env.TX_MAX_BATCH_SIZE) || 1000,
      maxWaitTime: parseInt(process.env.TX_MAX_WAIT_TIME) || 1000,  
      enableBatching: process.env.TX_ENABLE_BATCHING !== 'false'
    };
    
    this.stats = {
      totalRequests: 0,
      batchedRequests: 0,
      rpcCalls: 0,
      averageBatchSize: 0,
      timesSaved: 0  
    };
  }

  async getRawTransaction(txid) {
    if (!this.config.enableBatching) {
      return await serviceManager.getRawTransaction(txid);
    }

    this.stats.totalRequests++;

    return new Promise((resolve, reject) => {
      const requestId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      const request = {
        id: requestId,
        txid,
        resolve,
        reject,
        timestamp: Date.now()
      };

      if (!this.pendingRequests.has(txid)) {
        this.pendingRequests.set(txid, []);
      }
      this.pendingRequests.get(txid).push(request);
      if (!this.batchTimer) {
        this.scheduleBatch();
      }

      setTimeout(() => {
        if (this.pendingRequests.has(txid)) {
          const requests = this.pendingRequests.get(txid);
          const index = requests.findIndex(r => r.id === requestId);
          if (index !== -1) {
            requests.splice(index, 1);
            if (requests.length === 0) {
              this.pendingRequests.delete(txid);
            }
            reject(new Error('Request timeout'));
          }
        }
      }, this.config.maxWaitTime);
    });
  }

  scheduleBatch() {
    this.batchTimer = setTimeout(async () => {
      await this.processBatch();
    }, this.config.batchDelay);
  }

  async processBatch() {
    this.batchTimer = null;

    if (this.pendingRequests.size === 0) {
      return;
    }

    const currentRequests = new Map(this.pendingRequests);
    this.pendingRequests.clear();

    const uniqueTxids = Array.from(currentRequests.keys());
    const batchSize = Math.min(uniqueTxids.length, this.config.maxBatchSize);
    
    logger.debug(`TransactionAggregator: Processing batch of ${uniqueTxids.length} unique transactions`);

    try {
      const transactions = await serviceManager.getRawTransactions(uniqueTxids);
      
      // 统计实际被批处理的请求总数
      const totalRequestsInBatch = Array.from(currentRequests.values())
        .reduce((sum, requests) => sum + requests.length, 0);
      
      this.stats.batchedRequests += totalRequestsInBatch;
      this.stats.rpcCalls += 1;
      this.stats.timesSaved += totalRequestsInBatch - 1; 
      this.stats.averageBatchSize = this.stats.batchedRequests / this.stats.rpcCalls;

      const resultMap = new Map();
      for (let i = 0; i < uniqueTxids.length; i++) {
        const txid = uniqueTxids[i];
        const transaction = transactions[i];
        resultMap.set(txid, transaction);
      }

      for (const [txid, requests] of currentRequests) {
        const result = resultMap.get(txid);
        for (const request of requests) {
          request.resolve(result);
        }
      }

      logger.debug(`TransactionAggregator: Batch completed. Processed ${uniqueTxids.length} unique transactions in 1 RPC call (saved ${uniqueTxids.length - 1} calls)`);

    } catch (error) {
      logger.error('TransactionAggregator: Batch processing failed', {
        error: error.message,
        batchSize: uniqueTxids.length
      });

      for (const requests of currentRequests.values()) {
        for (const request of requests) {
          request.reject(error);
        }
      }
    }
  }

  getStats() {
    const totalRequestsHandled = this.stats.totalRequests;
    const efficiency = totalRequestsHandled > 0 ? 
      ((this.stats.timesSaved / totalRequestsHandled) * 100).toFixed(2) : 0;

    return {
      ...this.stats,
      pendingRequests: this.pendingRequests.size,
      efficiency: `${efficiency}%`, 
      config: this.config
    };
  }

  resetStats() {
    this.stats = {
      totalRequests: 0,
      batchedRequests: 0,
      rpcCalls: 0,
      averageBatchSize: 0,
      timesSaved: 0
    };
  }

  async shutdown() {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    if (this.pendingRequests.size > 0) {
      await this.processBatch();
    }

    logger.info('TransactionAggregator: Shutdown completed');
  }
}

const transactionAggregator = new TransactionAggregator();

export default transactionAggregator; 