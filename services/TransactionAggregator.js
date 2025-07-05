import dotenv from "dotenv";

import logger from "../config/logger.js";
import serviceManager from "./ServiceManager.js";

dotenv.config();

class TransactionAggregator {
  constructor() {
    this.pendingRequests = new Map(); 
    this.batchTimer = null;
    this.config = {
      batchDelay: parseInt(process.env.TX_BATCH_DELAY) || 100, 
      maxBatchSize: parseInt(process.env.TX_MAX_BATCH_SIZE) || 200,
      maxWaitTime: parseInt(process.env.TX_MAX_WAIT_TIME) || 200,  
      enableBatching: process.env.TX_ENABLE_BATCHING !== 'false'
    };
  }

  async getRawTransaction(txid) {
    if (!this.config.enableBatching) {
      return await serviceManager.getRawTransaction(txid);
    }

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
    
    logger.debug(`TransactionAggregator: Processing batch of ${uniqueTxids.length} unique transactions`);

    try {
      const transactions = await serviceManager.getRawTransactions(uniqueTxids);

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