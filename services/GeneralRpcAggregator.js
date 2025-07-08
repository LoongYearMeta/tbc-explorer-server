import dotenv from "dotenv";

import logger from "../config/logger.js";
import cache from "./Cache.js";

dotenv.config();

class GeneralRpcAggregator {
  constructor() {
    this.pendingRequests = new Map();
    this.batchTimers = new Map();

    this.config = {
      batchDelay: parseInt(process.env.GENERAL_RPC_BATCH_DELAY) || 100,
      maxWaitTime: parseInt(process.env.GENERAL_RPC_MAX_WAIT_TIME) || 2000,
      enableBatching: process.env.GENERAL_RPC_ENABLE_BATCHING !== 'false'
    };

    logger.info('GeneralRpcAggregator initialized', {
      config: this.config
    });
  }

  generateRequestKey(method, params = []) {
    return `${method}:${JSON.stringify(params)}`;
  }

  async callRpc(method, params = []) {
    if (!this.config.enableBatching) {
      return await this.executeRpcCall(method, params);
    }

    const requestKey = this.generateRequestKey(method, params);

    return new Promise((resolve, reject) => {
      if (!this.pendingRequests.has(requestKey)) {
        this.pendingRequests.set(requestKey, []);
      }

      const requests = this.pendingRequests.get(requestKey);
      requests.push({
        resolve,
        reject,
        timestamp: Date.now()
      });

      if (requests.length === 1) {
        this.scheduleBatch(requestKey, method, params);
      }

      setTimeout(() => {
        reject(new Error(`GeneralRpcAggregator: Request timeout for ${method}`));
      }, this.config.maxWaitTime);
    });
  }

  scheduleBatch(requestKey, method, params) {
    if (this.batchTimers.has(requestKey)) {
      return;
    }

    const timer = setTimeout(async () => {
      await this.processBatch(requestKey, method, params);
    }, this.config.batchDelay);

    this.batchTimers.set(requestKey, timer);
  }

  async processBatch(requestKey, method, params) {
    const requests = this.pendingRequests.get(requestKey);
    if (!requests || requests.length === 0) {
      return;
    }

    const timer = this.batchTimers.get(requestKey);
    if (timer) {
      clearTimeout(timer);
      this.batchTimers.delete(requestKey);
    }
    this.pendingRequests.delete(requestKey);

    const requestCount = requests.length;

    logger.debug(`GeneralRpcAggregator: Processing batch for ${method}`, {
      requestCount,
      params
    });

    try {
      const result = await this.executeRpcCall(method, params);
      requests.forEach(request => {
        request.resolve(result);
      });

      if (requestCount > 1) {
        logger.debug(`GeneralRpcAggregator: Batch completed for ${method}`, {
          requestCount,
          savedCalls: requestCount - 1
        });
      }

    } catch (error) {
      logger.error(`GeneralRpcAggregator: Batch failed for ${method}`, {
        error: error.message,
        requestCount,
        params
      });

      requests.forEach(request => {
        request.reject(error);
      });
    }
  }

  async executeRpcCall(method, params) {
    switch (method) {
      case 'getBlockchainInfo':
        return await cache.getCachedOrFetch('getBlockchainInfo', params);
      case 'getMiningInfo':
        return await cache.getCachedOrFetch('getMiningInfo', params);
      case 'getChainTxStats':
        return await cache.getCachedOrFetch('getChainTxStats', params);
      case 'getMempoolInfo':
        return await cache.getCachedOrFetch('getMempoolInfo', params);
      case 'getRawMempool':
        return await cache.getCachedOrFetch('getRawMempool', params);
      case 'getNetworkInfo':
        return await cache.getCachedOrFetch('getNetworkInfo', params);
      default:
        throw new Error(`Unsupported RPC method: ${method}`);
    }
  }

  async shutdown() {
    logger.info('GeneralRpcAggregator: Starting shutdown...');

    cache.shutdown();

    for (const timer of this.batchTimers.values()) {
      clearTimeout(timer);
    }
    this.batchTimers.clear();
    const totalPendingRequests = Array.from(this.pendingRequests.values())
      .reduce((sum, requests) => sum + requests.length, 0);

    if (totalPendingRequests > 0) {
      logger.warn(`GeneralRpcAggregator: Rejecting ${totalPendingRequests} pending requests during shutdown`);

      for (const requests of this.pendingRequests.values()) {
        requests.forEach(request => {
          request.reject(new Error('Service is shutting down'));
        });
      }
    }

    this.pendingRequests.clear();

    logger.info('GeneralRpcAggregator: Shutdown completed');
  }
}

const generalRpcAggregator = new GeneralRpcAggregator();

export default generalRpcAggregator; 