import axios from "axios";
import crypto from "crypto";
import bitcoin from "bitcoinjs-lib";
import net from "net";
import dotenv from "dotenv";

import servicesConfig from "../config/services.js";
import logger from "../config/logger.js";
import { generateRandomString, splitArrayIntoChunks } from "../lib/util.js";
import { coinConfig } from "../lib/coin.js";
import {
  getBlockTotalFeesFromCoinbaseTxAndBlockHeight,
  getMinerFromCoinbaseTx,
} from "../lib/util.js";

dotenv.config();

class ServiceManager {
  constructor() {
    this.rpcClients = {};
    this.initialized = false;

    this.performanceConfig = {
      defaultBatchSize: parseInt(process.env.BATCH_SIZE) || 100,
      maxBatchSize: parseInt(process.env.MAX_BATCH_SIZE) || 200,
      minBatchSize: parseInt(process.env.MIN_BATCH_SIZE) || 20,
      maxConcurrentBatches: parseInt(process.env.MAX_CONCURRENT_BATCHES) || 3,
      networkErrorMaxRetries: parseInt(process.env.NETWORK_ERROR_MAX_RETRIES) || 3,
      rpcErrorMaxRetries: parseInt(process.env.RPC_ERROR_MAX_RETRIES) || 1,
      circuitBreakerEnabled: process.env.CIRCUIT_BREAKER_ENABLED !== 'false',
      circuitBreakerFailureThreshold: parseInt(process.env.CIRCUIT_BREAKER_THRESHOLD) || 5,
      circuitBreakerResetTimeout: parseInt(process.env.CIRCUIT_BREAKER_RESET) || 30000,
    };

    this.circuitBreaker = {
      failures: 0,
      lastFailureTime: 0,
      state: 'CLOSED'
    };

    this.batchRetryConfig = {
      maxRetries: parseInt(process.env.BATCH_RETRY_MAX_RETRIES) || 2,
      retryDelay: parseInt(process.env.BATCH_RETRY_DELAY) || 1000,
      maxDelayMs: parseInt(process.env.BATCH_RETRY_MAX_DELAY) || 5000,
      enableIndividualRetry: process.env.BATCH_RETRY_ENABLE !== 'false'
    };
  }

  async initialize() {
    try {
      logger.info("Initializing external service connections...");
      await this.initializeRpcClients();
      this.initialized = true;
      logger.info("All external service connections initialized successfully");
    } catch (error) {
      logger.error("Service initialization failed", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  async initializeRpcClients() {
    const services = [servicesConfig.nodeRpc, servicesConfig.electrumxRpc];

    for (const service of services) {
      try {
        let client;

        if (service.type === "json-rpc") {
          if (service.protocol === "tcp") {
            client = {
              type: "tcp",
              host: service.host,
              port: service.port,
              timeout: service.timeout
            };

            logger.info(`Configuring TCP JSON-RPC client`, {
              url: service.url,
              protocol: service.protocol,
            });
          } else {
            const axiosConfig = {
              baseURL: service.url,
              timeout: service.timeout,
              headers: {
                "Content-Type": "application/json",
              },
            };

            if (service.name === "node-rpc" && service.username && service.password) {
              axiosConfig.auth = {
                username: service.username,
                password: service.password,
              };

              logger.info(`Configuring TBC Node RPC authentication`, {
                url: service.url,
                hasAuth: true,
              });
            }

            client = axios.create(axiosConfig);
          }
        } else {
          throw new Error(`Unsupported service type: ${service.type}`);
        }

        this.rpcClients[service.name] = {
          client,
          config: service,
        };

        logger.info(
          `Service client initialized successfully: ${service.name}`,
          {
            type: service.type,
            protocol: service.protocol,
            url: service.url,
            timeout: service.timeout,
          }
        );
      } catch (error) {
        logger.error(`Service client initialization failed: ${service.name}`, {
          type: service.type,
          protocol: service.protocol,
          url: service.url,
          error: error.message,
        });
        throw error;
      }
    }
  }

  isCircuitBreakerOpen() {
    if (!this.performanceConfig.circuitBreakerEnabled) {
      return false;
    }

    const now = Date.now();
    const { lastFailureTime, state } = this.circuitBreaker;
    const { circuitBreakerResetTimeout } = this.performanceConfig;

    if (state === 'OPEN') {
      if (now - lastFailureTime > circuitBreakerResetTimeout) {
        this.circuitBreaker.state = 'HALF_OPEN';
        logger.info('Circuit breaker transitioning to HALF_OPEN state');
        return false;
      }
      return true;
    }

    return false;
  }

  classifyError(error) {
    const errorMessage = error.message.toLowerCase();
    if (errorMessage.includes('timeout') ||
      errorMessage.includes('econnreset') ||
      errorMessage.includes('enotfound') ||
      errorMessage.includes('econnrefused') ||
      error.code === 'ECONNRESET' ||
      error.code === 'ETIMEDOUT') {
      return 'NETWORK_ERROR';
    }
    if (errorMessage.includes('rpc error') ||
      errorMessage.includes('method not found') ||
      errorMessage.includes('invalid params') ||
      errorMessage.includes('parse error')) {
      return 'RPC_ERROR';
    }
    return 'NETWORK_ERROR';
  }

  calculateOptimalBatchSize(totalRequests) {
    const { defaultBatchSize, maxBatchSize, minBatchSize } = this.performanceConfig;

    // 如果总数小于等于默认批次大小，直接返回总数
    if (totalRequests <= defaultBatchSize) {
      return totalRequests;
    }

    // 根据总请求数量动态调整，保证批次大小随请求数量合理增长
    if (totalRequests <= 200) {
      return Math.min(totalRequests, maxBatchSize);
    }

    if (totalRequests <= 500) {
      return Math.min(150, maxBatchSize);  // 中等数量用150个/批次
    }

    if (totalRequests <= 1000) {
      return Math.min(200, maxBatchSize);  // 较大数量用200个/批次（最大值）
    }

    // 超大数量时适当减少批次大小，避免单次请求过载
    return Math.min(150, maxBatchSize);
  }

  updateCircuitBreaker(success) {
    if (!this.performanceConfig.circuitBreakerEnabled) {
      return;
    }

    const { circuitBreakerFailureThreshold } = this.performanceConfig;

    if (success) {
      if (this.circuitBreaker.state === 'HALF_OPEN') {
        this.circuitBreaker.state = 'CLOSED';
        this.circuitBreaker.failures = 0;
        logger.info('Circuit breaker reset to CLOSED state');
      }
    } else {
      this.circuitBreaker.failures++;
      this.circuitBreaker.lastFailureTime = Date.now();

      if (this.circuitBreaker.failures >= circuitBreakerFailureThreshold) {
        this.circuitBreaker.state = 'OPEN';
        logger.warn(`Circuit breaker OPENED after ${this.circuitBreaker.failures} failures`);
      }
    }
  }

  async callTcpRpc(host, port, method, params, timeout = 10000) {
    return new Promise((resolve, reject) => {
      const client = new net.Socket();

      const payload = {
        jsonrpc: "2.0",
        method: method,
        params: params,
        id: generateRandomString(16),
      };

      const message = JSON.stringify(payload) + '\n';
      let responseData = '';

      client.setTimeout(timeout);

      client.on('data', (data) => {
        responseData += data.toString();
        if (responseData.includes('\n')) {
          try {
            const response = JSON.parse(responseData.trim());
            client.destroy();

            if (response.error) {
              reject(new Error(`RPC Error: ${response.error.message} (Code: ${response.error.code})`));
            } else {
              resolve(response.result);
            }
          } catch (error) {
            client.destroy();
            reject(new Error(`Failed to parse response: ${error.message}`));
          }
        }
      });

      client.on('error', (error) => {
        client.destroy();
        reject(error);
      });

      client.on('timeout', () => {
        client.destroy();
        reject(new Error('TCP connection timeout'));
      });

      client.connect(port, host, () => {
        client.write(message);
      });
    });
  }

  async callRpcMethod(serviceName, method, params = []) {
    if (!this.initialized) {
      throw new Error("ServiceManager not yet initialized");
    }

    if (this.isCircuitBreakerOpen()) {
      const error = new Error("Circuit breaker is OPEN - service temporarily unavailable");
      error.code = 'CIRCUIT_BREAKER_OPEN';
      throw error;
    }

    const service = this.rpcClients[serviceName];
    if (!service) {
      throw new Error(`Service not found: ${serviceName}`);
    }

    const { client, config } = service;
    let retries = config.retries;
    let lastError;

    while (retries > 0) {
      try {
        logger.debug(`Calling service method: ${serviceName}.${method}`, {
          type: config.type,
          protocol: config.protocol,
          params,
        });

        let result;

        if (config.type === "json-rpc") {
          if (config.protocol === "tcp") {
            result = await this.callTcpRpc(config.host, config.port, method, params, config.timeout);
          } else {
            const payload = {
              jsonrpc: "2.0",
              method: method,
              params: params,
              id: generateRandomString(16),
            };

            const response = await client.post("", payload);

            if (response.data.error) {
              throw new Error(
                `RPC Error: ${response.data.error.message} (Code: ${response.data.error.code})`
              );
            }

            result = response.data.result;
          }
        }

        logger.debug(`Service call successful: ${serviceName}.${method}`, {
          type: config.type,
          protocol: config.protocol,
          method,
          params,
        });
        this.updateCircuitBreaker(true);
        return result;
      } catch (error) {
        lastError = error;
        retries--;
        const errorType = this.classifyError(error);
        const maxRetriesForErrorType = errorType === 'NETWORK_ERROR'
          ? this.performanceConfig.networkErrorMaxRetries
          : this.performanceConfig.rpcErrorMaxRetries;
        const effectiveRetries = Math.min(retries, maxRetriesForErrorType - (config.retries - retries));

        logger.error(`Service call failed: ${serviceName}.${method}`, {
          type: config.type,
          protocol: config.protocol,
          method,
          params,
          error: error.message,
          errorType,
          retriesLeft: effectiveRetries,
          maxRetriesForType: maxRetriesForErrorType
        });

        if (effectiveRetries <= 0) {
          this.updateCircuitBreaker(false);
          throw error;
        }
        const baseDelay = parseInt(process.env.SINGLE_RPC_RETRY_DELAY) || 500;
        const delay = errorType === 'NETWORK_ERROR' ? baseDelay : baseDelay / 2;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
    this.updateCircuitBreaker(false);
    throw lastError;
  }

  async getRpcData(cmd) {
    return await this.callRpcMethod("node-rpc", cmd, []);
  }

  async getRpcDataWithParams(cmd, params) {
    return await this.callRpcMethod("node-rpc", cmd, params);
  }

  async executeBatch(batch, retryOptions = {}, serviceName = "node-rpc") {
    const {
      maxRetries = 2,
      retryDelay = 1000,
      maxDelayMs = 5000,
      enableIndividualRetry = true
    } = retryOptions;

    const service = this.rpcClients[serviceName];
    if (!service) {
      throw new Error(`Service not found: ${serviceName}`);
    }

    const client = service.client;

    const payload = batch.map((item, index) => ({
      jsonrpc: "2.0",
      method: item.method,
      params: item.parameters || [],
      id: index,
    }));

    try {
      const response = await client.post("", payload);
      if (Array.isArray(response.data)) {
        const results = new Array(batch.length);
        const failedRequests = [];
        const processedIds = new Set();
        for (const item of response.data) {
          if (typeof item.id !== 'number' || item.id < 0 || item.id >= batch.length) {
            logger.warn(`Invalid batch response id: ${item.id}`, {
              expectedRange: `0-${batch.length - 1}`,
              responseItem: item
            });
            continue;
          }

          processedIds.add(item.id);

          if (item.error) {
            failedRequests.push({
              index: item.id,
              request: batch[item.id],
              error: item.error,
              retryCount: 0
            });
            logger.debug(`Request failed, will retry: ${batch[item.id].method}`, {
              index: item.id,
              error: item.error.message,
              code: item.error.code
            });
          } else {
            results[item.id] = item.result;
          }
        }

        for (let i = 0; i < batch.length; i++) {
          if (!processedIds.has(i)) {
            failedRequests.push({
              index: i,
              request: batch[i],
              error: { message: 'No response received', code: -1 },
              retryCount: 0
            });
            logger.debug(`No response received for request: ${batch[i].method}`, {
              index: i
            });
          }
        }

        if (enableIndividualRetry && failedRequests.length > 0 && maxRetries > 0) {
          logger.info(`Retrying ${failedRequests.length} failed requests from batch`, {
            maxRetries,
            retryDelay
          });

          const retryResults = await this.retryFailedRequests(
            failedRequests,
            maxRetries,
            retryDelay,
            maxDelayMs,
            serviceName
          );

          for (const retryResult of retryResults) {
            if (retryResult.success) {
              results[retryResult.index] = retryResult.result;
            }
          }
        }

        const successCount = results.filter(r => r !== undefined).length;
        const finalFailedCount = batch.length - successCount;

        if (finalFailedCount > 0) {
          const stillFailedRequests = failedRequests.filter(req =>
            results[req.index] === undefined
          );

          logger.error(`Batch execution completed with ${finalFailedCount} final failures out of ${batch.length} requests`, {
            failures: stillFailedRequests.map(req => ({
              index: req.index,
              method: req.request.method,
              lastError: req.error.message,
              code: req.error.code,
              retriedTimes: req.retryCount
            }))
          });

          if (finalFailedCount === batch.length) {
            throw new Error(`All ${batch.length} requests in batch failed`);
          }
        } else {
          logger.debug(`Batch execution completed successfully: ${successCount}/${batch.length} requests`);
        }

        return results;
      } else {
        if (response.data.error) {
          throw new Error(
            `RPC Error: ${response.data.error.message} (Code: ${response.data.error.code})`
          );
        }
        return [response.data.result];
      }
    } catch (error) {
      logger.error("Batch execution failed", { error: error.message });
      throw error;
    }
  }

  async retryFailedRequests(failedRequests, maxRetries, retryDelay, maxDelayMs = 5000, serviceName = "node-rpc") {
    const service = this.rpcClients[serviceName];
    if (!service) {
      throw new Error(`Service not found: ${serviceName}`);
    }

    const client = service.client;
    const retryResults = [];

    for (const failedRequest of failedRequests) {
      let currentRetry = 0;
      let lastError = failedRequest.error;
      let success = false;
      let result = null;

      while (currentRetry < maxRetries && !success) {
        currentRetry++;

        try {
          logger.debug(`Retrying request ${failedRequest.index} (attempt ${currentRetry}/${maxRetries})`, {
            method: failedRequest.request.method,
            params: failedRequest.request.parameters
          });

          if (retryDelay > 0) {

            const backoffDelay = retryDelay * Math.pow(2, currentRetry - 1);
            const jitter = Math.random() * 0.1 * backoffDelay;
            const finalDelay = Math.min(backoffDelay + jitter, maxDelayMs);
            await new Promise(resolve => setTimeout(resolve, finalDelay));
          }

          const payload = {
            jsonrpc: "2.0",
            method: failedRequest.request.method,
            params: failedRequest.request.parameters || [],
            id: failedRequest.index,
          };

          const response = await client.post("", payload);

          if (response.data.error) {
            lastError = response.data.error;
            logger.debug(`Retry ${currentRetry} failed for request ${failedRequest.index}`, {
              error: response.data.error.message,
              code: response.data.error.code
            });
          } else {
            success = true;
            result = response.data.result;
            logger.debug(`Retry ${currentRetry} succeeded for request ${failedRequest.index}`);
          }
        } catch (error) {
          lastError = { message: error.message, code: -1 };
          logger.debug(`Retry ${currentRetry} threw exception for request ${failedRequest.index}`, {
            error: error.message
          });
        }
      }

      failedRequest.retryCount = currentRetry;
      failedRequest.error = lastError;

      retryResults.push({
        index: failedRequest.index,
        success: success,
        result: result,
        retryCount: currentRetry,
        lastError: lastError
      });

      if (success) {
        logger.info(`Request ${failedRequest.index} succeeded after ${currentRetry} retries`);
      } else {
        logger.warn(`Request ${failedRequest.index} failed after ${currentRetry} retries`, {
          method: failedRequest.request.method,
          lastError: lastError.message
        });
      }
    }

    return retryResults;
  }

  async executeBatchesSequentially(batches, retryOptions = null) {
    const batchId = generateRandomString(20);
    const effectiveRetryOptions = retryOptions || this.batchRetryConfig;

    logger.info(`Starting ${batches.length}-item batch ${batchId}...`, {
      retryConfig: effectiveRetryOptions
    });

    const accumulatedResults = [];
    let totalFailures = 0;

    for (let i = 0; i < batches.length; i++) {
      logger.info(
        `Executing item #${i + 1} (of ${batches.length}) for batch ${batchId}`
      );

      try {
        const results = await this.executeBatch(batches[i], effectiveRetryOptions);

        const successCount = results.filter(r => r !== undefined).length;
        const failureCount = batches[i].length - successCount;
        totalFailures += failureCount;

        accumulatedResults.push(...results);

        if (failureCount > 0) {
          logger.warn(`Batch ${i + 1} completed with ${failureCount} failures out of ${batches[i].length} requests`);
        }
      } catch (error) {
        logger.error(`Batch execution failed for batch ${batchId} item ${i + 1}`, {
          error: error.message,
        });
        throw error;
      }
    }

    logger.info(`Finishing batch ${batchId}...`, {
      totalRequests: accumulatedResults.length,
      totalFailures: totalFailures,
      successRate: ((accumulatedResults.length - totalFailures) / accumulatedResults.length * 100).toFixed(2) + '%'
    });

    return accumulatedResults;
  }

  async getBlockchainInfo() {
    return this.getRpcData("getblockchaininfo");
  }

  async getMempoolInfo() {
    return this.getRpcData("getmempoolinfo");
  }

  async getMiningInfo() {
    return this.getRpcData("getmininginfo");
  }

  async getRawMempool() {
    return this.getRpcDataWithParams("getrawmempool", [false]);
  }

  async getChainTxStats(blockCount) {
    return this.getRpcDataWithParams("getchaintxstats", [blockCount]);
  }

  async getBlockByHeight(blockHeight) {
    const results = await this.getBlocksByHeight([blockHeight]);
    if (results && results.length > 0) {
      return results[0];
    }
    return null;
  }

  async getBlocksByHeight(blockHeights) {
    logger.debug("getBlocksByHeight: " + blockHeights);
    const batch = [];
    for (let i = 0; i < blockHeights.length; i++) {
      batch.push({
        method: "getblockhash",
        parameters: [blockHeights[i]],
      });
    }

    try {
      const responses = await this.executeBatch(batch);
      const blockHashes = [];
      const failedHeights = [];

      for (let i = 0; i < responses.length; i++) {
        if (responses[i] != null) {
          blockHashes.push(responses[i]);
        } else {
          failedHeights.push(blockHeights[i]);
        }
      }

      if (failedHeights.length > 0) {
        logger.warn(`Failed to get block hashes for heights: ${failedHeights.join(', ')}`);
      }

      if (blockHashes.length === 0) {
        throw new Error("Failed to get any block hashes");
      }

      return await this.getBlocksByHash(blockHashes);
    } catch (error) {
      logger.error("Error in getBlocksByHeight", {
        error: error.message,
        blockHeights: blockHeights
      });
      throw error;
    }
  }

  async getBlockByHash(blockHash) {
    const results = await this.getBlocksByHash([blockHash]);
    if (results && results.length > 0) {
      return results[0];
    }
    return null;
  }

  async getBlocksByHash(blockHashes) {
    logger.debug("rpc.getBlocksByHash: " + blockHashes);
    const batch = [];
    for (let i = 0; i < blockHashes.length; i++) {
      batch.push({
        method: "getblock",
        parameters: [blockHashes[i], 1],
      });
    }

    try {
      const responses = await this.executeBatch(batch);
      const blocks = responses.filter((item) => item && item.tx);
      const coinbaseTxids = [];
      for (let i = 0; i < blocks.length; i++) {
        coinbaseTxids.push(blocks[i].tx[0]);
      }
      const coinbaseTxs = await this.getRawTransactions(coinbaseTxids);
      for (let i = 0; i < blocks.length; i++) {
        blocks[i].coinbaseTx = coinbaseTxs[i];
        blocks[i].totalFees = getBlockTotalFeesFromCoinbaseTxAndBlockHeight(
          coinbaseTxs[i],
          blocks[i].height
        );
        blocks[i].miner = getMinerFromCoinbaseTx(coinbaseTxs[i]);
      }

      return blocks;
    } catch (error) {
      logger.error("Error in getBlocksByHash", { error: error.message });
      throw error;
    }
  }

  async getRawTransaction(txid) {
    const results = await this.getRawTransactions([txid]);
    if (results && results.length > 0) {
      if (results[0] && results[0].txid) {
        return results[0];
      }
    }
    return null;
  }

  async getRawTransactionHex(txid) {
    const results = await this.getRawTransactionsHex([txid]);
    if (results && results.length > 0) {
      return results[0];
    }
    return null;
  }

  async getRawTransactionsHex(txids) {
    if (!txids || txids.length === 0) {
      return [];
    }

    const results = new Array(txids.length);
    const requests = [];
    const indexMap = new Map();

    for (let i = 0; i < txids.length; i++) {
      const txid = txids[i];
      if (txid) {
        const requestIndex = requests.length;
        indexMap.set(requestIndex, i);
        requests.push({
          method: "getrawtransaction",
          parameters: [txid, 0],
        });
      }
    }

    try {
      if (requests.length > 0) {
        const optimalBatchSize = this.calculateOptimalBatchSize(requests.length);
        const batches = splitArrayIntoChunks(requests, optimalBatchSize);
        const batchResults = await this.executeBatchesSequentially(batches);

        let resultIndex = 0;
        for (const result of batchResults) {
          if (result) {
            const originalIndex = indexMap.get(resultIndex);
            results[originalIndex] = result;
          }
          resultIndex++;
        }
      }

      return results;
    } catch (error) {
      logger.error("Error in getRawTransactionsHex", {
        error: error.message,
        txids: txids.length > 10 ? `${txids.slice(0, 10).join(', ')}... (${txids.length} total)` : txids.join(', ')
      });
      throw error;
    }
  }

  async getRawTransactions(txids) {
    const genesisCoinbaseTransactionId =
      coinConfig.genesisCoinbaseTransactionId;
    const genesisCoinbaseTransaction = coinConfig.genesisCoinbaseTransaction;

    if (!txids || txids.length === 0) {
      return [];
    }

    const results = new Array(txids.length);
    const requests = [];
    const indexMap = new Map();

    for (let i = 0; i < txids.length; i++) {
      const txid = txids[i];
      if (txid) {
        if (
          genesisCoinbaseTransactionId &&
          txid === genesisCoinbaseTransactionId
        ) {
          results[i] = genesisCoinbaseTransaction;
        } else {
          const requestIndex = requests.length;
          indexMap.set(requestIndex, i);
          requests.push({
            method: "getrawtransaction",
            parameters: [txid, 1],
          });
        }
      }
    }

    try {
      if (requests.length > 0) {
        const optimalBatchSize = this.calculateOptimalBatchSize(requests.length);
        const batches = splitArrayIntoChunks(requests, optimalBatchSize);
        const batchResults = await this.executeBatchesSequentially(batches);

        let resultIndex = 0;
        for (const result of batchResults) {
          if (result) {
            const originalIndex = indexMap.get(resultIndex);
            results[originalIndex] = result;
          }
          resultIndex++;
        }
      }

      return results;
    } catch (error) {
      logger.error("Error in getRawTransactions", { error: error.message });
      throw error;
    }
  }

  setBatchRetryConfig(retryConfig) {
    this.batchRetryConfig = {
      ...this.batchRetryConfig,
      ...retryConfig
    };

    logger.info('Batch retry configuration updated', {
      newConfig: this.batchRetryConfig
    });
  }

  getBatchRetryConfig() {
    return { ...this.batchRetryConfig };
  }

  async executeBatchWithCustomRetry(batch, customRetryOptions) {
    const retryOptions = {
      ...this.batchRetryConfig,
      ...customRetryOptions
    };

    logger.debug('Executing batch with custom retry options', {
      batchSize: batch.length,
      retryOptions
    });

    return await this.executeBatch(batch, retryOptions);
  }

  addressToScriptHash(address) {
    const script = bitcoin.payments.p2pkh({ address }).output;
    const hash = crypto.createHash('sha256').update(script).digest();
    return hash.reverse().toString('hex');
  }

  async getAddressBalance(address) {
    try {
      const scriptHash = this.addressToScriptHash(address);
      const result = await this.callRpcMethod("electrumx-rpc", "blockchain.scripthash.get_balance", [scriptHash]);
      return {
        ...result,
        address
      };
    } catch (error) {
      logger.error("Error getting address balance", {
        address,
        error: error.message
      });
      throw error;
    }
  }

  async getAddressTransactionIds(address) {
    try {
      const scriptHash = this.addressToScriptHash(address);
      const history = await this.callRpcMethod("electrumx-rpc", "blockchain.scripthash.get_history", [scriptHash]);
      if (!history || !Array.isArray(history)) {
        return {
          address,
          txIds: [],
          totalTransactions: 0
        };
      }
      const txIds = history.map(item => item.tx_hash);
      return {
        address,
        scriptHash,
        txIds,
        totalTransactions: txIds.length
      };
    } catch (error) {
      logger.error("Error getting address transaction IDs", {
        address,
        error: error.message
      });
      throw error;
    }
  }

  getServiceStatus() {
    return {
      initialized: this.initialized,
      rpcServices: Object.keys(this.rpcClients).map((name) => ({
        name,
        url: this.rpcClients[name].config.url,
      })),
      batchRetryConfig: this.batchRetryConfig,
      performanceConfig: this.performanceConfig,
      circuitBreaker: {
        ...this.circuitBreaker,
        isOpen: this.isCircuitBreakerOpen()
      },
    };
  }

  async shutdown() {
    logger.info('ServiceManager: Starting shutdown process...');

    try {
      this.rpcClients = {};
      this.initialized = false;

      logger.info('ServiceManager: Shutdown completed successfully');
    } catch (error) {
      logger.error('ServiceManager: Error during shutdown', {
        error: error.message,
        stack: error.stack
      });
    }
  }
}

const serviceManager = new ServiceManager();

export default serviceManager;
