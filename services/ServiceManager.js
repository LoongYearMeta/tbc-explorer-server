import axios from "axios";
import net from "net";
import dotenv from "dotenv";

import servicesConfig from "../config/services.js";
import logger from "../config/logger.js";
import { generateRandomString, splitArrayIntoChunks } from "../lib/util.js";
import { coinConfig } from "../lib/coin.js";
import {
  getBlockTotalFeesFromCoinbaseTxAndBlockHeight,
  getMinerFromCoinbaseTx,
  addressToScriptHash
} from "../lib/util.js";
import { getConnectionConfig } from "../config/connectionConfig.js";

dotenv.config();

class ServiceManager {
  constructor() {
    this.rpcClients = {};
    this.initialized = false;
    this.activeTcpConnections = new Set();

    const connectionConfig = getConnectionConfig();
    this.electrumxPool = {
      connections: [],
      maxConnections: connectionConfig.electrumx.maxConnections,
      minConnections: connectionConfig.electrumx.minConnections,
      currentIndex: 0,
      host: null,
      port: null,
      expansionBatchSize: connectionConfig.electrumx.expansionBatchSize,
      processType: connectionConfig.processType,
      workerId: connectionConfig.cluster.workerId,
      stats: {
        totalRequests: 0,
        poolHits: 0,
        poolMisses: 0,
        connectionErrors: 0
      }
    };

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

    this.connectionCleanupInterval = setInterval(() => {
      this.cleanupStaleConnections();
    }, 30000);
  }

  async createElectrumxConnection(host, port) {
    return new Promise((resolve, reject) => {
      const socket = new net.Socket();
      socket.setKeepAlive(true, 30000);
      socket.setTimeout(30000);

      const connectionInfo = {
        socket,
        host,
        port,
        connected: false,
        busy: false,
        createdAt: Date.now(),
        lastUsed: Date.now(),
        requestQueue: []
      };

      socket.on('connect', () => {
        connectionInfo.connected = true;
        logger.debug(`ElectrumX connection established to ${host}:${port}`);
        resolve(connectionInfo);
      });

      socket.on('error', (error) => {
        connectionInfo.connected = false;
        logger.error(`ElectrumX connection error: ${error.message}`);
        reject(error);
      });

      socket.on('close', () => {
        connectionInfo.connected = false;
        logger.debug(`ElectrumX connection closed to ${host}:${port}`);
      });

      socket.connect(port, host);
    });
  }

  async initializeElectrumxPool(host, port) {
    this.electrumxPool.host = host;
    this.electrumxPool.port = port;

    logger.info(`Initializing ElectrumX connection pool to ${host}:${port} with optimized configuration`, {
      processType: this.electrumxPool.processType,
      workerId: this.electrumxPool.workerId,
      minConnections: this.electrumxPool.minConnections,
      maxConnections: this.electrumxPool.maxConnections,
      expansionBatchSize: this.electrumxPool.expansionBatchSize,
      originalMaxConnections: parseInt(process.env.ELECTRUMX_MAX_CONNECTIONS) || 140
    });

    const initialBatchSize = 20;
    const maxIncrementalBatch = 10;
    let targetConnections = Math.min(this.electrumxPool.minConnections, 130);

    logger.info(`Creating connections gradually. Target: ${targetConnections} connections`);

    logger.info(`Creating initial batch of ${initialBatchSize} connections`);
    const initialPromises = [];
    for (let i = 0; i < initialBatchSize; i++) {
      initialPromises.push(this.createElectrumxConnection(host, port));
    }

    const initialResults = await Promise.allSettled(initialPromises);
    let successCount = 0;
    let failCount = 0;

    initialResults.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        this.electrumxPool.connections.push(result.value);
        successCount++;
        logger.debug(`Created initial ElectrumX connection ${index + 1}/${initialBatchSize}`);
      } else {
        failCount++;
        logger.error(`Failed to create initial ElectrumX connection ${index + 1}: ${result.reason.message}`);
      }
    });

    logger.info(`Initial batch completed: ${successCount} success, ${failCount} failed. Total: ${this.electrumxPool.connections.length}`);

    if (failCount > initialBatchSize * 0.3) {
      targetConnections = Math.min(targetConnections, this.electrumxPool.connections.length + 50);
      logger.warn(`High failure rate detected. Reducing target connections to ${targetConnections}`);
    }

    while (this.electrumxPool.connections.length < targetConnections) {
      const remaining = targetConnections - this.electrumxPool.connections.length;
      const currentBatchSize = Math.min(maxIncrementalBatch, remaining);

      logger.info(`Creating incremental batch ${this.electrumxPool.connections.length + 1}-${this.electrumxPool.connections.length + currentBatchSize}`);

      const batchPromises = [];
      for (let i = 0; i < currentBatchSize; i++) {
        batchPromises.push(this.createElectrumxConnection(host, port));
      }

      const batchResults = await Promise.allSettled(batchPromises);
      let batchSuccessCount = 0;
      let batchFailCount = 0;

      batchResults.forEach((result, index) => {
        if (result.status === 'fulfilled') {
          this.electrumxPool.connections.push(result.value);
          batchSuccessCount++;
        } else {
          batchFailCount++;
          logger.error(`Failed to create incremental connection: ${result.reason.message}`);
        }
      });

      logger.info(`Incremental batch completed: ${batchSuccessCount} success, ${batchFailCount} failed. Total: ${this.electrumxPool.connections.length}/${targetConnections}`);

      if (batchFailCount > currentBatchSize * 0.5) {
        logger.warn(`High failure rate in incremental batch. Stopping at ${this.electrumxPool.connections.length} connections`);
        break;
      }

      await new Promise(resolve => setTimeout(resolve, 100));
    }

    logger.info(`ElectrumX connection pool initialized with ${this.electrumxPool.connections.length}/${this.electrumxPool.minConnections} connections`);
  }

  async getAvailableElectrumxConnection() {
    const availableConnections = this.electrumxPool.connections.filter(conn => conn.connected && !conn.busy);

    if (availableConnections.length > 0) {
      this.electrumxPool.currentIndex = (this.electrumxPool.currentIndex + 1) % availableConnections.length;
      return availableConnections[this.electrumxPool.currentIndex];
    }

    if (this.electrumxPool.connections.length < this.electrumxPool.maxConnections) {
      const remainingSlots = this.electrumxPool.maxConnections - this.electrumxPool.connections.length;
      const batchSize = Math.min(this.electrumxPool.expansionBatchSize, remainingSlots);

      logger.info(`Expanding ElectrumX connection pool: creating ${batchSize} new connections (${this.electrumxPool.connections.length}/${this.electrumxPool.maxConnections})`);

      const connectionPromises = [];
      for (let i = 0; i < batchSize; i++) {
        connectionPromises.push(this.createElectrumxConnection(this.electrumxPool.host, this.electrumxPool.port));
      }

      try {
        const results = await Promise.allSettled(connectionPromises);
        const newConnections = [];

        results.forEach((result, index) => {
          if (result.status === 'fulfilled') {
            this.electrumxPool.connections.push(result.value);
            newConnections.push(result.value);
            logger.debug(`Created additional ElectrumX connection ${index + 1}/${batchSize}`);
          } else {
            logger.error(`Failed to create additional ElectrumX connection ${index + 1}/${batchSize}: ${result.reason.message}`);
            this.electrumxPool.stats.connectionErrors++;
          }
        });

        logger.info(`Successfully expanded connection pool by ${newConnections.length} connections. Total: ${this.electrumxPool.connections.length}/${this.electrumxPool.maxConnections}`);

        if (newConnections.length > 0) {
          return newConnections[0];
        }
      } catch (error) {
        logger.error(`Failed to expand ElectrumX connection pool: ${error.message}`);
        this.electrumxPool.stats.connectionErrors++;
      }
    }

    return null;
  }

  async callTcpRpcWithPool(host, port, method, params, timeout = 10000) {
    this.electrumxPool.stats.totalRequests++;

    const connection = await this.getAvailableElectrumxConnection();

    if (!connection) {
      logger.debug('No available pooled connection, creating new one');
      this.electrumxPool.stats.poolMisses++;
      return this.callTcpRpc(host, port, method, params, timeout);
    }

    this.electrumxPool.stats.poolHits++;

    return new Promise((resolve, reject) => {
      connection.busy = true;
      connection.lastUsed = Date.now();

      const payload = {
        jsonrpc: "2.0",
        method: method,
        params: params,
        id: generateRandomString(16),
      };

      const message = JSON.stringify(payload) + '\n';
      let responseData = '';
      let resolved = false;

      const cleanup = () => {
        connection.busy = false;
        resolved = true;
      };

      const timeoutId = setTimeout(() => {
        if (!resolved) {
          cleanup();
          reject(new Error('TCP connection timeout'));
        }
      }, timeout);

      const dataHandler = (data) => {
        responseData += data.toString();
        if (responseData.includes('\n')) {
          try {
            const response = JSON.parse(responseData.trim());
            clearTimeout(timeoutId);
            cleanup();

            connection.socket.removeListener('data', dataHandler);
            connection.socket.removeListener('error', errorHandler);

            if (response.error) {
              reject(new Error(`RPC Error: ${response.error.message} (Code: ${response.error.code})`));
            } else {
              resolve(response.result);
            }
          } catch (error) {
            clearTimeout(timeoutId);
            cleanup();
            connection.socket.removeListener('data', dataHandler);
            connection.socket.removeListener('error', errorHandler);
            reject(new Error(`Failed to parse response: ${error.message}`));
          }
        }
      };

      const errorHandler = (error) => {
        clearTimeout(timeoutId);
        cleanup();
        connection.socket.removeListener('data', dataHandler);
        connection.socket.removeListener('error', errorHandler);
        connection.connected = false;
        reject(error);
      };

      connection.socket.on('data', dataHandler);
      connection.socket.on('error', errorHandler);

      try {
        connection.socket.write(message);
      } catch (error) {
        clearTimeout(timeoutId);
        cleanup();
        connection.socket.removeListener('data', dataHandler);
        connection.socket.removeListener('error', errorHandler);
        reject(error);
      }
    });
  }

  async initialize() {
    try {
      logger.info("Initializing external service connections...");
      await this.initializeRpcClients();

      const electrumxService = this.rpcClients['electrumx-rpc'];
      if (electrumxService && electrumxService.config.protocol === 'tcp') {
        await this.initializeElectrumxPool(electrumxService.config.host, electrumxService.config.port);
      }

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

    if (totalRequests <= defaultBatchSize) {
      return totalRequests;
    }

    if (totalRequests <= 200) {
      return Math.min(totalRequests, maxBatchSize);
    }

    if (totalRequests <= 500) {
      return Math.min(150, maxBatchSize);
    }

    if (totalRequests <= 1000) {
      return Math.min(200, maxBatchSize);
    }

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
      const connectionId = `${host}:${port}:${Date.now()}:${Math.random()}`;

      this.activeTcpConnections.add({
        socket: client,
        connectionId,
        createdAt: Date.now(),
        host,
        port
      });

      const payload = {
        jsonrpc: "2.0",
        method: method,
        params: params,
        id: generateRandomString(16),
      };

      const message = JSON.stringify(payload) + '\n';
      let responseData = '';
      let isResolved = false;

      client.setTimeout(timeout);

      const cleanup = () => {
        if (!isResolved) {
          isResolved = true;
          this.activeTcpConnections.forEach(conn => {
            if (conn.connectionId === connectionId) {
              this.activeTcpConnections.delete(conn);
            }
          });

          try {
            if (!client.destroyed) {
              client.destroy();
            }
          } catch (error) {
            logger.debug('Error destroying TCP client', { error: error.message });
          }
        }
      };

      client.on('data', (data) => {
        responseData += data.toString();
        if (responseData.includes('\n')) {
          try {
            const response = JSON.parse(responseData.trim());
            cleanup();

            if (response.error) {
              reject(new Error(`RPC Error: ${response.error.message} (Code: ${response.error.code})`));
            } else {
              resolve(response.result);
            }
          } catch (error) {
            cleanup();
            reject(new Error(`Failed to parse response: ${error.message}`));
          }
        }
      });

      client.on('error', (error) => {
        cleanup();
        reject(error);
      });

      client.on('timeout', () => {
        cleanup();
        reject(new Error('TCP connection timeout'));
      });

      client.on('close', () => {
        cleanup();
      });

      try {
        client.connect(port, host, () => {
          client.write(message);
        });
      } catch (error) {
        cleanup();
        reject(error);
      }
    });
  }

  cleanupStaleConnections() {
    const now = Date.now();
    const maxAge = 60000;
    let cleanedCount = 0;

    this.activeTcpConnections.forEach(conn => {
      if (now - conn.createdAt > maxAge) {
        try {
          if (!conn.socket.destroyed) {
            conn.socket.destroy();
            cleanedCount++;
          }
        } catch (error) {
          logger.debug('Error cleaning up stale TCP connection', { error: error.message });
        }
        this.activeTcpConnections.delete(conn);
      }
    });

    const brokenConnections = this.electrumxPool.connections.filter(conn => !conn.connected);
    if (brokenConnections.length > 0) {
      logger.debug(`Cleaning up ${brokenConnections.length} broken ElectrumX pool connections`);
      this.electrumxPool.connections = this.electrumxPool.connections.filter(conn => conn.connected);
    }

    if (cleanedCount > 0) {
      logger.debug(`Cleaned up ${cleanedCount} stale TCP connections`);
    }
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
            result = await this.callTcpRpcWithPool(config.host, config.port, method, params, config.timeout);
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

  async getNetworkInfo() {
    return this.getRpcData("getnetworkinfo");
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

  async getAddressBalance(address) {
    try {
      const scriptHash = addressToScriptHash(address);
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
      const scriptHash = addressToScriptHash(address);
      const history = await this.callRpcMethod("electrumx-rpc", "blockchain.scripthash.get_history", [scriptHash]);

      if (!history || !Array.isArray(history)) {
        return {
          address,
          scriptHash,
          transactions: [],
          totalTransactions: 0
        };
      }

      const sortedHistory = history.map(item => ({
        tx_hash: item.tx_hash,
        height: item.height
      })).sort((a, b) => {
        if (a.height === 0 && b.height !== 0) return -1;
        if (a.height !== 0 && b.height === 0) return 1;

        if (a.height === 0 && b.height === 0) {
          return b.tx_hash.localeCompare(a.tx_hash);
        }

        if (b.height !== a.height) {
          return b.height - a.height;
        }

        return b.tx_hash.localeCompare(a.tx_hash);
      });

      return {
        address,
        scriptHash,
        transactions: sortedHistory,
        totalTransactions: sortedHistory.length
      };
    } catch (error) {
      logger.error("Error getting address transaction IDs", {
        address,
        error: error.message
      });
      throw error;
    }
  }

  async validateAddress(address) {
    try {
      const result = await this.callRpcMethod("node-rpc", "validateaddress", [address]);
      return {
        address,
        ...result
      };
    } catch (error) {
      logger.error("Error validating address", {
        address,
        error: error.message
      });
      throw error;
    }
  }

  async getAddressDetails(address) {
    try {
      const [validation, balance, transactionInfo] = await Promise.all([
        this.validateAddress(address),
        this.getAddressBalance(address),
        this.getAddressTransactionIds(address)
      ]);

      return {
        address,
        validation,
        balance: {
          confirmed: balance.confirmed || 0,
          unconfirmed: balance.unconfirmed || 0
        },
        transactions: transactionInfo.transactions,
        totalTransactions: transactionInfo.totalTransactions,
        scriptHash: transactionInfo.scriptHash
      };
    } catch (error) {
      logger.error("Error getting address details", {
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
      electrumxPool: {
        totalConnections: this.electrumxPool.connections.length,
        availableConnections: this.electrumxPool.connections.filter(conn => conn.connected && !conn.busy).length,
        busyConnections: this.electrumxPool.connections.filter(conn => conn.connected && conn.busy).length,
        brokenConnections: this.electrumxPool.connections.filter(conn => !conn.connected).length,
        host: this.electrumxPool.host,
        port: this.electrumxPool.port,
        minConnections: this.electrumxPool.minConnections,
        maxConnections: this.electrumxPool.maxConnections,
        expansionBatchSize: this.electrumxPool.expansionBatchSize,
        utilizationRate: this.electrumxPool.connections.length > 0 ?
          (this.electrumxPool.connections.filter(conn => conn.connected && conn.busy).length / this.electrumxPool.connections.length * 100).toFixed(1) + '%' : '0%',
        stats: {
          ...this.electrumxPool.stats,
          hitRate: this.electrumxPool.stats.totalRequests > 0 ?
            (this.electrumxPool.stats.poolHits / this.electrumxPool.stats.totalRequests * 100).toFixed(2) + '%' : '0%'
        }
      }
    };
  }

  async shutdown() {
    logger.info('ServiceManager: Starting shutdown process...');

    try {
      if (this.connectionCleanupInterval) {
        clearInterval(this.connectionCleanupInterval);
        this.connectionCleanupInterval = null;
      }

      logger.info(`ServiceManager: Closing ${this.activeTcpConnections.size} active TCP connections`);
      this.activeTcpConnections.forEach(conn => {
        try {
          if (!conn.socket.destroyed) {
            conn.socket.destroy();
          }
        } catch (error) {
          logger.debug('Error closing TCP connection during shutdown', { error: error.message });
        }
      });
      this.activeTcpConnections.clear();

      logger.info(`ServiceManager: Closing ${this.electrumxPool.connections.length} ElectrumX pool connections`);
      this.electrumxPool.connections.forEach(conn => {
        try {
          if (conn.socket && !conn.socket.destroyed) {
            conn.socket.destroy();
          }
        } catch (error) {
          logger.debug('Error closing ElectrumX pool connection during shutdown', { error: error.message });
        }
      });
      this.electrumxPool.connections = [];

      for (const [serviceName, serviceData] of Object.entries(this.rpcClients)) {
        try {
          const { client, config } = serviceData;

          if (config.type === "json-rpc" && config.protocol !== "tcp") {
            if (client && typeof client.defaults === 'object') {
              if (client.defaults.timeout) {
                client.defaults.timeout = 100;
              }
            }
          }

          logger.debug(`ServiceManager: Cleaned up service client: ${serviceName}`);
        } catch (error) {
          logger.warn(`ServiceManager: Error cleaning up service client ${serviceName}`, {
            error: error.message
          });
        }
      }

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
