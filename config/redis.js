import Redis from 'ioredis';
import dotenv from 'dotenv';

import logger from './logger.js';
import { getConnectionConfig } from './connectionConfig.js';

dotenv.config();

const redisConfig = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || '',
  db: parseInt(process.env.REDIS_DB) || 0,
  keyPrefix: process.env.REDIS_KEY_PREFIX || 'tbc-explorer:',

  lazyConnect: true,
  connectTimeout: 10000,
  commandTimeout: 5000,
  keepAlive: 30000,
  family: 4,

  retryDelayOnFailover: 100,
  maxRetriesPerRequest: 3,
  enableOfflineQueue: false,

  enableReadyCheck: false,
  showFriendlyErrorStack: true,
  autoResubscribe: true,
  autoResendUnfulfilledCommands: true,
};

const connectionConfig = getConnectionConfig();
const POOL_SIZE = connectionConfig.redis.poolSize;
let connectionPool = [];
let poolIndex = 0;

let connectionStats = {
  connects: 0,
  disconnects: 0,
  errors: 0,
  lastConnectTime: null,
  lastDisconnectTime: null,
  lastErrorTime: null,
  poolSize: POOL_SIZE,
  activeConnections: 0
};

const createRedisPool = () => {
  logger.info('Creating Redis connection pool with optimized configuration', {
    poolSize: POOL_SIZE,
    processType: connectionConfig.processType,
    workerId: connectionConfig.cluster.workerId,
    originalPoolSize: parseInt(process.env.REDIS_POOL_SIZE) || 20
  });

  for (let i = 0; i < POOL_SIZE; i++) {
    const redis = new Redis(redisConfig);
    redis.on('connect', () => {
      connectionStats.connects++;
      connectionStats.activeConnections++;
      connectionStats.lastConnectTime = new Date().toISOString();
      if (i === 0 || i === POOL_SIZE - 1) {
        logger.debug(`Redis connection ${i} established`, {
          poolIndex: i,
          connects: connectionStats.connects,
          poolSize: POOL_SIZE
        });
      }
    });

    redis.on('ready', () => {
      if (i === 0 || i === POOL_SIZE - 1) {
        logger.debug(`Redis connection ${i} ready`, {
          poolIndex: i,
          status: redis.status
        });
      }
    });

    redis.on('error', (err) => {
      connectionStats.errors++;
      connectionStats.lastErrorTime = new Date().toISOString();
      logger.error(`Redis connection ${i} error`, {
        error: err.message,
        totalErrors: connectionStats.errors
      });
    });

    redis.on('close', () => {
      connectionStats.disconnects++;
      connectionStats.activeConnections = Math.max(0, connectionStats.activeConnections - 1);
      connectionStats.lastDisconnectTime = new Date().toISOString();
    });

    connectionPool.push(redis);
  }

  logger.info('Redis connection pool created', {
    poolSize: POOL_SIZE,
    totalConnections: connectionPool.length
  });
};

const getRedisConnection = () => {
  if (connectionPool.length === 0) {
    throw new Error('Redis connection pool not initialized');
  }

  const connection = connectionPool[poolIndex];
  poolIndex = (poolIndex + 1) % connectionPool.length;

  return connection;
};

const redis = {
  get: (...args) => getRedisConnection().get(...args),
  set: (...args) => getRedisConnection().set(...args),
  del: (...args) => getRedisConnection().del(...args),
  exists: (...args) => getRedisConnection().exists(...args),
  lpush: (...args) => getRedisConnection().lpush(...args),
  rpush: (...args) => getRedisConnection().rpush(...args),
  lpop: (...args) => getRedisConnection().lpop(...args),
  rpop: (...args) => getRedisConnection().rpop(...args),
  llen: (...args) => getRedisConnection().llen(...args),
  lrange: (...args) => getRedisConnection().lrange(...args),
  keys: (...args) => getRedisConnection().keys(...args),
  call: (...args) => getRedisConnection().call(...args),

  get status() {
    return connectionPool.length > 0 ? connectionPool[0].status : 'disconnected';
  },

  get options() {
    return connectionPool.length > 0 ? connectionPool[0].options : {};
  }
};

let monitoringInterval = null;

const startMonitoring = () => {
  if (monitoringInterval) {
    clearInterval(monitoringInterval);
  }

  monitoringInterval = setInterval(async () => {
    try {
      if (connectionPool.length > 0 && connectionPool[0].status === 'ready') {
        const info = await connectionPool[0].info('memory');
        const memoryInfo = {};

        info.split('\n').forEach(line => {
          if (line.includes(':')) {
            const [key, value] = line.split(':');
            if (key.includes('memory') || key.includes('clients')) {
              memoryInfo[key.trim()] = value.trim();
            }
          }
        });

        const clientList = await connectionPool[0].client('list');
        const clientCount = clientList.split('\n').filter(line => line.trim()).length;

        const errorRate = connectionStats.errors / Math.max(connectionStats.connects, 1);
        const hasWarning = clientCount > 500 || errorRate > 0.1;

        if (hasWarning) {
          logger.info('Redis pool health check', {
            poolSize: connectionPool.length,
            activeConnections: connectionStats.activeConnections,
            clientConnections: clientCount,
            memoryUsed: memoryInfo.used_memory_human,
            memoryPeak: memoryInfo.used_memory_peak_human,
            connectedClients: memoryInfo.connected_clients,
            stats: connectionStats
          });
        }

        const warningThreshold = Math.max(500, POOL_SIZE * 5);
        if (clientCount > warningThreshold) {
          logger.warn('High Redis client connection count', {
            clientCount,
            threshold: warningThreshold,
            poolSize: connectionPool.length
          });
        }

        if (connectionStats.errors > 5) {
          const avgErrorRate = connectionStats.errors / Math.max(connectionStats.connects, 1);
          if (avgErrorRate > 0.05) {
            logger.warn('Redis connection pool may be undersized', {
              poolSize: connectionPool.length,
              errorRate: (avgErrorRate * 100).toFixed(2) + '%',
              suggestion: 'Consider increasing REDIS_POOL_SIZE if errors are due to connection exhaustion'
            });
          }
        }

        if (errorRate > 0.1) {
          logger.warn('High Redis error rate', {
            errorRate: (errorRate * 100).toFixed(2) + '%',
            errors: connectionStats.errors,
            connects: connectionStats.connects
          });
        }
      }
    } catch (error) {
      logger.debug('Redis monitoring failed', { error: error.message });
    }
  }, parseInt(process.env.REDIS_MONITOR_INTERVAL) || 300000);
};

const connectRedis = async () => {
  try {
    createRedisPool();
    const connectPromises = connectionPool.map(redis => redis.connect());
    await Promise.all(connectPromises);

    startMonitoring();

    logger.info('Redis connection pool connected successfully', {
      host: redisConfig.host,
      port: redisConfig.port,
      keyPrefix: redisConfig.keyPrefix,
      poolSize: connectionPool.length,
      status: connectionPool[0].status
    });
  } catch (error) {
    logger.error('Redis connection pool failed', {
      error: error.message,
      stack: error.stack,
    });
    throw error;
  }
};

const disconnectRedis = async () => {
  try {
    if (monitoringInterval) {
      clearInterval(monitoringInterval);
      monitoringInterval = null;
    }

    logger.info('Redis connection pool disconnecting', {
      finalStats: connectionStats,
      poolSize: connectionPool.length
    });

    const disconnectPromises = connectionPool.map((redis, index) => {
      logger.debug(`Disconnecting Redis connection ${index}`);
      return redis.disconnect();
    });

    await Promise.all(disconnectPromises);
    connectionPool = [];
    poolIndex = 0;

    logger.info('Redis connection pool disconnected successfully');
  } catch (error) {
    logger.error('Redis connection pool disconnect failed', {
      error: error.message,
      stack: error.stack,
    });
  }
};

const getRedisStats = () => ({
  ...connectionStats,
  status: redis.status,
  poolSize: connectionPool.length,
  config: {
    host: redisConfig.host,
    port: redisConfig.port,
    keyPrefix: redisConfig.keyPrefix,
    poolSize: POOL_SIZE
  }
});

export { redis, connectRedis, disconnectRedis, getRedisStats };