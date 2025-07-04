import Redis from 'ioredis';
import dotenv from 'dotenv';

import logger from './logger.js';

dotenv.config();

const redisConfig = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || '4e129bd6ad204840a829d00d1b4fcb8e',
  retryDelayOnFailover: 100,
  enableReadyCheck: false,
  maxRetriesPerRequest: null,
  family: 4,
  keyPrefix: process.env.REDIS_KEY_PREFIX || 'tbc-explorer:',
  lazyConnect: true,
  keepAlive: 30000,
  connectTimeout: 10000,
  commandTimeout: 5000,
  retryCount: 3,
  retryDelayOnClusterDown: 300,
  retryDelayOnFailover: 100,
  enableOfflineQueue: false,
  readOnly: false,
  showFriendlyErrorStack: true,
};

const redis = new Redis(redisConfig);

redis.on('connect', () => {
  logger.info('Redis connection established');
});

redis.on('ready', () => {
  logger.info('Redis connection ready');
});

redis.on('error', (err) => {
  logger.error('Redis connection error', {
    error: err.message,
    stack: err.stack,
  });
});

redis.on('close', () => {
  logger.warn('Redis connection closed');
});

redis.on('reconnecting', () => {
  logger.info('Redis reconnecting...');
});

redis.on('end', () => {
  logger.warn('Redis connection ended');
});

const connectRedis = async () => {
  try {
    await redis.connect();
    logger.info('Redis connected successfully', {
      host: redisConfig.host,
      port: redisConfig.port,
      keyPrefix: redisConfig.keyPrefix,
    });
  } catch (error) {
    logger.error('Redis connection failed', {
      error: error.message,
      stack: error.stack,
    });
    throw error;
  }
};

const disconnectRedis = async () => {
  try {
    redis.disconnect();
    logger.info('Redis disconnected successfully');
  } catch (error) {
    logger.error('Redis disconnect failed', {
      error: error.message,
      stack: error.stack,
    });
  }
};

export { redis, connectRedis, disconnectRedis };