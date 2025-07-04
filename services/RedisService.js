import { redis } from '../config/redis.js';
import logger from '../config/logger.js';

class RedisService {
    constructor() {
        this.redis = redis;
    }

    async set(key, value, ttl = null) {
        try {
            if (ttl) {
                return await this.redis.set(key, value, 'EX', ttl);
            } else {
                return await this.redis.set(key, value);
            }
        } catch (error) {
            logger.error('Redis SET operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async get(key) {
        try {
            return await this.redis.get(key);
        } catch (error) {
            logger.error('Redis GET operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async del(key) {
        try {
            return await this.redis.del(key);
        } catch (error) {
            logger.error('Redis DEL operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async exists(key) {
        try {
            const result = await this.redis.exists(key);
            return result === 1;
        } catch (error) {
            logger.error('Redis EXISTS operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async setJSON(key, obj, ttl = null) {
        try {
            const value = JSON.stringify(obj);
            return await this.set(key, value, ttl);
        } catch (error) {
            logger.error('Redis setJSON operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async getJSON(key) {
        try {
            const value = await this.get(key);
            return value ? JSON.parse(value) : null;
        } catch (error) {
            logger.error('Redis getJSON operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async lpush(key, value) {
        try {
            return await this.redis.lpush(key, value);
        } catch (error) {
            logger.error('Redis LPUSH operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async rpush(key, value) {
        try {
            return await this.redis.rpush(key, value);
        } catch (error) {
            logger.error('Redis RPUSH operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async lpop(key) {
        try {
            return await this.redis.lpop(key);
        } catch (error) {
            logger.error('Redis LPOP operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async rpop(key) {
        try {
            return await this.redis.rpop(key);
        } catch (error) {
            logger.error('Redis RPOP operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async llen(key) {
        try {
            return await this.redis.llen(key);
        } catch (error) {
            logger.error('Redis LLEN operation failed', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async lrange(key, start, stop) {
        try {
            return await this.redis.lrange(key, start, stop);
        } catch (error) {
            logger.error('Redis LRANGE operation failed', {
                key,
                start,
                stop,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    getStatus() {
        return this.redis.status;
    }

    async exec(command, ...args) {
        try {
            return await this.redis.call(command, ...args);
        } catch (error) {
            logger.error('Redis EXEC operation failed', {
                command,
                args,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async keys(pattern) {
        try {
            return await this.redis.keys(pattern);
        } catch (error) {
            logger.error('Redis KEYS operation failed', {
                pattern,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async delMultiple(keys) {
        try {
            if (!keys || keys.length === 0) return 0;
            return await this.redis.del(...keys);
        } catch (error) {
            logger.error('Redis DEL multiple operation failed', {
                keys,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    getKeyPrefix() {
        return this.redis.options.keyPrefix || '';
    }
}

export default new RedisService(); 