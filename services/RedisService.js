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

    async expire(key, ttl) {
        try {
            return await this.redis.expire(key, ttl);
        } catch (error) {
            logger.error('Redis EXPIRE operation failed', {
                key,
                ttl,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async ttl(key) {
        try {
            return await this.redis.ttl(key);
        } catch (error) {
            logger.error('Redis TTL operation failed', {
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

    async hset(key, field, value) {
        try {
            return await this.redis.hset(key, field, value);
        } catch (error) {
            logger.error('Redis HSET operation failed', {
                key,
                field,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async hget(key, field) {
        try {
            return await this.redis.hget(key, field);
        } catch (error) {
            logger.error('Redis HGET operation failed', {
                key,
                field,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async hgetall(key) {
        try {
            return await this.redis.hgetall(key);
        } catch (error) {
            logger.error('Redis HGETALL operation failed', {
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

    async incr(key, increment = 1) {
        try {
            if (increment === 1) {
                return await this.redis.incr(key);
            } else {
                return await this.redis.incrby(key, increment);
            }
        } catch (error) {
            logger.error('Redis INCR operation failed', {
                key,
                increment,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async decr(key, decrement = 1) {
        try {
            if (decrement === 1) {
                return await this.redis.decr(key);
            } else {
                return await this.redis.decrby(key, decrement);
            }
        } catch (error) {
            logger.error('Redis DECR operation failed', {
                key,
                decrement,
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
}

export default new RedisService(); 