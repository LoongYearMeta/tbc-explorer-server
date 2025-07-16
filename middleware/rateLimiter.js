import redisService from "../services/RedisService.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";

class RateLimiter {
    constructor() {
        this.config = {
            global: {
                windowMs: 60000,
                max: 100,
                message: 'Too many requests from this IP, please try again later',
                standardHeaders: true,
                legacyHeaders: false,
            },

            address: {
                windowMs: 60000,
                max: 10,
                message: 'Too many address requests from this IP, please try again later',
                standardHeaders: true,
                legacyHeaders: false,
            },

            transaction: {
                windowMs: 60000,
                max: 15,
                message: 'Too many transaction requests from this IP, please try again later',
                standardHeaders: true,
                legacyHeaders: false,
            },

            rawTransaction: {
                windowMs: 60000,
                max: 40,
                message: 'Too many raw transaction requests from this IP, please try again later',
                standardHeaders: true,
                legacyHeaders: false,
            },

            batchTransaction: {
                windowMs: 300000,
                max: 20,
                message: 'Too many batch transaction requests from this IP, please try again later',
                standardHeaders: true,
                legacyHeaders: false,
            }
        };
    }

    generateKey(ip, endpoint) {
        return `rate_limit:${endpoint}:${ip}`;
    }

    async getRateLimitInfo(key, windowMs) {
        try {
            const data = await redisService.get(key);

            if (!data) {
                const newRecord = {
                    count: 1,
                    resetTime: Date.now() + windowMs,
                    firstRequest: Date.now()
                };

                await redisService.set(key, JSON.stringify(newRecord), Math.ceil(windowMs / 1000));
                return newRecord;
            }

            const parsed = JSON.parse(data);
            const now = Date.now();

            if (now > parsed.resetTime) {
                const newRecord = {
                    count: 1,
                    resetTime: now + windowMs,
                    firstRequest: now
                };

                await redisService.set(key, JSON.stringify(newRecord), Math.ceil(windowMs / 1000));
                return newRecord;
            }

            parsed.count++;
            await redisService.set(key, JSON.stringify(parsed), Math.ceil((parsed.resetTime - now) / 1000));

            return parsed;
        } catch (error) {
            logger.error('Rate limiter Redis error', { error: error.message, key });
            return {
                count: 1,
                resetTime: Date.now() + windowMs,
                firstRequest: Date.now()
            };
        }
    }

    async getSlidingWindowRateLimitInfo(key, windowMs, maxRequests) {
        try {
            const now = Date.now();
            const windowStart = now - windowMs;

            const data = await redisService.get(key);
            let requestTimes = [];

            if (data) {
                requestTimes = JSON.parse(data);
                requestTimes = requestTimes.filter(time => time > windowStart);
            }

            requestTimes.push(now);
            const resetTime = requestTimes.length > 0 ?
                Math.min(...requestTimes) + windowMs :
                now + windowMs;

            await redisService.set(
                key,
                JSON.stringify(requestTimes),
                Math.ceil(windowMs / 1000)
            );

            return {
                count: requestTimes.length,
                resetTime: resetTime,
                firstRequest: requestTimes.length > 0 ? Math.min(...requestTimes) : now,
                requestTimes: requestTimes
            };

        } catch (error) {
            logger.error('Sliding window rate limiter Redis error', { error: error.message, key });
            return {
                count: 1,
                resetTime: Date.now() + windowMs,
                firstRequest: Date.now()
            };
        }
    }

    createLimiter(configName = 'global') {
        const config = this.config[configName];

        return async (req, res, next) => {
            try {
                const ip = getRealClientIP(req);
                const endpoint = configName;
                const key = this.generateKey(ip, endpoint);

                const rateLimitInfo = await this.getRateLimitInfo(key, config.windowMs);

                if (config.standardHeaders) {
                    res.set({
                        'RateLimit-Limit': config.max,
                        'RateLimit-Remaining': Math.max(0, config.max - rateLimitInfo.count),
                        'RateLimit-Reset': new Date(rateLimitInfo.resetTime).toISOString(),
                    });
                }

                if (rateLimitInfo.count > config.max) {
                    logger.warn('Rate limit exceeded', {
                        ip,
                        endpoint,
                        count: rateLimitInfo.count,
                        max: config.max,
                        resetTime: rateLimitInfo.resetTime,
                        url: req.originalUrl,
                        userAgent: req.get('User-Agent')
                    });

                    if (config.legacyHeaders) {
                        res.set({
                            'X-RateLimit-Limit': config.max,
                            'X-RateLimit-Remaining': 0,
                            'X-RateLimit-Reset': Math.ceil(rateLimitInfo.resetTime / 1000),
                        });
                    }

                    return res.status(429).json({
                        error: 'Rate limit exceeded',
                        message: config.message,
                        resetTime: rateLimitInfo.resetTime,
                        retryAfter: Math.ceil((rateLimitInfo.resetTime - Date.now()) / 1000)
                    });
                }
                logger.debug('Rate limit check passed', {
                    ip,
                    endpoint,
                    count: rateLimitInfo.count,
                    max: config.max,
                    remaining: config.max - rateLimitInfo.count
                });
                next();
            } catch (error) {
                logger.error('Rate limiter middleware error', {
                    error: error.message,
                    stack: error.stack,
                    url: req.originalUrl,
                    ip: getRealClientIP(req)
                });
                next();
            }
        };
    }

    createSlidingWindowLimiter(configName = 'global') {
        const config = this.config[configName];

        return async (req, res, next) => {
            try {
                const ip = getRealClientIP(req);
                const endpoint = `sliding_${configName}`;
                const key = this.generateKey(ip, endpoint);

                const rateLimitInfo = await this.getSlidingWindowRateLimitInfo(
                    key,
                    config.windowMs,
                    config.max
                );

                if (config.standardHeaders) {
                    res.set({
                        'RateLimit-Limit': config.max,
                        'RateLimit-Remaining': Math.max(0, config.max - rateLimitInfo.count),
                        'RateLimit-Reset': new Date(rateLimitInfo.resetTime).toISOString(),
                    });
                }

                if (rateLimitInfo.count > config.max) {
                    logger.warn('Sliding window rate limit exceeded', {
                        ip,
                        endpoint,
                        count: rateLimitInfo.count,
                        max: config.max,
                        resetTime: rateLimitInfo.resetTime,
                        url: req.originalUrl,
                        userAgent: req.get('User-Agent'),
                        windowInfo: {
                            requestTimes: rateLimitInfo.requestTimes,
                            windowStart: Date.now() - config.windowMs
                        }
                    });

                    if (config.legacyHeaders) {
                        res.set({
                            'X-RateLimit-Limit': config.max,
                            'X-RateLimit-Remaining': 0,
                            'X-RateLimit-Reset': Math.ceil(rateLimitInfo.resetTime / 1000),
                        });
                    }

                    return res.status(429).json({
                        error: 'Rate limit exceeded',
                        message: config.message,
                        resetTime: rateLimitInfo.resetTime,
                        retryAfter: Math.ceil((rateLimitInfo.resetTime - Date.now()) / 1000),
                        algorithm: 'sliding-window'
                    });
                }
                logger.debug('Sliding window rate limit check passed', {
                    ip,
                    endpoint,
                    count: rateLimitInfo.count,
                    max: config.max,
                    remaining: config.max - rateLimitInfo.count,
                    algorithm: 'sliding-window'
                });

                next();
            } catch (error) {
                logger.error('Sliding window rate limiter middleware error', {
                    error: error.message,
                    stack: error.stack,
                    url: req.originalUrl,
                    ip: getRealClientIP(req)
                });
                next();
            }
        };
    }

    async getStatus(ip, endpoint) {
        try {
            // 所有端点都使用滑动窗口状态检查
            return await this.getSlidingWindowStatus(ip, endpoint);
        } catch (error) {
            logger.error('Get rate limit status error', { error: error.message, ip, endpoint });
            return null;
        }
    }

    async getSlidingWindowStatus(ip, endpoint) {
        try {
            const key = this.generateKey(ip, `sliding_${endpoint}`);
            const data = await redisService.get(key);
            const config = this.config[endpoint];

            if (!data) {
                return {
                    count: 0,
                    remaining: config.max,
                    resetTime: null,
                    windowStart: null,
                    algorithm: 'sliding-window'
                };
            }

            const requestTimes = JSON.parse(data);
            const now = Date.now();
            const windowStart = now - config.windowMs;

            const validRequests = requestTimes.filter(time => time > windowStart);

            const resetTime = validRequests.length > 0 ?
                Math.min(...validRequests) + config.windowMs :
                null;

            return {
                count: validRequests.length,
                remaining: Math.max(0, config.max - validRequests.length),
                resetTime: resetTime,
                windowStart: windowStart,
                algorithm: 'sliding-window',
                requestTimes: validRequests
            };
        } catch (error) {
            logger.error('Get sliding window status error', { error: error.message, ip, endpoint });
            return null;
        }
    }

    async clearLimits(ip, endpoint) {
        try {
            // 清除固定窗口的key（兼容性）
            const key = this.generateKey(ip, endpoint);
            await redisService.del(key);
            
            // 清除滑动窗口的key（主要数据）
            const slidingKey = this.generateKey(ip, `sliding_${endpoint}`);
            await redisService.del(slidingKey);
            
            logger.info('Rate limit cleared', { ip, endpoint });
            return true;
        } catch (error) {
            logger.error('Clear rate limit error', { error: error.message, ip, endpoint });
            return false;
        }
    }
}

const rateLimiter = new RateLimiter();

// 导出预配置的限流中间件 - 全部使用滑动窗口算法
export const globalRateLimit = rateLimiter.createSlidingWindowLimiter('global');
export const addressRateLimit = rateLimiter.createSlidingWindowLimiter('address');
export const transactionRateLimit = rateLimiter.createSlidingWindowLimiter('transaction');
export const rawTransactionRateLimit = rateLimiter.createSlidingWindowLimiter('rawTransaction');
export const batchTransactionRateLimit = rateLimiter.createSlidingWindowLimiter('batchTransaction');

// 兼容性导出
export const slidingAddressRateLimit = rateLimiter.createSlidingWindowLimiter('address');
export const slidingTransactionRateLimit = rateLimiter.createSlidingWindowLimiter('transaction');

export default rateLimiter; 