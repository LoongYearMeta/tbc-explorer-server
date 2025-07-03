import zmq from "zeromq";

import logger from "../config/logger.js";

class ZeroMQService {
  constructor(config) {
    this.config = config;
    this.subscribers = new Map();
    this.isRunning = false;
    this.reconnectTimeouts = new Map();
    this.messageHandlers = new Map();
    this.stats = {
      hashblock: { received: 0, errors: 0, lastReceived: null }
    };
    this.setupDefaultHandlers();
  }

  setupDefaultHandlers() {
    this.messageHandlers.set('hashblock', (topic, message) => {
      const blockHash = message.toString('hex');
      logger.info(`[ZMQ] New block hash: ${blockHash}`);
      this.stats.hashblock.received++;
      this.stats.hashblock.lastReceived = new Date();
      this.onNewBlockHash(blockHash);
    });
  }

  async start() {
    if (!this.config.enabled) {
      logger.info('[ZMQ] ZeroMQ service is disabled');
      return;
    }

    if (this.isRunning) {
      logger.warn('[ZMQ] ZeroMQ service is already running');
      return;
    }

    this.isRunning = true;
    logger.info('[ZMQ] Starting ZeroMQ service...');

    const subscriptionPromises = [];
    for (const [subscriptionName, subscription] of Object.entries(this.config.subscriptions)) {
      if (subscription.enabled) {
        subscriptionPromises.push(this.createSubscription(subscriptionName, subscription));
      }
    }

    await Promise.allSettled(subscriptionPromises);

    const connectedCount = Array.from(this.subscribers.values()).filter(sub => sub.connected).length;
    const totalCount = this.subscribers.size;

    logger.info('[ZMQ] ZeroMQ service started');
    logger.info(`[ZMQ] Subscriptions: ${connectedCount}/${totalCount} connected`);
    
    if (connectedCount === 0) {
      logger.warn('[ZMQ] No ZeroMQ subscriptions connected - will attempt to reconnect');
    }
  }

  async createSubscription(name, subscription) {
    const address = `tcp://${this.config.host}:${subscription.port}`;
    
    try {
      const subscriber = new zmq.Subscriber();
      subscriber.subscribe(subscription.topic);
      subscriber.connect(address);
      
      this.subscribers.set(name, {
        subscriber,
        subscription,
        address,
        connected: false, 
        lastError: null
      });

      this.setupMessageHandler(subscriber, name, subscription.topic);

      logger.info(`[ZMQ] Attempting to connect to ${name} at ${address}`);
    } catch (error) {
      logger.error(`[ZMQ] Failed to create subscription for ${name}`, {
        error: error.message,
        address
      });
      
      this.subscribers.set(name, {
        subscriber: null,
        subscription,
        address,
        connected: false,
        lastError: error
      });
      this.scheduleReconnect(name);
    }
  }

  setupMessageHandler(subscriber, name, topic) {
    const messageHandler = this.messageHandlers.get(name);
    
    if (!messageHandler) {
      logger.warn(`[ZMQ] No message handler found for ${name}`);
      return;
    }

    (async () => {
      try {
        const subscription = this.subscribers.get(name);
        if (subscription) {
          subscription.connected = true;
        }

        for await (const [topicBuffer, message] of subscriber) {
          const receivedTopic = topicBuffer.toString();
          if (receivedTopic === topic) {
            if (subscription && !subscription.connected) {
              logger.info(`[ZMQ] Successfully connected to ${name}`);
              subscription.connected = true;
              subscription.lastError = null;
            }
            
            messageHandler(receivedTopic, message);
          }
        }
      } catch (error) {
        logger.error(`[ZMQ] Message handler error for ${name}`, {
          error: error.message
        });
        this.stats[name].errors++;
        const subscription = this.subscribers.get(name);
        if (subscription) {
          subscription.connected = false;
          subscription.lastError = error;
        }
        
        this.handleSubscriptionError(name, error);
      } finally {
        logger.warn(`[ZMQ] Message handler loop ended for ${name}`);
        const subscription = this.subscribers.get(name);
        if (subscription) {
          subscription.connected = false;
        }
      }
    })();
  }

  handleSubscriptionError(name, error) {
    const subscription = this.subscribers.get(name);
    if (subscription) {
      subscription.lastError = error;
      subscription.connected = false;
      this.scheduleReconnect(name);
    }
  }

  scheduleReconnect(name) {
    if (this.reconnectTimeouts.has(name)) {
      return;
    }

    const timeout = setTimeout(() => {
      this.reconnectTimeouts.delete(name);
      if (this.isRunning) {
        this.reconnectSubscription(name);
      }
    }, this.config.reconnectInterval);

    this.reconnectTimeouts.set(name, timeout);
    logger.info(`[ZMQ] Scheduled reconnect for ${name} in ${this.config.reconnectInterval}ms`);
  }


  async reconnectSubscription(name) {
    try {
      const subscriptionData = this.subscribers.get(name);
      if (!subscriptionData) {
        return;
      }

      logger.info(`[ZMQ] Attempting to reconnect ${name}...`);
      
      if (subscriptionData.subscriber) {
        try {
          await subscriptionData.subscriber.close();
        } catch (closeError) {
          logger.debug(`[ZMQ] Error closing existing connection for ${name}:`, closeError.message);
        }
      }
      
      await this.createSubscription(name, subscriptionData.subscription);
      
    } catch (error) {
      logger.error(`[ZMQ] Failed to reconnect ${name}`, {
        error: error.message
      });
      this.scheduleReconnect(name);
    }
  }

  getStatus() {
    const subscriptions = {};
    
    for (const [name, subscriptionData] of this.subscribers.entries()) {
      subscriptions[name] = {
        connected: subscriptionData.connected,
        address: subscriptionData.address,
        lastError: subscriptionData.lastError?.message || null,
        stats: this.stats[name]
      };
    }

    return {
      enabled: this.config.enabled,
      running: this.isRunning,
      subscriptions,
      totalSubscriptions: this.subscribers.size
    };
  }

  /**
   * 业务逻辑方法 - 新区块哈希处理
   */
  onNewBlockHash(blockHash) {
    // 这里可以添加自定义业务逻辑
    // 例如：触发缓存更新、通知WebSocket客户端、更新统计信息等
    logger.debug(`[ZMQ] Processing new block hash: ${blockHash}`);
  }
}

export default ZeroMQService;
