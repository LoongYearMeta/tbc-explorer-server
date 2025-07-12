import cluster from 'cluster';
import os from 'os';
import dotenv from 'dotenv';

dotenv.config();

function getProcessType() {
    if (cluster.isPrimary) {
        return 'master';
    }

    if (process.env.WORKER_TYPE) {
        return process.env.WORKER_TYPE;
    }

    if (cluster.isWorker) {
        return 'http';
    }

    return 'unknown';
}

function getClusterConfig() {
    const cpuCount = os.cpus().length;
    const clusterWorkers = parseInt(process.env.CLUSTER_WORKERS) || cpuCount;

    return {
        cpuCount,
        clusterWorkers,
        processType: getProcessType(),
        processId: process.pid,
        workerId: cluster.worker ? cluster.worker.id : null
    };
}

function getConnectionConfig() {
    const processType = getProcessType();
    const clusterConfig = getClusterConfig();

    const baseConfig = {
        mongodb: {
            maxPoolSize: parseInt(process.env.MONGO_MAX_POOL_SIZE) || 1500,
            minPoolSize: parseInt(process.env.MONGO_MIN_POOL_SIZE) || 50,
            maxIdleTimeMS: parseInt(process.env.MONGO_MAX_IDLE_TIME) || 30000,
            serverSelectionTimeoutMS: parseInt(process.env.MONGO_SERVER_SELECTION_TIMEOUT) || 5000,
            heartbeatFrequencyMS: parseInt(process.env.MONGO_HEARTBEAT_FREQUENCY) || 10000
        },
        redis: {
            poolSize: parseInt(process.env.REDIS_POOL_SIZE) || 20,
            monitorInterval: parseInt(process.env.REDIS_MONITOR_INTERVAL) || 300000
        },
        electrumx: {
            maxConnections: parseInt(process.env.ELECTRUMX_MAX_CONNECTIONS) || 140,
            minConnections: parseInt(process.env.ELECTRUMX_MIN_CONNECTIONS) || 120,
            expansionBatchSize: parseInt(process.env.ELECTRUMX_EXPANSION_BATCH_SIZE) || 10
        }
    };

    let adjustedConfig = { ...baseConfig };

    switch (processType) {
        case 'http':
            adjustedConfig.mongodb.maxPoolSize = parseInt(process.env.HTTP_MONGO_MAX_POOL_SIZE) || 300;
            adjustedConfig.mongodb.minPoolSize = parseInt(process.env.HTTP_MONGO_MIN_POOL_SIZE) || 50;
            adjustedConfig.redis.poolSize = parseInt(process.env.HTTP_REDIS_POOL_SIZE) || 20;
            adjustedConfig.electrumx.maxConnections = parseInt(process.env.HTTP_ELECTRUMX_MAX_CONNECTIONS) || 60;
            adjustedConfig.electrumx.minConnections = parseInt(process.env.HTTP_ELECTRUMX_MIN_CONNECTIONS) || 50;
            break;

        case 'zeromq':
            adjustedConfig.mongodb.maxPoolSize = parseInt(process.env.ZMQ_MONGO_MAX_POOL_SIZE) || 80;
            adjustedConfig.mongodb.minPoolSize = parseInt(process.env.ZMQ_MONGO_MIN_POOL_SIZE) || 20;
            adjustedConfig.redis.poolSize = parseInt(process.env.ZMQ_REDIS_POOL_SIZE) || 10;
            adjustedConfig.electrumx.maxConnections = parseInt(process.env.ZMQ_ELECTRUMX_MAX_CONNECTIONS) || 0;
            adjustedConfig.electrumx.minConnections = parseInt(process.env.ZMQ_ELECTRUMX_MIN_CONNECTIONS) || 0;
            break;

        case 'block':
            adjustedConfig.mongodb.maxPoolSize = parseInt(process.env.BLOCK_MONGO_MAX_POOL_SIZE) || 150;
            adjustedConfig.mongodb.minPoolSize = parseInt(process.env.BLOCK_MONGO_MIN_POOL_SIZE) || 30;
            adjustedConfig.redis.poolSize = parseInt(process.env.BLOCK_REDIS_POOL_SIZE) || 0;
            adjustedConfig.electrumx.maxConnections = parseInt(process.env.BLOCK_ELECTRUMX_MAX_CONNECTIONS) || 0;
            adjustedConfig.electrumx.minConnections = parseInt(process.env.BLOCK_ELECTRUMX_MIN_CONNECTIONS) || 0;
            break;

        case 'cache':
            adjustedConfig.mongodb.maxPoolSize = parseInt(process.env.CACHE_MONGO_MAX_POOL_SIZE) || 30;
            adjustedConfig.mongodb.minPoolSize = parseInt(process.env.CACHE_MONGO_MIN_POOL_SIZE) || 5;
            adjustedConfig.redis.poolSize = parseInt(process.env.CACHE_REDIS_POOL_SIZE) || 5;
            adjustedConfig.electrumx.maxConnections = parseInt(process.env.CACHE_ELECTRUMX_MAX_CONNECTIONS) || 0;
            adjustedConfig.electrumx.minConnections = parseInt(process.env.CACHE_ELECTRUMX_MIN_CONNECTIONS) || 0;
            break;

        default:
            break;
    }

    return {
        ...adjustedConfig,
        cluster: clusterConfig,
        processType
    };
}

function getConnectionStats() {
    const config = getConnectionConfig();
    const { clusterWorkers } = config.cluster;

    const estimatedConnections = {
        mongodb: {
            httpWorkers: clusterWorkers * (parseInt(process.env.HTTP_MONGO_MAX_POOL_SIZE) || 300),
            zeromqWorker: parseInt(process.env.ZMQ_MONGO_MAX_POOL_SIZE) || 80,
            blockWorker: parseInt(process.env.BLOCK_MONGO_MAX_POOL_SIZE) || 150,
            cacheWorker: parseInt(process.env.CACHE_MONGO_MAX_POOL_SIZE) || 30
        },
        redis: {
            httpWorkers: clusterWorkers * (parseInt(process.env.HTTP_REDIS_POOL_SIZE) || 20),
            zeromqWorker: parseInt(process.env.ZMQ_REDIS_POOL_SIZE) || 10,
            blockWorker: parseInt(process.env.BLOCK_REDIS_POOL_SIZE) || 0,
            cacheWorker: parseInt(process.env.CACHE_REDIS_POOL_SIZE) || 5
        },
        electrumx: {
            httpWorkers: clusterWorkers * (parseInt(process.env.HTTP_ELECTRUMX_MAX_CONNECTIONS) || 60),
            zeromqWorker: parseInt(process.env.ZMQ_ELECTRUMX_MAX_CONNECTIONS) || 0,
            blockWorker: parseInt(process.env.BLOCK_ELECTRUMX_MAX_CONNECTIONS) || 0,
            cacheWorker: parseInt(process.env.CACHE_ELECTRUMX_MAX_CONNECTIONS) || 0
        }
    };

    const totalConnections = {
        mongodb: Object.values(estimatedConnections.mongodb).reduce((sum, val) => sum + val, 0),
        redis: Object.values(estimatedConnections.redis).reduce((sum, val) => sum + val, 0),
        electrumx: Object.values(estimatedConnections.electrumx).reduce((sum, val) => sum + val, 0)
    };

    return {
        estimated: estimatedConnections,
        total: totalConnections,
        cluster: config.cluster
    };
}

function generateConnectionReport() {
    const config = getConnectionConfig();
    const stats = getConnectionStats();

    return {
        processInfo: {
            type: config.processType,
            pid: config.cluster.processId,
            workerId: config.cluster.workerId,
            cpuCount: config.cluster.cpuCount,
            clusterWorkers: config.cluster.clusterWorkers
        },
        currentProcessConfig: {
            mongodb: config.mongodb,
            redis: config.redis,
            electrumx: config.electrumx
        },
        estimatedTotalConnections: stats.total,
        detailedEstimate: stats.estimated,
        warnings: generateWarnings(stats.total)
    };
}

function generateWarnings(totalConnections) {
    const warnings = [];

    if (totalConnections.mongodb > 5000) {
        warnings.push(`High MongoDB connection count: ${totalConnections.mongodb}, consider checking database server configuration`);
    }

    if (totalConnections.redis > 1000) {
        warnings.push(`High Redis connection count: ${totalConnections.redis}, consider checking Redis server configuration`);
    }

    if (totalConnections.electrumx > 1000) {
        warnings.push(`High ElectrumX connection count: ${totalConnections.electrumx}, consider checking ElectrumX server configuration`);
    }

    return warnings;
}

export {
    getProcessType,
    getClusterConfig,
    getConnectionConfig,
    getConnectionStats,
    generateConnectionReport
}; 