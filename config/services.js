import dotenv from 'dotenv';

dotenv.config();

const servicesConfig = {
  nodeRpc: {
    name: "node-rpc",
    type: "json-rpc",
    protocol: "http",
    host: process.env.NODE_RPC_HOST || "localhost",
    port: process.env.NODE_RPC_PORT || 8332,
    path: process.env.NODE_RPC_PATH || "",
    timeout: parseInt(process.env.NODE_RPC_TIMEOUT) || 10000,
    retries: parseInt(process.env.NODE_RPC_RETRIES) || 3,
    username: process.env.NODE_RPC_USERNAME || "",
    password: process.env.NODE_RPC_PASSWORD || "",
    get url() {
      return `http://${this.host}:${this.port}${this.path}`;
    },
  },
  electrumxRpc: {
    name: "electrumx-rpc",
    type: "json-rpc",
    protocol: "tcp",
    host: process.env.ELECTRUMX_RPC_HOST || "localhost",
    port: process.env.ELECTRUMX_RPC_PORT || 50001,
    path: process.env.ELECTRUMX_RPC_PATH || "",
    timeout: parseInt(process.env.ELECTRUMX_RPC_TIMEOUT) || 10000,
    retries: parseInt(process.env.ELECTRUMX_RPC_RETRIES) || 3,
    username: process.env.ELECTRUMX_RPC_USERNAME || "",
    password: process.env.ELECTRUMX_RPC_PASSWORD || "",
    get url() {
      return this.protocol === "tcp"
        ? `tcp://${this.host}:${this.port}`
        : `http://${this.host}:${this.port}${this.path}`;
    },
  },
  zeromq: {
    name: "zeromq",
    type: "zeromq",
    host: process.env.ZMQ_HOST || "localhost",
    enabled: process.env.ZMQ_ENABLED !== 'false',
    reconnectInterval: parseInt(process.env.ZMQ_RECONNECT_INTERVAL) || 5000,
    subscriptions: {
      hashblock: {
        enabled: process.env.ZMQ_HASHBLOCK_ENABLED !== 'false',
        port: parseInt(process.env.ZMQ_HASHBLOCK_PORT) || 28332,
        topic: "hashblock"
      },
      hashtx: {
        enabled: process.env.ZMQ_HASHTX_ENABLED !== 'false',
        port: parseInt(process.env.ZMQ_HASHTX_PORT) || 28333,
        topic: "hashtx"
      }
    }
  }
};

export default servicesConfig;
