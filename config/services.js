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
    username: process.env.NODE_RPC_USERNAME || "tbcuser",
    password: process.env.NODE_RPC_PASSWORD || "randompasswd",
    get url() {
      return `http://${this.host}:${this.port}${this.path}`;
    },
  },
  electrumxRpc: {
    name: "electrumx-rpc",
    type: "json-rpc",
    protocol: "tcp",
    host: process.env.ELECTRUMX_RPC_HOST || "localhost",
    port: process.env.ELECTRUMX_RPC_PORT || 50002,
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
};

export default servicesConfig;
