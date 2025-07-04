import dotenv from "dotenv";
import fs from "fs/promises";
import path from "path";

import logger from "../config/logger.js";
import { connectDB, disconnectDB } from "../config/db.js";
import serviceManager from "../services/ServiceManager.js";
import { Block } from "../models/block.js";
import { Transaction } from "../models/transaction.js";

dotenv.config();

class BlockPreprocessorWorker {
  constructor() {
    this.isRunning = false;
    this.startHeight = null; 
    this.batchSize = 100;
    this.delayBetweenBatches = 5000;
    this.maxRetries = 3;
    this.currentHeight = null;
    this.configPath = path.join(process.cwd(), 'config', 'blockProgress.json');
    this.stats = {
      totalProcessed: 0,
      totalSaved: 0,
      totalErrors: 0,
      startTime: null,
      currentBatch: 0,
      latestHeight: null,
      txPreprocessing: {
        totalTxProcessed: 0,
        totalTxSaved: 0,
        totalTxErrors: 0,
        blocksProcessed: 0
      }
    };
  }

  async loadConfig() {
    try {
      const configData = await fs.readFile(this.configPath, 'utf8');
      return JSON.parse(configData);
    } catch (error) {
      logger.warn("BlockPreprocessorWorker: Failed to load config, using defaults", {
        error: error.message
      });
      return {
        lastProcessedHeight: 824190,
        lastUpdated: new Date().toISOString(),
        totalProcessed: 0,
        processingDirection: "forward"
      };
    }
  }

  async saveConfig(config) {
    try {
      await fs.writeFile(this.configPath, JSON.stringify(config, null, 2));
      logger.info("BlockPreprocessorWorker: Config saved successfully", {
        lastProcessedHeight: config.lastProcessedHeight,
        totalProcessed: config.totalProcessed
      });
    } catch (error) {
      logger.error("BlockPreprocessorWorker: Failed to save config", {
        error: error.message
      });
    }
  }

  async initialize() {
    try {
      logger.info("BlockPreprocessorWorker: Initializing...");
      await connectDB();
      await serviceManager.initialize();
      
      // 加载配置
      const config = await this.loadConfig();
      this.startHeight = config.lastProcessedHeight;
      
      logger.info("BlockPreprocessorWorker: Initialization completed", {
        startHeight: this.startHeight
      });
      await this.start();
    } catch (error) {
      logger.error("BlockPreprocessorWorker: Initialization failed", {
        error: error.message,
        stack: error.stack,
      });
      process.exit(1);
    }
  }

  async start() {
    if (this.isRunning) {
      logger.warn("BlockPreprocessorWorker: Already running");
      return;
    }

    try {
      const blockchainInfo = await serviceManager.getBlockchainInfo();
      const latestHeight = blockchainInfo.blocks;
      this.stats.latestHeight = latestHeight;

      const lastBlock = await Block.findOne({}).sort({ height: -1 }).select('height');
      const dbMaxHeight = lastBlock ? lastBlock.height : 0;

      this.currentHeight = this.startHeight;

      if (this.currentHeight >= latestHeight) {
        logger.info("BlockPreprocessorWorker: Already synchronized to latest height", {
          currentHeight: this.currentHeight,
          latestHeight: latestHeight,
          dbMaxHeight
        });

        logger.info("BlockPreprocessorWorker: No block work needed, proceeding to transaction preprocessing");
        await this.preprocessTransactions();
        setTimeout(() => {
          process.exit(0);
        }, 2000);
        return;
      }

      this.isRunning = true;
      this.stats.startTime = new Date();

      logger.info("BlockPreprocessorWorker: Starting block preprocessing (forward direction)", {
        startHeight: this.currentHeight,
        latestHeight: latestHeight,
        dbMaxHeight,
        totalToProcess: latestHeight - this.currentHeight + 1,
        batchSize: this.batchSize
      });

      await this.processBlocks(latestHeight);

    } catch (error) {
      logger.error("BlockPreprocessorWorker: Failed to start", {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  async processBlocks(targetLatestHeight) {
    try {
      while (this.currentHeight <= targetLatestHeight && this.isRunning) {
        const endHeight = Math.min(this.currentHeight + this.batchSize - 1, targetLatestHeight);
        const heights = [];
        for (let h = this.currentHeight; h <= endHeight; h++) {
          heights.push(h);
        }

        logger.debug(`BlockPreprocessorWorker: Processing batch ${this.stats.currentBatch + 1}: heights ${this.currentHeight} to ${endHeight}`);

        let retryCount = 0;
        let batchSuccess = false;

        while (retryCount < this.maxRetries && !batchSuccess) {
          try {
            await this.processBatch(heights);
            batchSuccess = true;
            this.stats.currentBatch++;
            this.stats.totalProcessed += heights.length;

            // 更新配置文件中的进度
            await this.updateProgress(endHeight);

            if (this.stats.currentBatch % 10 === 0) {
              const totalToProcess = targetLatestHeight - this.startHeight + 1;
              const processed = this.stats.totalProcessed;
              const progress = (processed / totalToProcess) * 100;
              logger.info(`BlockPreprocessorWorker: Progress ${progress.toFixed(1)}%`, {
                currentHeight: this.currentHeight,
                endHeight: endHeight,
                targetLatestHeight,
                batchesProcessed: this.stats.currentBatch,
                totalSaved: this.stats.totalSaved,
                totalErrors: this.stats.totalErrors
              });
            }

          } catch (error) {
            retryCount++;
            this.stats.totalErrors++;
            logger.error(`BlockPreprocessorWorker: Batch processing failed (attempt ${retryCount}/${this.maxRetries})`, {
              heights: heights.join(','),
              error: error.message
            });

            if (retryCount < this.maxRetries) {
              await new Promise(resolve => setTimeout(resolve, this.delayBetweenBatches));
            }
          }
        }

        if (!batchSuccess) {
          logger.error(`BlockPreprocessorWorker: Failed to process batch after ${this.maxRetries} retries, skipping`, {
            heights: heights.join(',')
          });
        }

        this.currentHeight = endHeight + 1;
        if (this.currentHeight <= targetLatestHeight) {
          await new Promise(resolve => setTimeout(resolve, this.delayBetweenBatches));
        }
      }

      if (this.isRunning) {
        await this.complete();
      }

    } catch (error) {
      logger.error("BlockPreprocessorWorker: Processing failed", {
        error: error.message,
        stack: error.stack,
        currentHeight: this.currentHeight
      });
      this.isRunning = false;
    }
  }

  async updateProgress(lastProcessedHeight) {
    try {
      const config = await this.loadConfig();
      config.lastProcessedHeight = lastProcessedHeight;
      config.lastUpdated = new Date().toISOString();
      config.totalProcessed = this.stats.totalProcessed;
      await this.saveConfig(config);
    } catch (error) {
      logger.error("BlockPreprocessorWorker: Failed to update progress", {
        error: error.message,
        lastProcessedHeight
      });
    }
  }

  async processBatch(heights) {
    const existingBlocks = await Block.find({ height: { $in: heights } }).select('height');
    const existingHeights = new Set(existingBlocks.map(b => b.height));
    const missingHeights = heights.filter(h => !existingHeights.has(h));

    if (missingHeights.length === 0) {
      logger.debug(`BlockPreprocessorWorker: All blocks in batch already exist: ${heights.join(',')}`);
      return;
    }

    logger.debug(`BlockPreprocessorWorker: Fetching ${missingHeights.length} missing blocks from RPC`, {
      heights: missingHeights.join(',')
    });
    const blocks = await serviceManager.getBlocksByHeight(missingHeights);

    const blockDocs = [];
    for (const block of blocks) {
      if (block) {
        blockDocs.push({
          hash: block.hash,
          height: block.height,
          confirmations: block.confirmations,
          size: block.size,
          version: block.version,
          versionHex: block.versionHex,
          merkleroot: block.merkleroot,
          num_tx: block.num_tx,
          time: block.time,
          mediantime: block.mediantime,
          nonce: block.nonce,
          bits: block.bits,
          difficulty: block.difficulty,
          chainwork: block.chainwork,
          previousblockhash: block.previousblockhash,
          nextblockhash: block.nextblockhash,
          tx: block.tx,
          coinbaseTx: block.coinbaseTx,
          totalFees: block.totalFees,
          miner: block.miner
        });
      }
    }

    let savedCount = 0;
    if (blockDocs.length > 0) {
      try {
        const result = await Block.insertMany(blockDocs, {
          ordered: false,
          rawResult: true
        });
        savedCount = result.insertedCount || blockDocs.length;
        logger.debug(`BlockPreprocessorWorker: Batch inserted ${savedCount} blocks successfully`);

      } catch (error) {
        if (error.code === 11000 && error.writeErrors) {
          const successCount = blockDocs.length - error.writeErrors.length;
          savedCount = successCount;
          logger.debug(`BlockPreprocessorWorker: Batch insert completed with ${error.writeErrors.length} duplicates, ${successCount} succeeded`);
        } else {
          logger.error(`BlockPreprocessorWorker: Batch insert failed`, {
            error: error.message,
            blocksCount: blockDocs.length
          });

          logger.debug(`BlockPreprocessorWorker: Falling back to individual inserts`);
          for (const blockData of blockDocs) {
            try {
              const blockDoc = new Block(blockData);
              await blockDoc.save();
              savedCount++;
            } catch (saveError) {
              if (saveError.code === 11000) {
                logger.debug(`BlockPreprocessorWorker: Block height ${blockData.height} already exists (concurrent write), skipping`);
              } else {
                logger.warn(`BlockPreprocessorWorker: Failed to save block height ${blockData.height}`, {
                  error: saveError.message
                });
              }
            }
          }
        }
      }
    }

    this.stats.totalSaved += savedCount;
    logger.debug(`BlockPreprocessorWorker: Saved ${savedCount} blocks from batch to database`);
  }

  async preprocessTransactions() {
    try {
      logger.info("BlockPreprocessorWorker: Starting transaction preprocessing for latest 1000 blocks");

      // First, check if we need to clean up old transactions (same as ZeroMQ)
      const MAX_BLOCKS = 10000;  // When reaching this, trigger cleanup
      const TARGET_BLOCKS = 9000; // Keep this many blocks after cleanup
      
      // Get the actual count of distinct block heights in the database
      const currentBlockCount = await Transaction.getDistinctBlockCount();
      
      if (currentBlockCount >= MAX_BLOCKS) {
        // When we reach 10000 blocks, keep only the newest 9000 blocks
        const sortedHeights = await Transaction.getDistinctBlockHeights(-1); // Sort descending
        const blocksToKeep = sortedHeights.slice(0, TARGET_BLOCKS);
        const minHeightToKeep = Math.min(...blocksToKeep);
        
        logger.info(`BlockPreprocessorWorker: Block count (${currentBlockCount}) reached ${MAX_BLOCKS}, cleaning up transactions below height ${minHeightToKeep}`);
        
        const deleteResult = await Transaction.deleteBeforeHeight(minHeightToKeep);
        
        logger.info(`BlockPreprocessorWorker: Cleaned up ${deleteResult.deletedCount} transactions from old blocks, maintaining ${TARGET_BLOCKS} newest blocks`);
      }

      const blockchainInfo = await serviceManager.getBlockchainInfo();
      const latestHeight = blockchainInfo.blocks;
      const startHeight = Math.max(0, latestHeight - 999); // Latest 1000 blocks

      logger.info("BlockPreprocessorWorker: Transaction preprocessing range", {
        startHeight,
        endHeight: latestHeight,
        totalBlocks: latestHeight - startHeight + 1
      });

      const heights = [];
      for (let h = latestHeight; h >= startHeight; h--) {
        heights.push(h);
      }

      const blocks = await Block.find({ height: { $in: heights } }).select('height tx').lean();
      const blockMap = new Map(blocks.map(b => [b.height, b]));

      const missingHeights = heights.filter(h => !blockMap.has(h));

      if (missingHeights.length > 0) {
        logger.info(`BlockPreprocessorWorker: Fetching ${missingHeights.length} missing blocks for transaction preprocessing`);
        const missingBlocks = await serviceManager.getBlocksByHeight(missingHeights);

        for (const block of missingBlocks) {
          if (block) {
            blockMap.set(block.height, { height: block.height, tx: block.tx });
          }
        }
      }

      // Process transactions block by block to maintain blockHeight information
      let totalProcessed = 0;
      let totalSaved = 0;
      let totalErrors = 0;
      let processedBlocks = 0;

      for (const height of heights) {
        const block = blockMap.get(height);
        if (block && block.tx) {
          logger.debug(`BlockPreprocessorWorker: Processing transactions from block ${height} (${block.tx.length} transactions)`);
          
          const blockResult = await this.processBlockTransactions(height, block.tx);
          totalProcessed += blockResult.totalProcessed;
          totalSaved += blockResult.totalSaved;
          totalErrors += blockResult.totalErrors;
          processedBlocks++;
        }
      }

      this.stats.txPreprocessing.blocksProcessed = processedBlocks;
      this.stats.txPreprocessing.totalTxProcessed = totalProcessed;
      this.stats.txPreprocessing.totalTxSaved = totalSaved;
      this.stats.txPreprocessing.totalTxErrors = totalErrors;

      logger.info("BlockPreprocessorWorker: Transaction preprocessing completed", {
        blocksProcessed: this.stats.txPreprocessing.blocksProcessed,
        totalTxProcessed: this.stats.txPreprocessing.totalTxProcessed,
        totalTxSaved: this.stats.txPreprocessing.totalTxSaved,
        totalTxErrors: this.stats.txPreprocessing.totalTxErrors
      });

    } catch (error) {
      logger.error("BlockPreprocessorWorker: Transaction preprocessing failed", {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  async processBlockTransactions(blockHeight, txIds) {
    try {
      logger.debug(`BlockPreprocessorWorker: Processing ${txIds.length} transactions from block ${blockHeight}`);

      // Get existing transactions to avoid duplicates
      const existingTransactions = await Transaction.find({ txid: { $in: txIds } }).select('txid');
      const existingTxIds = new Set(existingTransactions.map(tx => tx.txid));
      const missingTxIds = txIds.filter(txid => !existingTxIds.has(txid));

      if (missingTxIds.length === 0) {
        logger.debug(`BlockPreprocessorWorker: All transactions from block ${blockHeight} already exist in database`);
        return {
          totalProcessed: txIds.length,
          totalSaved: 0,
          totalErrors: 0
        };
      }

      // Process transactions in batches (same as ZeroMQ)
      const BATCH_SIZE = 500;
      const batches = [];
      for (let i = 0; i < missingTxIds.length; i += BATCH_SIZE) {
        batches.push(missingTxIds.slice(i, i + BATCH_SIZE));
      }

      let totalSaved = 0;
      let totalErrors = 0;

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        try {
          const rawTxs = await serviceManager.getRawTransactionsHex(batch);
          
          const txDocs = [];
          for (let j = 0; j < batch.length; j++) {
            const txid = batch[j];
            const rawTx = rawTxs[j];
            
            if (rawTx) {
              txDocs.push({
                txid: txid,
                raw: rawTx,
                blockHeight: blockHeight // Include blockHeight like ZeroMQ
              });
            } else {
              logger.warn(`BlockPreprocessorWorker: Failed to get raw transaction for txid: ${txid}`);
              totalErrors++;
            }
          }

          if (txDocs.length > 0) {
            try {
              const result = await Transaction.insertMany(txDocs, { 
                ordered: false,
                rawResult: true 
              });
              const savedCount = result.insertedCount || txDocs.length;
              totalSaved += savedCount;
              logger.debug(`BlockPreprocessorWorker: Saved ${savedCount} transactions from block ${blockHeight} batch ${i + 1}`);
            } catch (error) {
              if (error.code === 11000 && error.writeErrors) {
                const successCount = txDocs.length - error.writeErrors.length;
                totalSaved += successCount;
                logger.debug(`BlockPreprocessorWorker: Batch insert completed with ${error.writeErrors.length} duplicates, ${successCount} succeeded`);
              } else {
                throw error;
              }
            }
          }
        } catch (error) {
          logger.error(`BlockPreprocessorWorker: Error processing transaction batch from block ${blockHeight}`, {
            error: error.message,
            batchIndex: i,
            batchSize: batch.length
          });
          totalErrors += batch.length;
        }
      }

      logger.debug(`BlockPreprocessorWorker: Completed processing transactions from block ${blockHeight}`, {
        totalProcessed: missingTxIds.length,
        totalSaved,
        totalErrors,
        successRate: `${((totalSaved / missingTxIds.length) * 100).toFixed(1)}%`
      });

      return {
        totalProcessed: missingTxIds.length,
        totalSaved,
        totalErrors
      };

    } catch (error) {
      logger.error(`BlockPreprocessorWorker: Error in processBlockTransactions for block ${blockHeight}`, {
        error: error.message,
        stack: error.stack
      });
      return {
        totalProcessed: txIds.length,
        totalSaved: 0,
        totalErrors: txIds.length
      };
    }
  }

  async complete() {
    const endTime = new Date();
    const duration = endTime - this.stats.startTime;

    logger.info("BlockPreprocessorWorker: Block preprocessing completed successfully", {
      totalProcessed: this.stats.totalProcessed,
      totalSaved: this.stats.totalSaved,
      totalErrors: this.stats.totalErrors,
      totalBatches: this.stats.currentBatch,
      duration: `${Math.floor(duration / 1000)}s`,
      averageTimePerBatch: `${Math.floor(duration / this.stats.currentBatch)}ms`
    });

    // 最终更新配置文件
    try {
      const config = await this.loadConfig();
      config.lastProcessedHeight = this.stats.latestHeight;
      config.lastUpdated = new Date().toISOString();
      config.totalProcessed = this.stats.totalProcessed;
      await this.saveConfig(config);
      logger.info("BlockPreprocessorWorker: Final progress saved to config file", {
        lastProcessedHeight: config.lastProcessedHeight
      });
    } catch (error) {
      logger.error("BlockPreprocessorWorker: Failed to save final progress", {
        error: error.message
      });
    }

    this.isRunning = false;

    this.preprocessTransactions().then(() => {
      logger.info("BlockPreprocessorWorker: All tasks completed, shutting down worker process");
      setTimeout(async () => {
        await this.cleanup();
        process.exit(0);
      }, 3000);
    }).catch(async (error) => {
      logger.error("BlockPreprocessorWorker: Transaction preprocessing failed", {
        error: error.message,
        stack: error.stack
      });
      await this.cleanup();
      process.exit(1);
    });
  }

  async cleanup() {
    logger.info('BlockPreprocessorWorker: Starting cleanup process...');

    try {
      this.isRunning = false;
      if (serviceManager) {
        await serviceManager.shutdown();
      }
      await disconnectDB();

      logger.info('BlockPreprocessorWorker: Cleanup completed successfully');
    } catch (error) {
      logger.error('BlockPreprocessorWorker: Error during cleanup', {
        error: error.message,
        stack: error.stack
      });
    }
  }
}

const worker = new BlockPreprocessorWorker();

process.on('SIGTERM', async () => {
  logger.info('BlockPreprocessorWorker: Received SIGTERM signal');
  await worker.cleanup();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('BlockPreprocessorWorker: Received SIGINT signal');
  await worker.cleanup();
  process.exit(0);
});

process.on('uncaughtException', async (error) => {
  logger.error('BlockPreprocessorWorker: Uncaught exception', {
    error: error.message,
    stack: error.stack
  });
  await worker.cleanup();
  process.exit(1);
});

process.on('unhandledRejection', async (reason, promise) => {
  logger.error('BlockPreprocessorWorker: Unhandled promise rejection', {
    reason: reason,
    promise: promise
  });
  await worker.cleanup();
  process.exit(1);
});

worker.initialize().catch(async (error) => {
  logger.error('BlockPreprocessorWorker: Failed to initialize', {
    error: error.message,
    stack: error.stack
  });
  await worker.cleanup();
  process.exit(1);
}); 