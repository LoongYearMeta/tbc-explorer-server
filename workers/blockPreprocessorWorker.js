import dotenv from "dotenv";

import logger from "../config/logger.js";
import { connectDB, disconnectDB } from "../config/db.js";
import serviceManager from "../services/ServiceManager.js";
import { Block } from "../models/block.js";
import { Transaction } from "../models/transaction.js";

dotenv.config();

class BlockPreprocessorWorker {
  constructor() {
    this.isRunning = false;
    this.endHeight = 824190;
    this.batchSize = 100;
    this.delayBetweenBatches = 5000;
    this.maxRetries = 3;
    this.currentHeight = null;
    this.stats = {
      totalProcessed: 0,
      totalSaved: 0,
      totalErrors: 0,
      startTime: null,
      currentBatch: 0,
      initialHeight: null,
      txPreprocessing: {
        totalTxProcessed: 0,
        totalTxSaved: 0,
        totalTxErrors: 0,
        blocksProcessed: 0
      }
    };
  }

  async initialize() {
    try {
      logger.info("BlockPreprocessorWorker: Initializing...");
      await connectDB();
      await serviceManager.initialize();
      logger.info("BlockPreprocessorWorker: Initialization completed");
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

      const lastBlock = await Block.findOne({}).sort({ height: -1 }).select('height');
      const dbMaxHeight = lastBlock ? lastBlock.height : 0;

      this.currentHeight = latestHeight;

      if (latestHeight < this.endHeight) {
        logger.info("BlockPreprocessorWorker: Latest blockchain height is lower than end height", {
          dbMaxHeight,
          latestHeight,
          endHeight: this.endHeight,
          currentHeight: this.currentHeight
        });

        logger.info("BlockPreprocessorWorker: No work needed, shutting down worker process");
        setTimeout(() => {
          process.exit(0);
        }, 2000);
        return;
      }

      this.isRunning = true;
      this.stats.startTime = new Date();
      this.stats.initialHeight = this.currentHeight;

      logger.info("BlockPreprocessorWorker: Starting block preprocessing (reverse direction)", {
        startHeight: this.currentHeight,
        endHeight: this.endHeight,
        latestHeight: latestHeight,
        dbMaxHeight,
        totalToProcess: this.currentHeight - this.endHeight + 1,
        batchSize: this.batchSize
      });

      await this.processBlocks(this.endHeight);

    } catch (error) {
      logger.error("BlockPreprocessorWorker: Failed to start", {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  async processBlocks(targetEndHeight) {
    try {
      while (this.currentHeight >= targetEndHeight && this.isRunning) {
        const startHeight = Math.max(this.currentHeight - this.batchSize + 1, targetEndHeight);
        const heights = [];
        for (let h = this.currentHeight; h >= startHeight; h--) {
          heights.push(h);
        }

        logger.debug(`BlockPreprocessorWorker: Processing batch ${this.stats.currentBatch + 1}: heights ${this.currentHeight} to ${startHeight}`);

        let retryCount = 0;
        let batchSuccess = false;

        while (retryCount < this.maxRetries && !batchSuccess) {
          try {
            await this.processBatch(heights);
            batchSuccess = true;
            this.stats.currentBatch++;
            this.stats.totalProcessed += heights.length;

            if (this.stats.currentBatch % 10 === 0) {
              const totalToProcess = this.stats.initialHeight - targetEndHeight + 1;
              const processed = this.stats.totalProcessed;
              const progress = (processed / totalToProcess) * 100;
              logger.info(`BlockPreprocessorWorker: Progress ${progress.toFixed(1)}%`, {
                currentHeight: this.currentHeight,
                targetEndHeight,
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

        this.currentHeight = startHeight - 1;
        if (this.currentHeight >= targetEndHeight) {
          await new Promise(resolve => setTimeout(resolve, this.delayBetweenBatches));
        }
      }

      if (this.isRunning) {
        this.complete();
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
      logger.info("BlockPreprocessorWorker: Starting transaction preprocessing for latest 100 blocks");

      const blockchainInfo = await serviceManager.getBlockchainInfo();
      const latestHeight = blockchainInfo.blocks;
      const startHeight = Math.max(0, latestHeight - 99);

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

      const allTxIds = [];
      let processedBlocks = 0;

      for (const height of heights) {
        const block = blockMap.get(height);
        if (block && block.tx) {
          allTxIds.push(...block.tx);
          processedBlocks++;
        }
      }

      this.stats.txPreprocessing.blocksProcessed = processedBlocks;

      logger.info(`BlockPreprocessorWorker: Collected ${allTxIds.length} transaction IDs from ${processedBlocks} blocks`);

      if (allTxIds.length > 0) {
        await this.processTransactionBatch(allTxIds);
      }

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

  async processTransactionBatch(txIds) {
    try {
      const existingTransactions = await Transaction.find({ txid: { $in: txIds } }).select('txid');
      const existingTxIds = new Set(existingTransactions.map(tx => tx.txid));
      const missingTxIds = txIds.filter(txid => !existingTxIds.has(txid));

      logger.info(`BlockPreprocessorWorker: Found ${existingTransactions.length} existing transactions, ${missingTxIds.length} missing`);

      if (missingTxIds.length === 0) {
        logger.info("BlockPreprocessorWorker: All transactions already exist in database");
        return;
      }

      this.stats.txPreprocessing.totalTxProcessed = missingTxIds.length;

      const txBatchSize = 500;
      const txBatches = [];
      
      for (let i = 0; i < missingTxIds.length; i += txBatchSize) {
        txBatches.push(missingTxIds.slice(i, i + txBatchSize));
      }

      logger.info(`BlockPreprocessorWorker: Processing ${missingTxIds.length} transactions in ${txBatches.length} batches`);

      let totalSaved = 0;
      let totalErrors = 0;

      for (let i = 0; i < txBatches.length; i++) {
        const batch = txBatches[i];
        
        try {
          logger.info(`BlockPreprocessorWorker: Processing transaction batch ${i + 1}/${txBatches.length} (${batch.length} transactions)`);
          
          const rawTxs = await serviceManager.getRawTransactionsHex(batch);
          
          const txDocs = [];
          for (let j = 0; j < batch.length; j++) {
            const txid = batch[j];
            const rawTx = rawTxs[j];
            
            if (rawTx) {
              txDocs.push({
                txid: txid,
                raw: rawTx
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
              logger.info(`BlockPreprocessorWorker: Saved ${savedCount} transactions from batch ${i + 1}`);
              
            } catch (error) {
              if (error.code === 11000 && error.writeErrors) {
                const successCount = txDocs.length - error.writeErrors.length;
                totalSaved += successCount;
                logger.info(`BlockPreprocessorWorker: Batch ${i + 1} completed with ${error.writeErrors.length} duplicates, ${successCount} saved`);
              } else {
                logger.error(`BlockPreprocessorWorker: Transaction batch ${i + 1} insert failed`, {
                  error: error.message,
                  txCount: txDocs.length
                });
                
                for (const txData of txDocs) {
                  try {
                    const txDoc = new Transaction(txData);
                    await txDoc.save();
                    totalSaved++;
                  } catch (saveError) {
                    if (saveError.code === 11000) {
                      logger.debug(`BlockPreprocessorWorker: Transaction ${txData.txid} already exists, skipping`);
                    } else {
                      logger.warn(`BlockPreprocessorWorker: Failed to save transaction ${txData.txid}`, {
                        error: saveError.message
                      });
                      totalErrors++;
                    }
                  }
                }
              }
            }
          }

          if (i < txBatches.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 1000));
          }

        } catch (error) {
          logger.error(`BlockPreprocessorWorker: Transaction batch ${i + 1} processing failed`, {
            error: error.message,
            batchSize: batch.length
          });
          totalErrors += batch.length;
        }
      }

      this.stats.txPreprocessing.totalTxSaved = totalSaved;
      this.stats.txPreprocessing.totalTxErrors = totalErrors;

      logger.info(`BlockPreprocessorWorker: Transaction batch processing completed`, {
        totalProcessed: missingTxIds.length,
        totalSaved: totalSaved,
        totalErrors: totalErrors,
        successRate: `${((totalSaved / missingTxIds.length) * 100).toFixed(1)}%`
      });

      const finalStats = await Transaction.getStats();
      logger.info(`BlockPreprocessorWorker: Final transaction database statistics`, finalStats);

    } catch (error) {
      logger.error("BlockPreprocessorWorker: Transaction batch processing failed", {
        error: error.message,
        stack: error.stack
      });
      throw error;
    }
  }

  complete() {
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