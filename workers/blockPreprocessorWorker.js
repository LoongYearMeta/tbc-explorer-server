import dotenv from "dotenv";
import logger from "../config/logger.js";
import connectDB from "../config/db.js";
import serviceManager from "../services/ServiceManager.js";
import { Block } from "../models/block.js";

dotenv.config();

class BlockPreprocessorWorker {
  constructor() {
    this.isRunning = false;
    this.endHeight = 824190;
    this.batchSize = 50; 
    this.delayBetweenBatches = 5000; 
    this.maxRetries = 3;
    this.currentHeight = null;
    this.stats = {
      totalProcessed: 0,
      totalSaved: 0,
      totalErrors: 0,
      startTime: null,
      currentBatch: 0,
      initialHeight: null
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
      
      const actualEndHeight = Math.max(this.endHeight, dbMaxHeight + 1);
      
      if (this.currentHeight < actualEndHeight) {
        logger.info("BlockPreprocessorWorker: Database is already up to date", {
          dbMaxHeight,
          latestHeight,
          endHeight: this.endHeight,
          actualEndHeight,
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
        actualEndHeight,
        latestHeight: latestHeight,
        dbMaxHeight,
        totalToProcess: this.currentHeight - actualEndHeight + 1,
        batchSize: this.batchSize
      });

      await this.processBlocks(actualEndHeight);
      
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
    
    let savedCount = 0;
    for (const block of blocks) {
      if (block) {
        try {
          const blockDoc = new Block({
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
          
          await blockDoc.save();
          savedCount++;
          
        } catch (saveError) {
          if (saveError.code === 11000) {
            logger.debug(`BlockPreprocessorWorker: Block height ${block.height} already exists (concurrent write), skipping`);
          } else {
            logger.warn(`BlockPreprocessorWorker: Failed to save block height ${block.height}`, {
              error: saveError.message
            });
          }
        }
      }
    }

    this.stats.totalSaved += savedCount;
    logger.debug(`BlockPreprocessorWorker: Saved ${savedCount} blocks from batch to database`);
  }

  complete() {
    const endTime = new Date();
    const duration = endTime - this.stats.startTime;
    
    logger.info("BlockPreprocessorWorker: Preprocessing completed successfully", {
      totalProcessed: this.stats.totalProcessed,
      totalSaved: this.stats.totalSaved,
      totalErrors: this.stats.totalErrors,
      totalBatches: this.stats.currentBatch,
      duration: `${Math.floor(duration / 1000)}s`,
      averageTimePerBatch: `${Math.floor(duration / this.stats.currentBatch)}ms`
    });
    
    this.isRunning = false;
    logger.info("BlockPreprocessorWorker: Task completed, shutting down worker process");
    setTimeout(() => {
      process.exit(0);
    }, 3000);
  }
}

const worker = new BlockPreprocessorWorker();

process.on('SIGTERM', () => {
  logger.info('BlockPreprocessorWorker: Received SIGTERM signal');
  worker.isRunning = false;
  process.exit(0);
});

process.on('SIGINT', () => {
  logger.info('BlockPreprocessorWorker: Received SIGINT signal');
  worker.isRunning = false;
  process.exit(0);
});

process.on('uncaughtException', (error) => {
  logger.error('BlockPreprocessorWorker: Uncaught exception', {
    error: error.message,
    stack: error.stack
  });
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('BlockPreprocessorWorker: Unhandled promise rejection', {
    reason: reason,
    promise: promise
  });
  process.exit(1);
});

worker.initialize().catch(error => {
  logger.error('BlockPreprocessorWorker: Failed to initialize', {
    error: error.message,
    stack: error.stack
  });
  process.exit(1);
}); 