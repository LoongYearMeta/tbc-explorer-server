import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";
import { Block } from "../models/block.js";

const router = express.Router();

router.get("/height/:height", async (req, res, next) => {
  try {
    const height = parseInt(req.params.height);

    if (isNaN(height) || height < 0) {
      return res.status(400).json({
        error: "Invalid block height",
        timestamp: new Date().toISOString()
      });
    }

    logger.info("Block by height request", {
      height,
      ip: req.ip,
    });


    let block = await Block.findOne({ height }).lean();
    let source = "database";
    if (!block) {
      logger.debug(`Block height ${height} not found in database, fetching from RPC`);
      block = await serviceManager.getBlockByHeight(height);
      source = "rpc";

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
          logger.debug(`Saved block height ${height} to database`);
        } catch (saveError) {
          if (saveError.code === 11000) {
            logger.debug(`Block height ${height} already exists (concurrent write), skipping`);
          } else {
            logger.warn(`Failed to save block height ${height} to database: ${saveError.message}`);
          }
        }
      }
    }

    if (!block) {
      return res.status(404).json({
        error: "Block not found",
        height,
        timestamp: new Date().toISOString()
      });
    }

    res.status(200).json({
      block,
      source,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/hash/:hash", async (req, res, next) => {
  try {
    const { hash } = req.params;

    logger.info("Block by hash request", {
      hash,
      ip: req.ip,
    });

    let block = await Block.findOne({ hash }).lean();
    let source = "database";

    if (!block) {
      logger.debug(`Block hash ${hash} not found in database, fetching from RPC`);
      block = await serviceManager.getBlockByHash(hash);
      source = "rpc";
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
          logger.debug(`Saved block hash ${hash} to database`);
        } catch (saveError) {
          if (saveError.code === 11000) {
            logger.debug(`Block hash ${hash} already exists (concurrent write), skipping`);
          } else {
            logger.warn(`Failed to save block hash ${hash} to database: ${saveError.message}`);
          }
        }
      }
    }

    if (!block) {
      return res.status(404).json({
        error: "Block not found",
        hash,
        timestamp: new Date().toISOString()
      });
    }

    res.status(200).json({
      block,
      source,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.post("/heights", async (req, res, next) => {
  try {
    const { heights } = req.body;

    if (!Array.isArray(heights) || heights.length === 0) {
      return res.status(400).json({
        error: "heights array is required and cannot be empty",
        timestamp: new Date().toISOString()
      });
    }

    if (heights.length > 100) {
      return res.status(400).json({
        error: "Maximum 100 block heights allowed per request",
        timestamp: new Date().toISOString()
      });
    }

    const validHeights = heights.filter(h => Number.isInteger(h) && h >= 0);
    if (validHeights.length !== heights.length) {
      return res.status(400).json({
        error: "All heights must be non-negative integers",
        timestamp: new Date().toISOString()
      });
    }

    logger.info("Multiple blocks by heights request", {
      count: heights.length,
      ip: req.ip,
    });

    const dbBlocks = await Block.find({ height: { $in: heights } }).lean();
    const dbBlockHeights = new Set(dbBlocks.map(block => block.height));
    const missingHeights = heights.filter(height => !dbBlockHeights.has(height));

    let rpcBlocks = [];
    let savedCount = 0;

    if (missingHeights.length > 0) {
      logger.debug(`Fetching ${missingHeights.length} blocks from RPC: ${missingHeights.join(', ')}`);
      rpcBlocks = await serviceManager.getBlocksByHeight(missingHeights);
      const blockDocs = [];
      for (const block of rpcBlocks) {
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

      if (blockDocs.length > 0) {
        try {
          const result = await Block.insertMany(blockDocs, {
            ordered: false,
            rawResult: true
          });
          savedCount = result.insertedCount || blockDocs.length;
          logger.debug(`Batch inserted ${savedCount} blocks successfully`);
        } catch (error) {
          if (error.code === 11000 && error.writeErrors) {
            savedCount = blockDocs.length - error.writeErrors.length;
            logger.debug(`Batch insert completed with ${error.writeErrors.length} duplicates, ${savedCount} succeeded`);
          } else {
            logger.warn(`Batch insert failed, falling back to individual inserts: ${error.message}`);
            for (const blockData of blockDocs) {
              try {
                const blockDoc = new Block(blockData);
                await blockDoc.save();
                savedCount++;
              } catch (saveError) {
                if (saveError.code === 11000) {
                  logger.debug(`Block height ${blockData.height} already exists (concurrent write), skipping`);
                } else {
                  logger.warn(`Failed to save block height ${blockData.height} to database: ${saveError.message}`);
                }
              }
            }
          }
        }
      }

      if (savedCount > 0) {
        logger.debug(`Saved ${savedCount} new blocks to database`);
      }
    }

    const allBlocks = [...dbBlocks, ...rpcBlocks.filter(b => b !== null)];
    const blockMap = new Map(allBlocks.map(block => [block.height, block]));
    const orderedBlocks = heights.map(height => blockMap.get(height)).filter(block => block !== undefined);

    res.status(200).json({
      blocks: orderedBlocks,
      total: orderedBlocks.length,
      sources: {
        database: dbBlocks.length,
        rpc: rpcBlocks.filter(b => b !== null).length
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

router.get("/latest", async (req, res, next) => {
  try {
    logger.info("Latest 10 blocks request", {
      ip: req.ip,
    });
    const blockchainInfo = await serviceManager.getBlockchainInfo();
    const currentHeight = blockchainInfo.blocks;
    const startHeight = Math.max(0, currentHeight - 9);
    const heights = [];
    for (let i = currentHeight; i >= startHeight; i--) {
      heights.push(i);
    }

    logger.debug(`Getting latest blocks from height ${startHeight} to ${currentHeight}`);

    const dbBlocks = await Block.find({ height: { $in: heights } }).lean();
    const dbBlockHeights = new Set(dbBlocks.map(block => block.height));
    const missingHeights = heights.filter(height => !dbBlockHeights.has(height));

    let rpcBlocks = [];
    let savedCount = 0;

    if (missingHeights.length > 0) {
      logger.debug(`Fetching ${missingHeights.length} missing blocks from RPC: ${missingHeights.join(', ')}`);
      rpcBlocks = await serviceManager.getBlocksByHeight(missingHeights);

      const blockDocs = [];
      for (const block of rpcBlocks) {
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

      if (blockDocs.length > 0) {
        try {
          const result = await Block.insertMany(blockDocs, {
            ordered: false,
            rawResult: true
          });
          savedCount = result.insertedCount || blockDocs.length;
          logger.debug(`Batch inserted ${savedCount} blocks successfully`);
        } catch (error) {
          if (error.code === 11000 && error.writeErrors) {
            savedCount = blockDocs.length - error.writeErrors.length;
            logger.debug(`Batch insert completed with ${error.writeErrors.length} duplicates, ${savedCount} succeeded`);
          } else {
            logger.warn(`Batch insert failed, falling back to individual inserts: ${error.message}`);
            for (const blockData of blockDocs) {
              try {
                const blockDoc = new Block(blockData);
                await blockDoc.save();
                savedCount++;
              } catch (saveError) {
                if (saveError.code === 11000) {
                  logger.debug(`Block height ${blockData.height} already exists (concurrent write), skipping`);
                } else {
                  logger.warn(`Failed to save block height ${blockData.height} to database: ${saveError.message}`);
                }
              }
            }
          }
        }
      }

      if (savedCount > 0) {
        logger.debug(`Saved ${savedCount} new blocks to database`);
      }
    }

    const allBlocks = [...dbBlocks, ...rpcBlocks.filter(b => b !== null)];
    const blockMap = new Map(allBlocks.map(block => [block.height, block]));
    const orderedBlocks = heights.map(height => blockMap.get(height)).filter(block => block !== undefined);

    res.status(200).json({
      blocks: orderedBlocks,
      total: orderedBlocks.length,
      currentHeight: currentHeight,
      sources: {
        database: dbBlocks.length,
        rpc: rpcBlocks.filter(b => b !== null).length
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    next(error);
  }
});

export default router;
