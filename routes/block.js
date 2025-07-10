import express from "express";

import serviceManager from "../services/ServiceManager.js";
import redisService from "../services/RedisService.js";
import logger from "../config/logger.js";
import { Block } from "../models/block.js";
import { getRealClientIP } from "../lib/util.js";

const router = express.Router();

function formatBlockResponse(block) {
  if (!block) return null;
  return {
    tx: block.tx,
    hash: block.hash,
    confirmations: block.confirmations,
    size: block.size,
    height: block.height,
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
    coinbaseTx: block.coinbaseTx,
    totalFees: block.totalFees,
    miner: block.miner
  };
}

router.get("/height/:height", async (req, res, next) => {
  try {
    const height = parseInt(req.params.height);

    if (isNaN(height) || height < 0) {
      return res.status(400).json({
        error: "Invalid block height"
      });
    }

    logger.info("Block by height request", {
      height,
      ip: getRealClientIP(req),
    });

    let block = null;

    try {
      const cacheKey = `blocks:recent:${height}`;
      const cachedBlock = await redisService.getJSON(cacheKey);
      if (cachedBlock) {
        block = cachedBlock;
        logger.debug(`Block height ${height} found in Redis cache`);
      }
    } catch (error) {
      logger.warn(`Redis lookup failed for block height ${height}`, { error: error.message });
    }

    if (!block) {
      block = await Block.findOne({ height }).lean();
      if (block) {
        logger.debug(`Block height ${height} found in database`);
      } else {
        logger.debug(`Block height ${height} not found in database, fetching from RPC`);
        block = await serviceManager.getBlockByHeight(height);
        if (block) {
          logger.debug(`Block height ${height} fetched from RPC`);
          
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
    }

    if (!block) {
      return res.status(404).json({
        error: "Block not found",
        height
      });
    }

    res.status(200).json({
      block: formatBlockResponse(block),
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
      ip: getRealClientIP(req),
    });

    let block = null;

    try {
      const dbBlock = await Block.findOne({ hash }).select('height').lean();
      if (dbBlock) {
        const cacheKey = `blocks:recent:${dbBlock.height}`;
        const cachedBlock = await redisService.getJSON(cacheKey);
        if (cachedBlock && cachedBlock.hash === hash) {
          block = cachedBlock;
          logger.debug(`Block hash ${hash} found in Redis cache`);
        }
      }
    } catch (error) {
      logger.warn(`Redis lookup failed for block hash ${hash}`, { error: error.message });
    }

    if (!block) {
      block = await Block.findOne({ hash }).lean();
      if (block) {
        logger.debug(`Block hash ${hash} found in database`);
      } else {
        logger.debug(`Block hash ${hash} not found in database, fetching from RPC`);
        block = await serviceManager.getBlockByHash(hash);
        if (block) {
          logger.debug(`Block hash ${hash} fetched from RPC`);
          
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
    }

    if (!block) {
      return res.status(404).json({
        error: "Block not found",
        hash
      });
    }

    res.status(200).json({
      block: formatBlockResponse(block)
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
        error: "heights array is required and cannot be empty"
      });
    }

    if (heights.length > 100) {
      return res.status(400).json({
        error: "Maximum 100 block heights allowed per request"
      });
    }

    const validHeights = heights.filter(h => Number.isInteger(h) && h >= 0);
    if (validHeights.length !== heights.length) {
      return res.status(400).json({
        error: "All heights must be non-negative integers"
      });
    }

    logger.info("Multiple blocks by heights request", {
      count: heights.length,
      ip: getRealClientIP(req),
    });

    const redisBlockMap = new Map();
    try {
      const redisPromises = heights.map(height => 
        redisService.getJSON(`blocks:recent:${height}`).catch(err => {
          logger.debug(`Redis lookup failed for block height ${height}:`, err.message);
          return null;
        })
      );
      const redisResults = await Promise.all(redisPromises);
      
      redisResults.forEach((cachedBlock, index) => {
        if (cachedBlock) {
          redisBlockMap.set(heights[index], cachedBlock);
        }
      });
      
      logger.debug(`Found ${redisBlockMap.size} blocks in Redis cache`);
    } catch (error) {
      logger.warn('Redis batch lookup failed', { error: error.message });
    }

    const redisNotFoundHeights = heights.filter(height => !redisBlockMap.has(height));
    let dbBlockMap = new Map();
    
    if (redisNotFoundHeights.length > 0) {
      const dbBlocks = await Block.find({ height: { $in: redisNotFoundHeights } }).lean();
      dbBlockMap = new Map(dbBlocks.map(block => [block.height, block]));
      logger.debug(`Found ${dbBlocks.length} blocks in database`);
    }

    const dbNotFoundHeights = redisNotFoundHeights.filter(height => !dbBlockMap.has(height));
    let rpcBlocks = [];
    let savedCount = 0;

    if (dbNotFoundHeights.length > 0) {
      logger.debug(`Fetching ${dbNotFoundHeights.length} blocks from RPC: ${dbNotFoundHeights.join(', ')}`);
      rpcBlocks = await serviceManager.getBlocksByHeight(dbNotFoundHeights);
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

    const rpcBlockMap = new Map();
    rpcBlocks.forEach((block, index) => {
      if (block) {
        rpcBlockMap.set(dbNotFoundHeights[index], block);
      }
    });

    const orderedBlocks = heights.map(height => {
      return redisBlockMap.get(height) || dbBlockMap.get(height) || rpcBlockMap.get(height);
    }).filter(block => block !== undefined);

    logger.info("Batch block lookup completed", {
      total: heights.length,
      redisHits: redisBlockMap.size,
      dbHits: dbBlockMap.size,
      rpcHits: rpcBlockMap.size
    });

    res.status(200).json({
      blocks: orderedBlocks.map(block => formatBlockResponse(block)),
      total: orderedBlocks.length
    });
  } catch (error) {
    next(error);
  }
});

router.get("/latest", async (req, res, next) => {
  try {
    logger.info("Latest 11 blocks request", {
      ip: getRealClientIP(req),
    });
    const blockchainInfo = await serviceManager.getBlockchainInfo();
    const currentHeight = blockchainInfo.blocks;
    const startHeight = Math.max(0, currentHeight - 10);
    const heights = [];
    for (let i = currentHeight; i >= startHeight; i--) {
      heights.push(i);
    }

    logger.debug(`Getting latest blocks from height ${startHeight} to ${currentHeight}`);

    const redisBlockMap = new Map();
    try {
      const redisPromises = heights.map(height => 
        redisService.getJSON(`blocks:recent:${height}`).catch(err => {
          logger.debug(`Redis lookup failed for block height ${height}:`, err.message);
          return null;
        })
      );
      const redisResults = await Promise.all(redisPromises);
      
      redisResults.forEach((cachedBlock, index) => {
        if (cachedBlock) {
          redisBlockMap.set(heights[index], cachedBlock);
        }
      });
      
      logger.debug(`Found ${redisBlockMap.size} latest blocks in Redis cache`);
    } catch (error) {
      logger.warn('Redis batch lookup failed for latest blocks', { error: error.message });
    }

    const redisNotFoundHeights = heights.filter(height => !redisBlockMap.has(height));
    let dbBlockMap = new Map();
    
    if (redisNotFoundHeights.length > 0) {
      const dbBlocks = await Block.find({ height: { $in: redisNotFoundHeights } }).lean();
      dbBlockMap = new Map(dbBlocks.map(block => [block.height, block]));
      logger.debug(`Found ${dbBlocks.length} latest blocks in database`);
    }

    const dbNotFoundHeights = redisNotFoundHeights.filter(height => !dbBlockMap.has(height));
    let rpcBlocks = [];
    let savedCount = 0;

    if (dbNotFoundHeights.length > 0) {
      logger.debug(`Fetching ${dbNotFoundHeights.length} missing blocks from RPC: ${dbNotFoundHeights.join(', ')}`);
      rpcBlocks = await serviceManager.getBlocksByHeight(dbNotFoundHeights);

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

    const rpcBlockMap = new Map();
    rpcBlocks.forEach((block, index) => {
      if (block) {
        rpcBlockMap.set(dbNotFoundHeights[index], block);
      }
    });

    const orderedBlocks = heights.map(height => {
      return redisBlockMap.get(height) || dbBlockMap.get(height) || rpcBlockMap.get(height);
    }).filter(block => block !== undefined);

    logger.info("Latest blocks lookup completed", {
      total: heights.length,
      redisHits: redisBlockMap.size,
      dbHits: dbBlockMap.size,
      rpcHits: rpcBlockMap.size
    });

    res.status(200).json({
      blocks: orderedBlocks.map(block => formatBlockResponse(block)),
      total: orderedBlocks.length,
      currentHeight: currentHeight
    });
  } catch (error) {
    next(error);
  }
});

export default router;
