import express from "express";

import generalRpcAggregator from "../services/GeneralRpcAggregator.js";
import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";

const router = express.Router();

router.get("/", async (req, res, next) => {
  try {
    logger.info("Chain info request", {
      ip: getRealClientIP(req),
    });

    const [blockchainInfo, miningInfo] = await Promise.all([
      generalRpcAggregator.callRpc('getBlockchainInfo'),
      generalRpcAggregator.callRpc('getMiningInfo')
    ]);

    res.status(200).json({
      blockchain: blockchainInfo,
      mining: miningInfo,
    });
  } catch (error) {
    next(error);
  }
});

router.get("/txstats/:blockCount?", async (req, res, next) => {
  try {
    const blockCount = req.params.blockCount ? parseInt(req.params.blockCount) : undefined;

    if (blockCount !== undefined && (isNaN(blockCount) || blockCount <= 0)) {
      return res.status(400).json({
        error: "Block count must be a positive integer"
      });
    }

    logger.info("Chain transaction stats request", {
      blockCount,
      ip: getRealClientIP(req),
    });

    if (blockCount !== undefined) {
      const txStats = await serviceManager.getChainTxStats(blockCount);
      
      res.status(200).json({
        txStats,
        blockCount: blockCount
      });
    } else {
      const timePeriods = [
        { label: "1 day", blocks: 144 },      
        { label: "7 days", blocks: 1008 },   
        { label: "30 days", blocks: 4320 },   
        { label: "365 days", blocks: 52560 }  
      ];

      const txStatsPromises = timePeriods.map(period => 
        generalRpcAggregator.callRpc('getChainTxStats', [period.blocks])
          .then(stats => ({
            period: period.label,
            blockCount: period.blocks,
            stats: stats
          }))
          .catch(error => ({
            period: period.label,
            blockCount: period.blocks,
            error: error.message
          }))
      );

      const txStatsList = await Promise.all(txStatsPromises);

      let time = null;
      let txcount = null;
      
      const processedTxStatsList = txStatsList.map(item => {
        if (item.stats && !item.error) {
          if (time === null && txcount === null) {
            time = item.stats.time;
            txcount = item.stats.txcount;
          }
          
          const { time: statsTime, txcount: statsTxcount, ...remainingStats } = item.stats;
          
          return {
            period: item.period,
            blockCount: item.blockCount,
            stats: remainingStats
          };
        }
        return item; 
      });

      res.status(200).json({
        time,
        txcount,
        txStatsList: processedTxStatsList
      });
    }
  } catch (error) {
    next(error);
  }
});

export default router;
