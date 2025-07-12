import express from "express";

import serviceManager from "../services/ServiceManager.js";
import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";

const router = express.Router();

router.get("/:address", async (req, res, next) => {
  try {
    const { address } = req.params;

    logger.info("Comprehensive address info request", {
      address,
      ip: getRealClientIP(req),
    });

    const addressDetails = await serviceManager.getAddressDetails(address);

    res.status(200).json({
      address: addressDetails.address,
      validation: addressDetails.validation,
      balance: addressDetails.balance.confirmed,
      unconfirmed: addressDetails.balance.unconfirmed,
      transactions: addressDetails.transactions,
      totalTransactions: addressDetails.totalTransactions,
      scriptHash: addressDetails.scriptHash
    });
  } catch (error) {
    next(error);
  }
});

export default router; 