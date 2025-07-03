import mongoose from "mongoose";

const transactionSchema = new mongoose.Schema({
  txid: { type: String, required: true },
  raw: { type: String, required: true },
  blockHeight: { type: Number, required: true },
  blockHash: { type: String, required: true },
  timestamp: { type: Number, required: true },
  size: { type: Number, required: true },
});

export const Transaction = mongoose.model("Transaction", transactionSchema);