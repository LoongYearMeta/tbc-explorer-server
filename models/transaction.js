import mongoose from "mongoose";

const transactionSchema = new mongoose.Schema({
  txid: { type: String, required: true, unique: true, index: true },
  raw: { type: String, required: true }
}, {
  timestamps: true,
  collection: 'transactions'
});

transactionSchema.index({ txid: 1 });

export const Transaction = mongoose.model("Transaction", transactionSchema);