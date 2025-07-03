import mongoose from "mongoose";

const blockSchema = new mongoose.Schema({
  height: { type: Number, required: true },
  hash: { type: String, required: true },
  previousBlockHash: { type: String, required: true },
  timestamp: { type: Number, required: true },
  transactions: { type: Array, required: true },
});

export const Block = mongoose.model("Block", blockSchema);