import mongoose from "mongoose";

const blockSchema = new mongoose.Schema({
  hash: { type: String, required: true, unique: true, index: true },
  height: { type: Number, required: true, unique: true, index: true },
  size: { type: Number, required: true },
  version: { type: Number, required: true },
  versionHex: { type: String, required: true },
  merkleroot: { type: String, required: true },
  num_tx: { type: Number, required: true },
  time: { type: Number, required: true },
  mediantime: { type: Number, required: true },
  nonce: { type: Number, required: true },
  bits: { type: String, required: true },
  difficulty: { type: Number, required: true },
  chainwork: { type: String, required: true },
  previousblockhash: { type: String, required: true },
  nextblockhash: { type: String, default: null },
  tx: [{ type: String, required: true }], 
  totalFees: { type: String, required: true },
  miner: { type: String, required: true }
}, {
  collection: 'blocks',
  versionKey: false,
});

blockSchema.index({ hash: 1 });
blockSchema.index({ height: 1 });

export const Block = mongoose.model("Block", blockSchema);