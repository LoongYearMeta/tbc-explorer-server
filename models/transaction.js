import mongoose from "mongoose";

const transactionSchema = new mongoose.Schema({
    txid: { type: String, required: true, unique: true, index: true },
    raw: { type: String, required: true },
    blockHeight: { type: Number, required: true, index: true },
},{
    collection: 'transactions',
    versionKey: false,
});

transactionSchema.index({ txid: 1 });
transactionSchema.index({ blockHeight: 1 });
transactionSchema.index({ blockHeight: -1, txid: 1 });

transactionSchema.statics.getDistinctBlockCount = async function() {
    const distinctHeights = await this.distinct('blockHeight');
    return distinctHeights.length;
};

transactionSchema.statics.getDistinctBlockHeights = async function(sortOrder = -1) {
    const distinctHeights = await this.distinct('blockHeight');
    return distinctHeights.sort((a, b) => sortOrder === -1 ? b - a : a - b);
};

transactionSchema.statics.deleteByBlockHeight = async function(blockHeight) {
    const result = await this.deleteMany({ blockHeight });
    return result;
};

transactionSchema.statics.deleteByBlockHeights = async function(blockHeights) {
    const result = await this.deleteMany({ blockHeight: { $in: blockHeights } });
    return result;
};

transactionSchema.statics.deleteBeforeHeight = async function(height) {
    const result = await this.deleteMany({ blockHeight: { $lt: height } });
    return result;
};

transactionSchema.statics.getStats = async function() {
    const totalCount = await this.countDocuments();
    const distinctHeights = await this.distinct('blockHeight');
    const oldestHeight = distinctHeights.length > 0 ? Math.min(...distinctHeights) : null;
    const newestHeight = distinctHeights.length > 0 ? Math.max(...distinctHeights) : null;
    
    return {
        totalTransactions: totalCount,
        distinctBlockCount: distinctHeights.length,
        oldestBlockHeight: oldestHeight,
        newestBlockHeight: newestHeight,
        heightRange: oldestHeight && newestHeight ? newestHeight - oldestHeight + 1 : 0
    };
};

export const Transaction = mongoose.model("Transaction", transactionSchema);