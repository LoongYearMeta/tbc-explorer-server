import Decimal from "decimal.js";

export const coinConfig = {
  genesisBlockHash:
    "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
  genesisCoinbaseTransactionId:
    "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
  genesisCoinbaseTransaction: {
    hex: "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0804ffff001d02fd04ffffffff0100f2052a01000000434104f5eeb2b10c944c6b9fbcfff94c35bdeecd93df977882babc7f3a2cf7f5c81d3b09a68db7f0e04f21de5d4230e75e6dbe7ad16eefe0d4325a62067dc6f369446aac00000000",
    txid: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
    hash: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
    size: 204,
    vsize: 204,
    version: 1,
    confirmations: 475000,
    vin: [
      {
        coinbase:
          "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73",
        sequence: 4294967295,
      },
    ],
    vout: [
      {
        value: 50,
        n: 0,
        scriptPubKey: {
          asm: "04f5eeb2b10c944c6b9fbcfff94c35bdeecd93df977882babc7f3a2cf7f5c81d3b09a68db7f0e04f21de5d4230e75e6dbe7ad16eefe0d4325a62067dc6f369446a OP_CHECKSIG",
          hex: "4104f5eeb2b10c944c6b9fbcfff94c35bdeecd93df977882babc7f3a2cf7f5c81d3b09a68db7f0e04f21de5d4230e75e6dbe7ad16eefe0d4325a62067dc6f369446aac",
          reqSigs: 1,
          type: "pubkey",
          addresses: ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
        },
      },
    ],
    blockhash:
      "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
    time: 1230988505,
    blocktime: 1230988505,
  },
  genesisCoinbaseOutputAddressScripthash:
    "8b01df4e368ea28f8dc0423bcf7a4923e3a12d307c875e47a0cfbf90b5c39161",
  blockRewardFunction: function (blockHeight) {
    const Decimal6 = Decimal.clone({ precision: 6, rounding: 6 });
    var eras = [new Decimal6(5000)];
    for (var i = 1; i < 34; i++) {
      var previous = eras[i - 1];
      eras.push(new Decimal6(previous).dividedBy(2));
    }

    var index = Math.floor(blockHeight / 210000);
    return eras[index];
  },
};
