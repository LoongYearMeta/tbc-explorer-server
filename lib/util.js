import Decimal from "decimal.js";

import { coinConfig } from "./coin.js";

function generateRandomString(length) {
  const chars =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function splitArrayIntoChunks(array, chunkSize) {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

function getBlockTotalFeesFromCoinbaseTxAndBlockHeight(
  coinbaseTx,
  blockHeight
) {
  if (coinbaseTx == null) {
    return 0;
  }

  var blockReward = coinConfig.blockRewardFunction(blockHeight);

  var totalOutput = new Decimal(0);
  for (var i = 0; i < coinbaseTx.vout.length; i++) {
    var outputValue = coinbaseTx.vout[i].value;
    if (outputValue > 0) {
      totalOutput = totalOutput.plus(new Decimal(outputValue));
    }
  }

  return totalOutput.minus(new Decimal(blockReward));
}

function getMinerFromCoinbaseTx(tx) {
  if (tx == null || tx.vin == null || tx.vin.length == 0) {
    return null;
  }

  if (global.miningPoolsConfigs) {
    for (var i = 0; i < global.miningPoolsConfigs.length; i++) {
      var miningPoolsConfig = global.miningPoolsConfigs[i];

      for (var payoutAddress in miningPoolsConfig.payout_addresses) {
        if (miningPoolsConfig.payout_addresses.hasOwnProperty(payoutAddress)) {
          if (
            tx.vout &&
            tx.vout.length > 0 &&
            tx.vout[0].scriptPubKey &&
            tx.vout[0].scriptPubKey.addresses &&
            tx.vout[0].scriptPubKey.addresses.length > 0
          ) {
            if (tx.vout[0].scriptPubKey.addresses[0] == payoutAddress) {
              var minerInfo = miningPoolsConfig.payout_addresses[payoutAddress];
              minerInfo.identifiedBy = "payout address " + payoutAddress;

              return minerInfo;
            }
          }
        }
      }

      for (var coinbaseTag in miningPoolsConfig.coinbase_tags) {
        if (miningPoolsConfig.coinbase_tags.hasOwnProperty(coinbaseTag)) {
          if (hex2ascii(tx.vin[0].coinbase).indexOf(coinbaseTag) != -1) {
            var minerInfo = miningPoolsConfig.coinbase_tags[coinbaseTag];
            minerInfo.identifiedBy = "coinbase tag '" + coinbaseTag + "'";

            return minerInfo;
          }
        }
      }
    }
  }

  if (tx.vin[0].coinbase) {
    return hex2ascii(tx.vin[0].coinbase);
  }

  return null;
}

function hex2ascii(hex) {
  var str = "";
  for (var i = 0; i < hex.length; i += 2) {
    str += String.fromCharCode(parseInt(hex.substr(i, 2), 16));
  }

  return str;
}

function getRealClientIP(req) {
  const xForwardedFor = req.headers['x-forwarded-for'];
  if (xForwardedFor) {
    const ips = xForwardedFor.split(',').map(ip => ip.trim());
    for (const ip of ips) {
      if (ip && !isPrivateIP(ip)) {
        return ip;
      }
    }
    if (ips[0]) {
      return ips[0];
    }
  }

  const xRealIP = req.headers['x-real-ip'];
  if (xRealIP) {
    return xRealIP;
  }

  const xClientIP = req.headers['x-client-ip'];
  if (xClientIP) {
    return xClientIP;
  }

  const cfConnectingIP = req.headers['cf-connecting-ip'];
  if (cfConnectingIP) {
    return cfConnectingIP;
  }

  return req.ip || req.connection.remoteAddress;
}

function isPrivateIP(ip) {
  if (!ip) return false;

  const privateIPv4Patterns = [
    /^10\./,
    /^172\.(1[6-9]|2\d|3[01])\./,
    /^192\.168\./,
    /^127\./,
    /^169\.254\./,
    /^0\.0\.0\.0$/,
  ];

  const privateIPv6Patterns = [
    /^::1$/,
    /^fe80:/,
    /^fc00:/,
    /^fd00:/,
  ];

  return privateIPv4Patterns.some(pattern => pattern.test(ip)) ||
    privateIPv6Patterns.some(pattern => pattern.test(ip));
}

export {
  generateRandomString,
  splitArrayIntoChunks,
  getBlockTotalFeesFromCoinbaseTxAndBlockHeight,
  getMinerFromCoinbaseTx,
  getRealClientIP,
};
