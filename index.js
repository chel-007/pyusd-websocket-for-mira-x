const { Firestore } = require('@google-cloud/firestore');
const WebSocket = require('ws');
const express = require('express');

const credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);

const gcpProjectId = process.env.GOOGLE_CLOUD_PROJECT_ID;
const gcpApiKey = process.env.GOOGLE_CLOUD_KEY;

// Initialize Firestore with explicit credentials
const firestore = new Firestore({
  credentials: credentials,
  projectId: credentials.project_id,
});
const txCollection = firestore.collection('transfer_transactions');
const lpCollection = firestore.collection('lp_and_transfers');
const app = express();
const port = process.env.PORT || 8080;

const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const CURVE_SWAP_TOPIC = '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140';

const PYUSD_ADDRESS = '0x6c3ea9036406852006290770bedfcaba0e23a0e8'.toLowerCase();
const CURVE_POOL_1 = '0x383e6b4437b59fff47b619cba855ca29342a8559'.toLowerCase();
const CURVE_POOL_2 = '0x625e92624bc2d88619accc1788365a69767f6200'.toLowerCase();

// Cache for block timestamps to reduce RPC calls
const blockTimestampCache = new Map();

// Batch writing configuration
const BATCH_SIZE = 10;
const WRITE_INTERVAL_MS = 5000;
let pendingWrites = [];
let writeTimeout = null;

// Global uncaught exception handler
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  startWebSocket();
});

async function fetchWithRetry(method, params, retries = 3, delay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(
        `https://blockchain.googleapis.com/v1/projects/${gcpProjectId}/locations/us-central1/endpoints/ethereum-mainnet/rpc?key=${gcpApiKey}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            jsonrpc: "2.0",
            id: method === 'eth_getBlockByNumber' ? 2 : method === 'eth_getTransactionByHash' ? 3 : 4,
            method,
            params,
          }),
        }
      );
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }
      const data = await response.json();
      if (data.error) {
        throw new Error(data.error.message);
      }
      return data.result;
    } catch (error) {
      const attempt = i + 1;
      console.warn(`Retrying ${method} (attempt ${attempt}/${retries})... ${error.message}`);
      if (attempt === retries) {
        console.error(`Failed ${method} after ${retries} attempts: ${error.message}`);
        return null;
      }
      await new Promise(resolve => setTimeout(resolve, delay * (attempt ** 2))); // Quadratic backoff
    }
  }
}

async function getBlockTimestamp(blockNumberHex) {
  if (blockTimestampCache.has(blockNumberHex)) {
    return blockTimestampCache.get(blockNumberHex);
  }
  const block = await fetchWithRetry("eth_getBlockByNumber", [blockNumberHex, false]);
  if (!block || !block.timestamp) {
    console.warn(`Block ${blockNumberHex} missing timestamp, using current time`);
    const timestampMs = Date.now();
    blockTimestampCache.set(blockNumberHex, timestampMs);
    return timestampMs;
  }
  const timestampMs = parseInt(block.timestamp, 16) * 1000;
  blockTimestampCache.set(blockNumberHex, timestampMs);
  if (blockTimestampCache.size > 1000) {
    const oldestKey = blockTimestampCache.keys().next().value;
    blockTimestampCache.delete(oldestKey);
  }
  return timestampMs;
}

async function getTransactionDetails(txHash) {
  const [tx, receipt] = await Promise.all([
    fetchWithRetry("eth_getTransactionByHash", [txHash]),
    fetchWithRetry("eth_getTransactionReceipt", [txHash]),
  ]);
  // Fallback values if fetch fails
  const safeTx = tx || {};
  const safeReceipt = receipt || {};

  const gasPriceRaw = safeTx.gasPrice || "0x0"; // Default to 0 if missing
  const gasUsedRaw = safeReceipt.gasUsed || "0x0"; // Default to 0 if missing

  const gasPrice = parseInt(gasPriceRaw, 16);
  const gasUsed = parseInt(gasUsedRaw, 16);

  // Log invalid data for debugging
  if (isNaN(gasPrice) || isNaN(gasUsed)) {
    console.warn(`Invalid gas data for tx ${txHash}`, { gasPriceRaw, gasUsedRaw });
  }

  return {
    input: safeTx.input || '0x',
    gasPrice: isNaN(gasPrice) ? 0 : gasPrice, // Fallback to 0
    gasUsed: isNaN(gasUsed) ? 0 : gasUsed,    // Fallback to 0
    status: parseInt(safeReceipt.status || "0x1", 16), // Assume success if missing
    fromAddress: safeTx.from ? safeTx.from.toLowerCase() : null,
    toAddress: safeTx.to ? safeTx.to.toLowerCase() : null,
  };
}

async function processEvent(logData, eventType) {
  const txHash = logData.transactionHash;
  const blockNumberHex = logData.blockNumber;
  const blockNumber = parseInt(blockNumberHex, 16);
  const logIndex = parseInt(logData.logIndex, 16);
  const address = logData.address.toLowerCase();
  const eventSignature = logData.topics[0];
  const topics = logData.topics;

  // Fetch additional transaction details
  const { input, gasPrice, gasUsed, status, fromAddress, toAddress } = await getTransactionDetails(txHash);
  const timestampMs = await getBlockTimestamp(blockNumberHex);
  if (typeof timestampMs !== 'number' || isNaN(timestampMs)) {
    console.error(`Invalid timestamp for tx ${txHash}, using current time`);
    timestampMs = Date.now();
  }

  const timestampSec = Math.floor(timestampMs / 1000);

  let args = {};
  if (eventType === 'Transfer') {
    const rawValue = BigInt(logData.data);
    // Extract from and to addresses from topics
    const from = '0x' + logData.topics[1].slice(-40);
    const to = '0x' + logData.topics[2].slice(-40);
    // Store args as an array: [from, to, value]
    args = [from, to, rawValue.toString()];
  } else if (eventType === 'Swap' && [CURVE_POOL_1, CURVE_POOL_2].includes(address)) {
    // Curve TokenExchange event
    const dataHex = logData.data.slice(2);
    args = {
      buyer: '0x' + logData.topics[1].slice(-40),
      sold_id: parseInt(dataHex.slice(0, 64), 16).toString(),
      tokens_sold: BigInt('0x' + dataHex.slice(64, 128)).toString(),
      bought_id: parseInt(dataHex.slice(128, 192), 16).toString(),
      tokens_bought: BigInt('0x' + dataHex.slice(192, 256)).toString(),
    };
  }

  const eventData = {
    block_number: blockNumber,
    block_timestamp: timestampSec, // Seconds for lp_and_transfers
    tx_hash: txHash,
    log_index: logIndex,
    address,
    event_signature: eventSignature,
    topics,
    args,
    from_address: fromAddress,
    to_address: toAddress,
    input,
    gas_price: gasPrice,
    gas_used: gasUsed,
    status,
    event_type: eventType,
  };

  return eventData;
}

async function batchWriteToFirestore() {
  // Only write if there are events to write
  if (pendingWrites.length === 0) {
    return;
  }

  try {
    const batch = firestore.batch();
    const eventsToWrite = pendingWrites.splice(0, BATCH_SIZE); // Take up to BATCH_SIZE events
    for (const { collection, docId, data } of eventsToWrite) {
      batch.set(collection.doc(docId), data);
    }
    await batch.commit();
    console.log(`✅ Batch wrote ${eventsToWrite.length} events to Firestore`);

    // If there are still enough events in pendingWrites, schedule another write immediately
    if (pendingWrites.length >= BATCH_SIZE) {
      await batchWriteToFirestore();
    }
  } catch (error) {
    console.error("Error writing batch to Firestore:", error);
    // Add failed events back to pendingWrites for retry
    pendingWrites.unshift(...eventsToWrite);
  }
}

function scheduleBatchWrite() {
  // Clear any existing timeout
  if (writeTimeout) {
    clearTimeout(writeTimeout);
  }

  // If we have enough events, write immediately
  if (pendingWrites.length >= BATCH_SIZE) {
    batchWriteToFirestore();
  } else {
    // Otherwise, schedule a write after WRITE_INTERVAL_MS
    writeTimeout = setTimeout(() => {
      // Only write if we have at least 1 event
      if (pendingWrites.length > 0) {
        batchWriteToFirestore();
      }
      writeTimeout = null;
    }, WRITE_INTERVAL_MS);
  }
}

function attachWebSocketHandlers(ws) {
  ws.on("open", () => {
    console.log("✅ Connected to GCP WebSocket");

    // Subscribe to PYUSD Transfer events
    ws.send(
      JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "eth_subscribe",
        params: [
          "logs",
          {
            address: PYUSD_ADDRESS,
            topics: [TRANSFER_TOPIC],
          },
        ],
      })
    );

    // Subscribe to Curve TokenExchange events (both pools)
    ws.send(
      JSON.stringify({
        jsonrpc: "2.0",
        id: 2,
        method: "eth_subscribe",
        params: [
          "logs",
          {
            address: [CURVE_POOL_1, CURVE_POOL_2],
            topics: [CURVE_SWAP_TOPIC],
          },
        ],
      })
    );

  ws.on("message", async (data) => {
    try {
      const log = JSON.parse(data);
      const logData = log.params?.result;
      if (!logData || !logData.topics) return;

      const eventSignature = logData.topics[0];
      const address = logData.address.toLowerCase();
      const txHash = logData.transactionHash;

      if (eventSignature === TRANSFER_TOPIC && address === PYUSD_ADDRESS) {
        // Process PYUSD Transfer event
        const { input } = await getTransactionDetails(txHash);
        const rawValue = BigInt(logData.data);
        const adjustedValue = Number(rawValue) / 1_000_000;
        const sender = "0x" + logData.topics[1].slice(26);
        const receiver = "0x" + logData.topics[2].slice(26);
        const value = adjustedValue.toFixed(6);
        const blockNumberHex = logData.blockNumber;
        const blockNumber = parseInt(blockNumberHex, 16);
        const timestampMs = await getBlockTimestamp(blockNumberHex);
        const timestamp = new Date(timestampMs).toISOString();

        // Always store in transfer_transactions
        const txData = {
          txHash,
          sender,
          receiver,
          value,
          timestamp,
          blockNumber,
        };
        pendingWrites.push({
          collection: txCollection,
          docId: txHash,
          data: txData,
        });
        console.log(`✅ Uploaded to transfer_transactions: ${txHash} | ${sender} → ${receiver} | Amount: ${value}`);

        // For lp_and_transfers, only include if input starts with 0xa9059cbb
        if (input.startsWith('0xa9059cbb')) {
          const eventData = await processEvent(logData, 'Transfer');
          pendingWrites.push({
            collection: lpCollection,
            docId: `${txHash}-${logData.logIndex}`,
            data: eventData,
          });
          console.log(`✅ Processed Direct Transfer for lp_and_transfers: ${txHash}`);
        }
      } else if (eventSignature === CURVE_SWAP_TOPIC && [CURVE_POOL_1, CURVE_POOL_2].includes(address)) {
        // Process Curve TokenExchange event
        const eventData = await processEvent(logData, 'Swap');
        pendingWrites.push({
          collection: lpCollection,
          docId: `${txHash}-${logData.logIndex}`,
          data: eventData,
        });
        console.log(`✅ Processed Curve Swap: ${txHash} | Pool: ${address}`);
      }
      // Schedule a batch write
      scheduleBatchWrite();
    } catch (error) {
      console.error("Error processing WebSocket message:", error);
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error.message);
    console.log('Reconnecting due to error...');
    ws.close();
  });

  ws.on('close', () => {
    console.log('WebSocket closed, reconnecting...');
    if (writeTimeout) {
      clearTimeout(writeTimeout);
      writeTimeout = null;
    }
    // Write any remaining events before reconnecting
    batchWriteToFirestore().then(() => {
      pendingWrites = []; // Clear pending writes after writing
      setTimeout(startWebSocket, 2000);
    });
  });
}

function startWebSocket() {
  console.log('🚀 Starting WebSocket connection...');
  const ws = new WebSocket(`wss://blockchain.googleapis.com/v1/projects/${gcpProjectId}/locations/us-central1/endpoints/ethereum-mainnet/rpc?key=${gcpApiKey}`);
  attachWebSocketHandlers(ws);
}

app.get('/health', (req, res) => {
  res.status(200).send('OK');
  console.log('Health ping received');
});

app.get('/', (req, res) => res.status(200).send('WebSocket service running'));
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
  startWebSocket();
});
