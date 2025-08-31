const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
let runHistory = [];
let errorLog = [];
let logs = [];
let systemPaused = false;
let mismatches = [];
const missingCounters = new Map();

// Failsafe state management
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;

const jobLocks = { inventory: false, products: false, discontinued: false, fixTracking: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered === true || failsafeTriggered === 'pending' || token !== abortVersion;

const FAILSAFE_LIMITS = {
  MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 100),
  MIN_SHOPIFY_PRODUCTS: Number(process.env.MIN_SHOPIFY_PRODUCTS || 100),
  MAX_CHANGE_PERCENTAGE: Number(process.env.MAX_CHANGE_PERCENTAGE || 30),
  MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5),
  MAX_ERROR_RATE: Number(process.env.MAX_ERROR_RATE || 20),
  MAX_DISCONTINUED_AT_ONCE: Number(process.env.MAX_DISCONTINUED_AT_ONCE || 100),
  FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000)
};
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
let lastFailsafeNotified = '';

async function notifyTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try { await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 }); } catch (e) { addLog(`Telegram notify failed: ${e.message}`, 'warning'); }
}

let lastKnownGoodState = { apifyCount: 0, shopifyCount: 0, timestamp: null };

function addLog(message, type = 'info', jobType = 'system') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 200) logs = logs.slice(0, 200);
  console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`);
  if (type === 'error') { errorLog.push({ timestamp: new Date(), message, jobType }); if (errorLog.length > 500) errorLog = errorLog.slice(-500); }
}

function addToHistory(type, data) {
    runHistory.unshift({ type, timestamp: new Date().toISOString(), ...data });
    if (runHistory.length > 50) runHistory = runHistory.slice(0, 50);
}

function startBackgroundJob(key, name, fn) {
  if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning', key); return false; }
  jobLocks[key] = true;
  const token = getJobToken();
  addLog(`Started background job: ${name}`, 'info', key);
  setImmediate(async () => {
    try { await fn(token); } catch (e) { /* errors logged inside */ } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info', key); }
  });
  return true;
}

function triggerFailsafe(msg, contextData = {}, isConfirmable = false, action = null) {
  if (failsafeTriggered) return;
  failsafeReason = msg;
  systemPaused = true;
  abortVersion++;
  addLog(`⚠️ FAILSAFE TRIGGERED: ${failsafeReason}`, 'error', 'failsafe');
  if (isConfirmable && action) {
    failsafeTriggered = 'pending';
    pendingFailsafeAction = action;
    addLog('System paused, waiting for user confirmation.', 'warning', 'failsafe');
    notifyTelegram(`⚠️ <b>Failsafe Warning - Confirmation Required</b> ⚠️\n\n<b>Reason:</b>\n<pre>${msg}</pre>\n\nTo proceed, reply: <code>/confirm</code>\nTo abort, reply: <code>/abort</code>`);
  } else {
    failsafeTriggered = true;
    addLog('System automatically paused to prevent potential damage.', 'error', 'failsafe');
    if (lastFailsafeNotified !== msg) { lastFailsafeNotified = msg; notifyTelegram(`🚨 <b>Failsafe Triggered & System Paused</b> 🚨\n\n<b>Reason:</b>\n<pre>${msg}</pre>`); }
  }
}

function checkFailsafeConditions(context, data = {}, actionToConfirm = null) {
  const checks = [];
  let isConfirmable = false;
  switch (context) {
    case 'inventory':
      isConfirmable = true;
      if (data.totalApifyProducts > 0 && data.updatesNeeded > data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100)) {
        checks.push(`Too many inventory changes: ${data.updatesNeeded} > ${Math.floor(data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100))} (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}% of ${data.totalApifyProducts})`);
      }
      break;
    case 'discontinued':
        isConfirmable = false;
        if (data.toDiscontinue > FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE) {
            checks.push(`Too many products to discontinue: ${data.toDiscontinue} > ${FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE}`);
        }
        break;
  }
  if (checks.length > 0) {
    const reason = checks.join('; ');
    triggerFailsafe(reason, { checkContext: context, checkData: data }, isConfirmable, actionToConfirm);
    return { proceed: false, reason: isConfirmable ? 'pending_confirmation' : 'hard_stop' };
  }
  return { proceed: true };
}

const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2', urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// ... (All minor helpers like normalizeTitle, sanitizeProductTitle, etc. are assumed here for brevity)

async function getApifyProducts() { /* ... implementation ... */ return []; }
async function getShopifyProducts({ onlyApifyTag = true } = {}) { /* ... implementation ... */ return []; }
function processApifyProducts(apifyData, options = { processPrice: true }) { /* ... implementation ... */ return []; }
function buildShopifyMaps(shopifyData) { /* ... implementation ... */ return new Map(); }
function matchShopifyProduct(apifyProduct, maps) { /* ... implementation ... */ return { product: null }; }

async function getShopifyInventoryLevels(inventoryItemIds, locationId) {
    const inventoryMap = new Map();
    const batchSize = 50;
    addLog(`Fetching inventory levels for ${inventoryItemIds.length} items from location ${locationId}...`, 'info', 'inventory');
    for (let i = 0; i < inventoryItemIds.length; i += batchSize) {
        const batch = inventoryItemIds.slice(i, i + batchSize);
        try {
            const url = `/inventory_levels.json?inventory_item_ids=${batch.join(',')}&location_ids=${locationId}`;
            const response = await shopifyClient.get(url);
            for (const level of response.data.inventory_levels) {
                inventoryMap.set(level.inventory_item_id, level.available || 0);
            }
        } catch (error) { addLog(`Failed to fetch inventory batch: ${error.message}`, 'error', 'inventory'); stats.errors++; }
        await new Promise(r => setTimeout(r, 500));
    }
    addLog(`Successfully fetched ${inventoryMap.size} inventory levels.`, 'info', 'inventory');
    return inventoryMap;
}

// MODIFIED: executeInventoryUpdates now ensures connection before setting stock
async function executeInventoryUpdates(updates, token) {
  let updated = 0, errors = 0;
  if (!updates || updates.length === 0) return { updated, errors };
  addLog(`Executing ${updates.length} inventory updates...`, 'info', 'inventory');

  for (const update of updates) {
      if (shouldAbort(token)) { addLog('Aborting inventory execution...', 'warning', 'inventory'); break; }
      try {
          // STEP 1: Ensure the item is connected to the location.
          try {
              await shopifyClient.post('/inventory_levels/connect.json', {
                  location_id: parseInt(config.shopify.locationId),
                  inventory_item_id: update.inventoryItemId,
              });
              addLog(`Ensured connection for: ${update.title}`, 'info', 'inventory');
          } catch (connectError) {
              // This error is expected if the connection already exists (Shopify returns 422).
              // We can safely ignore it and proceed. We only log if it's an unexpected error.
              if (connectError.response?.status !== 422) {
                  addLog(`Note: Non-422 error during connect for ${update.title}: ${connectError.message}`, 'warning', 'inventory');
              }
          }

          // STEP 2: Now that connection is guaranteed, set the inventory level.
          await shopifyClient.post('/inventory_levels/set.json', {
              location_id: parseInt(config.shopify.locationId),
              inventory_item_id: update.inventoryItemId,
              available: update.newInventory
          });
          addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success', 'inventory');
          updated++;
          stats.inventoryUpdates++;
          await new Promise(r => setTimeout(r, 400));
      } catch (error) {
          errors++;
          stats.errors++;
          addLog(`✗ Failed to update ${update.title}: ${error.message}`, 'error', 'inventory');
      }
  }
  addLog(`Execution finished: ${updated} updated, ${errors} errors.`, 'info', 'inventory');
  return { updated, errors };
}

async function updateInventoryJob(token) {
  if (systemPaused) return { updated: 0, errors: 0, total: 0 };
  let errors = 0;
  try {
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    if (shouldAbort(token)) return { updated: 0, errors: 0 };
    
    const inventoryItemIds = shopifyData.map(p => p.variants?.[0]?.inventory_item_id).filter(Boolean);
    const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds, config.shopify.locationId);
    
    const processedProducts = processApifyProducts(apifyData, { processPrice: false });
    const maps = buildShopifyMaps(shopifyData);
    const inventoryUpdates = [];
    let alreadyInSyncCount = 0;

    processedProducts.forEach((apifyProduct) => {
        const { product: shopifyProduct } = matchShopifyProduct(apifyProduct, maps);
        if (!shopifyProduct || !shopifyProduct.variants?.[0]?.inventory_item_id) return;
        
        const inventoryItemId = shopifyProduct.variants[0].inventory_item_id;
        let currentInventory = parseInt(inventoryLevels.get(inventoryItemId), 10);
        if (isNaN(currentInventory)) { currentInventory = 0; }
        const targetInventory = parseInt(apifyProduct.inventory, 10) || 0;

        if (currentInventory === targetInventory) { alreadyInSyncCount++; return; }
        
        inventoryUpdates.push({
            title: shopifyProduct.title,
            currentInventory,
            newInventory: targetInventory,
            inventoryItemId: inventoryItemId,
        });
    });

    addLog(`Inventory updates prepared: ${inventoryUpdates.length} changes needed. ${alreadyInSyncCount} products already in sync.`, 'info', 'inventory');
    
    const actionToConfirm = { type: 'inventory', data: inventoryUpdates };
    const failsafeCheck = checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalApifyProducts: apifyData.length }, actionToConfirm);

    if (!failsafeCheck.proceed) {
        addLog(`Failsafe triggered: ${failsafeCheck.reason}. Job will not proceed automatically.`, 'warning', 'inventory');
        return { updated: 0, errors: 0 };
    }

    const { updated, errors: execErrors } = await executeInventoryUpdates(inventoryUpdates, token);
    errors += execErrors;

    stats.lastSync = new Date().toISOString();
    addToHistory('inventory', { updated, errors, attempted: inventoryUpdates.length });
    addLog(`Result: ${updated} updated, ${alreadyInSyncCount} already in sync, ${errors} errors`, 'info', 'inventory');
    return { updated, errors };

  } catch (error) {
    addLog(`Inventory workflow failed: ${error.message}`, 'error', 'inventory');
    stats.errors++;
    return { updated: 0, errors: errors + 1 };
  }
}

async function handleDiscontinuedProductsJob(token) { /* ... implementation ... */ return { discontinued: 0, errors: 0}; }
async function createNewProductsJob(token, apifyProducts) { /* ... implementation ... */ return { created: 0, errors: 0}; }

// ... (UI, API Endpoints, Telegram Webhook, and Schedules remain the same as the last working version)
app.get('/', (req, res) => { /* ... UI HTML ... */ });
app.get('/api/status', (req, res) => { /* ... */ });
app.post('/api/stats/reset', (req, res) => { /* ... */ });
app.post('/api/failsafe/clear', (req, res) => { /* ... */ });
app.post('/api/failsafe/confirm', async (req, res) => { /* ... */ });
app.post('/api/failsafe/abort', (req, res) => { /* ... */ });
app.post('/api/pause', (req, res) => { /* ... */ });
app.post('/api/sync/inventory', (req, res) => { /* ... */ });
app.post('/api/sync/products', (req, res) => { /* ... */ });
app.post('/api/sync/discontinued', (req, res) => { /* ... */ });
app.get('/api/debug/check-product/:handle', async (req, res) => { /* ... */ });
app.post('/telegram/webhook/:secret?', async (req, res) => { /* ... */ });
cron.schedule('0 1 * * *', () => { /* ... */ });
cron.schedule('0 2 * * 5', () => { /* ... */ });
cron.schedule('0 3 * * *', () => { /* ... */ });
cron.schedule('0 9 * * 1', async () => { /* ... */ });

app.listen(PORT, () => {
  console.log(`Server started on port ${PORT}`);
});
