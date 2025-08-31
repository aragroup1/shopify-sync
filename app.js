const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage and state management
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
let runHistory = [];
let errorLog = [];
let logs = [];
let systemPaused = false;
let mismatches = [];
const missingCounters = new Map();
let failsafeTriggered = false; // Can be: false, 'pending', true
let failsafeReason = '';
let pendingFailsafeAction = null;
const jobLocks = { inventory: false, products: false, discontinued: false, fixTracking: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered === true || failsafeTriggered === 'pending' || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 100), MIN_SHOPIFY_PRODUCTS: Number(process.env.MIN_SHOPIFY_PRODUCTS || 100), MAX_CHANGE_PERCENTAGE: Number(process.env.MAX_CHANGE_PERCENTAGE || 30), MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), MAX_ERROR_RATE: Number(process.env.MAX_ERROR_RATE || 20), MAX_DISCONTINUED_AT_ONCE: Number(process.env.MAX_DISCONTINUED_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2', urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper Functions ---
function addLog(message, type = 'info', jobType = 'system') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 200) logs.length = 200;
  console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`);
  if (type === 'error') { errorLog.push({ timestamp: new Date(), message, jobType }); if (errorLog.length > 500) errorLog.length = 500; }
}
function addToHistory(type, data) {
    runHistory.unshift({ type, timestamp: new Date().toISOString(), ...data });
    if (runHistory.length > 50) runHistory.length = 50;
}
function startBackgroundJob(key, name, fn) {
  if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning', key); return false; }
  jobLocks[key] = true;
  const token = getJobToken();
  addLog(`Started background job: ${name}`, 'info', key);
  setImmediate(async () => {
    try { await fn(token); } catch (e) { addLog(`Unhandled error in ${name}: ${e.message}`, 'error', key); } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info', key); }
  });
  return true;
}
function triggerFailsafe(msg, contextData = {}, isConfirmable = false, action = null) { /* ... implementation ... */ }
function checkFailsafeConditions(context, data = {}, actionToConfirm = null) { /* ... implementation ... */ return { proceed: true };}
// ... other small helpers

// --- Data Fetching & Processing ---
async function getApifyProducts() { /* ... full implementation ... */ return []; }
async function getShopifyProducts({ onlyApifyTag = true, fields = 'id,handle,title,variants,tags' } = {}) {
    let allProducts = [];
    let sinceId = null;
    const limit = 250;
    addLog(`Starting Shopify fetch (onlyApifyTag: ${onlyApifyTag})...`, 'info', 'fetch');
    try {
        while (true) {
            let url = `/products.json?limit=${limit}&fields=${fields}`;
            if (sinceId) url += `&since_id=${sinceId}`;
            const response = await shopifyClient.get(url);
            const products = response.data.products;
            allProducts.push(...products);
            if (products.length < limit) break;
            sinceId = products[products.length - 1].id;
            await new Promise(r => setTimeout(r, 500));
        }
    } catch (error) {
        addLog(`Shopify fetch error: ${error.message}`, 'error', 'fetch');
        triggerFailsafe(`Shopify fetch failed: ${error.message}`);
        throw error;
    }
    const filtered = onlyApifyTag ? allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify')) : allProducts;
    addLog(`Shopify fetch complete: ${filtered.length} products matching criteria.`, 'info', 'fetch');
    return filtered;
}
function processApifyProducts(apifyData, options = { processPrice: true }) { /* ... full implementation ... */ return []; }
function buildShopifyMaps(shopifyData) { /* ... full implementation ... */ return new Map(); }
function matchShopifyProduct(apifyProduct, maps) { /* ... full implementation ... */ return { product: null }; }
async function getShopifyInventoryLevels(inventoryItemIds, locationId) {
    const inventoryMap = new Map();
    const batchSize = 50;
    addLog(`Fetching inventory for ${inventoryItemIds.length} items...`, 'info', 'inventory');
    for (let i = 0; i < inventoryItemIds.length; i += batchSize) {
        const batch = inventoryItemIds.slice(i, i + batchSize);
        let retries = 0;
        const maxRetries = 5;
        let success = false;
        while (!success && retries < maxRetries) {
            try {
                const url = `/inventory_levels.json?inventory_item_ids=${batch.join(',')}&location_ids=${locationId}`;
                const response = await shopifyClient.get(url);
                for (const level of response.data.inventory_levels) { inventoryMap.set(level.inventory_item_id, level.available || 0); }
                success = true;
            } catch (error) {
                if (error.response?.status === 429) {
                    retries++;
                    const retryAfter = error.response.headers['retry-after'] || (2 ** retries);
                    addLog(`Rate limit hit. Retrying in ${retryAfter}s... (Attempt ${retries}/${maxRetries})`, 'warning', 'inventory');
                    await new Promise(r => setTimeout(r, retryAfter * 1000));
                } else { addLog(`Batch fetch failed: ${error.message}`, 'error', 'inventory'); break; }
            }
        }
        if (!success) { addLog(`Batch failed after ${maxRetries} retries. Skipping.`, 'error', 'inventory'); }
        await new Promise(r => setTimeout(r, 600));
    }
    addLog(`Fetched ${inventoryMap.size} of ${inventoryItemIds.length} possible inventory levels.`, 'info', 'inventory');
    return inventoryMap;
}

// --- CORE JOB LOGIC ---

// NEW: Dedicated job to fix product configuration state
async function fixInventoryTrackingJob(token) {
    addLog('Starting job to fix inventory tracking for all products...', 'info', 'fixTracking');
    let fixedCount = 0;
    let errors = 0;
    try {
        const allShopifyProducts = await getShopifyProducts({ onlyApifyTag: false, fields: 'id,title,variants' });
        
        for (const product of allShopifyProducts) {
            if (shouldAbort(token)) { addLog('Aborting fix job.', 'warning', 'fixTracking'); break; }
            const variant = product.variants?.[0];
            if (!variant) continue;

            let needsFix = false;
            if (variant.inventory_management !== 'shopify') {
                needsFix = true;
                addLog(`Fixing tracking for "${product.title}"...`, 'info', 'fixTracking');
                try {
                    await shopifyClient.put(`/variants/${variant.id}.json`, {
                        variant: { id: variant.id, inventory_management: 'shopify' }
                    });
                } catch (e) {
                    addLog(`Error enabling tracking for "${product.title}": ${e.message}`, 'error', 'fixTracking');
                    errors++;
                    continue;
                }
            }
            
            if (variant.inventory_item_id) {
                try {
                    await shopifyClient.post('/inventory_levels/connect.json', {
                        location_id: parseInt(config.shopify.locationId),
                        inventory_item_id: variant.inventory_item_id,
                    });
                } catch (connectError) {
                    if (connectError.response?.status !== 422) { // 422 is expected if already connected
                        addLog(`Error connecting "${product.title}": ${connectError.message}`, 'error', 'fixTracking');
                        errors++;
                    }
                }
            }
            
            if (needsFix) {
                fixedCount++;
                await new Promise(r => setTimeout(r, 500));
            }
        }
    } catch (e) {
        addLog(`Critical error during fix job: ${e.message}`, 'error', 'fixTracking');
        errors++;
    }
    addLog(`Fix job complete. Products fixed: ${fixedCount}. Errors: ${errors}.`, 'success', 'fixTracking');
    return { fixed: fixedCount, errors };
}

// SIMPLIFIED: This job now only sets quantity.
async function executeInventoryUpdates(updates, token) {
  let updated = 0, errors = 0;
  addLog(`Executing ${updates.length} inventory quantity updates...`, 'info', 'inventory');
  for (const update of updates) {
      if (shouldAbort(token)) break;
      try {
          await shopifyClient.post('/inventory_levels/set.json', {
              location_id: parseInt(config.shopify.locationId),
              inventory_item_id: update.inventoryItemId,
              available: update.newInventory
          });
          addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success', 'inventory');
          updated++;
          stats.inventoryUpdates++;
          await new Promise(r => setTimeout(r, 500));
      } catch (error) {
          errors++;
          stats.errors++;
          addLog(`✗ Failed to set quantity for ${update.title}: ${error.message}`, 'error', 'inventory');
      }
  }
  return { updated, errors };
}

// SIMPLIFIED: updateInventoryJob no longer tries to fix config.
async function updateInventoryJob(token) {
  addLog('Starting inventory sync...', 'info', 'inventory');
  try {
    const [apifyData, shopifyData] = await Promise.all([
        getApifyProducts(),
        getShopifyProducts({ onlyApifyTag: true })
    ]);
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
        
        const variant = shopifyProduct.variants[0];
        const inventoryItemId = variant.inventory_item_id;
        
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
    
    addLog(`Updates prepared: ${inventoryUpdates.length}. In sync: ${alreadyInSyncCount}.`, 'info', 'inventory');
    
    const failsafeCheck = checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalApifyProducts: apifyData.length }, { type: 'inventory', data: inventoryUpdates });
    if (!failsafeCheck.proceed) { return { updated: 0, errors: 0 }; }

    const { updated, errors } = await executeInventoryUpdates(inventoryUpdates, token);
    addToHistory('inventory', { updated, errors, attempted: inventoryUpdates.length });
    addLog(`Inventory sync result: ${updated} updated, ${errors} errors.`, 'info', 'inventory');
    return { updated, errors };
  } catch (error) {
    addLog(`Inventory workflow failed: ${error.message}`, 'error', 'inventory');
    stats.errors++;
    return { updated: 0, errors: 1 };
  }
}

async function handleDiscontinuedProductsJob(token) { /* ... full implementation ... */ return { discontinued: 0, errors: 0}; }
async function createNewProductsJob(token, apifyProducts) { /* ... full implementation ... */ return { created: 0, errors: 0}; }

// --- UI AND API ---

app.get('/', (req, res) => {
    let failsafeBanner = '';
    if (failsafeTriggered === 'pending') {
        failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-yellow-900 border-2 border-yellow-500"><h3 class="font-bold text-yellow-300">⚠️ CONFIRMATION REQUIRED</h3><p class="text-sm text-yellow-400 mb-4">Reason: ${failsafeReason}</p><div class="flex gap-4"><button onclick="confirmFailsafe()" class="bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover">Proceed Anyway</button><button onclick="abortFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Abort & Pause</button></div></div>`;
    } else if (failsafeTriggered === true) {
        failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-red-900 border-2 border-red-500"><div class="flex items-center justify-between"><div><h3 class="font-bold text-red-300">🚨 FAILSAFE TRIGGERED</h3><p class="text-sm text-red-400">${failsafeReason}</p></div><button onclick="clearFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Clear Failsafe</button></div></div>`;
    }
    res.send(`<!DOCTYPE html><html lang="en" class="dark"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Shopify Sync Dashboard</title><script src="https://cdn.tailwindcss.com"></script><style>body { background-color: #111827; color: #e5e7eb; } .card { background-color: #1f2937; border: 1px solid #374151; } .btn-hover:hover { transform: translateY(-1px); } .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #60a5fa; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;} @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} }</style></head><body class="min-h-screen font-sans p-8"><div class="max-w-7xl mx-auto space-y-8"><h1 class="text-4xl font-bold">Shopify Sync Dashboard</h1><div class="card p-6 rounded-lg">${failsafeBanner}</div><div class="card p-6 rounded-lg"><h2 class="text-2xl font-semibold mb-4">System Controls</h2><div class="flex flex-wrap gap-4"><button onclick="triggerSync('inventory')" class="bg-green-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Update Inventory<div id="inventorySpinner" class="spinner"></div></button><button onclick="triggerFix()" class="bg-indigo-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Fix Inventory Tracking<div id="fixSpinner" class="spinner"></div></button></div></div><div class="card p-6 rounded-lg"><h2 class="text-2xl font-semibold mb-4">Activity Log</h2><div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer">${logs.map(log => \`<div class="\${log.type === 'success' ? 'text-green-400' : log.type === 'error' ? 'text-red-400' : log.type === 'warning' ? 'text-yellow-400' : 'text-gray-300'}">[\${new Date(log.timestamp).toLocaleTimeString()}] \${log.message}</div>\`).join('')}</div></div></div><script>async function triggerSync(type) { const spinner = document.getElementById(type + 'Spinner'); spinner.style.display = 'inline-block'; await fetch('/api/sync/' + type, { method: 'POST' }); spinner.style.display = 'none'; } async function triggerFix() { if (!confirm('This will scan all products and ensure inventory tracking is enabled. This may take several minutes. Continue?')) return; const spinner = document.getElementById('fixSpinner'); spinner.style.display = 'inline-block'; await fetch('/api/fix/inventory-tracking', { method: 'POST' }); spinner.style.display = 'none'; } async function confirmFailsafe() { await fetch('/api/failsafe/confirm', { method: 'POST' }); location.reload(); } async function abortFailsafe() { await fetch('/api/failsafe/abort', { method: 'POST' }); location.reload(); } async function clearFailsafe() { await fetch('/api/failsafe/clear', { method: 'POST' }); location.reload(); }</script></body></html>`);
});

app.post('/api/fix/inventory-tracking', (req, res) => {
    const started = startBackgroundJob('fixTracking', 'Fix Inventory Tracking', async (token) => {
        await fixInventoryTrackingJob(token);
    });
    if (started) {
        res.json({ success: true, message: 'Inventory tracking fix job started.' });
    } else {
        res.status(409).json({ success: false, message: 'Fix job is already running.' });
    }
});
app.post('/api/sync/inventory', (req, res) => {
    startBackgroundJob('inventory', 'Manual Inventory Sync', async (t) => updateInventoryJob(t)) 
        ? res.json({success: true, message: "Inventory sync started."}) 
        : res.status(409).json({success: false, message: "Inventory sync already running."});
});
// ... Other API endpoints remain the same

cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', async (token) => await updateInventoryJob(token)); });
// ... Other schedules

app.listen(PORT, () => {
  console.log(`Server started on port ${PORT}`);
  const requiredEnv = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'];
  const missing = requiredEnv.filter(key => !process.env[key]);
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error');
    process.exit(1);
  }
});
