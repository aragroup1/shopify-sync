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
let failsafeTriggered = false;
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
function triggerFailsafe(msg, contextData = {}, isConfirmable = false, action = null) { /* ... full implementation ... */ }
function checkFailsafeConditions(context, data = {}, actionToConfirm = null) { /* ... full implementation ... */ return { proceed: true };}
function normalizeTitle(text = '') { return String(text).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim(); }
function sanitizeProductTitle(raw='') { /* ... full implementation ... */ return raw; }

// --- Data Fetching & Processing ---
async function getApifyProducts() {
    let allItems = [];
    let offset = 0;
    const limit = 1000;
    addLog('Starting Apify product fetch...', 'info', 'fetch');
    try {
        while (true) {
            const response = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=${limit}&offset=${offset}`);
            const items = response.data;
            allItems.push(...items);
            if (items.length < limit) break;
            offset += limit;
        }
    } catch (error) { addLog(`Apify fetch error: ${error.message}`, 'error', 'fetch'); triggerFailsafe(`Apify fetch failed: ${error.message}`); throw error; }
    addLog(`Apify fetch complete: ${allItems.length} total products.`, 'info', 'fetch');
    return allItems;
}
async function getShopifyProducts({ onlyApifyTag = true, fields = 'id,handle,title,variants,tags,status' } = {}) {
    let allProducts = [];
    let pageNum = 0;
    addLog(`Starting Shopify fetch (onlyApifyTag: ${onlyApifyTag})...`, 'info', 'fetch');
    try {
        let url = `/products.json?limit=250&fields=${fields}`;
        while (url) {
            pageNum++;
            const response = await shopifyClient.get(url);
            const products = response.data.products;
            allProducts.push(...products);
            addLog(`Shopify page ${pageNum}: fetched ${products.length} products (total: ${allProducts.length})`, 'info', 'fetch');

            // CRITICAL FIX: Correctly parse Link header for cursor-based pagination
            const linkHeader = response.headers.link;
            url = null; // Assume no next page unless found
            if (linkHeader) {
                const links = linkHeader.split(',');
                const nextLink = links.find(s => s.includes('rel="next"'));
                if (nextLink) {
                    const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/);
                    if (pageInfoMatch) {
                        url = `/products.json?limit=250&fields=${fields}&page_info=${pageInfoMatch[1]}`;
                    }
                }
            }
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
function processApifyProducts(apifyData, options = { processPrice: true }) { /* ... full implementation ... */ return apifyData; }
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
                    await shopifyClient.put(`/variants/${variant.id}.json`, { variant: { id: variant.id, inventory_management: 'shopify' } });
                } catch (e) { addLog(`Error enabling tracking for "${product.title}": ${e.message}`, 'error', 'fixTracking'); errors++; continue; }
            }
            if (variant.inventory_item_id) {
                try {
                    await shopifyClient.post('/inventory_levels/connect.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: variant.inventory_item_id });
                } catch (connectError) {
                    if (connectError.response?.status !== 422) { addLog(`Error connecting "${product.title}": ${connectError.message}`, 'error', 'fixTracking'); errors++; }
                }
            }
            if (needsFix) { fixedCount++; await new Promise(r => setTimeout(r, 500)); }
        }
    } catch (e) { addLog(`Critical error during fix job: ${e.message}`, 'error', 'fixTracking'); errors++; }
    addLog(`Fix job complete. Products fixed: ${fixedCount}. Errors: ${errors}.`, 'success', 'fixTracking');
    return { fixed: fixedCount, errors };
}

async function executeInventoryUpdates(updates, token) {
  let updated = 0, errors = 0;
  addLog(`Executing ${updates.length} inventory quantity updates...`, 'info', 'inventory');
  for (const update of updates) {
      if (shouldAbort(token)) break;
      try {
          await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: update.inventoryItemId, available: update.newInventory });
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

async function updateInventoryJob(token) {
  addLog('Starting inventory sync...', 'info', 'inventory');
  try {
    const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts({ onlyApifyTag: true }) ]);
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
        
        inventoryUpdates.push({ title: shopifyProduct.title, currentInventory, newInventory: targetInventory, inventoryItemId: inventoryItemId });
    });
    
    addLog(`Updates prepared: ${inventoryUpdates.length}. In sync: ${alreadyInSyncCount}.`, 'info', 'inventory');
    const failsafeCheck = checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalApifyProducts: apifyData.length }, { type: 'inventory', data: inventoryUpdates });
    if (!failsafeCheck.proceed) { return { updated: 0, errors: 0 }; }

    const { updated, errors } = await executeInventoryUpdates(inventoryUpdates, token);
    addToHistory('inventory', { updated, errors, attempted: inventoryUpdates.length });
    addLog(`Inventory sync result: ${updated} updated, ${errors} errors.`, 'info', 'inventory');
    return { updated, errors };
  } catch (error) { addLog(`Inventory workflow failed: ${error.message}`, 'error', 'inventory'); stats.errors++; return { updated: 0, errors: 1 }; }
}

async function handleDiscontinuedProductsJob(token) {
    addLog('Starting discontinued products check...', 'info', 'discontinued');
    let discontinuedCount = 0, errors = 0;
    try {
        const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts({ onlyApifyTag: true, fields: 'id,handle,title,variants,tags,status' }) ]);

        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        const shopifyMaps = buildShopifyMaps(shopifyData);
        const matchedShopifyIds = new Set();
        apifyProcessed.forEach(p => { const { product } = matchShopifyProduct(p, maps); if (product) matchedShopifyIds.add(product.id); });

        const candidates = shopifyData.filter(p => !matchedShopifyIds.has(p.id));
        const nowMissing = [];
        for (const p of candidates) {
            const key = p.handle.toLowerCase();
            const count = (missingCounters.get(key) || 0) + 1;
            missingCounters.set(key, count);
            if (count >= DISCONTINUE_MISS_RUNS) nowMissing.push(p);
        }
        shopifyData.forEach(p => { if (matchedShopifyIds.has(p.id)) missingCounters.delete(p.handle.toLowerCase()); });

        addLog(`${nowMissing.length} products marked for discontinuation after ${DISCONTINUE_MISS_RUNS} checks.`, 'info', 'discontinued');
        
        for (const product of nowMissing) {
            if (shouldAbort(token)) break;
            try {
                if (product.variants?.[0]?.inventory_quantity > 0) {
                    await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: product.variants[0].inventory_item_id, available: 0 });
                }
                if (product.status === 'active') {
                    await shopifyClient.put(`/products/${product.id}.json`, { product: { id: product.id, status: 'draft' } });
                }
                addLog(`Discontinued: "${product.title}" (0 stock, status set to DRAFT).`, 'success', 'discontinued');
                discontinuedCount++;
                stats.discontinued++;
                await new Promise(r => setTimeout(r, 500));
            } catch (e) {
                errors++;
                stats.errors++;
                addLog(`Error discontinuing "${product.title}": ${e.message}`, 'error', 'discontinued');
            }
        }
        addToHistory('discontinued', { discontinued: discontinuedCount, errors });
    } catch(e) { addLog(`Discontinued job failed critically: ${e.message}`, 'error', 'discontinued'); }
    return { discontinued: discontinuedCount, errors };
}
async function createNewProductsJob(token, apifyProducts) { /* ... full implementation ... */ return { created: 0, errors: 0}; }

// --- UI AND API ---
app.get('/', (req, res) => {
    let failsafeBanner = '';
    if (failsafeTriggered === 'pending') {
        failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-yellow-900 border-2 border-yellow-500"><h3 class="font-bold text-yellow-300">⚠️ CONFIRMATION REQUIRED</h3><p class="text-sm text-yellow-400 mb-4">Reason: ${failsafeReason}</p><div class="flex gap-4"><button onclick="confirmFailsafe()" class="bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover">Proceed Anyway</button><button onclick="abortFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Abort & Pause</button></div></div>`;
    } else if (failsafeTriggered === true) {
        failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-red-900 border-2 border-red-500"><div class="flex items-center justify-between"><div><h3 class="font-bold text-red-300">🚨 FAILSAFE TRIGGERED</h3><p class="text-sm text-red-400">${failsafeReason}</p></div><button onclick="clearFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Clear Failsafe</button></div></div>`;
    }
    res.send(`<!DOCTYPE html><html lang="en" class="dark"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Shopify Sync Dashboard</title><script src="https://cdn.tailwindcss.com"></script><style>body { background-color: #111827; color: #e5e7eb; } .card { background-color: #1f2937; border: 1px solid #374151; } .btn-hover:hover { transform: translateY(-1px); } .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #60a5fa; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;} @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} }</style></head><body class="min-h-screen font-sans p-8"><div class="max-w-7xl mx-auto space-y-8"><h1 class="text-4xl font-bold">Shopify Sync Dashboard</h1><div class="card p-6 rounded-lg">${failsafeBanner}</div><div class="card p-6 rounded-lg"><h2 class="text-2xl font-semibold mb-4">System Controls</h2><div class="flex flex-wrap gap-4"><button onclick="triggerSync('inventory')" class="bg-green-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Update Inventory<div id="inventorySpinner" class="spinner"></div></button><button onclick="triggerSync('products')" class="bg-blue-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Create Products<div id="productsSpinner" class="spinner"></div></button><button onclick="triggerSync('discontinued')" class="bg-yellow-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Check Discontinued<div id="discontinuedSpinner" class="spinner"></div></button><button onclick="triggerFix()" class="bg-indigo-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Fix Inventory Tracking<div id="fixSpinner" class="spinner"></div></button></div></div><div class="card p-6 rounded-lg"><h2 class="text-2xl font-semibold mb-4">Activity Log</h2><div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer">${logs.map(log => `<div>[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}</div></div></div><script>async function triggerSync(type) { const spinner = document.getElementById(type + 'Spinner'); spinner.style.display = 'inline-block'; await fetch('/api/sync/' + type, { method: 'POST' }); spinner.style.display = 'none'; } async function triggerFix() { if (!confirm('This will scan all products and ensure their inventory tracking is enabled. This may take several minutes. Continue?')) return; const spinner = document.getElementById('fixSpinner'); spinner.style.display = 'inline-block'; await fetch('/api/fix/inventory-tracking', { method: 'POST' }); spinner.style.display = 'none'; } async function confirmFailsafe() { await fetch('/api/failsafe/confirm', { method: 'POST' }); location.reload(); } async function abortFailsafe() { await fetch('/api/failsafe/abort', { method: 'POST' }); location.reload(); } async function clearFailsafe() { await fetch('/api/failsafe/clear', { method: 'POST' }); location.reload(); }</script></body></html>`);
});

app.post('/api/fix/inventory-tracking', (req, res) => {
    startBackgroundJob('fixTracking', 'Fix Inventory Tracking', async (t) => fixInventoryTrackingJob(t)) ? res.json({s:1,m:"Fix job started."}) : res.status(409).json({s:0,m:"Fix job already running."});
});
app.post('/api/sync/inventory', (req, res) => {
    startBackgroundJob('inventory', 'Manual Inventory Sync', async (t) => updateInventoryJob(t)) ? res.json({s:1,m:"Sync started."}) : res.status(409).json({s:0,m:"Sync already running."});
});
app.post('/api/sync/products', (req, res) => { /* ... full implementation ... */});
app.post('/api/sync/discontinued', (req, res) => {
    startBackgroundJob('discontinued', 'Manual Discontinued Sync', async (t) => handleDiscontinuedProductsJob(t)) ? res.json({s:1,m:"Discontinued check started."}) : res.status(409).json({s:0,m:"Check already running."});
});
// ... Other API endpoints ...

cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', async (t) => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { /* ... */ });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', async (t) => handleDiscontinuedProductsJob(t)); });

app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  const requiredEnv = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'];
  const missing = requiredEnv.filter(key => !process.env[key]);
  if (missing.length > 0) { addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); process.exit(1); }
});
