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
let failsafeTriggered = false; // Can be: false, 'pending', true
let failsafeReason = '';
let pendingFailsafeAction = null;

const jobLocks = { inventory: false, products: false, discontinued: false, fixTracking: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered === true || failsafeTriggered === 'pending' || token !== abortVersion;

// Failsafe configuration
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
  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' },
      { timeout: 15000 }
    );
  } catch (e) {
    addLog(`Telegram notify failed: ${e.message}`, 'warning');
  }
}

let lastKnownGoodState = { apifyCount: 0, shopifyCount: 0, timestamp: null };

function addLog(message, type = 'info', jobType = 'system') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 200) logs = logs.slice(0, 200);
  console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`);

  if (type === 'error') {
    errorLog.push({ timestamp: new Date(), message, jobType });
    if (errorLog.length > 500) errorLog = errorLog.slice(-500);
  }
}

function addToHistory(type, data) {
    runHistory.unshift({ type, timestamp: new Date().toISOString(), ...data });
    if (runHistory.length > 50) runHistory = runHistory.slice(0, 50);
}

function startBackgroundJob(key, name, fn) {
  if (jobLocks[key]) {
    addLog(`${name} already running; ignoring duplicate start`, 'warning', key);
    return false;
  }
  jobLocks[key] = true;
  const token = getJobToken();
  addLog(`Started background job: ${name}`, 'info', key);
  setImmediate(async () => {
    try {
      await fn(token);
    } catch (e) { /* errors logged inside */ } finally {
      jobLocks[key] = false;
      addLog(`${name} job finished`, 'info', key);
    }
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
    const notification = `⚠️ <b>Failsafe Warning - Confirmation Required</b> ⚠️\n\n<b>Reason:</b>\n<pre>${msg}</pre>\n\nTo proceed, reply: <code>/confirm</code>\nTo abort, reply: <code>/abort</code>`;
    notifyTelegram(notification);
  } else {
    failsafeTriggered = true;
    addLog('System automatically paused to prevent potential damage.', 'error', 'failsafe');
    if (lastFailsafeNotified !== msg) {
        lastFailsafeNotified = msg;
        notifyTelegram(`🚨 <b>Failsafe Triggered & System Paused</b> 🚨\n\n<b>Reason:</b>\n<pre>${msg}</pre>`);
    }
  }
}

function checkFailsafeConditions(context, data = {}, actionToConfirm = null) {
  const checks = [];
  let isConfirmable = false;

  switch (context) {
    case 'inventory': {
      isConfirmable = true;
      if (data.totalApifyProducts > 0 && data.updatesNeeded > data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100)) {
        checks.push(`Too many inventory changes: ${data.updatesNeeded} > ${Math.floor(data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100))} (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}% of ${data.totalApifyProducts})`);
      }
      break;
    }
    case 'discontinued': {
        // This is a hard failsafe. We don't want to accidentally discontinue hundreds of products.
        isConfirmable = false;
        if (data.toDiscontinue > FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE) {
            checks.push(`Too many products to discontinue: ${data.toDiscontinue} > ${FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE}`);
        }
        break;
    }
    // ... other cases
  }

  if (checks.length > 0) {
    const reason = checks.join('; ');
    triggerFailsafe(reason, { checkContext: context, checkData: data }, isConfirmable, actionToConfirm);
    return { proceed: false, reason: isConfirmable ? 'pending_confirmation' : 'hard_stop' };
  }
  return { proceed: true };
}

// ... (Configuration, API clients, helpers all remain the same)
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2', urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
function normalizeTitle(text = '') { return String(text).toLowerCase().replace(/\b\d{4}\b/g, '').replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '').replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '').replace(/[^a-z0-9]+/g, ' ').trim(); }
const TITLE_SMALL_WORDS = new Set(['and','or','the','for','of','in','on','with','a','an','to','at','by','from']);
function toTitleCase(str='') { const words = str.split(' ').filter(Boolean); return words.map((w, i) => { const lw = w.toLowerCase(); if (i !== 0 && i !== words.length - 1 && TITLE_SMALL_WORDS.has(lw)) return lw; return lw.charAt(0).toUpperCase() + lw.slice(1); }).join(' ');}
function sanitizeProductTitle(raw='') { let t = String(raw); t = t.replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, ''); t = t.replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' '); t = t.replace(/[^a-zA-Z0-9 -]+/g, ' '); t = t.replace(/[\s-]+/g, ' ').trim(); t = toTitleCase(t); return t || 'Untitled Product'; }
async function getApifyProducts() { let allItems = []; let offset = 0; const limit = 500; addLog('Starting Apify product fetch...', 'info', 'fetch'); try { while (true) { const response = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=${limit}&offset=${offset}`); const items = response.data; allItems.push(...items); if (items.length < limit) break; offset += limit; await new Promise(r => setTimeout(r, 1000)); } } catch (error) { addLog(`Apify fetch error: ${error.message}`, 'error', 'fetch'); stats.errors++; triggerFailsafe(`Apify fetch failed: ${error.message}`); throw error; } addLog(`Apify fetch complete: ${allItems.length} total products`, 'info', 'fetch'); if (!checkFailsafeConditions('fetch', { apifyCount: allItems.length }).proceed) throw new Error('Failsafe triggered: Apify product count anomaly'); return allItems; }
async function getShopifyProducts({ onlyApifyTag = true } = {}) { let allProducts = []; let sinceId = null; const limit = 250; const fields = 'id,handle,title,variants,tags'; addLog('Starting Shopify product fetch...', 'info', 'fetch'); try { while (true) { let url = `/products.json?limit=${limit}&fields=${fields}`; if (sinceId) url += `&since_id=${sinceId}`; const response = await shopifyClient.get(url); const products = response.data.products; allProducts.push(...products); if (products.length < limit) break; sinceId = products[products.length - 1].id; await new Promise(r => setTimeout(r, 500)); } } catch (error) { addLog(`Shopify fetch error: ${error.message}`, 'error', 'fetch'); stats.errors++; triggerFailsafe(`Shopify fetch failed: ${error.message}`); throw error; } const filtered = onlyApifyTag ? allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify')) : allProducts; addLog(`Shopify fetch complete: ${allProducts.length} total products, ${onlyApifyTag ? filtered.length + ' with Supplier:Apify tag' : 'using ALL products for matching'}`, 'info', 'fetch'); if (!checkFailsafeConditions('fetch', { shopifyCount: filtered.length }).proceed) throw new Error('Failsafe triggered: Shopify product count anomaly'); return filtered; }
function processApifyProducts(apifyData, options = { processPrice: true }) { return apifyData.map((item, index) => { /* ... implementation ... */ }).filter(Boolean); }
function buildShopifyMaps(shopifyData) { /* ... implementation ... */ }
function matchShopifyProduct(apifyProduct, maps) { /* ... implementation ... */ }
async function getShopifyInventoryLevels(inventoryItemIds, locationId) { const inventoryMap = new Map(); const batchSize = 50; addLog(`Fetching inventory levels for ${inventoryItemIds.length} items from location ${locationId}...`, 'info', 'inventory'); for (let i = 0; i < inventoryItemIds.length; i += batchSize) { const batch = inventoryItemIds.slice(i, i + batchSize); try { const url = `/inventory_levels.json?inventory_item_ids=${batch.join(',')}&location_ids=${locationId}`; const response = await shopifyClient.get(url); for (const level of response.data.inventory_levels) { inventoryMap.set(level.inventory_item_id, level.available || 0); } } catch (error) { addLog(`Failed to fetch inventory batch: ${error.message}`, 'error', 'inventory'); stats.errors++; } await new Promise(r => setTimeout(r, 500)); } addLog(`Successfully fetched ${inventoryMap.size} inventory levels.`, 'info', 'inventory'); return inventoryMap; }

// MODIFIED: Discontinued job logic
async function handleDiscontinuedProductsJob(token) {
  // CRITICAL FIX: Only check for manual system pause at the start.
  // This allows the job to run even if another job has a 'pending' failsafe.
  if (systemPaused) {
    addLog('Discontinued check skipped - system is manually paused', 'warning', 'discontinued');
    return { discontinued: 0, errors: 0 };
  }
  
  let discontinued = 0, errors = 0;

  try {
    addLog('Checking for discontinued products...', 'info', 'discontinued');
    // The fetches will trigger their own hard failsafes on failure, which is correct.
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);

    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
    const shopifyMaps = buildShopifyMaps(shopifyData);

    const matchedShopifyIds = new Set();
    apifyProcessed.forEach(apifyProd => {
      const { product } = matchShopifyProduct(apifyProd, shopifyMaps);
      if (product) matchedShopifyIds.add(product.id);
    });

    const candidates = shopifyData.filter(p => !matchedShopifyIds.has(p.id));
    const nowMissing = [];
    for (const p of candidates) {
      if (shouldAbort(token)) { addLog('Aborting discontinued check due to system state change', 'warning', 'discontinued'); break; }
      const key = p.handle.toLowerCase();
      const count = (missingCounters.get(key) || 0) + 1;
      missingCounters.set(key, count);
      if (count >= DISCONTINUE_MISS_RUNS) nowMissing.push(p);
    }
    // Clear counters for products that are now present again
    shopifyData.forEach(p => { if (matchedShopifyIds.has(p.id)) missingCounters.delete(p.handle.toLowerCase()); });

    addLog(`Consecutive-miss filter: ${nowMissing.length} eligible after ${DISCONTINUE_MISS_RUNS} runs`, 'info', 'discontinued');

    // This is the job-specific failsafe. It will still run and protect against discontinuing too many products.
    if (!checkFailsafeConditions('discontinued', { toDiscontinue: nowMissing.length }).proceed) {
      addLog('Failsafe triggered for discontinued products. Aborting.', 'error', 'discontinued');
      return { discontinued: 0, errors: 0 };
    }

    for (const product of nowMissing) {
      if (shouldAbort(token)) { addLog('Aborting discontinued execution due to system state change', 'warning', 'discontinued'); break; }
      try {
        if (product.variants?.[0]?.inventory_quantity > 0) {
            await shopifyClient.post('/inventory_levels/set.json', {
              location_id: config.shopify.locationId,
              inventory_item_id: product.variants[0].inventory_item_id,
              available: 0
            });
            addLog(`Discontinued: ${product.title} (set to 0 stock)`, 'success', 'discontinued');
            discontinued++;
            stats.discontinued++;
        }
        await new Promise(r => setTimeout(r, 400));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`Failed to discontinue ${product.title}: ${error.message}`, 'error', 'discontinued');
      }
    }

    addToHistory('discontinued', { discontinued, errors, attempted: nowMissing.length });
    addLog(`Discontinued check completed: ${discontinued} discontinued, ${errors} errors`, 'info', 'discontinued');
    return { discontinued, errors };

  } catch (error) {
    addLog(`Discontinued workflow failed: ${error.message}`, 'error', 'discontinued');
    stats.errors++;
    return { discontinued, errors: errors + 1 };
  }
}


// ... (executeInventoryUpdates, updateInventoryJob, createNewProductsJob, etc. remain the same as the previous version)
async function executeInventoryUpdates(updates, token) { /* ... implementation ... */ }
async function updateInventoryJob(token) { /* ... implementation ... */ }
async function createNewProductsJob(token, apifyProducts) { /* ... implementation ... */ }


// ... (UI, API Endpoints, Telegram Webhook, Schedules all remain the same)
app.get('/', (req, res) => {
  let failsafeBanner = '';
  if (failsafeTriggered === 'pending') {
    failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-yellow-900 border-2 border-yellow-500"><h3 class="font-bold text-yellow-300">⚠️ CONFIRMATION REQUIRED</h3><p class="text-sm text-yellow-400 mb-4">Reason: ${failsafeReason}</p><div class="flex gap-4"><button onclick="confirmFailsafe()" class="bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover">Proceed Anyway</button><button onclick="abortFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Abort & Pause</button></div></div>`;
  } else if (failsafeTriggered === true) {
    failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-red-900 border-2 border-red-500"><div class="flex items-center justify-between"><div><h3 class="font-bold text-red-300">🚨 FAILSAFE TRIGGERED</h3><p class="text-sm text-red-400">${failsafeReason}</p></div><button onclick="clearFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Clear Failsafe</button></div></div>`;
  }
  res.send(`<!DOCTYPE html> ... (rest of UI HTML with sanity check tool) ... </html>`);
});

app.get('/api/status', (req, res) => res.json({ stats, runHistory, systemPaused, failsafeTriggered, failsafeReason, logs: logs.slice(0, 50), mismatches: mismatches.slice(0, 50) }));
app.post('/api/stats/reset', (req, res) => { stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null }; addLog('Counters manually reset.', 'info'); res.json({ success: true }); });
app.post('/api/failsafe/clear', (req, res) => { failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; systemPaused = false; lastFailsafeNotified = ''; addLog('Failsafe cleared, system resumed.', 'info'); notifyTelegram('✅ Failsafe cleared, system resumed.'); res.json({ success: true }); });
app.post('/api/failsafe/confirm', async (req, res) => { if (failsafeTriggered !== 'pending' || !pendingFailsafeAction) { return res.status(400).json({ success: false, message: 'No pending action.' }); } addLog('Failsafe action confirmed. Executing...', 'info'); notifyTelegram('▶️ User confirmed failsafe action.'); const action = pendingFailsafeAction; failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; systemPaused = false; if (action.type === 'inventory') { startBackgroundJob('inventory', 'Confirmed Inventory Sync', async (token) => { const { updated, errors } = await executeInventoryUpdates(action.data, token); addToHistory('inventory', { updated, errors, attempted: action.data.length, confirmed: true }); }); } res.json({ success: true, message: 'Action executing.' }); });
app.post('/api/failsafe/abort', (req, res) => { if (failsafeTriggered !== 'pending') { return res.status(400).json({ success: false, message: 'No pending action.' }); } failsafeTriggered = true; pendingFailsafeAction = null; systemPaused = true; addLog('Pending action aborted. System remains paused.', 'warning'); notifyTelegram('⏹️ User aborted failsafe action.'); res.json({ success: true }); });
app.post('/api/pause', (req, res) => { systemPaused = !systemPaused; abortVersion++; addLog(`System ${systemPaused ? 'paused' : 'resumed'}`, 'info'); res.json({ success: true, paused: systemPaused }); });
app.post('/api/sync/products', (req, res) => { /* ... */ });
app.post('/api/sync/inventory', (req, res) => { /* ... */ });
app.post('/api/sync/discontinued', (req, res) => { /* ... */ });
app.get('/api/debug/check-product/:handle', async (req, res) => { /* ... */ });
app.post('/telegram/webhook/:secret?', async (req, res) => { /* ... */ });

// Schedules
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', async (token) => await updateInventoryJob(token)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled product sync', async (token) => { /* ... */ }); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled discontinued check', async (token) => await handleDiscontinuedProductsJob(token)); });
cron.schedule('0 9 * * 1', async () => { /* Weekly Error Summary */ });

app.listen(PORT, () => {
  // ... (startup logs)
});
