const express = require('express');
const axios =require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0 };
let lastRun = { inventory: { updated: 0, errors: 0, at: null }, products: { created: 0, errors: 0, at: null, createdItems: [] }, discontinued: { discontinued: 0, errors: 0, at: null, discontinuedItems: [] } };
let logs = [];
let systemPaused = false;
let mismatches = [];
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;
const missingCounters = new Map();

// ADDED mapSkus to jobLocks
const jobLocks = { inventory: false, products: false, discontinued: false, mapSkus: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered === true || failsafeTriggered === 'pending' || token !== abortVersion;

// Configuration (Unchanged from your version)
const FAILSAFE_LIMITS = { MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), MAX_DISCONTINUE_PERCENTAGE: Number(process.env.MAX_DISCONTINUE_PERCENTAGE || 5), MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const MAX_UPDATES_PER_RUN = Number(process.env.MAX_UPDATES_PER_RUN || 100);
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper Functions (Unchanged) ---
function addLog(message, type = 'info') { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 200) logs.length = 200; console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`); }
async function notifyTelegram(text) { if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return; try { await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 }); } catch (e) { addLog(`Telegram notify failed: ${e.message}`, 'warning'); } }
function startBackgroundJob(key, name, fn) { if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning'); return false; } jobLocks[key] = true; const token = getJobToken(); addLog(`Started background job: ${name}`, 'info'); setImmediate(async () => { try { await fn(token); } catch (e) { addLog(`Unhandled error in ${name}: ${e.message}`, 'error'); } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info'); } }); return true; }
function triggerFailsafe(msg, isConfirmable = false, action = null) { if (failsafeTriggered) return; failsafeReason = msg; systemPaused = true; abortVersion++; addLog(`⚠️ FAILSAFE TRIGGERED: ${failsafeReason}`, 'error'); if (isConfirmable && action) { failsafeTriggered = 'pending'; pendingFailsafeAction = action; addLog('System paused, waiting for user confirmation.', 'warning'); notifyTelegram(`⚠️ <b>Failsafe Warning</b> ⚠️\n\n<b>Reason:</b>\n<pre>${msg}</pre>\n\nTo proceed, use the dashboard or reply: <code>/confirm</code>`); } else { failsafeTriggered = true; addLog('System automatically paused.', 'error'); notifyTelegram(`🚨 <b>Failsafe Triggered & System Paused</b> 🚨\n\n<b>Reason:</b>\n<pre>${msg}</pre>`); } }
function checkFailsafeConditions(context, data = {}, actionToConfirm = null) { /* ... Unchanged ... */ }

// --- Data Fetching & Processing (Unchanged) ---
async function getApifyProducts() { /* ... Unchanged ... */ }
async function getShopifyProducts({ onlyApifyTag = true, fields = 'id,handle,title,variants,tags,status' } = {}) { /* ... Unchanged ... */ }
async function getShopifyInventoryLevels(inventoryItemIds) { /* ... Unchanged ... */ }
function normalizeHandle(input = '', index) { /* ... Unchanged ... */ }
function stripHandleSuffixes(handle) { /* ... Unchanged ... */ }
function normalizeTitle(text = '') { /* ... Unchanged ... */ }
function applyMarkup(price, markupPercent) { /* ... Unchanged ... */ }
function processApifyProducts(apifyData) { /* ... Unchanged ... */ }
function buildShopifyMaps(shopifyData) { /* ... Unchanged ... */ }

// --- Matching Logic ---

// This is the old, flexible matching logic, now used ONLY for the one-time SKU mapping job.
function flexibleMatchForMapping(apifyProduct, maps) {
  let product = maps.handleMap.get(apifyProduct.handle.toLowerCase());
  if (product) return { product, matchType: 'handle' };
  const strippedApifyHandle = stripHandleSuffixes(apifyProduct.handle);
  product = maps.strippedHandleMap.get(strippedApifyHandle);
  if (product) return { product, matchType: 'handle-stripped' };
  if (apifyProduct.sku) {
    const bySku = maps.skuMap.get(apifyProduct.sku.toLowerCase());
    if (bySku) return { product: bySku, matchType: 'sku' };
    const bySkuHandle = maps.skuAsHandleMap.get(apifyProduct.sku.toLowerCase());
    if (bySkuHandle) return { product: bySkuHandle, matchType: 'sku-as-handle' };
  }
  const byTitle = maps.titleMap.get(normalizeTitle(apifyProduct.title));
  if (byTitle) return { product: byTitle, matchType: 'title' };
  return { product: null, matchType: 'none' };
}

// This is the NEW, fast, and reliable SKU-only matching for all regular jobs.
function matchShopifyProductBySku(apifyProduct, maps) {
    if (apifyProduct.sku) {
        const product = maps.skuMap.get(apifyProduct.sku.toLowerCase());
        if (product) {
            return { product, matchType: 'sku' };
        }
    }
    return { product: null, matchType: 'none' };
}

// --- CORE JOB LOGIC ---

// NEW: One-Time SKU Mapping Job
async function mapSkusJob(token) {
    addLog('--- Starting One-Time SKU Mapping Job ---', 'warning');
    let updated = 0, errors = 0, alreadyMatched = 0, noApifySku = 0, skippedDuplicate = 0;
    try {
        const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts() ]);
        if (shouldAbort(token)) return;

        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        const shopifyMaps = buildShopifyMaps(shopifyData);
        
        // This time, we iterate Apify -> Shopify to ensure one Apify SKU maps to one Shopify product
        const usedShopifyIds = new Set();

        for (const apifyProduct of apifyProcessed) {
            if (shouldAbort(token)) break;
            if (!apifyProduct.sku) {
                noApifySku++;
                continue;
            }

            const { product: shopifyProduct } = flexibleMatchForMapping(apifyProduct, shopifyMaps);

            if (shopifyProduct) {
                if (usedShopifyIds.has(shopifyProduct.id)) {
                    skippedDuplicate++;
                    continue; // This Shopify product is already mapped, skip.
                }

                const shopifyVariant = shopifyProduct.variants?.[0];
                if (!shopifyVariant) continue;

                if (shopifyVariant.sku?.toLowerCase() === apifyProduct.sku.toLowerCase()) {
                    alreadyMatched++;
                } else {
                    try {
                        addLog(`Mapping "${shopifyProduct.title}". Old SKU: "${shopifyVariant.sku}" -> New SKU: "${apifyProduct.sku}"`, 'info');
                        await shopifyClient.put(`/variants/${shopifyVariant.id}.json`, {
                            variant: { id: shopifyVariant.id, sku: apifyProduct.sku }
                        });
                        updated++;
                    } catch (e) {
                        errors++;
                        addLog(`Error updating SKU for "${shopifyProduct.title}": ${e.message}`, 'error');
                    }
                    await new Promise(r => setTimeout(r, 500)); // Rate limit
                }
                usedShopifyIds.add(shopifyProduct.id);
            }
        }
    } catch (e) {
        addLog(`Critical error in SKU mapping job: ${e.message}`, 'error');
        errors++;
    }
    addLog(`--- SKU Mapping Job Complete ---`, 'warning');
    addLog(`Summary: Updated: ${updated}, Errors: ${errors}, Already Matched: ${alreadyMatched}, Skipped Duplicates: ${skippedDuplicate}, Apify products with no SKU: ${noApifySku}`, 'success');
}

// UPDATED: All regular jobs below now use `matchShopifyProductBySku`

async function updateInventoryJob(token) {
    addLog('Starting inventory sync (SKU-only)...', 'info');
    // ... logic is the same, but uses `matchShopifyProductBySku` ...
    try {
        const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts() ]);
        if (shouldAbort(token)) return;
        const inventoryLevels = await getShopifyInventoryLevels(shopifyData.map(p => p.variants?.[0]?.inventory_item_id).filter(Boolean));
        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        const maps = buildShopifyMaps(shopifyData);

        const inventoryUpdates = [];
        let alreadyInSync = 0, notFound = 0;
        
        for (const apifyProduct of apifyProcessed) {
            const { product: shopifyProduct } = matchShopifyProductBySku(apifyProduct, maps);
            if (shopifyProduct) {
                const variant = shopifyProduct.variants?.[0];
                if (!variant?.inventory_item_id) continue;
                const currentInventory = inventoryLevels.get(variant.inventory_item_id) ?? 0;
                const targetInventory = parseInt(apifyProduct.inventory, 10) || 0;
                if (currentInventory === targetInventory) {
                    alreadyInSync++;
                } else {
                    inventoryUpdates.push({ title: shopifyProduct.title, currentInventory, newInventory: targetInventory, inventoryItemId: variant.inventory_item_id });
                }
            } else {
                notFound++;
            }
        }
        addLog(`Inventory Summary: Updates needed: ${inventoryUpdates.length}, In Sync: ${alreadyInSync}, Not Found (needs creating): ${notFound}`, 'info');
        // ... execute updates logic ...
    } catch (e) { addLog(`Inventory job failed: ${e.message}`, 'error'); }
}

async function createNewProductsJob(token) {
    addLog('Starting new product creation (SKU-based check)...', 'info');
    // ... logic is the same, but uses `matchShopifyProductBySku` to check for existence ...
    try {
        const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts() ]);
        if (shouldAbort(token)) return;
        const apifyProcessed = processApifyProducts(apifyData);
        const maps = buildShopifyMaps(shopifyData);
        const toCreate = apifyProcessed.filter(p => p.sku && !matchShopifyProductBySku(p, maps).product);
        addLog(`Found ${toCreate.length} products to create.`, 'info');
        // ... execute creation logic ...
    } catch (e) { addLog(`Create products job failed: ${e.message}`, 'error'); }
}

async function handleDiscontinuedProductsJob(token) {
    addLog('Starting discontinued check (SKU-only)...', 'info');
    // ... logic is updated to be SKU-centric ...
    try {
        const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts() ]);
        if (shouldAbort(token)) return;

        const apifySkus = new Set(processApifyProducts(apifyData, {processPrice: false}).map(p => p.sku?.toLowerCase()).filter(Boolean));
        
        const candidates = shopifyData.filter(p => {
            const shopifySku = p.variants?.[0]?.sku?.toLowerCase();
            return shopifySku && !apifySkus.has(shopifySku);
        });
        addLog(`Found ${candidates.length} Shopify products with SKUs not in Apify source.`, 'info');
        // ... execute discontinuation logic ...
    } catch (e) { addLog(`Discontinued job failed: ${e.message}`, 'error'); }
}


// --- UI AND API ---
app.get('/', (req, res) => {
    // UPDATED HTML for new button and dashboard sections
    const html = `<!DOCTYPE html><html lang="en" class="dark"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Shopify Sync Dashboard</title><script src="https://cdn.tailwindcss.com"></script><style> body { background: linear-gradient(to bottom right, #1a1a2e, #16213e); } .card-hover { background: rgba(31,41,55,.2); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,.1); } .btn-hover:hover { transform: translateY(-1px); } .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #ff6e7f; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;} @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} } ul { max-height: 150px; overflow-y: auto; } </style></head><body class="min-h-screen font-sans text-gray-200"><div class="container mx-auto px-4 py-8"><h1 class="text-4xl font-extrabold tracking-tight text-white mb-8">Shopify Sync Dashboard</h1><!-- Failsafe Banner --><div id="failsafeBanner"></div><div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-4"><div class="rounded-2xl p-6 card-hover"><h3>New Products (Total)</h3><p class="text-3xl font-bold" id="newProducts">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Inventory Updates (Total)</h3><p class="text-3xl font-bold" id="inventoryUpdates">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Discontinued (Total)</h3><p class="text-3xl font-bold" id="discontinued">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Errors (Total)</h3><p class="text-3xl font-bold" id="errors">0</p></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">System Controls</h2><div class="mb-6 p-4 rounded-lg" id="statusContainer"><div class="flex items-center justify-between"><div><h3 class="font-medium" id="systemStatus"></h3><p class="text-sm" id="systemStatusDesc"></p></div><div class="flex items-center"><button onclick="togglePause()" id="pauseButton" class="text-white px-4 py-2 rounded-lg btn-hover"></button></div></div></div><div class="flex flex-wrap gap-4"><button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Create New Products<div id="productsSpinner" class="spinner"></div></button><button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Update Inventory<div id="inventorySpinner" class="spinner"></div></button><button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Check Discontinued<div id="discontinuedSpinner" class="spinner"></div></button></div><div class="mt-6 border-t border-gray-700 pt-6"><h3 class="text-lg font-semibold text-purple-400 mb-2">One-Time SKU Migration</h3><p class="text-sm text-gray-400 mb-4">Run this job ONCE to align all your Shopify product SKUs with the Apify source data. This is a required step before regular syncs will work correctly.</p><button onclick="triggerMapSkus()" class="bg-purple-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Map & Override Shopify SKUs<div id="mapSkusSpinner" class="spinner"></div></button></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">Last Run Statistics</h2><div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-center"><div class="p-4 bg-gray-900/50 rounded-lg"><h3 class="font-semibold text-green-400">Inventory Sync</h3><p class="text-sm text-gray-300" id="lastInventoryUpdate">Not run yet.</p></div><div class="p-4 bg-gray-900/50 rounded-lg"><h3 class="font-semibold text-blue-400">Product Creation</h3><p class="text-sm text-gray-300" id="lastProductCreate">Not run yet.</p></div><div class="p-4 bg-gray-900/50 rounded-lg"><h3 class="font-semibold text-orange-400">Discontinued Check</h3><p class="text-sm text-gray-300" id="lastDiscontinuedCheck">Not run yet.</p></div></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">Recent Activity Details</h2><div class="grid grid-cols-1 md:grid-cols-2 gap-6"><div><h3 class="font-semibold text-blue-400 mb-2">Newly Created Products</h3><ul class="list-disc list-inside text-sm text-gray-300 space-y-1" id="newlyCreatedList"><li>None in the last run.</li></ul></div><div><h3 class="font-semibold text-orange-400 mb-2">Recently Discontinued Products</h3><ul class="list-disc list-inside text-sm text-gray-300 space-y-1" id="recentlyDiscontinuedList"><li>None in the last run.</li></ul></div></div></div><div class="rounded-2xl p-6 card-hover"><h2 class="text-2xl font-semibold text-white mb-4">Activity Log</h2><div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer"></div></div></div><script>let systemPaused=${systemPaused}; let failsafeTriggered=${JSON.stringify(failsafeTriggered)}; function togglePause(){fetch('/api/pause',{method:'POST'})} function clearFailsafe(){fetch('/api/failsafe/clear',{method:'POST'})} function confirmFailsafe(){fetch('/api/failsafe/confirm',{method:'POST'})} function abortFailsafe(){fetch('/api/failsafe/abort',{method:'POST'})} function triggerSync(type){const spinner=document.getElementById(type+'Spinner'); spinner.style.display='inline-block';fetch('/api/sync/'+type,{method:'POST'}).finally(()=>spinner.style.display='none')} function triggerMapSkus(){if(confirm('WARNING: This will override existing Shopify SKUs with SKUs from Apify based on the best possible match. This is a destructive, one-time operation. Are you sure you want to proceed?')){const spinner=document.getElementById('mapSkusSpinner'); spinner.style.display='inline-block';fetch('/api/sync/map-skus',{method:'POST'}).finally(()=>spinner.style.display='none')}} function formatLastRun(runData){if(!runData||!runData.at)return'Not run yet.';const time=new Date(runData.at).toLocaleTimeString();let stats='';if(runData.updated!==undefined)stats+=`Updated: ${runData.updated}, `;if(runData.created!==undefined)stats+=`Created: ${runData.created}, `;if(runData.discontinued!==undefined)stats+=`Discontinued: ${runData.discontinued}, `;stats+=`Errors: ${runData.errors||0}`;return`${stats} at ${time}`} function updateSystemStatus(){/*...unchanged...*/} setInterval(async()=>{try{const res=await fetch('/api/status');const data=await res.json();/*...unchanged stat updates...*/ document.getElementById('lastInventoryUpdate').textContent=formatLastRun(data.lastRun.inventory); document.getElementById('lastProductCreate').textContent=formatLastRun(data.lastRun.products); document.getElementById('lastDiscontinuedCheck').textContent=formatLastRun(data.lastRun.discontinued); const createdList=document.getElementById('newlyCreatedList'); const discontinuedList=document.getElementById('recentlyDiscontinuedList'); if(data.lastRun.products&&data.lastRun.products.createdItems?.length>0){createdList.innerHTML=data.lastRun.products.createdItems.map(item=>`<li>${item}</li>`).join('')}else{createdList.innerHTML='<li>None in the last run.</li>'} if(data.lastRun.discontinued&&data.lastRun.discontinued.discontinuedItems?.length>0){discontinuedList.innerHTML=data.lastRun.discontinued.discontinuedItems.map(item=>`<li>${item}</li>`).join('')}else{discontinuedList.innerHTML='<li>None in the last run.</li>'} /*...rest of interval unchanged...*/}catch(e){}},3000);updateSystemStatus();</script></body></html>`;
    res.send(html);
});
app.get('/api/status', (req, res) => { res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason }); });
app.post('/api/pause', (req, res) => { /*... Unchanged ...*/ });
app.post('/api/failsafe/clear', (req, res) => { /*... Unchanged ...*/ });
app.post('/api/failsafe/confirm', (req, res) => { /*... Unchanged ...*/ });
app.post('/api/failsafe/abort', (req, res) => { /*... Unchanged ...*/ });

// UPDATED: Added new map-skus endpoint
app.post('/api/sync/map-skus', (req, res) => {
    startBackgroundJob('mapSkus', 'Map & Override SKUs', (t) => mapSkusJob(t)) 
        ? res.json({success: true, message: "SKU mapping job started."}) 
        : res.status(409).json({success: false, message: "Job already running."});
});
app.post('/api/sync/:type', (req, res) => { /*... Unchanged ...*/ });

// Scheduled jobs (Unchanged)
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', (t) => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', (t) => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', (t) => handleDiscontinuedProductsJob(t)); });

// Server Start & Graceful Shutdown (Unchanged)
const server = app.listen(PORT, () => { /*... Unchanged ...*/ });
process.on('SIGTERM', () => { /*... Unchanged ...*/ });
process.on('SIGINT', () => { /*... Unchanged ...*/ });
