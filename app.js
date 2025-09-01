const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0 };
let lastRun = { inventory: {}, products: { createdItems: [] }, discontinued: { discontinuedItems: [] }, deduplicate: {}, mapSkus: {} };
let logs = [];
let systemPaused = false;
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;
const missingCounters = new Map();
let errorSummary = new Map(); // NEW: For weekly error reporting

const jobLocks = { inventory: false, products: false, discontinued: false, mapSkus: false, deduplicate: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), MAX_DISCONTINUE_PERCENTAGE: Number(process.env.MAX_DISCONTINUE_PERCENTAGE || 5), MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper Functions ---
function addLog(message, type = 'info', error = null) {
    const log = { timestamp: new Date().toISOString(), message, type };
    logs.unshift(log);
    if (logs.length > 200) logs.length = 200;
    console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`);

    if (type === 'error') {
        stats.errors++;
        // Normalize error message to group similar ones
        let normalizedError = (error?.message || message)
            .replace(/"[^"]+"/g, '"{VAR}"') // Anonymize product titles/handles
            .replace(/\b\d{5,}\b/g, '{ID}'); // Anonymize IDs
        const currentCount = errorSummary.get(normalizedError) || 0;
        errorSummary.set(normalizedError, currentCount + 1);
    }
}
function startBackgroundJob(key, name, fn) { if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning'); return false; } jobLocks[key] = true; const token = getJobToken(); addLog(`Started background job: ${name}`, 'info'); setImmediate(async () => { try { await fn(token); } catch (e) { addLog(`Unhandled error in ${name}: ${e.message}\n${e.stack}`, 'error', e); } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info'); } }); return true; }
function getWordOverlap(str1, str2) { const words1 = new Set(str1.split(' ')); const words2 = new Set(str2.split(' ')); const intersection = new Set([...words1].filter(x => words2.has(x))); return (intersection.size / Math.max(words1.size, words2.size)) * 100; }

// --- Data Fetching & Processing ---
async function getApifyProducts() { /* Unchanged */ }
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) { /* Unchanged */ }
function normalizeHandle(input = '') { /* Unchanged */ }
function stripHandleSuffixes(handle) { /* Unchanged */ }
function normalizeTitle(text = '') { /* Unchanged */ }
function calculateRetailPrice(supplierCostString) { /* Unchanged */ }
function processApifyProducts(apifyData, { processPrice = true } = {}) { /* Unchanged */ }
function buildShopifyMaps(shopifyData) { /* Unchanged */ }

// --- Matching Logic ---
function findBestMatch(apifyProduct, shopifyMaps) { /* Unchanged */ }
function matchShopifyProductBySku(apifyProduct, skuMap) { /* Unchanged */ }

// --- CORE JOB LOGIC ---
async function deduplicateProductsJob(token) {
    addLog('--- Starting One-Time Duplicate Cleanup Job ---', 'warning');
    let deletedCount = 0, errors = 0;
    try {
        const allShopifyProducts = await getShopifyProducts();
        if (shouldAbort(token)) return;

        const productsByTitle = new Map();
        for (const product of allShopifyProducts) {
            const normalized = normalizeTitle(product.title);
            if (!productsByTitle.has(normalized)) productsByTitle.set(normalized, []);
            productsByTitle.get(normalized).push(product);
        }

        const toDeleteIds = [];
        for (const products of productsByTitle.values()) {
            if (products.length > 1) {
                addLog(`Found ${products.length} duplicates for title: "${products[0].title}"`, 'warning');
                // CORRECTED LOGIC: Sort by creation date, OLDEST first
                products.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
                const toKeep = products.shift(); // Keep the first (oldest) one
                addLog(`  Keeping ORIGINAL product ID ${toKeep.id} (created at ${toKeep.created_at})`, 'info');
                products.forEach(p => {
                    addLog(`  Marking NEWER duplicate for deletion ID ${p.id} (created at ${p.created_at})`, 'info');
                    toDeleteIds.push(p.id);
                });
            }
        }
        
        if (toDeleteIds.length > 0) {
            addLog(`Preparing to delete ${toDeleteIds.length} newer duplicate products...`, 'warning');
            for (const id of toDeleteIds) {
                if (shouldAbort(token)) break;
                try {
                    await shopifyClient.delete(`/products/${id}.json`);
                    deletedCount++;
                    addLog(`  ✓ Deleted product ID ${id}`, 'success');
                } catch (e) { errors++; addLog(`  ✗ Error deleting product ID ${id}: ${e.message}`, 'error', e); }
                await new Promise(r => setTimeout(r, 600));
            }
        } else { addLog('No duplicate products found to delete.', 'success'); }
    } catch (e) { addLog(`Critical error in deduplication job: ${e.message}`, 'error', e); errors++; }
    lastRun.deduplicate = { at: new Date().toISOString(), deleted: deletedCount, errors };
}

async function mapSkusJob(token) { /* Unchanged */ }
async function createNewProductsJob(token) { /* Unchanged */ }
async function updateInventoryJob(token) { /* Unchanged */ }
async function handleDiscontinuedProductsJob(token) { /* Unchanged */ }

// NEW: Weekly Error Reporting Job
async function generateAndSendErrorReport() {
    addLog('Generating weekly error report...', 'info');
    if (errorSummary.size === 0) {
        addLog('No errors recorded in the last week. All clear!', 'success');
        await notifyTelegram('✅ <b>Weekly System Report</b> ✅\n\nNo errors were recorded in the last week. System is running smoothly!');
        return;
    }

    const sortedErrors = [...errorSummary.entries()].sort((a, b) => b[1] - a[1]);
    
    let reportText = '🚨 <b>Weekly System Error Report</b> 🚨\n\nTop errors from the past week:\n\n';
    sortedErrors.slice(0, 10).forEach(([message, count]) => {
        reportText += `<b>${count}x</b>: <pre>${message}</pre>\n`;
    });

    await notifyTelegram(reportText);
    addLog('Weekly error report sent. Clearing summary for next week.', 'info');
    errorSummary.clear();
}


// --- UI AND API ---
app.get('/', (req, res) => {
    const html = `<!DOCTYPE html><html lang="en" class="dark"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Shopify Sync Dashboard</title><script src="https://cdn.tailwindcss.com"></script><style> body { background: linear-gradient(to bottom right, #1a1a2e, #16213e); } .card-hover { background: rgba(31,41,55,.2); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,.1); } .btn-hover:hover { transform: translateY(-1px); } .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #ff6e7f; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;} @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} } ul { max-height: 150px; overflow-y: auto; } </style></head><body class="min-h-screen font-sans text-gray-200"><div class="container mx-auto px-4 py-8"><h1 class="text-4xl font-extrabold tracking-tight text-white mb-8">Shopify Sync Dashboard</h1><!-- Failsafe Banner --><div id="failsafeBanner"></div><div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-4"><div class="rounded-2xl p-6 card-hover"><h3>New Products (Total)</h3><p class="text-3xl font-bold" id="newProducts">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Inventory Updates (Total)</h3><p class="text-3xl font-bold" id="inventoryUpdates">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Discontinued (Total)</h3><p class="text-3xl font-bold" id="discontinued">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Errors (Total)</h3><p class="text-3xl font-bold" id="errors">0</p></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">System Controls</h2><div class="mb-6 p-4 rounded-lg" id="statusContainer"><div class="flex items-center justify-between"><div><h3 class="font-medium" id="systemStatus"></h3><p class="text-sm" id="systemStatusDesc"></p></div><div class="flex items-center"><button onclick="togglePause()" id="pauseButton" class="text-white px-4 py-2 rounded-lg btn-hover"></button></div></div></div><div class="flex flex-wrap gap-4"><button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Create New Products<div id="productsSpinner" class="spinner"></div></button><button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Update Inventory<div id="inventorySpinner" class="spinner"></div></button><button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Check Discontinued<div id="discontinuedSpinner" class="spinner"></div></button></div></div><div class="rounded-2xl p-6 card-hover mb-8 bg-red-900/20 border-red-500/50 border"><h2 class="text-2xl font-semibold text-white mb-4">System Health & Cleanup</h2><div class="grid grid-cols-1 md:grid-cols-2 gap-6"><div class="border-r border-gray-700 pr-6"> <h3 class="text-lg font-semibold text-red-400 mb-2">Step 1: Find & Delete Duplicates</h3> <p class="text-sm text-gray-400 mb-4">Permanently deletes newer Shopify products that have the same title, keeping only the original one. Run this before mapping SKUs.</p><button onclick="triggerDedupe()" class="bg-red-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Find & Delete Duplicates<div id="deduplicateSpinner" class="spinner"></div></button></div><div> <h3 class="text-lg font-semibold text-purple-400 mb-2">Step 2: Map & Override SKUs</h3> <p class="text-sm text-gray-400 mb-4">Matches products using advanced logic and overrides Shopify SKUs with Apify SKUs. Run this after cleaning duplicates.</p><button onclick="triggerMapSkus()" class="bg-purple-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Map & Override Shopify SKUs<div id="mapSkusSpinner" class="spinner"></div></button></div></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">Last Run Statistics</h2><div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-center"><div class="p-4 bg-gray-900/50 rounded-lg"><h3 class="font-semibold text-green-400">Inventory Sync</h3><p class="text-sm text-gray-300" id="lastInventoryUpdate">Not run yet.</p></div><div class="p-4 bg-gray-900/50 rounded-lg"><h3 class="font-semibold text-blue-400">Product Creation</h3><p class="text-sm text-gray-300" id="lastProductCreate">Not run yet.</p></div><div class="p-4 bg-gray-900/50 rounded-lg"><h3 class="font-semibold text-orange-400">Discontinued Check</h3><p class="text-sm text-gray-300" id="lastDiscontinuedCheck">Not run yet.</p></div></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">Recent Activity & Errors</h2><div class="grid grid-cols-1 md:grid-cols-3 gap-6"><div><h3 class="font-semibold text-blue-400 mb-2">Newly Created Products</h3><ul class="list-disc list-inside text-sm text-gray-300 space-y-1" id="newlyCreatedList"><li>None yet.</li></ul></div><div><h3 class="font-semibold text-orange-400 mb-2">Recently Discontinued</h3><ul class="list-disc list-inside text-sm text-gray-300 space-y-1" id="recentlyDiscontinuedList"><li>None yet.</li></ul></div><div><h3 class="font-semibold text-red-400 mb-2">Top Errors This Week</h3><ul class="text-sm text-gray-300 space-y-1" id="errorSummaryList"><li>No errors yet.</li></ul></div></div></div><div class="rounded-2xl p-6 card-hover"><h2 class="text-2xl font-semibold text-white mb-4">Activity Log</h2><div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer"></div></div></div><script>let systemPaused=${systemPaused}; let failsafeTriggered=${JSON.stringify(failsafeTriggered)}; function togglePause(){fetch('/api/pause',{method:'POST'})} function clearFailsafe(){fetch('/api/failsafe/clear',{method:'POST'})} function confirmFailsafe(){fetch('/api/failsafe/confirm',{method:'POST'})} function abortFailsafe(){fetch('/api/failsafe/abort',{method:'POST'})} function triggerSync(type){const spinner=document.getElementById(type+'Spinner'); spinner.style.display='inline-block';fetch('/api/sync/'+type,{method:'POST'}).finally(()=>spinner.style.display='none')} function triggerDedupe(){const confirmText=prompt('CRITICAL ACTION: This will find all products with duplicate titles and PERMANENTLY DELETE the newer ones, keeping only the original. This cannot be undone. To proceed, type DELETE in the box below.'); if(confirmText==='DELETE'){const spinner=document.getElementById('deduplicateSpinner'); spinner.style.display='inline-block';fetch('/api/sync/deduplicate',{method:'POST'}).finally(()=>spinner.style.display='none')}} function triggerMapSkus(){if(confirm('This will override Shopify SKUs with SKUs from Apify using advanced matching. Run this AFTER cleaning duplicates. Are you sure?')){const spinner=document.getElementById('mapSkusSpinner'); spinner.style.display='inline-block';fetch('/api/sync/map-skus',{method:'POST'}).finally(()=>spinner.style.display='none')}} function formatLastRun(runData){if(!runData||!runData.at)return'Not run yet.';const time=new Date(runData.at).toLocaleTimeString();let stats='';if(runData.updated!==undefined)stats+=`Updated: ${runData.updated}, `;if(runData.created!==undefined)stats+=`Created: ${runData.created}, `;if(runData.discontinued!==undefined)stats+=`Discontinued: ${runData.discontinued}, `;if(runData.deleted!==undefined)stats+=`Deleted: ${runData.deleted}, `;if(runData.alreadyMatched!==undefined)stats+=`Matched: ${runData.updated+runData.alreadyMatched}, `;stats+=`Errors: ${runData.errors||0}`;return`${stats} at ${time}`} setInterval(async()=>{try{const res=await fetch('/api/status');const data=await res.json();document.getElementById('newProducts').textContent=data.stats.newProducts;document.getElementById('inventoryUpdates').textContent=data.stats.inventoryUpdates;document.getElementById('discontinued').textContent=data.stats.discontinued;document.getElementById('errors').textContent=data.stats.errors;document.getElementById('logContainer').innerHTML=data.logs.map(log=>`<div class="${log.type==='error'?'text-red-400':log.type==='warning'?'text-yellow-400':'text-gray-300'}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('');document.getElementById('lastInventoryUpdate').textContent=formatLastRun(data.lastRun.inventory);document.getElementById('lastProductCreate').textContent=formatLastRun(data.lastRun.products);document.getElementById('lastDiscontinuedCheck').textContent=formatLastRun(data.lastRun.discontinued);const createdList=document.getElementById('newlyCreatedList');const discontinuedList=document.getElementById('recentlyDiscontinuedList');if(data.lastRun.products&&data.lastRun.products.createdItems?.length>0){createdList.innerHTML=data.lastRun.products.createdItems.map(item=>`<li>${item}</li>`).join('')}else{createdList.innerHTML='<li>None yet.</li>'} if(data.lastRun.discontinued&&data.lastRun.discontinued.discontinuedItems?.length>0){discontinuedList.innerHTML=data.lastRun.discontinued.discontinuedItems.map(item=>`<li>${item}</li>`).join('')}else{discontinuedList.innerHTML='<li>None yet.</li>'} const errorList=document.getElementById('errorSummaryList');if(data.errorSummary&&data.errorSummary.length>0){errorList.innerHTML=data.errorSummary.sort((a,b)=>b.count-a.count).slice(0,10).map(err=>`<li><span class="font-bold text-red-400">${err.count}x</span> ${err.msg}</li>`).join('')}else{errorList.innerHTML='<li>No errors yet.</li>'} if(data.systemPaused!==systemPaused||data.failsafeTriggered!==failsafeTriggered)location.reload()}catch(e){}},3000);</script></body></html>`;
    res.send(html);
});
app.get('/api/status', (req, res) => res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason, errorSummary: Array.from(errorSummary.entries()).map(([msg, count]) => ({msg, count})) }));
app.post('/api/pause', (req, res) => { /*...*/ });
app.post('/api/failsafe/clear', (req, res) => { /*...*/ });
app.post('/api/failsafe/confirm', (req, res) => { /*...*/ });
app.post('/api/failsafe/abort', (req, res) => { /*...*/ });
app.post('/api/sync/deduplicate', (req, res) => startBackgroundJob('deduplicate', 'Find & Delete Duplicates', t => deduplicateProductsJob(t)) ? res.json({s:1}) : res.status(409).json({s:0}));
app.post('/api/sync/map-skus', (req, res) => startBackgroundJob('mapSkus', 'Map & Override SKUs', t => mapSkusJob(t)) ? res.json({s:1}) : res.status(409).json({s:0}));
app.post('/api/sync/:type', (req, res) => { const jobs = { inventory: updateInventoryJob, products: createNewProductsJob, discontinued: handleDiscontinuedProductsJob }; const { type } = req.params; if (!jobs[type]) return res.status(400).json({s:0}); startBackgroundJob(type, `Manual ${type} sync`, t => jobs[type](t)) ? res.json({s:1}) : res.status(409).json({s:0}); });

// Scheduled jobs
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', t => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', t => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', t => handleDiscontinuedProductsJob(t)); });
cron.schedule('0 9 * * 1', () => { startBackgroundJob('errorReport', 'Weekly Error Report', () => generateAndSendErrorReport()); }); // 9 AM every Monday

// Server Start & Graceful Shutdown
const server = app.listen(PORT, () => { addLog(`Server started on port ${PORT}`, 'success'); const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]); if (missing.length > 0) { addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); process.exit(1); } });
process.on('SIGTERM', () => { addLog('SIGTERM received...', 'warning'); server.close(() => process.exit(0)); });
process.on('SIGINT', () => { addLog('SIGINT received...', 'warning'); server.close(() => process.exit(0)); });
