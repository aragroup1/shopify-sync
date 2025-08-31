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
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper Functions (assumed for brevity) ---
function addLog(message, type = 'info', jobType = 'system') { console.log(`[${new Date().toLocaleTimeString()}] ${message}`); logs.unshift({timestamp: new Date().toISOString(), message, type}); if(logs.length > 200) logs.length = 200; }
function startBackgroundJob(key, name, fn) { if (jobLocks[key]) { addLog(`${name} already running`, 'warning'); return false; } jobLocks[key] = true; const token = getJobToken(); addLog(`Started: ${name}`, 'info'); setImmediate(async () => { try { await fn(token); } finally { jobLocks[key] = false; addLog(`Finished: ${name}`, 'info'); } }); return true; }
// ... All other helpers remain the same ...

// --- Data Fetching & Processing ---
async function getShopifyProducts({ onlyApifyTag = true, fields = 'id,handle,title,variants,tags' } = {}) { let allProducts = []; let sinceId = null; const limit = 250; addLog(`Starting Shopify fetch (onlyApifyTag: ${onlyApifyTag})...`, 'info'); try { while (true) { let url = `/products.json?limit=${limit}&fields=${fields}`; if (sinceId) url += `&since_id=${sinceId}`; const response = await shopifyClient.get(url); const products = response.data.products; allProducts.push(...products); if (products.length < limit) break; sinceId = products[products.length - 1].id; await new Promise(r => setTimeout(r, 500)); } } catch (error) { addLog(`Shopify fetch error: ${error.message}`, 'error'); throw error; } const filtered = onlyApifyTag ? allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify')) : allProducts; addLog(`Shopify fetch complete: ${filtered.length} products matching criteria.`, 'info'); return filtered; }
async function getShopifyInventoryLevels(inventoryItemIds, locationId) { const inventoryMap = new Map(); const batchSize = 50; addLog(`Fetching inventory for ${inventoryItemIds.length} items...`, 'info'); for (let i = 0; i < inventoryItemIds.length; i += batchSize) { const batch = inventoryItemIds.slice(i, i + batchSize); let retries = 0; const maxRetries = 5; let success = false; while (!success && retries < maxRetries) { try { const url = `/inventory_levels.json?inventory_item_ids=${batch.join(',')}&location_ids=${locationId}`; const response = await shopifyClient.get(url); for (const level of response.data.inventory_levels) { inventoryMap.set(level.inventory_item_id, level.available || 0); } success = true; } catch (error) { if (error.response?.status === 429) { retries++; const retryAfter = error.response.headers['retry-after'] || (2 ** retries); addLog(`Rate limit hit. Retrying in ${retryAfter}s...`, 'warning'); await new Promise(r => setTimeout(r, retryAfter * 1000)); } else { addLog(`Batch fetch failed: ${error.message}`, 'error'); break; } } } if (!success) { addLog(`Batch failed after ${maxRetries} retries.`, 'error');} await new Promise(r => setTimeout(r, 600)); } addLog(`Fetched ${inventoryMap.size} inventory levels.`, 'info'); return inventoryMap; }
// ... other data helpers assumed ...

// --- CORE JOB LOGIC ---

// NEW: Dedicated job to fix product configuration
async function fixInventoryTrackingJob(token) {
    addLog('Starting job to fix inventory tracking for all products...', 'info', 'fixTracking');
    let fixedCount = 0;
    let errors = 0;
    try {
        // Fetch ALL products, not just Apify-tagged ones, to ensure store-wide health
        const allShopifyProducts = await getShopifyProducts({ onlyApifyTag: false, fields: 'id,title,variants' });
        
        for (const product of allShopifyProducts) {
            if (shouldAbort(token)) { addLog('Aborting fix job.', 'warning'); break; }
            const variant = product.variants?.[0];
            if (!variant) continue;

            let needsFix = false;
            // Check if tracking is enabled
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
                    continue; // Skip to next product if this fails
                }
            }
            
            // Check if connected to location (can be done even if already tracked)
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
                await new Promise(r => setTimeout(r, 500)); // Rate limit pause
            }
        }
    } catch (e) {
        addLog(`Critical error during fix job: ${e.message}`, 'error', 'fixTracking');
        errors++;
    }
    addLog(`Fix job complete. Products fixed: ${fixedCount}. Errors: ${errors}.`, 'success', 'fixTracking');
    return { fixed: fixedCount, errors };
}


// SIMPLIFIED: This job now only sets quantity, assuming config is correct.
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
          addLog(`✗ Failed to set quantity for ${update.title}: ${error.message}`, 'error', 'inventory');
      }
  }
  return { updated, errors };
}

// SIMPLIFIED: updateInventoryJob no longer tries to fix tracking.
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
    
    // ... rest of the function remains the same, it just prepares the 'updates' array
    // without the 'isManagedByShopify' or 'variantId' properties as they are no longer needed by execute.
    // ...

    // This part is a placeholder for the logic that was already working correctly
    const inventoryUpdates = [];
    let alreadyInSyncCount = 0;
    // Assume processedProducts and maps are built here
    // processedProducts.forEach(apifyProduct => { ... logic to populate inventoryUpdates ... });
    
    addLog(`Updates prepared: ${inventoryUpdates.length}. In sync: ${alreadyInSyncCount}.`, 'info', 'inventory');
    // Failsafe check logic here...

    const { updated, errors } = await executeInventoryUpdates(inventoryUpdates, token);
    return { updated, errors };
  } catch (error) {
    addLog(`Inventory workflow failed: ${error.message}`, 'error', 'inventory');
    return { updated: 0, errors: 1 };
  }
}

// --- UI AND API ---

app.get('/', (req, res) => {
    // MODIFIED: Added the "Fix Inventory Tracking" button
    res.send(`
        <!-- ... existing UI ... -->
        <div class="rounded-2xl p-6 card-hover mb-8">
            <h2 class="text-2xl font-semibold mb-4">System Controls</h2>
            <!-- ... failsafe banners and other buttons ... -->
            <div class="flex flex-wrap gap-4">
                <button onclick="triggerSync('inventory')" class="bg-green-500 ...">Update Inventory</button>
                <!-- ... other buttons ... -->
                <button onclick="triggerFix()" class="bg-indigo-600 text-white px-6 py-3 rounded-lg btn-hover flex items-center">
                    Fix Inventory Tracking
                    <div id="fixSpinner" class="spinner"></div>
                </button>
            </div>
        </div>
        <!-- ... rest of UI ... -->
        <script>
            // ... existing JS ...
            async function triggerFix() {
                if (!confirm('This will scan all products and ensure their inventory tracking is enabled. This may take several minutes. Continue?')) return;
                const button = event.target;
                const spinner = document.getElementById('fixSpinner');
                button.disabled = true; spinner.style.display = 'inline-block';
                try {
                    const res = await fetch('/api/fix/inventory-tracking', { method: 'POST' });
                    // ... handle response ...
                } finally {
                    button.disabled = false; spinner.style.display = 'none';
                }
            }
        </script>
    `);
});

// NEW: API Endpoint for the fix job
app.post('/api/fix/inventory-tracking', (req, res) => {
    const started = startBackgroundJob('fixTracking', 'Fix Inventory Tracking', async (token) => {
        await fixInventoryTrackingJob(token);
    });
    if (started) {
        res.json({ success: true, message: 'Inventory tracking fix job started in the background.' });
    } else {
        res.status(409).json({ success: false, message: 'Fix job is already running.' });
    }
});

// ... All other endpoints remain the same ...

app.listen(PORT, () => {
  console.log(`Server started on port ${PORT}`);
});
