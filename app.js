const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

// ... (initial setup and all configurations remain the same)
const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());
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
const FAILSAFE_LIMITS = { MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 100), MIN_SHOPIFY_PRODUCTS: Number(process.env.MIN_SHOPIFY_PRODUCTS || 100), MAX_CHANGE_PERCENTAGE: Number(process.env.MAX_CHANGE_PERCENTAGE || 30), MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), MAX_ERROR_RATE: Number(process.env.MAX_ERROR_RATE || 20), MAX_DISCONTINUED_AT_ONCE: Number(process.env.MAX_DISCONTINUED_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
let lastFailsafeNotified = '';
// ... (helper functions like notifyTelegram, addLog, startBackgroundJob, triggerFailsafe, etc. remain the same)
// ... (API clients and data processing functions like getApifyProducts, sanitizeProductTitle, etc. also remain the same)
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2', urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });


// MODIFIED: updateInventoryJob with robust type checking and logging
async function updateInventoryJob(token) {
  if (systemPaused) return { updated: 0, errors: 0, total: 0 };
  let errors = 0;

  try {
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    if (shouldAbort(token)) return { updated: 0, errors: 0 };
    
    // Step 1: Pre-compute the accurate inventory map
    const inventoryItemIds = shopifyData.map(p => p.variants?.[0]?.inventory_item_id).filter(Boolean);
    const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds, config.shopify.locationId);
    addLog(`Inventory map pre-computed with ${inventoryLevels.size} entries.`, 'info', 'inventory');

    // PREPARE PHASE
    const processedProducts = processApifyProducts(apifyData, { processPrice: false });
    const maps = buildShopifyMaps(shopifyData);
    const inventoryUpdates = [];
    let alreadyInSyncCount = 0;

    processedProducts.forEach((apifyProduct) => {
        const { product: shopifyProduct } = matchShopifyProduct(apifyProduct, maps);
        if (!shopifyProduct || !shopifyProduct.variants?.[0]?.inventory_item_id) {
            return;
        }

        const inventoryItemId = shopifyProduct.variants[0].inventory_item_id;
        
        // ROBUSTNESS FIX: Ensure both values are treated as integers for comparison
        const currentInventory = parseInt(inventoryLevels.get(inventoryItemId), 10);
        if (isNaN(currentInventory)) {
            // This case handles items that exist but aren't stocked at our location yet.
            // We treat their inventory as 0.
            currentInventory = 0; 
        }

        const targetInventory = parseInt(apifyProduct.inventory, 10) || 0;

        // Enhanced debug logging to see the exact comparison
        if (processedProducts.length < 20) { // Limit verbose logging to small runs for clarity
            addLog(`[DEBUG] Compare: "${shopifyProduct.title}" | Shopify (num): ${currentInventory} | Apify (num): ${targetInventory}`, 'info', 'inventory');
        }

        if (currentInventory === targetInventory) {
            alreadyInSyncCount++;
            return; // Skip if they already match
        }
        
        inventoryUpdates.push({
            title: shopifyProduct.title,
            currentInventory,
            newInventory: targetInventory,
            inventoryItemId: inventoryItemId,
        });
    });

    addLog(`Inventory updates prepared: ${inventoryUpdates.length} changes needed. ${alreadyInSyncCount} products already in sync.`, 'info', 'inventory');
    
    // Failsafe Check
    const actionToConfirm = { type: 'inventory', data: inventoryUpdates };
    const failsafeCheck = checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalApifyProducts: apifyData.length }, actionToConfirm);

    if (!failsafeCheck.proceed) {
        addLog(`Failsafe triggered: ${failsafeCheck.reason}. Job will not proceed automatically.`, 'warning', 'inventory');
        return { updated: 0, errors: 0 };
    }

    // EXECUTE PHASE
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

// ... (All other functions: getShopifyInventoryLevels, handleDiscontinuedProductsJob, createNewProductsJob, executeInventoryUpdates, etc. remain exactly the same as the previous version)
async function getShopifyInventoryLevels(inventoryItemIds, locationId) { const inventoryMap = new Map(); const batchSize = 50; addLog(`Fetching inventory levels for ${inventoryItemIds.length} items from location ${locationId}...`, 'info', 'inventory'); for (let i = 0; i < inventoryItemIds.length; i += batchSize) { const batch = inventoryItemIds.slice(i, i + batchSize); try { const url = `/inventory_levels.json?inventory_item_ids=${batch.join(',')}&location_ids=${locationId}`; const response = await shopifyClient.get(url); for (const level of response.data.inventory_levels) { inventoryMap.set(level.inventory_item_id, level.available || 0); } } catch (error) { addLog(`Failed to fetch inventory batch: ${error.message}`, 'error', 'inventory'); stats.errors++; } await new Promise(r => setTimeout(r, 500)); } addLog(`Successfully fetched ${inventoryMap.size} inventory levels.`, 'info', 'inventory'); return inventoryMap; }
async function handleDiscontinuedProductsJob(token) { /* ... */ }
async function createNewProductsJob(token, apifyProducts) { /* ... */ }
async function executeInventoryUpdates(updates, token) { /* ... */ }


// ... (The entire UI, API Endpoints, Telegram Webhook, and Schedules sections remain exactly the same as the previous version)
app.get('/', (req, res) => { /* ... UI HTML ... */ });
app.get('/api/status', (req, res) => { /* ... */ });
app.post('/api/stats/reset', (req, res) => { /* ... */ });
app.post('/api/failsafe/clear', (req, res) => { /* ... */ });
app.post('/api/failsafe/confirm', async (req, res) => { /* ... */ });
app.post('/api/failsafe/abort', (req, res) => { /* ... */ });
app.post('/api/pause', (req, res) => { /* ... */ });
app.post('/api/sync/products', (req, res) => { /* ... */ });
app.post('/api/sync/inventory', (req, res) => { /* ... */ });
app.post('/api/sync/discontinued', (req, res) => { /* ... */ });
app.get('/api/debug/check-product/:handle', async (req, res) => { /* ... */ });
app.post('/telegram/webhook/:secret?', async (req, res) => { /* ... */ });

cron.schedule('0 1 * * *', () => { /* ... */ });
cron.schedule('0 2 * * 5', () => { /* ... */ });
cron.schedule('0 3 * * *', () => { /* ... */ });
cron.schedule('0 9 * * 1', async () => { /* ... */ });

app.listen(PORT, () => {
  // ... (startup logs)
});
