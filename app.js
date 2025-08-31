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

// --- Helper Functions (assumed for brevity) ---
function addLog(message, type = 'info', jobType = 'system') { console.log(`[${new Date().toLocaleTimeString()}] ${message}`); /* ... more logic ... */ }
// ... All other helpers remain the same ...

// --- Data Fetching & Processing ---
async function getApifyProducts() { /* ... implementation ... */ return []; }
async function getShopifyProducts({ onlyApifyTag = true } = {}) { /* ... implementation ... */ return []; }
function processApifyProducts(apifyData, options = { processPrice: true }) { /* ... implementation ... */ return []; }
function buildShopifyMaps(shopifyData) { /* ... implementation ... */ return new Map(); }
function matchShopifyProduct(apifyProduct, maps) { /* ... implementation ... */ return { product: null }; }

// MODIFIED: getShopifyInventoryLevels with robust rate limit handling
async function getShopifyInventoryLevels(inventoryItemIds, locationId) {
    const inventoryMap = new Map();
    const batchSize = 50;
    addLog(`Fetching inventory levels for ${inventoryItemIds.length} items from location ${locationId}...`, 'info', 'inventory');

    for (let i = 0; i < inventoryItemIds.length; i += batchSize) {
        const batch = inventoryItemIds.slice(i, i + batchSize);
        let retries = 0;
        const maxRetries = 5;
        let success = false;

        while (!success && retries < maxRetries) {
            try {
                const url = `/inventory_levels.json?inventory_item_ids=${batch.join(',')}&location_ids=${locationId}`;
                const response = await shopifyClient.get(url);
                for (const level of response.data.inventory_levels) {
                    inventoryMap.set(level.inventory_item_id, level.available || 0);
                }
                success = true; // Mark as successful to exit the retry loop
            } catch (error) {
                if (error.response?.status === 429) {
                    retries++;
                    const retryAfter = error.response.headers['retry-after'] || (2 ** retries); // Use header or exponential backoff
                    addLog(`Rate limit hit. Retrying batch in ${retryAfter} seconds... (Attempt ${retries}/${maxRetries})`, 'warning', 'inventory');
                    await new Promise(r => setTimeout(r, retryAfter * 1000));
                } else {
                    addLog(`Failed to fetch inventory batch (non-retryable error): ${error.message}`, 'error', 'inventory');
                    stats.errors++;
                    break; // Break from while loop for non-429 errors
                }
            }
        }
        if (!success) {
            addLog(`Batch starting at index ${i} failed after ${maxRetries} retries. Skipping this batch.`, 'error', 'inventory');
        }
        await new Promise(r => setTimeout(r, 600)); // Maintain a small delay between successful batches
    }
    addLog(`Successfully fetched ${inventoryMap.size} of ${inventoryItemIds.length} possible inventory levels.`, 'info', 'inventory');
    return inventoryMap;
}

// --- CORE JOB LOGIC ---

async function executeInventoryUpdates(updates, token) {
  let updated = 0, errors = 0;
  if (!updates || updates.length === 0) return { updated, errors };
  addLog(`Executing ${updates.length} updates with Track-Connect-Set sequence...`, 'info', 'inventory');

  for (const update of updates) {
      if (shouldAbort(token)) { addLog('Aborting execution...', 'warning', 'inventory'); break; }
      try {
          if (!update.isManagedByShopify) {
              addLog(`Enabling inventory tracking for: ${update.title}`, 'info', 'inventory');
              await shopifyClient.put(`/variants/${update.variantId}.json`, { variant: { id: update.variantId, inventory_management: 'shopify' } });
              await new Promise(r => setTimeout(r, 400));
          }
          try {
              await shopifyClient.post('/inventory_levels/connect.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: update.inventoryItemId });
          } catch (connectError) {
              if (connectError.response?.status !== 422) { addLog(`Note: Non-422 error during connect for ${update.title}: ${connectError.message}`, 'warning', 'inventory'); }
          }
          await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: update.inventoryItemId, available: update.newInventory });
          addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success', 'inventory');
          updated++;
          stats.inventoryUpdates++;
          await new Promise(r => setTimeout(r, 400));
      } catch (error) {
          errors++;
          stats.errors++;
          addLog(`✗ Failed full update for ${update.title}: ${error.message}`, 'error', 'inventory');
      }
  }
  return { updated, errors };
}

async function updateInventoryJob(token) {
  if (systemPaused) return { updated: 0, errors: 0 };
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
            variantId: variant.id,
            isManagedByShopify: variant.inventory_management === 'shopify'
        });
    });

    addLog(`Inventory updates prepared: ${inventoryUpdates.length} changes needed. ${alreadyInSyncCount} products already in sync.`, 'info', 'inventory');
    
    const actionToConfirm = { type: 'inventory', data: inventoryUpdates };
    const failsafeCheck = checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalApifyProducts: apifyData.length }, actionToConfirm);
    if (!failsafeCheck.proceed) { return { updated: 0, errors: 0 }; }

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

// --- API Endpoints, UI, and Schedules ---
// (These sections remain unchanged)
app.get('/', (req, res) => { /* ... UI HTML ... */ });
app.get('/api/status', (req, res) => { /* ... */ });
// ... all other endpoints
cron.schedule('0 1 * * *', () => { /* ... */ });
// ... all other schedules

app.listen(PORT, () => {
  console.log(`Server started on port ${PORT}`);
});
