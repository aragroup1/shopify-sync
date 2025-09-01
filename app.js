const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
let lastRun = {
  inventory: { updated: 0, errors: 0, at: null },
  products: { created: 0, errors: 0, at: null },
  discontinued: { discontinued: 0, errors: 0, at: null }
};
let logs = [];
let systemPaused = false;
let mismatches = [];
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;
const missingCounters = new Map();

const jobLocks = { inventory: false, products: false, discontinued: false, fixTracking: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered === true || failsafeTriggered === 'pending' || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { 
  MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 100), 
  MIN_SHOPIFY_PRODUCTS: Number(process.env.MIN_SHOPIFY_PRODUCTS || 100), 
  MAX_CHANGE_PERCENTAGE: Number(process.env.MAX_CHANGE_PERCENTAGE || 30), 
  MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 50), 
  MAX_ERROR_RATE: Number(process.env.MAX_ERROR_RATE || 20), 
  MAX_DISCONTINUED_AT_ONCE: Number(process.env.MAX_DISCONTINUED_AT_ONCE || 100), 
  FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) 
};
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const MAX_UPDATES_PER_RUN = Number(process.env.MAX_UPDATES_PER_RUN || 100);
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const config = { 
  apify: { 
    token: process.env.APIFY_TOKEN, 
    actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', 
    baseUrl: 'https://api.apify.com/v2', 
    urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/' 
  }, 
  shopify: { 
    domain: process.env.SHOPIFY_DOMAIN, 
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN, 
    locationId: process.env.SHOPIFY_LOCATION_ID, 
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` 
  } 
};

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ 
  baseURL: config.shopify.baseUrl, 
  headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, 
  timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT 
});

// --- Helper Functions ---
function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 200) logs.length = 200;
  console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`);
}

async function notifyTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try { 
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, 
      { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, 
      { timeout: 15000 }
    ); 
  } catch (e) { 
    addLog(`Telegram notify failed: ${e.message}`, 'warning'); 
  }
}

function startBackgroundJob(key, name, fn) {
  if (jobLocks[key]) { 
    addLog(`${name} already running; ignoring duplicate start`, 'warning'); 
    return false; 
  }
  jobLocks[key] = true;
  const token = getJobToken();
  addLog(`Started background job: ${name}`, 'info');
  setImmediate(async () => {
    try { 
      await fn(token); 
    } catch (e) { 
      addLog(`Unhandled error in ${name}: ${e.message}`, 'error'); 
    } finally { 
      jobLocks[key] = false; 
      addLog(`${name} job finished`, 'info'); 
    }
  });
  return true;
}

function triggerFailsafe(msg, isConfirmable = false, action = null) {
  if (failsafeTriggered) return;
  failsafeReason = msg;
  systemPaused = true;
  abortVersion++;
  addLog(`⚠️ FAILSAFE TRIGGERED: ${failsafeReason}`, 'error');
  if (isConfirmable && action) {
    failsafeTriggered = 'pending';
    pendingFailsafeAction = action;
    addLog('System paused, waiting for user confirmation.', 'warning');
    notifyTelegram(`⚠️ <b>Failsafe Warning - Confirmation Required</b> ⚠️\n\n<b>Reason:</b>\n<pre>${msg}</pre>\n\nTo proceed, use the dashboard or reply: <code>/confirm</code>`);
  } else {
    failsafeTriggered = true;
    addLog('System automatically paused to prevent potential damage.', 'error');
    notifyTelegram(`🚨 <b>Failsafe Triggered & System Paused</b> 🚨\n\n<b>Reason:</b>\n<pre>${msg}</pre>`);
  }
}

function checkFailsafeConditions(context, data = {}, actionToConfirm = null) {
  const checks = [];
  let isConfirmable = false;
  if (context === 'inventory') {
    isConfirmable = true;
    if (data.totalApifyProducts > 0 && data.updatesNeeded > data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100)) {
      checks.push(`Too many inventory changes: ${data.updatesNeeded} > ${Math.floor(data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100))} (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}% of total)`);
    }
  }
  if (context === 'discontinued') {
    if (data.toDiscontinue > FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE) {
      checks.push(`Too many products to discontinue: ${data.toDiscontinue} > ${FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE}`);
    }
  }
  if (checks.length > 0) {
    const reason = checks.join('; ');
    triggerFailsafe(reason, isConfirmable, actionToConfirm);
    return false;
  }
  return true;
}

// --- Data Fetching & Processing ---
async function getApifyProducts() {
  let allItems = [];
  let offset = 0;
  const limit = 1000;
  addLog('Starting Apify product fetch...', 'info');
  try {
    while (true) {
      const response = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=${limit}&offset=${offset}`);
      const items = response.data;
      allItems.push(...items);
      if (items.length < limit) break;
      offset += limit;
    }
  } catch (error) { 
    addLog(`Apify fetch error: ${error.message}`, 'error'); 
    triggerFailsafe(`Apify fetch failed: ${error.message}`); 
    throw error; 
  }
  addLog(`Apify fetch complete: ${allItems.length} total products.`, 'info');
  return allItems;
}

async function getShopifyProducts({ onlyApifyTag = true, fields = 'id,handle,title,variants,tags,status' } = {}) {
  let allProducts = [];
  addLog(`Starting Shopify fetch (onlyApifyTag: ${onlyApifyTag})...`, 'info');
  try {
    let url = `/products.json?limit=250&fields=${fields}`;
    while (url) {
      const response = await shopifyClient.get(url);
      const products = response.data.products;
      allProducts.push(...products);
      const linkHeader = response.headers.link;
      url = null;
      if (linkHeader) {
        const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"'));
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
    addLog(`Shopify fetch error: ${error.message}`, 'error'); 
    triggerFailsafe(`Shopify fetch failed: ${error.message}`); 
    throw error; 
  }
  const filtered = onlyApifyTag ? allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify')) : allProducts;
  addLog(`Shopify fetch complete: ${filtered.length} products matching criteria.`, 'info');
  return filtered;
}

async function getShopifyInventoryLevels(inventoryItemIds, locationId) {
  const inventoryMap = new Map();
  const batchSize = 50;
  addLog(`Fetching inventory for ${inventoryItemIds.length} items...`, 'info');
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
        success = true;
      } catch (error) {
        if (error.response?.status === 429) {
          retries++;
          const retryAfter = error.response.headers['retry-after'] || (2 ** retries);
          addLog(`Rate limit hit. Retrying in ${retryAfter}s... (Attempt ${retries}/${maxRetries})`, 'warning');
          await new Promise(r => setTimeout(r, retryAfter * 1000));
        } else { 
          addLog(`Batch fetch failed: ${error.message}`, 'error'); 
          break; 
        }
      }
    }
    if (!success) { 
      addLog(`Batch failed after ${maxRetries} retries. Skipping.`, 'error'); 
    }
    await new Promise(r => setTimeout(r, 600));
  }
  addLog(`Fetched ${inventoryMap.size} of ${inventoryItemIds.length} possible inventory levels.`, 'info');
  return inventoryMap;
}

function normalizeHandle(input, index) { 
  let handle = String(input || `product-${index}`).toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, ''); 
  if (!handle || handle === '-' || handle.length < 2) handle = `product-${index}-${Date.now()}`; 
  return handle; 
}

function stripHandleSuffixes(handle) {
  return String(handle).toLowerCase().replace(/-(parcel|large-letter|letter)-rate$/i, '').replace(/-rate$/i, '').replace(/-p\d+$/i, '');
}

function normalizeTitle(text = '') { 
  return String(text).toLowerCase().replace(/\b\d{4}\b/g, '').replace(/KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE/g, '').replace(/[^a-z0-9]+/g, ' ').trim(); 
}

function processApifyProducts(apifyData, options = { processPrice: true }) {
  return apifyData.map((item, index) => {
    const handle = normalizeHandle(item.handle || item.title, index);
    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20;
    const rawStatus = item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '';
    if (String(rawStatus).toLowerCase().includes('out')) inventory = 0;
    let price = '0.00';
    if (options.processPrice && item.variants?.[0]?.price?.value) {
      price = String(item.variants[0].price.value);
    }
    let sku = item.sku || item.variants?.[0]?.sku || '';
    return { handle, title: item.title, inventory, sku, price };
  }).filter(Boolean);
}

function buildShopifyMaps(shopifyData) {
  const handleMap = new Map();
  const strippedHandleMap = new Map();
  const titleMap = new Map();
  const skuMap = new Map();
  const skuAsHandleMap = new Map();
  
  shopifyData.forEach(product => {
    handleMap.set(product.handle.toLowerCase(), product);
    strippedHandleMap.set(stripHandleSuffixes(product.handle), product);
    titleMap.set(normalizeTitle(product.title), product);
    const sku = product.variants?.[0]?.sku;
    if (sku) {
      skuMap.set(sku.toLowerCase(), product);
      if (/^\d+$/.test(product.handle) || /^[a-z0-9]{3,10}$/i.test(product.handle)) {
        skuAsHandleMap.set(product.handle.toLowerCase(), product);
      }
    }
  });
  return { handleMap, strippedHandleMap, titleMap, skuMap, skuAsHandleMap };
}

function matchShopifyProduct(apifyProduct, maps) {
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
  
  for (const [shopifyHandle, shopifyProduct] of maps.handleMap) {
    if (shopifyHandle.includes(strippedApifyHandle) || strippedApifyHandle.includes(shopifyHandle)) {
      return { product: shopifyProduct, matchType: 'handle-partial' };
    }
  }
  
  return { product: null, matchType: 'none' };
}

// --- CORE JOB LOGIC ---
async function executeInventoryUpdates(updates, token) {
  let updated = 0, errors = 0;
  addLog(`Executing ${updates.length} inventory quantity updates...`, 'info');
  for (const update of updates) {
    if (shouldAbort(token)) break;
    try {
      await shopifyClient.post('/inventory_levels/set.json', { 
        location_id: parseInt(config.shopify.locationId), 
        inventory_item_id: update.inventoryItemId, 
        available: update.newInventory 
      });
      updated++;
      stats.inventoryUpdates++;
      addLog(`✓ Updated inventory for "${update.title}": ${update.currentInventory} → ${update.newInventory}`, 'success');
    } catch (error) { 
      errors++; 
      stats.errors++; 
      addLog(`✗ Failed to set quantity for ${update.title}: ${error.message}`, 'error'); 
    }
    await new Promise(r => setTimeout(r, 500));
  }
  return { updated, errors };
}

async function updateInventoryJob(token) {
  addLog('Starting inventory sync...', 'info');
  mismatches = [];
  try {
    const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts({ onlyApifyTag: true }) ]);
    if (shouldAbort(token) || apifyData.length === 0) return addLog('Aborting sync.', 'warning');
    
    const inventoryItemIds = shopifyData.map(p => p.variants?.[0]?.inventory_item_id).filter(Boolean);
    const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds, config.shopify.locationId);
    const processedProducts = processApifyProducts(apifyData, { processPrice: false });
    const maps = buildShopifyMaps(shopifyData);
    
    const matchedShopifyIds = new Set();
    const inventoryUpdates = [];
    let alreadyInSyncCount = 0;
    let matchedCount = 0;
    let skippedDuplicates = 0;
    
    processedProducts.forEach((apifyProduct) => {
      const { product: shopifyProduct, matchType } = matchShopifyProduct(apifyProduct, maps);
      if (!shopifyProduct || !shopifyProduct.variants?.[0]?.inventory_item_id) {
        mismatches.push({ apifyTitle: apifyProduct.title, apifyHandle: apifyProduct.handle, apifySku: apifyProduct.sku, shopifyHandle: 'NOT FOUND' });
        return;
      }
      if (matchedShopifyIds.has(shopifyProduct.id)) {
        skippedDuplicates++;
        return;
      }
      matchedShopifyIds.add(shopifyProduct.id);
      matchedCount++;
      const inventoryItemId = shopifyProduct.variants[0].inventory_item_id;
      const currentInventory = inventoryLevels.get(inventoryItemId) ?? 0;
      const targetInventory = parseInt(apifyProduct.inventory, 10) || 0;
      if (currentInventory === targetInventory) { 
        alreadyInSyncCount++; 
        return; 
      }
      inventoryUpdates.push({ title: shopifyProduct.title, currentInventory, newInventory: targetInventory, inventoryItemId: inventoryItemId });
    });
    
    addLog(`Summary: Updates: ${inventoryUpdates.length}, In sync: ${alreadyInSyncCount}, Matched: ${matchedCount}, Skipped duplicates: ${skippedDuplicates}, Mismatches: ${mismatches.length}`, 'info');
    
    if (inventoryUpdates.length > MAX_UPDATES_PER_RUN) {
      addLog(`WARNING: Too many updates (${inventoryUpdates.length}). Limiting to first ${MAX_UPDATES_PER_RUN} for safety.`, 'warning');
      inventoryUpdates.length = MAX_UPDATES_PER_RUN;
    }
    
    if (!checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalApifyProducts: apifyData.length }, { type: 'inventory', data: inventoryUpdates })) return;
    
    const { updated, errors } = await executeInventoryUpdates(inventoryUpdates, token);
    lastRun.inventory = { updated, errors, at: new Date().toISOString() };
    
  } catch (error) { 
    addLog(`Inventory workflow failed: ${error.message}`, 'error'); 
    stats.errors++; 
    lastRun.inventory = { updated: 0, errors: (lastRun.inventory.errors || 0) + 1, at: new Date().toISOString() }; 
  }
}

// THIS IS THE UPDATED FUNCTION
async function handleDiscontinuedProductsJob(token) {
  addLog('Starting discontinued products check...', 'info');
  let discontinuedCount = 0, errors = 0;
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(), 
      getShopifyProducts({ onlyApifyTag: true, fields: 'id,handle,title,variants,tags,status' })
    ]);
    
    if (shouldAbort(token)) return;

    // --- NEW, MORE ACCURATE LOGIC ---

    // 1. Create a Set of all Shopify product IDs that are initially considered "unmatched".
    const unmatchedShopifyIds = new Set(shopifyData.map(p => p.id));
    // Also create a Map to easily look up the full product object by its ID later.
    const shopifyProductMap = new Map(shopifyData.map(p => [p.id, p]));

    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
    const shopifyMaps = buildShopifyMaps(shopifyData);

    // 2. Iterate through each Apify product. If a match is found in Shopify,
    //    remove it from our "unmatched" set. This prevents one Apify product
    //    from incorrectly "saving" multiple Shopify products.
    apifyProcessed.forEach(p => {
      const { product: matchedProduct } = matchShopifyProduct(p, shopifyMaps);
      if (matchedProduct) {
        unmatchedShopifyIds.delete(matchedProduct.id);
      }
    });

    // 3. Any product ID remaining in the set is a true "orphan" with no Apify counterpart.
    //    These are our candidates for discontinuation.
    const candidates = Array.from(unmatchedShopifyIds).map(id => shopifyProductMap.get(id));
    addLog(`Found ${candidates.length} Shopify products with no match in Apify source.`, 'info');
    
    // --- End of new logic ---

    const nowMissing = [];
    for (const p of candidates) {
      if (!p) continue; // Safety check
      const key = p.handle.toLowerCase();
      const count = (missingCounters.get(key) || 0) + 1;
      missingCounters.set(key, count);
      
      addLog(`Product "${p.title}" has been missing for ${count} run(s). Needs ${DISCONTINUE_MISS_RUNS} to be discontinued.`, 'info');

      if (count >= DISCONTINUE_MISS_RUNS) {
        nowMissing.push(p);
      }
    }
    
    // Reset the counter for any product that was missing but has now reappeared.
    const candidateHandles = new Set(candidates.map(p => p.handle.toLowerCase()));
    for (const p of shopifyData) {
        if (!candidateHandles.has(p.handle.toLowerCase())) {
            missingCounters.delete(p.handle.toLowerCase());
        }
    }
    
    if (nowMissing.length > 0) {
        addLog(`Found ${nowMissing.length} products to discontinue (missing for ≥ ${DISCONTINUE_MISS_RUNS} runs).`, 'warning');
    } else {
        addLog('No products met the criteria for discontinuation this run.', 'info');
    }
    
    if (!checkFailsafeConditions('discontinued', { toDiscontinue: nowMissing.length })) return;
    
    for (const product of nowMissing) {
      if (shouldAbort(token)) break;
      try {
        // Action 1: Set inventory to 0 if it has an inventory item ID
        if (product.variants?.[0]?.inventory_item_id) { 
          await shopifyClient.post('/inventory_levels/set.json', { 
            location_id: parseInt(config.shopify.locationId), 
            inventory_item_id: product.variants[0].inventory_item_id, 
            available: 0 
          }); 
        }
        // Action 2: Change status to "draft" if it's currently active
        if (product.status === 'active') { 
          await shopifyClient.put(`/products/${product.id}.json`, { 
            product: { id: product.id, status: 'draft' } 
          }); 
        }
        addLog(`✓ Discontinued: "${product.title}" (0 stock, status set to DRAFT).`, 'success');
        discontinuedCount++;
        stats.discontinued++;
        missingCounters.delete(product.handle.toLowerCase()); // Clean up counter after discontinuing
      } catch (e) { 
        errors++; 
        stats.errors++; 
        addLog(`✗ Error discontinuing "${product.title}": ${e.message}`, 'error'); 
      }
      await new Promise(r => setTimeout(r, 500));
    }
  } catch(e) { 
    addLog(`Discontinued job failed critically: ${e.message}`, 'error'); 
    errors++; 
  }
  lastRun.discontinued = { discontinued: discontinuedCount, errors, at: new Date().toISOString() };
}


async function createNewProductsJob(token) {
  addLog('Starting new product creation job...', 'info');
  let created = 0, errors = 0;
  try {
    const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts({ onlyApifyTag: true }) ]);
    if (shouldAbort(token)) return;
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: true });
    const shopifyMaps = buildShopifyMaps(shopifyData);
    const toCreate = [];
    for (const apifyProduct of apifyProcessed) {
      if (shouldAbort(token)) break;
      const { product: existingProduct } = matchShopifyProduct(apifyProduct, shopifyMaps);
      if (!existingProduct) {
        toCreate.push(apifyProduct);
      }
    }
    addLog(`Found ${toCreate.length} products to create`, 'info');
    const limitedToCreate = toCreate.slice(0, MAX_CREATE_PER_RUN);
    if (limitedToCreate.length > 0) {
      addLog(`Creating ${limitedToCreate.length} products (limited to ${MAX_CREATE_PER_RUN} per run)`, 'info');
    }
    for (const product of limitedToCreate) {
      if (shouldAbort(token)) break;
      try {
        const shopifyProduct = {
          title: product.title,
          handle: product.handle,
          vendor: 'Manchester Wholesale',
          tags: 'Supplier:Apify',
          status: 'active',
          variants: [{ price: product.price || '0.00', sku: product.sku || '', inventory_management: 'shopify', inventory_quantity: product.inventory }]
        };
        const response = await shopifyClient.post('/products.json', { product: shopifyProduct });
        if (response.data.product?.variants?.[0]?.inventory_item_id) {
          const invItemId = response.data.product.variants[0].inventory_item_id;
          await shopifyClient.post('/inventory_levels/connect.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: invItemId }).catch(connectError => { if (connectError.response?.status !== 422) { addLog(`Warning: Could not connect inventory location for "${product.title}"`, 'warning'); } });
          await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: invItemId, available: product.inventory });
        }
        created++;
        stats.newProducts++;
        addLog(`✓ Created: "${product.title}" with ${product.inventory} stock`, 'success');
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`✗ Failed to create "${product.title}": ${error.message}`, 'error');
      }
      await new Promise(r => setTimeout(r, 1000));
    }
  } catch (error) {
    addLog(`Product creation job failed: ${error.message}`, 'error');
    errors++;
  }
  lastRun.products = { created, errors, at: new Date().toISOString() };
  addLog(`Product creation complete. Created: ${created}, Errors: ${errors}`, 'success');
}

// --- UI AND API ---
app.get('/', (req, res) => {
  let failsafeBanner = '';
  if (failsafeTriggered === 'pending') {
    failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-yellow-900 border-2 border-yellow-500"><h3 class="font-bold text-yellow-300">⚠️ CONFIRMATION REQUIRED</h3><p class="text-sm text-yellow-400 mb-4">Reason: ${failsafeReason}</p><div class="flex gap-4"><button onclick="confirmFailsafe()" class="bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover">Proceed Anyway</button><button onclick="abortFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Abort & Pause</button></div></div>`;
  } else if (failsafeTriggered === true) {
    failsafeBanner = `<div class="mb-4 p-4 rounded-lg bg-red-900 border-2 border-red-500"><div class="flex items-center justify-between"><div><h3 class="font-bold text-red-300">🚨 FAILSAFE TRIGGERED</h3><p class="text-sm text-red-400">${failsafeReason}</p></div><button onclick="clearFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Clear Failsafe</button></div></div>`;
  }
  const html = `<!DOCTYPE html><html lang="en" class="dark"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Shopify Sync Dashboard</title><script src="https://cdn.tailwindcss.com"></script><style> body { background: linear-gradient(to bottom right, #1a1a2e, #16213e); } .card-hover { transition: all .3s ease-in-out; background: rgba(31,41,55,.2); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,.1); } .btn-hover:hover { transform: translateY(-1px); } .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #ff6e7f; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;} @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} } </style></head><body class="min-h-screen font-sans text-gray-200"><div class="container mx-auto px-4 py-8"><h1 class="text-4xl font-extrabold tracking-tight text-white mb-8">Shopify Sync Dashboard</h1>${failsafeBanner}<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-4"><div class="rounded-2xl p-6 card-hover"><h3>New Products (Total)</h3><p class="text-3xl font-bold" id="newProducts">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Inventory Updates (Total)</h3><p class="text-3xl font-bold" id="inventoryUpdates">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Discontinued (Total)</h3><p class="text-3xl font-bold" id="discontinued">0</p></div><div class="rounded-2xl p-6 card-hover"><h3>Errors (Total)</h3><p class="text-3xl font-bold" id="errors">0</p></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">System Controls</h2><div class="mb-6 p-4 rounded-lg" id="statusContainer"><div class="flex items-center justify-between"><div><h3 class="font-medium" id="systemStatus"></h3><p class="text-sm" id="systemStatusDesc"></p></div><div class="flex items-center"><button onclick="togglePause()" id="pauseButton" class="text-white px-4 py-2 rounded-lg btn-hover"></button><div id="pauseSpinner" class="spinner"></div></div></div></div><div class="flex flex-wrap gap-4"><button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Create New Products<div id="productsSpinner" class="spinner"></div></button><button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Update Inventory<div id="inventorySpinner" class="spinner"></div></button><button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover flex items-center">Check Discontinued<div id="discontinuedSpinner" class="spinner"></div></button></div></div><div class="rounded-2xl p-6 card-hover mb-8"><h2 class="text-2xl font-semibold text-white mb-4">Mismatch Report</h2><div class="overflow-x-auto"><table class="w-full text-sm text-left text-gray-400 mismatch-table"><thead><tr><th>Apify Title</th><th>Apify Handle</th><th>Apify SKU</th><th>Shopify Handle</th></tr></thead><tbody id="mismatchBody"></tbody></table></div></div><div class="rounded-2xl p-6 card-hover"><h2 class="text-2xl font-semibold text-white mb-4">Activity Log</h2><div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer"></div></div></div><script>let systemPaused = ${systemPaused}; let failsafeTriggered = ${JSON.stringify(failsafeTriggered)}; function togglePause() { fetch('/api/pause', { method: 'POST' }); } function clearFailsafe() { fetch('/api/failsafe/clear', { method: 'POST' }); } function confirmFailsafe() { fetch('/api/failsafe/confirm', { method: 'POST' }); } function abortFailsafe() { fetch('/api/failsafe/abort', { method: 'POST' }); } function triggerSync(type) { const spinner = document.getElementById(type + 'Spinner'); spinner.style.display = 'inline-block'; fetch('/api/sync/' + type, { method: 'POST' }).finally(() => spinner.style.display = 'none'); } function updateSystemStatus() { const statusEl = document.getElementById('systemStatus'); const descEl = document.getElementById('systemStatusDesc'); const btnEl = document.getElementById('pauseButton'); const containerEl = document.getElementById('statusContainer'); if (systemPaused) { statusEl.textContent = 'System Status: PAUSED'; statusEl.className='font-medium text-red-300'; descEl.textContent = 'Automatic syncing is disabled'; btnEl.textContent = 'Resume System'; btnEl.className='bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover'; containerEl.className='mb-6 p-4 rounded-lg bg-red-900 border-red-700 border'; } else { statusEl.textContent = 'System Status: ACTIVE'; statusEl.className='font-medium text-green-300'; descEl.textContent = 'Automatic syncing is active'; btnEl.textContent = 'Pause System'; btnEl.className='bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover'; containerEl.className='mb-6 p-4 rounded-lg bg-green-900 border-green-700 border'; } } setInterval(async () => { try { const res = await fetch('/api/status'); const data = await res.json(); document.getElementById('newProducts').textContent = data.stats.newProducts; document.getElementById('inventoryUpdates').textContent = data.stats.inventoryUpdates; document.getElementById('discontinued').textContent = data.stats.discontinued; document.getElementById('errors').textContent = data.stats.errors; document.getElementById('logContainer').innerHTML = data.logs.map(log => \`<div class="\${log.type === 'error' ? 'text-red-400' : log.type === 'warning' ? 'text-yellow-400' : 'text-gray-300'}">[\${new Date(log.timestamp).toLocaleTimeString()}] \${log.message}</div>\`).join(''); document.getElementById('mismatchBody').innerHTML = data.mismatches.slice(0, 20).map(m => \`<tr><td>\${m.apifyTitle || ''}</td><td>\${m.apifyHandle || ''}</td><td>\${m.apifySku || ''}</td><td>\${m.shopifyHandle || ''}</td></tr>\`).join(''); if (data.systemPaused !== systemPaused || data.failsafeTriggered !== failsafeTriggered) { location.reload(); } updateSystemStatus(); } catch (e) {} }, 5000); updateSystemStatus(); </script></body></html>`;
  res.send(html);
});

app.get('/api/status', (req, res) => { res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason, mismatches }); });
app.post('/api/pause', (req, res) => { systemPaused = !systemPaused; abortVersion++; addLog(`System manually ${systemPaused ? 'paused' : 'resumed'}.`, 'warning'); res.json({ success: true, paused: systemPaused }); });
app.post('/api/failsafe/clear', (req, res) => { failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; systemPaused = false; addLog('Failsafe cleared, system resumed.', 'info'); res.json({ success: true }); });
app.post('/api/failsafe/confirm', (req, res) => { if (failsafeTriggered !== 'pending' || !pendingFailsafeAction) { return res.status(400).json({success: false}); } addLog('Failsafe action confirmed. Executing...', 'info'); const action = pendingFailsafeAction; failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; systemPaused = false; if (action.type === 'inventory') { startBackgroundJob('inventory', 'Confirmed Inventory Sync', async (t) => executeInventoryUpdates(action.data, t)); } res.json({success: true}); });
app.post('/api/failsafe/abort', (req, res) => { if (failsafeTriggered !== 'pending') { return res.status(400).json({success: false}); } failsafeTriggered = true; pendingFailsafeAction = null; systemPaused = true; addLog('Pending action aborted. System remains paused.', 'warning'); res.json({success: true}); });
app.post('/api/sync/:type', (req, res) => { const { type } = req.params; const jobs = { inventory: updateInventoryJob, products: createNewProductsJob, discontinued: handleDiscontinuedProductsJob }; if (!jobs[type]) return res.status(400).json({success: false, message: 'Invalid sync type'}); startBackgroundJob(type, `Manual ${type} sync`, (t) => jobs[type](t)) ? res.json({success: true, message: "Sync started."}) : res.status(409).json({success: false, message: "Sync already running."}); });

// Scheduled jobs
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', (t) => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', (t) => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', (t) => handleDiscontinuedProductsJob(t)); });

// Graceful shutdown
const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  const requiredEnv = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'];
  const missing = requiredEnv.filter(key => !process.env[key]);
  if (missing.length > 0) { 
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); 
    process.exit(1); 
  }
});

process.on('SIGTERM', () => { addLog('SIGTERM received, shutting down...', 'warning'); server.close(() => { addLog('Server closed.', 'info'); process.exit(0); }); });
process.on('SIGINT', () => { addLog('SIGINT received, shutting down...', 'warning'); server.close(() => { addLog('Server closed.', 'info'); process.exit(0); }); });
