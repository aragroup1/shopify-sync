const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null, skusMigrated: 0 };
let lastRun = {
  inventory: { updated: 0, errors: 0, at: null },
  products: { created: 0, errors: 0, at: null, details: [] },
  discontinued: { discontinued: 0, errors: 0, at: null, details: [] },
  skuMigration: { migrated: 0, errors: 0, at: null, details: [] }
};
let recentlyCreated = [];
let recentlyDiscontinued = [];
let logs = [];
let systemPaused = false;
let mismatches = [];
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;
const missingCounters = new Map();

const jobLocks = { inventory: false, products: false, discontinued: false, skuMigration: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered === true || failsafeTriggered === 'pending' || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { 
  MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 100), 
  MIN_SHOPIFY_PRODUCTS: Number(process.env.MIN_SHOPIFY_PRODUCTS || 100), 
  MAX_CHANGE_PERCENTAGE: Number(process.env.MAX_CHANGE_PERCENTAGE || 30), 
  MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5),
  MAX_DISCONTINUED_PERCENTAGE: Number(process.env.MAX_DISCONTINUED_PERCENTAGE || 5),
  MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100),
  MAX_ERROR_RATE: Number(process.env.MAX_ERROR_RATE || 20), 
  MAX_DISCONTINUED_AT_ONCE: Number(process.env.MAX_DISCONTINUED_AT_ONCE || 100), 
  FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) 
};

// PRICING CONFIGURATION
const PRICING_CONFIG = {
  MARKUP_TYPE: process.env.MARKUP_TYPE || 'percentage',
  MARKUP_PERCENTAGE: Number(process.env.MARKUP_PERCENTAGE || 30),
  MARKUP_FIXED: Number(process.env.MARKUP_FIXED || 5),
  MINIMUM_PRICE: Number(process.env.MINIMUM_PRICE || 0.99),
  MARKUP_TIERS: [
    { max: 10, markup: 50 },
    { max: 25, markup: 40 },
    { max: 50, markup: 30 },
    { max: 100, markup: 25 },
    { max: Infinity, markup: 20 }
  ]
};

const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const MAX_UPDATES_PER_RUN = Number(process.env.MAX_UPDATES_PER_RUN || 100);
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const VENDOR_NAME = process.env.VENDOR_NAME || 'LandOfEssentials';

const config = { 
  apify: { 
    token: process.env.APIFY_TOKEN, 
    actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', 
    baseUrl: 'https://api.apify.com/v2'
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

function calculateSellingPrice(costPrice) {
  const cost = parseFloat(costPrice) || 0;
  let finalPrice = cost;
  
  switch(PRICING_CONFIG.MARKUP_TYPE) {
    case 'fixed':
      finalPrice = cost + PRICING_CONFIG.MARKUP_FIXED;
      break;
    case 'percentage':
      finalPrice = cost * (1 + PRICING_CONFIG.MARKUP_PERCENTAGE / 100);
      break;
    case 'tiered':
      const tier = PRICING_CONFIG.MARKUP_TIERS.find(t => cost <= t.max);
      const markup = tier ? tier.markup : 20;
      finalPrice = cost * (1 + markup / 100);
      break;
  }
  
  finalPrice = Math.max(finalPrice, PRICING_CONFIG.MINIMUM_PRICE);
  return finalPrice.toFixed(2);
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
    notifyTelegram(`⚠️ <b>Failsafe Warning - Confirmation Required</b> ⚠️\n\n<b>Reason:</b>\n<pre>${msg}</pre>`);
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
    isConfirmable = true;
    if (data.toDiscontinue > FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE) {
      checks.push(`Too many products to discontinue: ${data.toDiscontinue} > ${FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE}`);
    }
    if (data.totalShopifyProducts > 0 && data.toDiscontinue > data.totalShopifyProducts * (FAILSAFE_LIMITS.MAX_DISCONTINUED_PERCENTAGE / 100)) {
      checks.push(`Discontinue percentage too high: ${data.toDiscontinue} > ${Math.floor(data.totalShopifyProducts * (FAILSAFE_LIMITS.MAX_DISCONTINUED_PERCENTAGE / 100))} (${FAILSAFE_LIMITS.MAX_DISCONTINUED_PERCENTAGE}% of total)`);
    }
  }
  
  if (context === 'products') {
    isConfirmable = true;
    if (data.toCreate > FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE) {
      checks.push(`Too many products to create: ${data.toCreate} > ${FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE}`);
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

function normalizeTitle(text = '') { 
  return String(text).toLowerCase()
    .replace(/\b\d{4}\b/g, '')
    .replace(/KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim(); 
}

function processApifyProducts(apifyData, options = { processPrice: true, fullData: false }) {
  return apifyData.map((item, index) => {
    const handle = normalizeHandle(item.handle || item.title, index);
    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20;
    const rawStatus = item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '';
    if (String(rawStatus).toLowerCase().includes('out')) inventory = 0;
    
    let costPrice = '0.00';
    if (item.variants?.[0]?.price?.value) {
      costPrice = String(item.variants[0].price.value);
    }
    
    // IMPORTANT: Extract SKU from Apify data
    const sku = item.sku || item.variants?.[0]?.sku || `APF-${handle}`;
    
    const result = { 
      handle, 
      title: item.title, 
      inventory, 
      sku, // Always include SKU
      costPrice,
      sellingPrice: calculateSellingPrice(costPrice)
    };
    
    if (options.fullData) {
      result.description = item.description || item.variants?.[0]?.description || '';
      result.images = item.images || [];
      result.productType = item.productType || '';
      result.tags = item.tags || [];
      result.vendor = VENDOR_NAME;
    }
    
    return result;
  }).filter(Boolean);
}

// SIMPLIFIED: SKU-based matching only
function matchProductBySku(apifyProduct, shopifySkuMap) {
  if (!apifyProduct.sku) return null;
  return shopifySkuMap.get(apifyProduct.sku.toLowerCase()) || null;
}

// NEW: SKU Migration Job - One-time migration to sync all SKUs
async function migrateSkusJob(token) {
  addLog('🔄 Starting SKU Migration - This will update all Shopify products with Apify SKUs...', 'info');
  let migrated = 0, skipped = 0, errors = 0;
  const migrationDetails = [];
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(),
      getShopifyProducts({ onlyApifyTag: true })
    ]);
    
    if (shouldAbort(token)) return;
    
    const apifyProcessed = processApifyProducts(apifyData);
    
    // Build maps for flexible matching during migration
    const apifyByTitle = new Map();
    const apifyByHandle = new Map();
    apifyProcessed.forEach(p => {
      apifyByTitle.set(normalizeTitle(p.title), p);
      // Try multiple handle variations
      apifyByHandle.set(p.handle.toLowerCase(), p);
      const strippedHandle = p.handle.toLowerCase()
        .replace(/-(parcel|large-letter|letter)-rate$/i, '')
        .replace(/-p\d+$/i, '');
      apifyByHandle.set(strippedHandle, p);
    });
    
    addLog(`Processing ${shopifyData.length} Shopify products for SKU migration...`, 'info');
    
    for (const shopifyProduct of shopifyData) {
      if (shouldAbort(token)) break;
      
      // Skip if already has a valid Apify-style SKU
      const currentSku = shopifyProduct.variants?.[0]?.sku;
      if (currentSku && apifyProcessed.find(a => a.sku === currentSku)) {
        skipped++;
        continue;
      }
      
      // Find matching Apify product
      let apifyMatch = null;
      
      // Try title match first (most reliable for initial migration)
      const normalizedShopifyTitle = normalizeTitle(shopifyProduct.title);
      apifyMatch = apifyByTitle.get(normalizedShopifyTitle);
      
      // Try handle match if title didn't work
      if (!apifyMatch) {
        const shopifyHandle = shopifyProduct.handle.toLowerCase();
        apifyMatch = apifyByHandle.get(shopifyHandle);
        
        if (!apifyMatch) {
          // Try stripped handle
          const strippedShopifyHandle = shopifyHandle
            .replace(/-(parcel|large-letter|letter)-rate$/i, '')
            .replace(/-p\d+$/i, '');
          apifyMatch = apifyByHandle.get(strippedShopifyHandle);
        }
      }
      
      if (apifyMatch && apifyMatch.sku) {
        try {
          // Update the SKU in Shopify
          const variantId = shopifyProduct.variants[0].id;
          await shopifyClient.put(`/variants/${variantId}.json`, {
            variant: {
              id: variantId,
              sku: apifyMatch.sku
            }
          });
          
          migrated++;
          stats.skusMigrated++;
          migrationDetails.push({
            title: shopifyProduct.title,
            oldSku: currentSku || 'none',
            newSku: apifyMatch.sku
          });
          
          addLog(`✓ Migrated SKU for "${shopifyProduct.title}": ${currentSku || 'none'} → ${apifyMatch.sku}`, 'success');
          
          await new Promise(r => setTimeout(r, 500)); // Rate limiting
        } catch (error) {
          errors++;
          addLog(`✗ Failed to update SKU for "${shopifyProduct.title}": ${error.message}`, 'error');
        }
      } else {
        addLog(`⚠️ No Apify match found for "${shopifyProduct.title}" - skipping`, 'warning');
        skipped++;
      }
    }
    
    addLog(`✅ SKU Migration Complete! Migrated: ${migrated}, Skipped: ${skipped}, Errors: ${errors}`, 'success');
    
    if (migrated > 0) {
      notifyTelegram(`✅ <b>SKU Migration Complete</b>\n\nMigrated: ${migrated}\nSkipped: ${skipped}\nErrors: ${errors}\n\nYour products are now properly mapped by SKU!`);
    }
    
  } catch (error) {
    addLog(`SKU Migration failed: ${error.message}`, 'error');
    errors++;
  }
  
  lastRun.skuMigration = { 
    migrated, 
    errors, 
    at: new Date().toISOString(), 
    details: migrationDetails 
  };
}

// UPDATED: All jobs now use SKU-based matching
async function updateInventoryJob(token) {
  addLog('Starting inventory sync (SKU-based)...', 'info');
  mismatches = [];
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(), 
      getShopifyProducts({ onlyApifyTag: true })
    ]);
    
    if (shouldAbort(token) || apifyData.length === 0) return addLog('Aborting sync.', 'warning');
    
    // Build SKU map for Shopify products
    const shopifySkuMap = new Map();
    shopifyData.forEach(product => {
      const sku = product.variants?.[0]?.sku;
      if (sku) shopifySkuMap.set(sku.toLowerCase(), product);
    });
    
    const inventoryItemIds = shopifyData.map(p => p.variants?.[0]?.inventory_item_id).filter(Boolean);
    const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds, config.shopify.locationId);
    const processedProducts = processApifyProducts(apifyData);
    
    const inventoryUpdates = [];
    let alreadyInSyncCount = 0;
    let noSkuCount = 0;
    
    processedProducts.forEach((apifyProduct) => {
      if (!apifyProduct.sku) {
        noSkuCount++;
        return;
      }
      
      const shopifyProduct = matchProductBySku(apifyProduct, shopifySkuMap);
      
      if (!shopifyProduct || !shopifyProduct.variants?.[0]?.inventory_item_id) {
        mismatches.push({ 
          apifyTitle: apifyProduct.title, 
          apifySku: apifyProduct.sku,
          reason: 'SKU not found in Shopify'
        });
        return;
      }
      
      const inventoryItemId = shopifyProduct.variants[0].inventory_item_id;
      const currentInventory = inventoryLevels.get(inventoryItemId) ?? 0;
      const targetInventory = parseInt(apifyProduct.inventory, 10) || 0;
      
      if (currentInventory === targetInventory) { 
        alreadyInSyncCount++; 
        return; 
      }
      
      inventoryUpdates.push({ 
        title: shopifyProduct.title, 
        sku: apifyProduct.sku,
        currentInventory, 
        newInventory: targetInventory, 
        inventoryItemId 
      });
    });
    
    addLog(`Summary: Updates: ${inventoryUpdates.length}, In sync: ${alreadyInSyncCount}, No SKU: ${noSkuCount}, Mismatches: ${mismatches.length}`, 'info');
    
    if (inventoryUpdates.length > MAX_UPDATES_PER_RUN) {
      addLog(`Limiting to first ${MAX_UPDATES_PER_RUN} updates.`, 'warning');
      inventoryUpdates.length = MAX_UPDATES_PER_RUN;
    }
    
    if (!checkFailsafeConditions('inventory', { 
      updatesNeeded: inventoryUpdates.length, 
      totalApifyProducts: apifyData.length 
    }, { type: 'inventory', data: inventoryUpdates })) return;
    
    const { updated, errors } = await executeInventoryUpdates(inventoryUpdates, token);
    lastRun.inventory = { updated, errors, at: new Date().toISOString() };
    
  } catch (error) { 
    addLog(`Inventory workflow failed: ${error.message}`, 'error'); 
    stats.errors++; 
  }
}

async function handleDiscontinuedProductsJob(token) {
  addLog('Starting discontinued products check (SKU-based)...', 'info');
  let discontinuedCount = 0, errors = 0;
  const discontinuedList = [];
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(), 
      getShopifyProducts({ onlyApifyTag: true, fields: 'id,handle,title,variants,tags,status' })
    ]);
    
    if (shouldAbort(token)) return;
    
    // Build set of all Apify SKUs
    const apifySkus = new Set();
    const apifyProcessed = processApifyProducts(apifyData);
    apifyProcessed.forEach(p => {
      if (p.sku) apifySkus.add(p.sku.toLowerCase());
    });
    
    // Find Shopify products whose SKUs don't exist in Apify
    const candidates = [];
    for (const shopifyProduct of shopifyData) {
      const sku = shopifyProduct.variants?.[0]?.sku;
      if (!sku) {
        addLog(`⚠️ Product "${shopifyProduct.title}" has no SKU - cannot check if discontinued`, 'warning');
        continue;
      }
      
      if (!apifySkus.has(sku.toLowerCase())) {
        candidates.push(shopifyProduct);
      }
    }
    
    addLog(`Found ${candidates.length} Shopify products with SKUs not in Apify source.`, 'info');
    
    const nowMissing = [];
    for (const p of candidates) {
      const key = p.variants[0].sku.toLowerCase();
      const count = (missingCounters.get(key) || 0) + 1;
      missingCounters.set(key, count);
      
      if (count >= DISCONTINUE_MISS_RUNS) {
        nowMissing.push(p);
      } else {
        addLog(`Product "${p.title}" (SKU: ${p.variants[0].sku}) missing for ${count}/${DISCONTINUE_MISS_RUNS} runs`, 'info');
      }
    }
    
    // Clean up counters for products that were found
    for (const p of shopifyData) {
      const sku = p.variants?.[0]?.sku;
      if (sku && apifySkus.has(sku.toLowerCase())) {
        missingCounters.delete(sku.toLowerCase());
      }
    }
    
    addLog(`${nowMissing.length} products will be discontinued.`, nowMissing.length > 0 ? 'warning' : 'info');
    
    if (!checkFailsafeConditions('discontinued', { 
      toDiscontinue: nowMissing.length, 
      totalShopifyProducts: shopifyData.length 
    })) return;
    
    for (const product of nowMissing) {
      if (shouldAbort(token)) break;
      try {
        if (product.variants?.[0]?.inventory_item_id) { 
          await shopifyClient.post('/inventory_levels/set.json', { 
            location_id: parseInt(config.shopify.locationId), 
            inventory_item_id: product.variants[0].inventory_item_id, 
            available: 0 
          }); 
        }
        if (product.status === 'active') { 
          await shopifyClient.put(`/products/${product.id}.json`, { 
            product: { id: product.id, status: 'draft' } 
          }); 
        }
        
        discontinuedList.push({ 
          title: product.title, 
          sku: product.variants[0].sku,
          at: new Date().toISOString() 
        });
        recentlyDiscontinued.unshift({ 
          title: product.title, 
          sku: product.variants[0].sku,
          at: new Date().toISOString() 
        });
        if (recentlyDiscontinued.length > 50) recentlyDiscontinued.length = 50;
        
        addLog(`✓ Discontinued: "${product.title}" (SKU: ${product.variants[0].sku})`, 'success');
        discontinuedCount++;
        stats.discontinued++;
        missingCounters.delete(product.variants[0].sku.toLowerCase());
      } catch (e) { 
        errors++; 
        stats.errors++; 
        addLog(`✗ Error discontinuing "${product.title}": ${e.message}`, 'error'); 
      }
      await new Promise(r => setTimeout(r, 500));
    }
  } catch(e) { 
    addLog(`Discontinued job failed: ${e.message}`, 'error'); 
    errors++; 
  }
  lastRun.discontinued = { discontinued: discontinuedCount, errors, at: new Date().toISOString(), details: discontinuedList };
}

async function createNewProductsJob(token) {
  addLog('Starting new product creation job (SKU-based)...', 'info');
  let created = 0, errors = 0;
  const createdList = [];
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(), 
      getShopifyProducts({ onlyApifyTag: true })
    ]);
    if (shouldAbort(token)) return;
    
    // Build set of existing Shopify SKUs
    const existingSkus = new Set();
    shopifyData.forEach(p => {
      const sku = p.variants?.[0]?.sku;
      if (sku) existingSkus.add(sku.toLowerCase());
    });
    
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: true, fullData: true });
    const toCreate = [];
    
    // Find Apify products whose SKUs don't exist in Shopify
    for (const apifyProduct of apifyProcessed) {
      if (!apifyProduct.sku) {
        addLog(`⚠️ Apify product "${apifyProduct.title}" has no SKU - skipping`, 'warning');
        continue;
      }
      
      if (!existingSkus.has(apifyProduct.sku.toLowerCase())) {
        toCreate.push(apifyProduct);
      }
    }
    
    addLog(`Found ${toCreate.length} new products to create`, 'info');
    
    if (!checkFailsafeConditions('products', { toCreate: toCreate.length }, { type: 'products', data: toCreate })) return;
    
    const limitedToCreate = toCreate.slice(0, MAX_CREATE_PER_RUN);
    
    for (const product of limitedToCreate) {
      if (shouldAbort(token)) break;
      try {
        const shopifyProduct = {
          title: product.title,
          handle: product.handle,
          vendor: product.vendor,
          body_html: product.description,
          product_type: product.productType,
          tags: ['Supplier:Apify', ...(product.tags || [])].join(','),
          status: 'active',
          variants: [{
            price: product.sellingPrice,
            compare_at_price: product.costPrice,
            sku: product.sku, // IMPORTANT: Set the SKU
            inventory_management: 'shopify',
            inventory_quantity: product.inventory
          }]
        };
        
        if (product.images && product.images.length > 0) {
          shopifyProduct.images = product.images.map(url => ({ src: url }));
        }
        
        const response = await shopifyClient.post('/products.json', { product: shopifyProduct });
        
        if (response.data.product?.variants?.[0]?.inventory_item_id) {
          const invItemId = response.data.product.variants[0].inventory_item_id;
          await shopifyClient.post('/inventory_levels/connect.json', { 
            location_id: parseInt(config.shopify.locationId), 
            inventory_item_id: invItemId 
          }).catch(() => {});
          await shopifyClient.post('/inventory_levels/set.json', { 
            location_id: parseInt(config.shopify.locationId), 
            inventory_item_id: invItemId, 
            available: product.inventory 
          });
        }
        
        createdList.push({ 
          title: product.title, 
          sku: product.sku,
          price: product.sellingPrice,
          at: new Date().toISOString() 
        });
        recentlyCreated.unshift({ 
          title: product.title, 
          sku: product.sku,
          price: product.sellingPrice,
          at: new Date().toISOString() 
        });
        if (recentlyCreated.length > 50) recentlyCreated.length = 50;
        
        created++;
        stats.newProducts++;
        addLog(`✓ Created: "${product.title}" (SKU: ${product.sku}) at £${product.sellingPrice}`, 'success');
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
  lastRun.products = { created, errors, at: new Date().toISOString(), details: createdList };
  addLog(`Product creation complete. Created: ${created}, Errors: ${errors}`, 'success');
}

// --- ENHANCED UI with SKU Migration Button ---
app.get('/', (req, res) => {
  const formatLastRun = (run) => {
    if (!run.at) return 'Never';
    return new Date(run.at).toLocaleString();
  };
  
  const html = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Shopify Sync Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background: linear-gradient(to bottom right, #1a1a2e, #16213e); }
    .card-hover { transition: all .3s ease-in-out; background: rgba(31,41,55,.2); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,.1); }
    .btn-hover:hover { transform: translateY(-1px); }
    .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #ff6e7f; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;}
    @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} }
  </style>
</head>
<body class="min-h-screen font-sans text-gray-200">
  <div class="container mx-auto px-4 py-8">
    <h1 class="text-4xl font-extrabold tracking-tight text-white mb-8">Shopify Sync Dashboard</h1>
    
    <!-- SKU Migration Alert -->
    <div class="mb-6 p-4 rounded-lg bg-blue-900 border-2 border-blue-500">
      <h3 class="font-bold text-blue-300 mb-2">🔄 SKU-Based Matching System</h3>
      <p class="text-sm text-blue-200 mb-3">This system now uses SKUs as the primary identifier for all operations. Run the SKU migration to sync your existing products.</p>
      <div class="flex items-center gap-4">
        <button onclick="migrateSKUs()" class="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-lg btn-hover flex items-center">
          Migrate SKUs
          <div id="skuSpinner" class="spinner"></div>
        </button>
        <span class="text-sm text-blue-300">SKUs Migrated: ${stats.skusMigrated || 0}</span>
        ${lastRun.skuMigration?.at ? `<span class="text-sm text-gray-400">Last run: ${formatLastRun(lastRun.skuMigration)}</span>` : ''}
      </div>
    </div>
    
    <!-- Total Stats -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-4">
      <div class="rounded-2xl p-6 card-hover">
        <h3>New Products (Total)</h3>
        <p class="text-3xl font-bold">${stats.newProducts}</p>
      </div>
      <div class="rounded-2xl p-6 card-hover">
        <h3>Inventory Updates (Total)</h3>
        <p class="text-3xl font-bold">${stats.inventoryUpdates}</p>
      </div>
      <div class="rounded-2xl p-6 card-hover">
        <h3>Discontinued (Total)</h3>
        <p class="text-3xl font-bold">${stats.discontinued}</p>
      </div>
      <div class="rounded-2xl p-6 card-hover">
        <h3>Errors (Total)</h3>
        <p class="text-3xl font-bold">${stats.errors}</p>
      </div>
    </div>
    
    <!-- Previous Run Stats -->
    <div class="rounded-2xl p-6 card-hover mb-8">
      <h2 class="text-2xl font-semibold text-white mb-4">Previous Run Statistics</h2>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div class="bg-gray-800 rounded-lg p-4">
          <h3 class="text-lg font-medium text-green-400 mb-2">Last Inventory Sync</h3>
          <p class="text-sm text-gray-400">${formatLastRun(lastRun.inventory)}</p>
          <div class="mt-2">
            <span class="text-green-300">✓ Updated: ${lastRun.inventory.updated || 0}</span><br>
            <span class="text-red-300">✗ Errors: ${lastRun.inventory.errors || 0}</span>
          </div>
        </div>
        <div class="bg-gray-800 rounded-lg p-4">
          <h3 class="text-lg font-medium text-blue-400 mb-2">Last Product Creation</h3>
          <p class="text-sm text-gray-400">${formatLastRun(lastRun.products)}</p>
          <div class="mt-2">
            <span class="text-green-300">✓ Created: ${lastRun.products.created || 0}</span><br>
            <span class="text-red-300">✗ Errors: ${lastRun.products.errors || 0}</span>
          </div>
        </div>
        <div class="bg-gray-800 rounded-lg p-4">
          <h3 class="text-lg font-medium text-orange-400 mb-2">Last Discontinued Check</h3>
          <p class="text-sm text-gray-400">${formatLastRun(lastRun.discontinued)}</p>
          <div class="mt-2">
            <span class="text-green-300">✓ Discontinued: ${lastRun.discontinued.discontinued || 0}</span><br>
            <span class="text-red-300">✗ Errors: ${lastRun.discontinued.errors || 0}</span>
          </div>
        </div>
      </div>
    </div>
    
    <!-- Recently Created/Discontinued -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
      <div class="rounded-2xl p-6 card-hover">
        <h2 class="text-2xl font-semibold text-white mb-4">Recently Created</h2>
        <div class="overflow-x-auto max-h-48">
          <table class="w-full text-sm">
            <thead class="text-gray-400">
              <tr><th class="text-left">Title</th><th class="text-left">SKU</th><th class="text-left">Price</th></tr>
            </thead>
            <tbody id="recentlyCreatedBody" class="text-gray-300"></tbody>
          </table>
        </div>
      </div>
      <div class="rounded-2xl p-6 card-hover">
        <h2 class="text-2xl font-semibold text-white mb-4">Recently Discontinued</h2>
        <div class="overflow-x-auto max-h-48">
          <table class="w-full text-sm">
            <thead class="text-gray-400">
              <tr><th class="text-left">Title</th><th class="text-left">SKU</th></tr>
            </thead>
            <tbody id="recentlyDiscontinuedBody" class="text-gray-300"></tbody>
          </table>
        </div>
      </div>
    </div>
    
    <!-- System Controls -->
    <div class="rounded-2xl p-6 card-hover mb-8">
      <h2 class="text-2xl font-semibold text-white mb-4">System Controls</h2>
      <div class="mb-6 p-4 rounded-lg" id="statusContainer">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="font-medium" id="systemStatus"></h3>
            <p class="text-sm" id="systemStatusDesc"></p>
          </div>
          <button onclick="togglePause()" id="pauseButton" class="text-white px-4 py-2 rounded-lg btn-hover"></button>
        </div>
      </div>
      <div class="flex flex-wrap gap-4">
        <button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover">Create New Products</button>
        <button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover">Update Inventory</button>
        <button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover">Check Discontinued</button>
      </div>
    </div>
    
    <!-- Activity Log -->
    <div class="rounded-2xl p-6 card-hover">
      <h2 class="text-2xl font-semibold text-white mb-4">Activity Log</h2>
      <div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer"></div>
    </div>
  </div>
  
  <script>
    let systemPaused = ${systemPaused};
    
    function togglePause() { fetch('/api/pause', { method: 'POST' }); }
    function triggerSync(type) { fetch('/api/sync/' + type, { method: 'POST' }); }
    
    function migrateSKUs() {
      if (!confirm('This will update all Shopify product SKUs to match Apify. Continue?')) return;
      const spinner = document.getElementById('skuSpinner');
      spinner.style.display = 'inline-block';
      fetch('/api/migrate-skus', { method: 'POST' }).finally(() => spinner.style.display = 'none');
    }
    
    function updateSystemStatus() {
      const statusEl = document.getElementById('systemStatus');
      const descEl = document.getElementById('systemStatusDesc');
      const btnEl = document.getElementById('pauseButton');
      const containerEl = document.getElementById('statusContainer');
      
      if (systemPaused) {
        statusEl.textContent = 'System Status: PAUSED';
        statusEl.className = 'font-medium text-red-300';
        descEl.textContent = 'Automatic syncing is disabled';
        btnEl.textContent = 'Resume System';
        btnEl.className = 'bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover';
        containerEl.className = 'mb-6 p-4 rounded-lg bg-red-900 border-red-700 border';
      } else {
        statusEl.textContent = 'System Status: ACTIVE';
        statusEl.className = 'font-medium text-green-300';
        descEl.textContent = 'Automatic syncing is active';
        btnEl.textContent = 'Pause System';
        btnEl.className = 'bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover';
        containerEl.className = 'mb-6 p-4 rounded-lg bg-green-900 border-green-700 border';
      }
    }
    
    setInterval(async () => {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        
        // Update recently created
        document.getElementById('recentlyCreatedBody').innerHTML = (data.recentlyCreated || []).slice(0, 5).map(p => 
          \`<tr><td class="truncate max-w-xs">\${p.title}</td><td>\${p.sku}</td><td>£\${p.price}</td></tr>\`
        ).join('');
        
        // Update recently discontinued
        document.getElementById('recentlyDiscontinuedBody').innerHTML = (data.recentlyDiscontinued || []).slice(0, 5).map(p => 
          \`<tr><td class="truncate max-w-xs">\${p.title}</td><td>\${p.sku}</td></tr>\`
        ).join('');
        
        // Update logs
        document.getElementById('logContainer').innerHTML = data.logs.map(log => 
          \`<div class="\${log.type === 'error' ? 'text-red-400' : log.type === 'warning' ? 'text-yellow-400' : log.type === 'success' ? 'text-green-400' : 'text-gray-300'}">
            [\${new Date(log.timestamp).toLocaleTimeString()}] \${log.message}
          </div>\`
        ).join('');
        
        if (data.systemPaused !== systemPaused) {
          systemPaused = data.systemPaused;
          updateSystemStatus();
        }
      } catch (e) {}
    }, 3000);
    
    updateSystemStatus();
  </script>
</body>
</html>`;
  
  res.send(html);
});

app.get('/api/status', (req, res) => { 
  res.json({ 
    stats, 
    lastRun, 
    logs, 
    systemPaused, 
    failsafeTriggered, 
    failsafeReason, 
    mismatches,
    recentlyCreated,
    recentlyDiscontinued
  }); 
});

app.post('/api/pause', (req, res) => { 
  systemPaused = !systemPaused; 
  abortVersion++; 
  addLog(`System manually ${systemPaused ? 'paused' : 'resumed'}.`, 'warning'); 
  res.json({ success: true, paused: systemPaused }); 
});

app.post('/api/migrate-skus', (req, res) => { 
  startBackgroundJob('skuMigration', 'SKU Migration', (t) => migrateSkusJob(t)) 
    ? res.json({ success: true, message: "SKU migration started." }) 
    : res.status(409).json({ success: false, message: "Migration already running." }); 
});

app.post('/api/sync/:type', (req, res) => { 
  const { type } = req.params; 
  const jobs = { inventory: updateInventoryJob, products: createNewProductsJob, discontinued: handleDiscontinuedProductsJob }; 
  if (!jobs[type]) return res.status(400).json({success: false}); 
  startBackgroundJob(type, `Manual ${type} sync`, (t) => jobs[type](t)) 
    ? res.json({success: true}) 
    : res.status(409).json({success: false}); 
});

// Scheduled jobs
cron.schedule('0 1 * * *', () => { 
  if (!systemPaused && !failsafeTriggered) 
    startBackgroundJob('inventory', 'Scheduled inventory sync', (t) => updateInventoryJob(t)); 
});
cron.schedule('0 2 * * 5', () => { 
  if (!systemPaused && !failsafeTriggered) 
    startBackgroundJob('products', 'Scheduled Product Sync', (t) => createNewProductsJob(t)); 
});
cron.schedule('0 3 * * *', () => { 
  if (!systemPaused && !failsafeTriggered) 
    startBackgroundJob('discontinued', 'Scheduled Discontinued Check', (t) => handleDiscontinuedProductsJob(t)); 
});

const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]);
  if (missing.length > 0) { 
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); 
    process.exit(1); 
  }
  addLog(`System now uses SKU-based matching. Run "Migrate SKUs" to sync existing products.`, 'info');
  addLog(`Pricing: ${PRICING_CONFIG.MARKUP_TYPE} markup, Vendor: ${VENDOR_NAME}`, 'info');
});

process.on('SIGTERM', () => { server.close(() => process.exit(0)); });
process.on('SIGINT', () => { server.close(() => process.exit(0)); });
