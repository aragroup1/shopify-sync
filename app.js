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
let failsafeTriggered = false;
let failsafeReason = '';
const missingCounters = new Map();

// Failsafe pending state for waiting on user decision
let failsafePending = null;
let failsafeDecisionMade = false;

// Job locks to avoid double-running
const jobLocks = { inventory: false, products: false, discontinued: false, fixTracking: false };

// Global abort version
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered || token !== abortVersion;

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

// Telegram
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
let lastFailsafeNotified = '';

// Debug tracking for inventory updates
const inventoryDebugMap = new Map();

async function notifyTelegram(text, options = {}) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const payload = { 
      chat_id: TELEGRAM_CHAT_ID, 
      text, 
      parse_mode: 'HTML'
    };
    
    if (options.reply_markup) {
      payload.reply_markup = options.reply_markup;
    }
    
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      payload,
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
  console.log(`[${log.timestamp}] ${message}`);

  if (type === 'error') {
    errorLog.push({ timestamp: new Date(), message, jobType });
    if (errorLog.length > 500) errorLog = errorLog.slice(-500);
  }
}

function addToHistory(type, data) {
  runHistory.unshift({
    type,
    timestamp: new Date().toISOString(),
    ...data
  });
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
    } catch (e) {
      // errors are already logged inside
    } finally {
      jobLocks[key] = false;
      addLog(`${name} job finished`, 'info', key);
    }
  });
  return true;
}

async function waitForFailsafeDecision(timeoutMs = 300000) {
  const startTime = Date.now();
  while (failsafePending && !failsafeDecisionMade) {
    if (Date.now() - startTime > timeoutMs) {
      addLog('Failsafe decision timeout - cancelling operation', 'error', 'failsafe');
      failsafePending = null;
      failsafeDecisionMade = false;
      return false;
    }
    await new Promise(r => setTimeout(r, 1000));
  }
  const decision = failsafeDecisionMade;
  failsafeDecisionMade = false;
  return decision;
}

async function triggerFailsafe(msg, contextData = {}, allowOverride = true) {
  if (failsafeTriggered) return false;
  
  addLog(`⚠️ FAILSAFE WARNING: ${msg}`, 'error', 'failsafe');
  
  if (!allowOverride) {
    failsafeTriggered = true;
    failsafeReason = msg;
    systemPaused = true;
    abortVersion++;
    addLog('System automatically paused - critical failsafe (no override)', 'error', 'failsafe');
    
    const debugInfo = `
🚨 <b>CRITICAL Failsafe - System Halted</b> 🚨

<b>Reason:</b>
${msg}

<b>Action Required:</b>
System has been stopped. Manual intervention required.

<b>Context:</b>
${JSON.stringify(contextData, null, 2).substring(0, 1000)}
    `;
    await notifyTelegram(debugInfo);
    return false;
  }
  
  systemPaused = true;
  failsafePending = {
    reason: msg,
    context: contextData.checkContext,
    data: contextData.checkData,
    timestamp: new Date()
  };
  
  addLog('System paused - waiting for failsafe decision', 'warning', 'failsafe');
  
  const debugInfo = `
⚠️ <b>Failsafe Warning - Decision Required</b> ⚠️

<b>Reason:</b>
${msg}

<b>Context:</b>
${JSON.stringify(contextData, null, 2).substring(0, 1000)}

<b>Stats:</b>
- New Products: ${stats.newProducts}
- Inventory Updates: ${stats.inventoryUpdates}
- Discontinued: ${stats.discontinued}
- Errors: ${stats.errors}

Reply with /proceed to continue anyway or /cancel to abort.
  `;
  
  await notifyTelegram(debugInfo, {
    reply_markup: {
      inline_keyboard: [
        [
          { text: '✅ Proceed Anyway', callback_data: 'failsafe_proceed' },
          { text: '❌ Cancel Operation', callback_data: 'failsafe_cancel' }
        ]
      ]
    }
  });
  
  addLog('Waiting for failsafe decision...', 'info', 'failsafe');
  const shouldProceed = await waitForFailsafeDecision();
  
  if (shouldProceed) {
    addLog('Failsafe override - proceeding with operation', 'warning', 'failsafe');
    systemPaused = false;
    failsafePending = null;
    await notifyTelegram('✅ Failsafe overridden - operation continuing');
    return true;
  } else {
    addLog('Failsafe decision: Operation cancelled', 'error', 'failsafe');
    failsafeTriggered = true;
    failsafeReason = msg;
    failsafePending = null;
    await notifyTelegram('❌ Operation cancelled due to failsafe');
    return false;
  }
}

async function checkFailsafeConditions(context, data = {}) {
  const checks = [];
  switch (context) {
    case 'fetch': {
      if (typeof data.apifyCount === 'number' && data.apifyCount < FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS) {
        checks.push(`Apify products too low: ${data.apifyCount} < ${FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS}`);
      }
      if (typeof data.shopifyCount === 'number' && data.shopifyCount < FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS) {
        checks.push(`Shopify products too low: ${data.shopifyCount} < ${FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS}`);
      }
      if (lastKnownGoodState.apifyCount > 0 && typeof data.apifyCount === 'number') {
        const changePercent = Math.abs((data.apifyCount - lastKnownGoodState.apifyCount) / lastKnownGoodState.apifyCount * 100);
        if (changePercent > FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE) {
          checks.push(`Apify product count changed by ${changePercent.toFixed(1)}% (was ${lastKnownGoodState.apifyCount}, now ${data.apifyCount})`);
        }
      }
      break;
    }
    case 'inventory': {
      if (data.totalApifyProducts > 0 && data.updatesNeeded > data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100)) {
        checks.push(`Too many inventory changes: ${data.updatesNeeded} > ${Math.floor(data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100))} (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}% of ${data.totalApifyProducts})`);
      }
      if (typeof data.errorRate === 'number' && data.errorRate > FAILSAFE_LIMITS.MAX_ERROR_RATE) {
        checks.push(`Error rate too high: ${data.errorRate.toFixed(1)}% > ${FAILSAFE_LIMITS.MAX_ERROR_RATE}%`);
      }
      break;
    }
    case 'discontinued': {
      if (data.toDiscontinue > FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE) {
        checks.push(`Too many products to discontinue: ${data.toDiscontinue} > ${FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE}`);
      }
      break;
    }
    case 'products': {
      if (data.existingCount > 0 && data.toCreate > data.existingCount * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100)) {
        checks.push(`Too many new products: ${data.toCreate} > ${Math.floor(data.existingCount * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100))}`);
      }
      break;
    }
  }
  
  if (checks.length > 0) {
    const reason = checks.join('; ');
    const shouldProceed = await triggerFailsafe(reason, { checkContext: context, checkData: data });
    return shouldProceed;
  }
  return true;
}

// Configuration
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

// API clients
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' },
  timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT
});

// Helpers
function normalizeTitle(text = '') {
  return String(text).toLowerCase()
    .replace(/\b\d{4}\b/g, '')
    .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
    .replace(/\s*KATEX_INLINE_OPEN[^)]*KATEX_INLINE_CLOSE\s*/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

const TITLE_SMALL_WORDS = new Set(['and','or','the','for','of','in','on','with','a','an','to','at','by','from']);
function toTitleCase(str='') {
  const words = str.split(' ').filter(Boolean);
  return words.map((w, i) => {
    const lw = w.toLowerCase();
    if (i !== 0 && i !== words.length - 1 && TITLE_SMALL_WORDS.has(lw)) return lw;
    return lw.charAt(0).toUpperCase() + lw.slice(1);
  }).join(' ');
}

function sanitizeProductTitle(raw='') {
  let t = String(raw);
  t = t.replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '');
  t = t.replace(/\s*KATEX_INLINE_OPEN[^)]*KATEX_INLINE_CLOSE\s*/g, ' ');
  t = t.replace(/[^a-zA-Z0-9 -]+/g, ' ');
  t = t.replace(/[\s-]+/g, ' ').trim();
  t = toTitleCase(t);
  return t || 'Untitled Product';
}

async function getApifyProducts() {
  let allItems = [];
  let offset = 0;
  let pageCount = 0;
  const limit = 500;

  addLog('Starting Apify product fetch...', 'info', 'fetch');

  try {
    while (true) {
      const response = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=${limit}&offset=${offset}`);
      const items = response.data;
      allItems.push(...items);
      pageCount++;
      addLog(`Apify page ${pageCount}: fetched ${items.length} products (total: ${allItems.length})`, 'info', 'fetch');
      if (items.length < limit) break;
      offset += limit;
      await new Promise(r => setTimeout(r, 1000));
    }
  } catch (error) {
    addLog(`Apify fetch error: ${error.message}`, 'error', 'fetch');
    stats.errors++;
    await triggerFailsafe(`Apify fetch failed: ${error.message}`, { error: error.message }, false);
    throw error;
  }

  addLog(`Apify fetch complete: ${allItems.length} total products`, 'info', 'fetch');
  const canProceed = await checkFailsafeConditions('fetch', { apifyCount: allItems.length });
  if (!canProceed) throw new Error('Failsafe: Apify product count anomaly - operation cancelled');
  return allItems;
}

async function getShopifyProducts({ onlyApifyTag = true } = {}) {
  let allProducts = [];
  let sinceId = null;
  let pageCount = 0;
  const limit = 250;
  const fields = 'id,handle,title,variants,tags';

  addLog('Starting Shopify product fetch...', 'info', 'fetch');

  try {
    while (true) {
      let url = `/products.json?limit=${limit}&fields=${fields}`;
      if (sinceId) url += `&since_id=${sinceId}`;
      const response = await shopifyClient.get(url);
      const products = response.data.products;
      allProducts.push(...products);
      pageCount++;
      addLog(`Shopify page ${pageCount}: fetched ${products.length} products (total: ${allProducts.length})`, 'info', 'fetch');
      if (products.length < limit) break;
      sinceId = products[products.length - 1].id;
      await new Promise(r => setTimeout(r, 500));
    }
  } catch (error) {
    addLog(`Shopify fetch error: ${error.message}`, 'error', 'fetch');
    stats.errors++;
    await triggerFailsafe(`Shopify fetch failed: ${error.message}`, { error: error.message }, false);
    throw error;
  }

  const filtered = onlyApifyTag ? allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify')) : allProducts;
  addLog(`Shopify fetch complete: ${allProducts.length} total products, ${onlyApifyTag ? filtered.length + ' with Supplier:Apify tag' : 'using ALL products for matching'}`, 'info', 'fetch');
  const canProceed = await checkFailsafeConditions('fetch', { shopifyCount: filtered.length });
  if (!canProceed) throw new Error('Failsafe: Shopify product count anomaly - operation cancelled');
  return filtered;
}

// FIXED: Better rate limit handling for inventory levels
async function getActualInventoryLevels(inventoryItemIds) {
  const inventoryMap = new Map();
  const batchSize = 20; // Reduced batch size to avoid rate limits
  let rateLimitDelay = 500; // Start with 500ms delay
  
  addLog(`Fetching inventory levels for ${inventoryItemIds.length} items...`, 'info', 'inventory');
  
  for (let i = 0; i < inventoryItemIds.length; i += batchSize) {
    const batch = inventoryItemIds.slice(i, i + batchSize);
    let retries = 3;
    let success = false;
    
    while (retries > 0 && !success) {
      try {
        const url = `/inventory_levels.json?inventory_item_ids=${batch.join(',')}&location_ids=${config.shopify.locationId}`;
        const response = await shopifyClient.get(url);
        
        for (const level of response.data.inventory_levels) {
          inventoryMap.set(level.inventory_item_id, level.available || 0);
        }
        
        success = true;
        rateLimitDelay = Math.max(500, rateLimitDelay - 100); // Reduce delay on success
        
        if (i % 200 === 0 && i > 0) {
          addLog(`Progress: Fetched ${Math.min(i + batchSize, inventoryItemIds.length)} of ${inventoryItemIds.length} inventory levels`, 'info', 'inventory');
        }
      } catch (error) {
        if (error.response?.status === 429) {
          retries--;
          rateLimitDelay = Math.min(5000, rateLimitDelay * 2); // Increase delay on rate limit
          addLog(`Rate limit hit, waiting ${rateLimitDelay}ms before retry (${retries} retries left)`, 'warning', 'inventory');
          await new Promise(r => setTimeout(r, rateLimitDelay));
        } else {
          addLog(`Failed to fetch inventory batch: ${error.message}`, 'error', 'inventory');
          retries = 0; // Don't retry on non-rate-limit errors
        }
      }
    }
    
    if (!success) {
      addLog(`Failed to fetch batch after retries, continuing...`, 'error', 'inventory');
    }
    
    await new Promise(r => setTimeout(r, rateLimitDelay));
  }
  
  addLog(`Successfully fetched ${inventoryMap.size} inventory levels`, 'info', 'inventory');
  return inventoryMap;
}

function normalizeHandle(input, index, isTitle = false) {
  let handle = input || '';
  if (!isTitle && handle && handle !== 'undefined') {
    handle = handle.replace(config.apify.urlPrefix, '')
      .replace(/\.html$/, '')
      .replace(/\s*KATEX_INLINE_OPEN[^)]*KATEX_INLINE_CLOSE\s*/g, '')
      .replace(/[^a-z0-9-]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '')
      .toLowerCase();
    if (handle !== input && handle.length > 0) {
      if (index < 5) addLog(`Handle from URL: "${input}" → "${handle}"`, 'info');
      return handle;
    }
  }
  let baseText = (isTitle ? input : `product-${index}`);
  if (/^\d+$/.test((baseText || '').trim()) || (baseText || '').trim().length < 3) {
    baseText = `product-${(baseText || '').trim()}`;
  }
  handle = String(baseText).toLowerCase()
    .replace(/\b\d{4}\b/g, '')
    .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
    .replace(/\s*KATEX_INLINE_OPEN[^)]*KATEX_INLINE_CLOSE\s*/g, '')
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  if (!handle || handle === '-' || handle.length < 2) handle = `product-${index}-${Date.now()}`;
  if (index < 5) addLog(`Generated handle: "${input}" → "${handle}"`, 'info');
  return handle;
}

function extractHandleFromCanonicalUrl(item, index) {
  const urlFields = [item.canonicalUrl, item.url, item.productUrl, item.source?.url];
  const validUrl = urlFields.find(url => url && url !== 'undefined');
  if (index < 5) addLog(`Debug product ${index}: canonicalUrl="${item.canonicalUrl}", url="${item.url}", productUrl="${item.productUrl}", title="${item.title}"`, 'info');
  const handle = normalizeHandle(validUrl || item.title || `product-${index}`, index, !validUrl);
  return handle || `product-${index}-${Date.now()}`;
}

function buildMetaTitle(product) {
  const base = `${product.title} | LandOfEssentials`;
  return base.length > 60 ? `${product.title.slice(0, 57)}...` : base;
}

function extractFeaturesFromTitle(title = '') {
  const t = title.toLowerCase();
  const features = [];
  if (t.includes('halloween')) features.push('Perfect for Halloween celebrations');
  if (t.includes('kids') || t.includes('children')) features.push('Designed for children');
  if (t.includes('game') || t.includes('puzzle')) features.push('Fun and engaging activities');
  if (t.includes('mask')) features.push('Comfortable and easy to wear');
  if (t.includes('decoration')) features.push('Enhances any space with festive decor');
  if (t.includes('toy')) features.push('Safe and durable construction');
  return features.slice(0, 6);
}

function generateSEODescription(product) {
  const title = product.title;
  const originalDescription = product.description || '';
  const features = extractFeaturesFromTitle(title);
  let seo = `${title} - Premium quality ${title.toLowerCase()} available at LandOfEssentials. `;
  if (originalDescription && originalDescription.length > 20) seo += `${originalDescription.substring(0, 150)}... `;
  if (features.length > 0) seo += `This product is ${features.join(', ')}. `;
  seo += `Order now for fast delivery. Shop with confidence at LandOfEssentials - your trusted online retailer for quality products.`;
  return seo;
}

function buildProductHtml(product) {
  const features = extractFeaturesFromTitle(product.title);
  const featureList = features.length ? `<ul>${features.map(f => `<li>${f}</li>`).join('')}</ul>` : '';
  return `
<div>
  <p>${product.seoDescription}</p>
  ${featureList}
  <p><em>Fast UK delivery by LandOfEssentials.</em></p>
</div>`.trim();
}

function buildShopifyMaps(shopifyData) {
  const handleMap = new Map();
  const titleMap = new Map();
  const skuMap = new Map();
  shopifyData.forEach(product => {
    handleMap.set(product.handle.toLowerCase(), product);
    const altHandle = product.handle.replace(/-\d+$/, '').toLowerCase();
    if (altHandle !== product.handle.toLowerCase()) handleMap.set(altHandle, product);
    const t = normalizeTitle(product.title);
    if (t) titleMap.set(t, product);
    const sku = product.variants?.[0]?.sku;
    if (sku) skuMap.set(sku.toLowerCase(), product);
  });
  return { handleMap, titleMap, skuMap };
}

function matchShopifyProduct(apifyProduct, maps) {
  const key = apifyProduct.handle.toLowerCase();
  let product = maps.handleMap.get(key);
  if (product) return { product, matchType: 'handle' };
  if (apifyProduct.sku) {
    const bySku = maps.skuMap.get(apifyProduct.sku.toLowerCase());
    if (bySku) return { product: bySku, matchType: 'sku' };
  }
  const t = normalizeTitle(apifyProduct.title);
  const byTitle = maps.titleMap.get(t);
  if (byTitle) return { product: byTitle, matchType: 'title' };
  const parts = key.split('-').filter(p => p.length > 3);
  if (parts.length > 2) {
    for (const [shopifyHandle, prod] of maps.handleMap) {
      const matching = parts.filter(part => shopifyHandle.includes(part));
      if (matching.length >= parts.length * 0.6) return { product: prod, matchType: 'partial-handle' };
    }
  }
  return { product: null, matchType: 'none' };
}

function processApifyProducts(apifyData, options = { processPrice: true, applyMinimumStock: true }) {
  return apifyData.map((item, index) => {
    const handle = extractHandleFromCanonicalUrl(item, index);
    if (!handle) {
      addLog(`Failed to generate handle for ${item.title || 'unknown'}`, 'error', 'processing');
      stats.errors++;
      return null;
    }

    const rawTitle = item.title || 'Untitled Product';
    const cleanTitle = sanitizeProductTitle(rawTitle);

    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20;
    const rawStatus = item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || item.variants?.[0]?.stockStatus || '';
    const normalizedStatus = String(rawStatus).trim().toLowerCase().replace(/[\s_-]/g, '');
    const isOut = ['outofstock', 'soldout', 'unavailable', 'nostock'].includes(normalizedStatus);
    const isIn = ['instock', 'available', 'instockavailable'].includes(normalizedStatus);
    
    if (isOut) {
      inventory = 0;
    } else if (isIn && (!inventory || inventory === 0)) {
      inventory = 20;
    }
    
    // ONLY apply minimum stock rule for NEW products, not inventory updates
    const originalInventory = inventory;
    if (options.applyMinimumStock && !isOut && inventory > 0 && inventory < 20) {
      inventory = 20;
      if (index < 5) addLog(`Stock adjusted for NEW product "${cleanTitle}": ${originalInventory} → 20`, 'info', 'processing');
    }

    let price = 0, finalPrice = 0, compareAtPrice = 0;
    if (options.processPrice) {
      if (item.variants?.length > 0) {
        const variant = item.variants[0];
        if (variant.price) price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
      }
      if (!price) price = parseFloat(item.price || 0);
      const originalPrice = price;
      const hasDecimals = String(price).includes('.');

      if (price > 0 && price < 10 && hasDecimals) {
        // already pounds
      } else if (!hasDecimals && price > 50) {
        price = price / 100;
        if (index < 5) addLog(`Price converted from pence: ${originalPrice} → ${price}`, 'info', 'processing');
      } else if (price > 1000) {
        price = price / 100;
        if (index < 5) addLog(`Price converted from pence (high): ${originalPrice} → ${price}`, 'info', 'processing');
      }

      if (price <= 1) finalPrice = price + 6;
      else if (price <= 2) finalPrice = price + 7;
      else if (price <= 3) finalPrice = price + 8;
      else if (price <= 5) finalPrice = price + 9;
      else if (price <= 8) finalPrice = price + 10;
      else if (price <= 15) finalPrice = price * 2;
      else if (price <= 30) finalPrice = price * 2.2;
      else if (price <= 50) finalPrice = price * 1.8;
      else if (price <= 75) finalPrice = price * 1.6;
      else finalPrice = price * 1.4;

      if (!finalPrice || finalPrice < 5) {
        addLog(`Minimum price applied for ${item.title}: ${price.toFixed(2)} → 15.00`, 'warning', 'processing');
        price = 5.00;
        finalPrice = 15.00;
      }

      compareAtPrice = (finalPrice * 1.2).toFixed(2);
      if (index < 5) addLog(`Price debug for ${cleanTitle}: base=${price.toFixed(2)}, final=${finalPrice.toFixed(2)}`, 'info', 'processing');
    }

    const images = [];
    if (Array.isArray(item.medias)) {
      for (let i = 0; i < Math.min(5, item.medias.length); i++) {
        if (item.medias[i]?.url) images.push(item.medias[i].url);
      }
    }

    const productData = {
      handle, title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '',
      originalPrice: (price || 0).toFixed(2),
      price: (finalPrice || 0).toFixed(2),
      compareAtPrice,
      inventory, images, vendor: 'LandOfEssentials'
    };
    if (options.processPrice) {
      productData.seoDescription = generateSEODescription(productData);
      productData.seoHtml = buildProductHtml(productData);
      productData.seoTitle = buildMetaTitle(productData);
      productData.seoMetaDescription = (productData.seoDescription || '').slice(0, 320);
    }
    return productData;
  }).filter(Boolean);
}

async function enableInventoryTracking(productId, variantId) {
  try {
    await shopifyClient.put(`/variants/${variantId}.json`, {
      variant: { id: variantId, inventory_management: 'shopify', inventory_policy: 'deny' }
    });
    addLog(`Enabled inventory tracking for variant ${variantId}`, 'success', 'shopify');
    return true;
  } catch (error) {
    addLog(`Failed to enable inventory tracking for variant ${variantId}: ${error.message}`, 'error', 'shopify');
    return false;
  }
}

async function createNewProductsJob(token, apifyProducts) {
  if (shouldAbort(token)) {
    addLog('Product creation skipped - paused/failsafe', 'warning', 'products');
    return { created: 0, errors: 0, total: 0 };
  }

  const shopifyAll = await getShopifyProducts({ onlyApifyTag: false });
  const maps = buildShopifyMaps(shopifyAll);

  const newCandidates = [];
  apifyProducts.forEach(p => {
    const { product } = matchShopifyProduct(p, maps);
    if (!product) newCandidates.push(p);
  });

  addLog(`Planned new products (after robust matching): ${newCandidates.length}`, 'info', 'products');

  const canProceed = await checkFailsafeConditions('products', { toCreate: newCandidates.length, existingCount: shopifyAll.length });
  if (!canProceed) {
    return { created: 0, errors: 0, total: newCandidates.length };
  }

  const toCreateList = newCandidates.slice(0, MAX_CREATE_PER_RUN);
  if (newCandidates.length > toCreateList.length) {
    addLog(`Create cap applied: ${toCreateList.length}/${newCandidates.length} this run (MAX_CREATE_PER_RUN=${MAX_CREATE_PER_RUN})`, 'warning', 'products');
  }

  let created = 0, errors = 0;
  for (const product of toCreateList) {
    if (shouldAbort(token)) { addLog('Aborting product creation due to pause/failsafe', 'warning', 'products'); break; }
    try {
      const shopifyProduct = {
        title: product.title,
        body_html: product.seoHtml || (product.seoDescription || '').replace(/\n/g, '<br>'),
        handle: product.handle,
        vendor: product.vendor,
        product_type: 'General',
        status: 'active',
        tags: `Supplier:Apify,Cost:${product.originalPrice},SKU:${product.sku},Auto-Sync`,
        metafields_global_title_tag: product.seoTitle || product.title,
        metafields_global_description_tag: product.seoMetaDescription || product.seoDescription || '',
        images: (product.images || []).slice(0, 5).map(src => ({ src })),
        variants: [{
          price: product.price,
          compare_at_price: product.compareAtPrice,
          sku: product.sku,
          inventory_management: 'shopify',
          inventory_policy: 'deny',
          inventory_quantity: product.inventory,
          fulfillment_service: 'manual',
          requires_shipping: true
        }]
      };
      await shopifyClient.post('/products.json', { product: shopifyProduct });
      addLog(`Created: ${product.title}`, 'success', 'products');
      created++;
      stats.newProducts++;
      await new Promise(r => setTimeout(r, 1500));
    } catch (error) {
      errors++;
      stats.errors++;
      addLog(`Failed to create ${product.title}: ${error.message}`, 'error', 'products');
      if (error.response?.status === 429) {
        addLog('Rate limit hit - waiting 30 seconds', 'warning', 'products');
        await new Promise(r => setTimeout(r, 30000));
      }
    }
  }

  addToHistory('products', { created, errors, attempted: toCreateList.length });
  addLog(`Product creation completed: ${created} created, ${errors} errors`, 'info', 'products');
  return { created, errors, total: newCandidates.length };
}

async function handleDiscontinuedProductsJob(token) {
  let discontinued = 0, errors = 0;

  try {
    addLog('Checking for discontinued products...', 'info', 'discontinued');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false, applyMinimumStock: false });
    const shopifyMaps = buildShopifyMaps(shopifyData);

    const matchedShopifyIds = new Set();
    apifyProcessed.forEach(apifyProd => {
      const { product } = matchShopifyProduct(apifyProd, shopifyMaps);
      if (product) matchedShopifyIds.add(product.id);
    });

    const candidates = shopifyData.filter(p => !matchedShopifyIds.has(p.id));
    addLog(`Found ${candidates.length} Shopify products not in Apify`, 'info', 'discontinued');
    
    const nowMissing = [];
    for (const p of candidates) {
      const key = p.handle.toLowerCase();
      const count = (missingCounters.get(key) || 0) + 1;
      missingCounters.set(key, count);
      if (count >= DISCONTINUE_MISS_RUNS) nowMissing.push(p);
    }
    
    shopifyData.forEach(p => { 
      if (matchedShopifyIds.has(p.id)) missingCounters.delete(p.handle.toLowerCase()); 
    });

    addLog(`Consecutive-miss filter: ${nowMissing.length} eligible after ${DISCONTINUE_MISS_RUNS} runs`, 'info', 'discontinued');

    const canProceed = await checkFailsafeConditions('discontinued', { toDiscontinue: nowMissing.length });
    if (!canProceed) {
      return { discontinued: 0, errors: 0, total: nowMissing.length };
    }

    for (const product of nowMissing) {
      if (shouldAbort(token)) {
        addLog('Aborting discontinued check due to pause/failsafe', 'warning', 'discontinued');
        break;
      }
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
    return { discontinued, errors, total: nowMissing.length };

  } catch (error) {
    addLog(`Discontinued workflow failed: ${error.message}`, 'error', 'discontinued');
    stats.errors++;
    return { discontinued, errors: errors + 1, total: 0 };
  }
}

// FIXED: Better inventory update with connection handling
async function updateInventoryJob(token) {
  let updated = 0, errors = 0, trackingEnabled = 0, skipped = 0, connected = 0;
  mismatches = [];
  const repeatUpdates = [];

  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info', 'inventory');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    if (apifyData.length > FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS && shopifyData.length > FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS) {
      lastKnownGoodState = { apifyCount: apifyData.length, shopifyCount: shopifyData.length, timestamp: new Date().toISOString() };
    }
    
    // Get actual inventory levels with better rate limit handling
    addLog('Fetching actual inventory levels from Shopify...', 'info', 'inventory');
    const inventoryItemIds = shopifyData.map(p => p.variants?.[0]?.inventory_item_id).filter(Boolean);
    const actualInventoryLevels = await getActualInventoryLevels(inventoryItemIds);
    addLog(`Fetched ${actualInventoryLevels.size} inventory levels from location ${config.shopify.locationId}`, 'info', 'inventory');
    
    const processedProducts = processApifyProducts(apifyData, { processPrice: false, applyMinimumStock: false });
    const maps = buildShopifyMaps(shopifyData);
    const inventoryUpdates = [];
    const notConnectedToLocation = [];
    
    // Track what was updated in previous runs
    const previouslyUpdated = inventoryDebugMap;
    
    processedProducts.forEach((apifyProduct, idx) => {
      const { product: shopifyProduct } = matchShopifyProduct(apifyProduct, maps);
      if (!shopifyProduct) {
        if (mismatches.length < 100) {
          mismatches.push({ 
            apifyTitle: apifyProduct.title, 
            apifyHandle: apifyProduct.handle, 
            apifyUrl: apifyProduct.url || 'N/A', 
            shopifyHandle: 'NOT FOUND' 
          });
        }
        return;
      }
      
      if (!shopifyProduct.variants?.[0]?.inventory_item_id) return;
      
      const inventoryItemId = shopifyProduct.variants[0].inventory_item_id;
      
      // Use actual inventory level from the API
      const currentInventory = actualInventoryLevels.get(inventoryItemId);
      const targetInventory = parseInt(apifyProduct.inventory, 10) || 0;
      
      // If inventory level doesn't exist at this location, product needs to be connected
      if (currentInventory === undefined) {
        notConnectedToLocation.push({
          title: shopifyProduct.title,
          handle: shopifyProduct.handle,
          inventoryItemId: inventoryItemId,
          targetInventory: targetInventory,
          productId: shopifyProduct.id,
          variantId: shopifyProduct.variants[0].id
        });
        return;
      }
      
      if (currentInventory === targetInventory) {
        skipped++;
        inventoryDebugMap.delete(shopifyProduct.handle);
        return;
      }
      
      // Check if this was updated in the last run
      const prevUpdate = previouslyUpdated.get(shopifyProduct.handle);
      if (prevUpdate) {
        repeatUpdates.push({
          title: shopifyProduct.title,
          handle: shopifyProduct.handle,
          previousTarget: prevUpdate.targetQty,
          currentShopify: currentInventory,
          newTarget: targetInventory
        });
      }
      
      // Track this update
      inventoryDebugMap.set(shopifyProduct.handle, {
        title: shopifyProduct.title,
        targetQty: targetInventory,
        updatedAt: new Date().toISOString()
      });

      inventoryUpdates.push({
        title: shopifyProduct.title,
        handle: shopifyProduct.handle,
        currentInventory,
        newInventory: targetInventory,
        inventoryItemId: inventoryItemId,
        productId: shopifyProduct.id,
        variantId: shopifyProduct.variants[0].id
      });
    });
    
    // Log products not connected to location
    if (notConnectedToLocation.length > 0) {
      addLog(`⚠️ ${notConnectedToLocation.length} products not connected to location ${config.shopify.locationId}`, 'warning', 'inventory');
      if (notConnectedToLocation.length <= 10) {
        notConnectedToLocation.forEach(p => {
          addLog(`  - ${p.title}: needs connection to location`, 'warning', 'inventory');
        });
      }
    }
    
    // Check for repeating updates
    if (repeatUpdates.length > 0) {
      addLog(`⚠️ WARNING: ${repeatUpdates.length} products need updating AGAIN after previous update!`, 'error', 'inventory');
      repeatUpdates.slice(0, 5).forEach(item => {
        addLog(`REPEAT: ${item.title} - Was set to ${item.previousTarget}, now shows ${item.currentShopify}, trying to set to ${item.newTarget}`, 'error', 'inventory');
      });
      
      if (repeatUpdates.length > 100) {
        addLog('🚨 CRITICAL: Over 100 products repeating - likely a sync bug!', 'error', 'inventory');
      }
    }
    
    addLog(`Inventory Analysis:`, 'info', 'inventory');
    addLog(`- Total Apify products: ${processedProducts.length}`, 'info', 'inventory');
    addLog(`- Matched in Shopify: ${processedProducts.length - mismatches.length}`, 'info', 'inventory');
    addLog(`- Already in sync: ${skipped}`, 'info', 'inventory');
    addLog(`- Updates needed: ${inventoryUpdates.length}`, 'info', 'inventory');
    addLog(`- Not connected to location: ${notConnectedToLocation.length}`, 'warning', 'inventory');
    addLog(`- Repeating from last run: ${repeatUpdates.length}`, repeatUpdates.length > 0 ? 'warning' : 'info', 'inventory');
    addLog(`- Unmatched products: ${mismatches.length}`, 'warning', 'inventory');
    
    const errorRate = errors > 0 ? (errors / Math.max(inventoryUpdates.length, 1) * 100) : 0;
    const canProceed = await checkFailsafeConditions('inventory', { 
      updatesNeeded: inventoryUpdates.length + notConnectedToLocation.length, 
      totalApifyProducts: apifyData.length, 
      errorRate 
    });
    
    if (!canProceed) {
      return { updated: 0, errors: 0, total: inventoryUpdates.length };
    }
    
    if (!config.shopify.locationId) {
      addLog('ERROR: SHOPIFY_LOCATION_ID environment variable is not set!', 'error', 'inventory');
      return { updated: 0, errors: 1, total: inventoryUpdates.length };
    }
    
    // FIXED: Connect products with better error handling
    addLog(`Connecting ${notConnectedToLocation.length} products to location...`, 'info', 'inventory');
    for (const product of notConnectedToLocation) {
      if (shouldAbort(token)) break;
      try {
        // Ensure tracking is enabled first
        const variantResp = await shopifyClient.get(`/products/${product.productId}.json?fields=variants`);
        const variant = variantResp.data.product.variants[0];
        if (!variant.inventory_management) {
          await enableInventoryTracking(product.productId, variant.id);
          trackingEnabled++;
          await new Promise(r => setTimeout(r, 1000));
        }
        
        // Connect to location with retry logic
        let connectSuccess = false;
        let connectRetries = 3;
        
        while (!connectSuccess && connectRetries > 0) {
          try {
            await shopifyClient.post('/inventory_levels/connect.json', { 
              location_id: parseInt(config.shopify.locationId), 
              inventory_item_id: product.inventoryItemId,
              relocate_if_necessary: true
            });
            connectSuccess = true;
            connected++;
            addLog(`✓ Connected ${product.title} to location`, 'success', 'inventory');
          } catch (connectError) {
            if (connectError.response?.status === 429) {
              connectRetries--;
              addLog(`Rate limit on connect, waiting 2s (${connectRetries} retries left)`, 'warning', 'inventory');
              await new Promise(r => setTimeout(r, 2000));
            } else if (connectError.response?.data?.errors?.includes('already connected')) {
              connectSuccess = true; // Already connected, continue
              addLog(`${product.title} already connected`, 'info', 'inventory');
            } else {
              throw connectError;
            }
          }
        }
        
        if (connectSuccess) {
          // Set inventory level after connecting
          await new Promise(r => setTimeout(r, 500));
          await shopifyClient.post('/inventory_levels/set.json', { 
            location_id: parseInt(config.shopify.locationId), 
            inventory_item_id: product.inventoryItemId, 
            available: product.targetInventory 
          });
          addLog(`✓ Set inventory for newly connected: ${product.title} → ${product.targetInventory}`, 'success', 'inventory');
          updated++;
          stats.inventoryUpdates++;
        }
        
        await new Promise(r => setTimeout(r, 800));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`✗ Failed to connect/update ${product.title}: ${error.message}`, 'error', 'inventory');
        if (error.response?.status === 429) {
          await new Promise(r => setTimeout(r, 5000));
        }
      }
    }
    
    // Then update existing inventory levels
    addLog(`Updating ${inventoryUpdates.length} inventory levels...`, 'info', 'inventory');
    for (const update of inventoryUpdates) {
      if (shouldAbort(token)) { 
        addLog('Aborting inventory update due to pause/failsafe', 'warning', 'inventory'); 
        break; 
      }
      try {
        // Ensure tracking is enabled
        const variantResp = await shopifyClient.get(`/products/${update.productId}.json?fields=variants`);
        const variant = variantResp.data.product.variants[0];
        if (!variant.inventory_management) {
          await enableInventoryTracking(update.productId, variant.id);
          trackingEnabled++;
          await new Promise(r => setTimeout(r, 800));
        }
        
        // Update inventory level
        await shopifyClient.post('/inventory_levels/set.json', { 
          location_id: parseInt(config.shopify.locationId), 
          inventory_item_id: update.inventoryItemId, 
          available: update.newInventory 
        });
        
        addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success', 'inventory');
        updated++;
        stats.inventoryUpdates++;
        await new Promise(r => setTimeout(r, 600));
      } catch (error) {
        errors++;
        stats.errors++;
        const errText = JSON.stringify(error.response?.data?.errors || error.message);
        addLog(`✗ Failed: ${update.title} - ${errText}`, 'error', 'inventory');
        
        // Remove from debug map if it failed
        inventoryDebugMap.delete(update.handle);
        
        if (error.response?.status === 429) {
          addLog('Rate limit hit - waiting 30 seconds', 'warning', 'inventory');
          await new Promise(r => setTimeout(r, 30000));
        }
      }
    }
    
    stats.lastSync = new Date().toISOString();
    addToHistory('inventory', { 
      updated, 
      errors, 
      attempted: inventoryUpdates.length + notConnectedToLocation.length, 
      trackingEnabled, 
      skipped,
      connected 
    });
    addLog(`=== INVENTORY UPDATE COMPLETE ===`, 'info', 'inventory');
    addLog(`Result: ${updated} updated, ${connected} connected, ${skipped} already in sync, ${trackingEnabled} tracking enabled, ${errors} errors`, 'info', 'inventory');
    
    // If we still have many repeating updates, suggest manual investigation
    if (repeatUpdates.length > 50) {
      await notifyTelegram(`⚠️ Inventory Sync Report\n\n${repeatUpdates.length} products still repeating after update.\n\nConnected ${connected} products to location.\n\nPlease run another sync to verify the issue is resolved.`);
    }
    
    return { updated, errors, total: inventoryUpdates.length };
  } catch (error) {
    addLog(`Inventory workflow failed: ${error.message}`, 'error', 'inventory');
    stats.errors++;
    return { updated, errors: errors + 1, total: 0 };
  }
}

async function markProductOutOfStockByKey(key) {
  let handleCandidate = key;
  try {
    if (key.includes('/products/')) {
      let part = key.split('/products/')[1];
      if (part.includes('?')) part = part.split('?')[0];
      handleCandidate = part.replace(/\/+$/, '').trim();
    }
  } catch {}

  const shopifyAll = await getShopifyProducts({ onlyApifyTag: false });
  const maps = buildShopifyMaps(shopifyAll);

  let product = maps.handleMap.get(handleCandidate.toLowerCase());

  if (!product) {
    const t = normalizeTitle(key);
    product = maps.titleMap.get(t);
  }

  if (!product) return null;

  const variant = product.variants?.[0];
  if (!variant) throw new Error('No variant found');

  if (!variant.inventory_management) {
    await enableInventoryTracking(product.id, variant.id);
    await new Promise(r => setTimeout(r, 500));
  }

  const checkResponse = await shopifyClient.get(
    `/inventory_levels.json?inventory_item_ids=${variant.inventory_item_id}&location_ids=${config.shopify.locationId}`
  ).catch(() => null);
  if (!checkResponse?.data?.inventory_levels?.length) {
    await shopifyClient.post('/inventory_levels/connect.json', {
      location_id: parseInt(config.shopify.locationId),
      inventory_item_id: variant.inventory_item_id
    }).catch(() => {});
  }

  await shopifyClient.post('/inventory_levels/set.json', {
    location_id: parseInt(config.shopify.locationId),
    inventory_item_id: variant.inventory_item_id,
    available: 0
  });

  addLog(`Telegram OOS: ${product.title} set to 0`, 'success', 'telegram');
  return { id: product.id, title: product.title, handle: product.handle };
}

// UI
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Shopify Sync Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background: linear-gradient(to bottom right, #1a1a2e, #16213e); color: #e5e7eb; }
    .gradient-bg { background: linear-gradient(135deg, #ff6e7f 0%, #bfe9ff 100%); box-shadow: 0 10px 20px rgba(255,110,127,0.4); }
    .card-hover { transition: all .3s ease-in-out; background: rgba(31,41,55,.2); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,.1); box-shadow: 0 8px 32px rgba(0,0,0,.4); }
    .card-hover:hover { transform: translateY(-8px) scale(1.03); box-shadow: 0 12px 40px rgba(0,0,0,.3); border-color: rgba(255,110,127,.5); }
    .btn-hover { transition: all .2s ease-in-out; } .btn-hover:hover { transform: translateY(-1px); }
    .fade-in { animation: fadeIn .6s ease-in-out; } @keyframes fadeIn { from { opacity: 0; transform: translateY(30px); } to { opacity: 1; transform: translateY(0); } }
    .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #ff6e7f; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;}
    @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} }
    .pulse { animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .5; } }
  </style>
</head>
<body class="min-h-screen font-sans">
  <div class="container mx-auto px-4 py-8">
    <div class="relative bg-gray-800 rounded-2xl shadow-lg p-8 mb-8 gradient-bg text-white fade-in">
      <h1 class="text-4xl font-extrabold tracking-tight">Shopify Sync Dashboard</h1>
      <p class="mt-2 text-lg opacity-90">Seamless product synchronization with Apify, optimized for SEO</p>
    </div>

    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-blue-400">New Products (Total)</h3><p class="text-3xl font-bold text-gray-100" id="newProducts">${stats.newProducts}</p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-green-400">Inventory Updates (Total)</h3><p class="text-3xl font-bold text-gray-100" id="inventoryUpdates">${stats.inventoryUpdates}</p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-orange-400">Discontinued (Total)</h3><p class="text-3xl font-bold text-gray-100" id="discontinued">${stats.discontinued}</p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-red-400">Errors (Total)</h3><p class="text-3xl font-bold text-gray-100" id="errors">${stats.errors}</p></div>
    </div>

    <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
      <h2 class="text-2xl font-semibold text-gray-100 mb-4">System Controls</h2>
      
      ${failsafePending ? `
      <div class="mb-4 p-4 rounded-lg bg-yellow-900 border-2 border-yellow-500">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="font-bold text-yellow-300 pulse">⚠️ FAILSAFE WARNING - Decision Required</h3>
            <p class="text-sm text-yellow-400">${failsafePending.reason}</p>
            <p class="text-xs text-yellow-500 mt-2">Waiting since: ${new Date(failsafePending.timestamp).toLocaleString()}</p>
          </div>
          <div class="flex gap-2">
            <button onclick="failsafeDecision('proceed')" class="bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover">Proceed Anyway</button>
            <button onclick="failsafeDecision('cancel')" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Cancel Operation</button>
          </div>
        </div>
      </div>` : ''}
      
      ${failsafeTriggered && !failsafePending ? `
      <div class="mb-4 p-4 rounded-lg bg-red-900 border-2 border-red-500">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="font-bold text-red-300">🚨 FAILSAFE TRIGGERED</h3>
            <p class="text-sm text-red-400">${failsafeReason}</p>
          </div>
          <button onclick="clearFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Clear Failsafe</button>
        </div>
      </div>` : ''}

      <div class="mb-6 p-4 rounded-lg ${systemPaused ? 'bg-red-900 border-red-700' : 'bg-green-900 border-green-700'} border">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="font-medium ${systemPaused ? 'text-red-300' : 'text-green-300'}" id="systemStatus">System Status: ${systemPaused ? 'PAUSED' : 'ACTIVE'}</h3>
            <p class="text-sm ${systemPaused ? 'text-red-400' : 'text-green-400'}" id="systemStatusDesc">${systemPaused ? 'Automatic syncing is disabled' : 'Syncing is active on schedule'}</p>
          </div>
          <div class="flex items-center">
            <button onclick="togglePause()" id="pauseButton" class="${systemPaused ? 'bg-green-500 hover:bg-green-600' : 'bg-red-500 hover:bg-red-600'} text-white px-4 py-2 rounded-lg btn-hover">${systemPaused ? 'Resume System' : 'Pause System'}</button>
            <div id="pauseSpinner" class="spinner"></div>
          </div>
        </div>
      </div>

      <div class="flex flex-wrap gap-4">
        <button onclick="triggerSync('products')" class="flex items-center bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover">Create New Products<span id="productsSpinner" class="spinner"></span></button>
        <button onclick="triggerSync('inventory')" class="flex items-center bg-green-500 text-white px-6 py-3 rounded-lg btn-hover">Update Inventory<span id="inventorySpinner" class="spinner"></span></button>
        <button onclick="triggerSync('discontinued')" class="flex items-center bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover">Check Discontinued<span id="discontinuedSpinner" class="spinner"></span></button>
        <button onclick="resetCounters()" class="flex items-center bg-gray-500 text-white px-6 py-3 rounded-lg btn-hover">Reset Counters<span id="resetSpinner" class="spinner"></span></button>
        <button onclick="fixInventoryTracking()" class="flex items-center bg-purple-500 text-white px-6 py-3 rounded-lg btn-hover">Fix Inventory Tracking<span id="fixSpinner" class="spinner"></span></button>
      </div>
      
      <div class="mt-4 p-4 rounded-lg bg-gray-700">
        <p class="text-sm text-gray-400"><strong>Schedule:</strong></p>
        <ul class="text-xs text-gray-400 mt-2">
          <li>• Inventory Updates: Daily at 1 AM</li>
          <li>• New Products: Weekly on Friday at 2 AM</li>
          <li>• Discontinued Check: Daily at 3 AM</li>
          <li>• Error Summary: Weekly on Monday at 9 AM</li>
        </ul>
      </div>
    </div>
    
    <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
      <h2 class="text-2xl font-semibold text-gray-100 mb-4">Run History</h2>
      <div class="overflow-x-auto max-h-96">
        <table class="w-full text-sm text-left text-gray-400">
          <thead class="text-xs text-gray-400 uppercase bg-gray-700 sticky top-0">
            <tr><th class="px-4 py-2">Type</th><th class="px-4 py-2">Timestamp</th><th class="px-4 py-2">Result</th></tr>
          </thead>
          <tbody id="runHistoryBody">
            ${runHistory.map(r => `
              <tr class="border-b border-gray-700">
                <td class="px-4 py-2 capitalize font-semibold ${r.type === 'inventory' ? 'text-green-400' : r.type === 'products' ? 'text-blue-400' : 'text-orange-400'}">${r.type}</td>
                <td class="px-4 py-2">${new Date(r.timestamp).toLocaleString()}</td>
                <td class="px-4 py-2">
                  ${r.type === 'inventory' ? `Updated: ${r.updated || 0}, Connected: ${r.connected || 0}, Skipped: ${r.skipped || 0}, Errors: ${r.errors || 0}` : ''}
                  ${r.type === 'products' ? `Created: ${r.created || 0}, Errors: ${r.errors || 0}` : ''}
                  ${r.type === 'discontinued' ? `Discontinued: ${r.discontinued || 0}, Errors: ${r.errors || 0}` : ''}
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>

    <div class="rounded-2xl p-6 card-hover fade-in">
      <h2 class="text-2xl font-semibold text-gray-100 mb-4">Activity Log</h2>
      <div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer">
        ${logs.map(log => `<div class="${log.type === 'success' ? 'text-green-400' : log.type === 'error' ? 'text-red-400' : log.type === 'warning' ? 'text-yellow-400' : 'text-gray-300'}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}
      </div>
    </div>
  </div>

  <script>
    let systemPaused = ${systemPaused};
    let failsafePending = ${failsafePending ? 'true' : 'false'};

    async function failsafeDecision(decision) {
      try {
        const res = await fetch('/api/failsafe/decision', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ decision })
        });
        const result = await res.json();
        addLogEntry(result.message, result.success ? 'success' : 'error');
        setTimeout(() => location.reload(), 1000);
      } catch (e) {
        addLogEntry('Failed to submit failsafe decision', 'error');
      }
    }

    async function resetCounters() {
      if (!confirm('Are you sure you want to reset all total counters to zero?')) return;
      const button = event.target;
      const spinner = document.getElementById('resetSpinner');
      button.disabled = true; spinner.style.display = 'inline-block';
      try {
        await fetch('/api/stats/reset', { method: 'POST' });
        addLogEntry('✅ Counters have been reset.', 'success');
        fetchAndUpdateStatus();
      } catch (e) {
        addLogEntry('❌ Failed to reset counters.', 'error');
      } finally {
        button.disabled = false; spinner.style.display = 'none';
      }
    }

    async function fixInventoryTracking() {
      if (!confirm('This will enable inventory tracking for all products. Continue?')) return;
      const button = event.target;
      const spinner = document.getElementById('fixSpinner');
      button.disabled = true; spinner.style.display = 'inline-block';
      try {
        const res = await fetch('/api/fix/inventory-tracking', { method: 'POST' });
        const result = await res.json();
        addLogEntry(result.success ? '✅ ' + result.message : '❌ ' + result.message, result.success ? 'success' : 'error');
      } catch (e) {
        addLogEntry('❌ Failed to fix inventory tracking', 'error');
      } finally {
        button.disabled = false; spinner.style.display = 'none';
      }
    }

    async function togglePause() {
      const button = document.getElementById('pauseButton');
      const spinner = document.getElementById('pauseSpinner');
      button.disabled = true; spinner.style.display = 'inline-block';
      try {
        const res = await fetch('/api/pause', { method: 'POST' });
        const result = await res.json();
        if (result.success) {
          systemPaused = result.paused;
          updateSystemStatus();
          addLogEntry('🔄 System ' + (result.paused ? 'paused' : 'resumed'), 'info');
        }
      } finally {
        button.disabled = false; spinner.style.display = 'none';
      }
    }
    
    function updateSystemStatus() {
      const statusEl = document.getElementById('systemStatus');
      const statusDescEl = document.getElementById('systemStatusDesc');
      const buttonEl = document.getElementById('pauseButton');
      statusEl.textContent = \`System Status: \${systemPaused ? 'PAUSED' : 'ACTIVE'}\`;
      statusEl.className = \`font-medium \${systemPaused ? 'text-red-300' : 'text-green-300'}\`;
      statusDescEl.textContent = \`\${systemPaused ? 'Automatic syncing is disabled' : 'Syncing is active on schedule'}\`;
      buttonEl.textContent = systemPaused ? 'Resume System' : 'Pause System';
      buttonEl.className = \`text-white px-4 py-2 rounded-lg btn-hover \${systemPaused ? 'bg-green-500 hover:bg-green-600' : 'bg-red-500 hover:bg-red-600'}\`;
    }
    
    async function clearFailsafe() {
      await fetch('/api/failsafe/clear', { method: 'POST' });
      location.reload();
    }
    
    async function triggerSync(type) {
      const button = event.target;
      const spinner = document.getElementById(type + 'Spinner');
      button.disabled = true; spinner.style.display = 'inline-block';
      try {
        const res = await fetch('/api/sync/' + type, { method: 'POST' });
        const result = await res.json();
        addLogEntry(result.success ? '✅ ' + result.message : '❌ ' + result.message, result.success ? 'success' : 'error');
      } catch (e) {
        addLogEntry('❌ Failed to trigger ' + type + ' sync', 'error');
      } finally {
        button.disabled = false; spinner.style.display = 'none';
      }
    }
    
    function addLogEntry(message, type) {
      const logContainer = document.getElementById('logContainer');
      const time = new Date().toLocaleTimeString();
      const color = type === 'success' ? 'text-green-400' : type === 'error' ? 'text-red-400' : type === 'warning' ? 'text-yellow-400' : 'text-gray-300';
      logContainer.insertAdjacentHTML('afterbegin', \`<div class="\${color}">[\${time}] \${message}</div>\`);
    }

    async function fetchAndUpdateStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        document.getElementById('newProducts').textContent = data.stats.newProducts;
        document.getElementById('inventoryUpdates').textContent = data.stats.inventoryUpdates;
        document.getElementById('discontinued').textContent = data.stats.discontinued;
        document.getElementById('errors').textContent = data.stats.errors;

        if (data.systemPaused !== systemPaused) {
          systemPaused = data.systemPaused;
          updateSystemStatus();
        }
        
        if (data.failsafePending !== failsafePending) {
          location.reload();
        }
        
        const historyBody = document.getElementById('runHistoryBody');
        historyBody.innerHTML = (data.runHistory || []).map(r => \`
          <tr class="border-b border-gray-700">
            <td class="px-4 py-2 capitalize font-semibold \${r.type === 'inventory' ? 'text-green-400' : r.type === 'products' ? 'text-blue-400' : 'text-orange-400'}">\${r.type}</td>
            <td class="px-4 py-2">\${new Date(r.timestamp).toLocaleString()}</td>
            <td class="px-4 py-2">
              \${r.type === 'inventory' ? \`Updated: \${r.updated || 0}, Connected: \${r.connected || 0}, Skipped: \${r.skipped || 0}, Errors: \${r.errors || 0}\` : ''}
              \${r.type === 'products' ? \`Created: \${r.created || 0}, Errors: \${r.errors || 0}\` : ''}
              \${r.type === 'discontinued' ? \`Discontinued: \${r.discontinued || 0}, Errors: \${r.errors || 0}\` : ''}
            </td>
          </tr>
        \`).join('');
      } catch (e) {
        console.error("Failed to refresh status", e);
      }
    }
    
    setInterval(fetchAndUpdateStatus, 10000);
  </script>
</body>
</html>
  `);
});

// API endpoints (rest remains the same...)
app.get('/api/status', (req, res) => {
  res.json({
    stats,
    runHistory,
    systemPaused,
    failsafeTriggered,
    failsafePending: !!failsafePending,
    failsafeReason,
    logs: logs.slice(0, 50),
    mismatches: mismatches.slice(0, 50),
  });
});

app.post('/api/failsafe/decision', (req, res) => {
  const { decision } = req.body;
  
  if (!failsafePending) {
    return res.json({ success: false, message: 'No failsafe pending' });
  }
  
  if (decision === 'proceed') {
    failsafeDecisionMade = true;
    addLog('Failsafe override accepted via dashboard', 'warning', 'failsafe');
    res.json({ success: true, message: 'Proceeding with operation despite failsafe warning' });
  } else {
    failsafeDecisionMade = false;
    failsafePending = null;
    failsafeTriggered = true;
    failsafeReason = failsafePending?.reason || 'User cancelled operation';
    addLog('Failsafe decision: Operation cancelled by user', 'error', 'failsafe');
    res.json({ success: true, message: 'Operation cancelled' });
  }
});

app.post('/api/stats/reset', (req, res) => {
  stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
  addLog('Counters have been manually reset.', 'info', 'system');
  res.json({ success: true });
});

app.post('/api/failsafe/clear', (req, res) => {
  failsafeTriggered = false;
  failsafeReason = '';
  failsafePending = null;
  failsafeDecisionMade = false;
  lastFailsafeNotified = '';
  addLog('Failsafe cleared manually', 'info', 'system');
  notifyTelegram('✅ Failsafe cleared');
  res.json({ success: true });
});

app.post('/api/pause', (req, res) => {
  systemPaused = !systemPaused;
  abortVersion++;
  addLog(`System ${systemPaused ? 'paused' : 'resumed'}`, 'info', 'system');
  res.json({ success: true, paused: systemPaused });
});

app.post('/telegram/webhook/:secret?', async (req, res) => {
  try {
    if (TELEGRAM_WEBHOOK_SECRET && req.params.secret !== TELEGRAM_WEBHOOK_SECRET) {
      return res.status(403).json({ ok: true });
    }
    
    const update = req.body || {};
    
    if (update.callback_query) {
      const query = update.callback_query;
      const data = query.data;
      
      if (data === 'failsafe_proceed' || data === 'failsafe_cancel') {
        if (!failsafePending) {
          await notifyTelegram('No failsafe decision pending');
        } else {
          if (data === 'failsafe_proceed') {
            failsafeDecisionMade = true;
            await notifyTelegram('✅ Proceeding with operation despite failsafe warning');
          } else {
            failsafeDecisionMade = false;
            failsafePending = null;
            failsafeTriggered = true;
            await notifyTelegram('❌ Operation cancelled');
          }
        }
      }
      
      try {
        await axios.post(
          `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`,
          { callback_query_id: query.id }
        );
      } catch {}
      
      return res.json({ ok: true });
    }
    
    const msg = update.message || update.edited_message;
    if (!msg || !msg.text) return res.json({ ok: true });

    const chatId = String(msg.chat?.id || '');
    if (TELEGRAM_CHAT_ID && String(TELEGRAM_CHAT_ID) !== chatId) {
      return res.json({ ok: true });
    }

    const originalText = String(msg.text).trim();
    const text = originalText.toLowerCase();

    if (text === '/proceed' || text === 'proceed') {
      if (!failsafePending) {
        await notifyTelegram('No failsafe decision pending');
      } else {
        failsafeDecisionMade = true;
        await notifyTelegram('✅ Proceeding with operation despite failsafe warning');
      }
      return res.json({ ok: true });
    }
    
    if (text === '/cancel' || text === 'cancel') {
      if (!failsafePending) {
        await notifyTelegram('No failsafe decision pending');
      } else {
        failsafeDecisionMade = false;
        failsafePending = null;
        failsafeTriggered = true;
        await notifyTelegram('❌ Operation cancelled');
      }
      return res.json({ ok: true });
    }

    if (text === '/status' || text === 'status') {
      const pendingText = failsafePending ? ' (FAILSAFE PENDING)' : '';
      const summary =
        `Status: ${systemPaused ? 'PAUSED' : 'ACTIVE'}${failsafeTriggered ? ' (FAILSAFE)' : ''}${pendingText}\n` +
        `Totals: new=${stats.newProducts}, inv=${stats.inventoryUpdates}, disc=${stats.discontinued}, errors=${stats.errors}`;
      await notifyTelegram(summary);
      return res.json({ ok: true });
    }

    if (text === '/pause' || text === 'pause') {
      systemPaused = true;
      abortVersion++;
      addLog('System paused via Telegram', 'warning', 'telegram');
      await notifyTelegram('⏸ System paused');
      return res.json({ ok: true });
    }

    if (text === '/resume' || text === 'resume') {
      failsafeTriggered = false;
      failsafeReason = '';
      failsafePending = null;
      systemPaused = false;
      addLog('System resumed via Telegram', 'info', 'telegram');
      await notifyTelegram('▶️ System resumed');
      return res.json({ ok: true });
    }

    if (text.startsWith('/oos ') || text.startsWith('oos ')) {
      const key = originalText.split(' ').slice(1).join(' ').trim();
      if (!key) {
        await notifyTelegram('Usage: /oos <handle-or-product-url-or-title>');
        return res.json({ ok: true });
      }
      try {
        const result = await markProductOutOfStockByKey(key);
        await notifyTelegram(result ? `✅ Set OOS: ${result.title}` : '❌ Product not found');
      } catch (e) {
        addLog(`Telegram OOS error: ${e.message}`, 'error', 'telegram');
        await notifyTelegram(`❌ Failed to set OOS: ${e.message}`);
      }
      return res.json({ ok: true });
    }

    await notifyTelegram('Commands:\n/status\n/pause\n/resume\n/proceed (when failsafe pending)\n/cancel (when failsafe pending)\n/oos <handle|url|title>');
    res.json({ ok: true });
  } catch (e) {
    addLog(`Telegram webhook error: ${e.message}`, 'error', 'telegram');
    res.json({ ok: true });
  }
});

app.post('/api/sync/products', async (req, res) => {
  const started = startBackgroundJob('products', 'Product sync', async (token) => {
    try {
      const apifyData = await getApifyProducts();
      if (shouldAbort(token)) return;
      const processedProducts = processApifyProducts(apifyData, { processPrice: true, applyMinimumStock: true });
      if (shouldAbort(token)) return;
      await createNewProductsJob(token, processedProducts);
    } catch (error) {
      addLog(`Product sync failed: ${error.message}`, 'error', 'products');
      stats.errors++;
    }
  });
  res.json({ success: started, message: started ? 'Product sync started' : 'Product sync already running' });
});

app.post('/api/sync/inventory', async (req, res) => {
  const started = startBackgroundJob('inventory', 'Inventory sync', async (token) => {
    try {
      await updateInventoryJob(token);
    } catch (error) {
      addLog(`Inventory sync failed: ${error.message}`, 'error', 'inventory');
      stats.errors++;
    }
  });
  res.json({ success: started, message: started ? 'Inventory sync started' : 'Inventory sync already running' });
});

app.post('/api/sync/discontinued', async (req, res) => {
  const started = startBackgroundJob('discontinued', 'Discontinued check', async (token) => {
    try {
      await handleDiscontinuedProductsJob(token);
    } catch (error) {
      addLog(`Discontinued check failed: ${error.message}`, 'error', 'discontinued');
      stats.errors++;
    }
  });
  res.json({ success: started, message: started ? 'Discontinued check started' : 'Discontinued check already running' });
});

app.post('/api/fix/inventory-tracking', async (req, res) => {
  const started = startBackgroundJob('fixTracking', 'Fix inventory tracking', async (token) => {
    try {
      const shopifyData = await getShopifyProducts();
      let fixed = 0, errors = 0;
      for (const product of shopifyData) {
        if (shouldAbort(token)) { addLog('Aborting fix-tracking due to pause/failsafe', 'warning'); break; }
        const variant = product.variants?.[0];
        if (!variant) continue;
        if (!variant.inventory_management) {
          try {
            addLog(`Fixing inventory tracking for: ${product.title}`, 'info');
            await shopifyClient.put(`/variants/${variant.id}.json`, {
              variant: { id: variant.id, inventory_management: 'shopify', inventory_policy: 'deny' }
            });
            if (variant.inventory_item_id) {
              await shopifyClient.post('/inventory_levels/connect.json', {
                location_id: parseInt(config.shopify.locationId),
                inventory_item_id: variant.inventory_item_id
              }).catch(() => {});
            }
            fixed++;
            await new Promise(r => setTimeout(r, 300));
          } catch (e) {
            errors++;
            addLog(`Failed to fix ${product.title}: ${e.message}`, 'error');
          }
        }
      }
      addLog(`Inventory tracking fix complete: ${fixed} fixed, ${errors} errors`, 'success');
    } catch (error) {
      addLog(`Inventory tracking fix failed: ${error.message}`, 'error');
      stats.errors++;
    }
  });
  if (!started) return res.json({ success: false, message: 'Fix inventory tracking already running' });
  res.json({ success: true, message: 'Fix inventory tracking started in background' });
});

// Schedules
cron.schedule('0 1 * * *', () => {
  if (systemPaused || failsafeTriggered) return;
  startBackgroundJob('inventory', 'Scheduled inventory sync', async (token) => {
    await updateInventoryJob(token);
  });
});

cron.schedule('0 2 * * 5', () => {
  if (systemPaused || failsafeTriggered) return;
  startBackgroundJob('products', 'Scheduled product sync', async (token) => {
    try {
      const apifyData = await getApifyProducts();
      if (shouldAbort(token)) return;
      const processedProducts = processApifyProducts(apifyData, { processPrice: true, applyMinimumStock: true });
      await createNewProductsJob(token, processedProducts);
    } catch (error) {
      addLog(`Scheduled product sync failed: ${error.message}`, 'error', 'products');
    }
  });
});

cron.schedule('0 3 * * *', () => {
  if (systemPaused || failsafeTriggered) return;
  startBackgroundJob('discontinued', 'Scheduled discontinued check', async (token) => {
    await handleDiscontinuedProductsJob(token);
  });
});

cron.schedule('0 9 * * 1', async () => {
  addLog('Running weekly error summary job...', 'info', 'system');
  const oneWeekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  const recentErrors = errorLog.filter(e => e.timestamp > oneWeekAgo);

  if (recentErrors.length === 0) {
    notifyTelegram('✅ <b>Weekly Error Summary</b>\n\nNo errors were logged in the past 7 days. Great job!');
    return;
  }
  
  const summary = new Map();
  recentErrors.forEach(err => {
    const key = `${err.jobType}: ${err.message.substring(0, 100)}`;
    summary.set(key, (summary.get(key) || 0) + 1);
  });

  let summaryText = `
📄 <b>Weekly Error Summary</b>
Total errors in the last 7 days: <b>${recentErrors.length}</b>

<b>Breakdown:</b>
`;
  for (const [message, count] of summary.entries()) {
    summaryText += `- ${message} (x${count})\n`;
  }

  notifyTelegram(summaryText);
  errorLog = errorLog.filter(e => e.timestamp >= oneWeekAgo);
});

app.listen(PORT, () => {
  const requiredEnv = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'];
  const missing = requiredEnv.filter(key => !process.env[key]);
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error', 'system');
    process.exit(1);
  }
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success', 'system');
  addLog('Schedules: Inventory (Daily 1am), Products (Weekly Fri 2am), Discontinued (Daily 3am)', 'info', 'system');
  addLog('Enhanced: Better rate limit handling with retries and progressive delays', 'info', 'system');
  addLog('CRITICAL FIX: Proper product-location connection with error handling', 'info', 'system');
  if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) addLog('Telegram notifications: ENABLED', 'info', 'system');
});
