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
const problemProducts = new Map(); // Track products that repeatedly fail

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
  
  // If we have problem products, include them in the debug info
  let problemProductInfo = '';
  if (problemProducts.size > 0) {
    const samples = Array.from(problemProducts.entries()).slice(0, 3);
    problemProductInfo = '\n\n<b>Problem Products (samples):</b>\n';
    samples.forEach(([handle, info]) => {
      problemProductInfo += `- ${info.title}: ${info.issue}\n`;
    });
  }
  
  const debugInfo = `
⚠️ <b>Failsafe Warning - Decision Required</b> ⚠️

<b>Reason:</b>
${msg}

<b>Context:</b>
${JSON.stringify(contextData, null, 2).substring(0, 1000)}
${problemProductInfo}

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

// Enhanced inventory level fetching with better diagnostics
async function getActualInventoryLevels(inventoryItemIds) {
  const inventoryMap = new Map();
  const batchSize = 20;
  let rateLimitDelay = 500;
  
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
        rateLimitDelay = Math.max(500, rateLimitDelay - 100);
        
        if (i % 200 === 0 && i > 0) {
          addLog(`Progress: Fetched ${Math.min(i + batchSize, inventoryItemIds.length)} of ${inventoryItemIds.length} inventory levels`, 'info', 'inventory');
        }
      } catch (error) {
        if (error.response?.status === 429) {
          retries--;
          rateLimitDelay = Math.min(5000, rateLimitDelay * 2);
          addLog(`Rate limit hit, waiting ${rateLimitDelay}ms before retry (${retries} retries left)`, 'warning', 'inventory');
          await new Promise(r => setTimeout(r, rateLimitDelay));
        } else {
          addLog(`Failed to fetch inventory batch: ${error.message}`, 'error', 'inventory');
          retries = 0;
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

// Enhanced inventory update with deep diagnostics for problem products
async function updateInventoryJob(token) {
  let updated = 0, errors = 0, trackingEnabled = 0, skipped = 0, connected = 0, trackingFixed = 0;
  mismatches = [];
  const repeatUpdates = [];
  problemProducts.clear(); // Clear previous problem products

  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info', 'inventory');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    if (apifyData.length > 
