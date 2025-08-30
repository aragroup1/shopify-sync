const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
let lastRun = { inventory: { updated: 0, errors: 0, at: null }, products: { created: 0, errors: 0, at: null }, discontinued: { discontinued: 0, errors: 0, at: null } };
let logs = [];
let systemPaused = false;
let mismatches = [];
let failsafeTriggered = false;
let failsafeReason = '';
const missingCounters = new Map(); // for discontinued safety

// Job locks to avoid double-running
const jobLocks = { inventory: false, products: false, discontinued: false, fixTracking: false };

// Global abort version; bump on pause/failsafe to signal all jobs to abort ASAP
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered || token !== abortVersion;

// Failsafe configuration
const FAILSAFE_LIMITS = {
  MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 100),
  MIN_SHOPIFY_PRODUCTS: Number(process.env.MIN_SHOPIFY_PRODUCTS || 100),
  MAX_CHANGE_PERCENTAGE: Number(process.env.MAX_CHANGE_PERCENTAGE || 30),
  MAX_ERROR_RATE: Number(process.env.MAX_ERROR_RATE || 20),
  MAX_DISCONTINUED_AT_ONCE: Number(process.env.MAX_DISCONTINUED_AT_ONCE || 100),
  FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) // 5 min
};
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);

// Telegram notifications (optional)
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
let lastFailsafeNotified = '';

async function notifyTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 });
  } catch (e) {
    addLog(`Telegram notify failed: ${e.message}`, 'warning');
  }
}

// Track last known good state
let lastKnownGoodState = { apifyCount: 0, shopifyCount: 0, timestamp: null };

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 200) logs = logs.slice(0, 200);
  console.log(`[${log.timestamp}] ${message}`);
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
      // already logged
    } finally {
      jobLocks[key] = false;
      addLog(`${name} job finished`, 'info');
    }
  });
  return true;
}

function triggerFailsafe(msg) {
  if (!failsafeTriggered) {
    failsafeTriggered = true;
    failsafeReason = msg;
    systemPaused = true;
    abortVersion++; // signal all jobs to abort
    addLog(`⚠️ FAILSAFE TRIGGERED: ${failsafeReason}`, 'error');
    addLog('System automatically paused to prevent potential damage', 'error');
    if (lastFailsafeNotified !== msg) {
      lastFailsafeNotified = msg;
      notifyTelegram(`🚨 Failsafe triggered\nReason: ${msg}`);
    }
  }
}

function checkFailsafeConditions(context, data = {}) {
  const checks = [];
  switch (context) {
    case 'fetch': {
      if (typeof data.apifyCount === 'number' && data.apifyCount < FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS) checks.push(`Apify products too low: ${data.apifyCount} < ${FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS}`);
      if (typeof data.shopifyCount === 'number' && data.shopifyCount < FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS) checks.push(`Shopify products too low: ${data.shopifyCount} < ${FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS}`);
      if (lastKnownGoodState.apifyCount > 0 && typeof data.apifyCount === 'number') {
        const changePercent = Math.abs((data.apifyCount - lastKnownGoodState.apifyCount) / lastKnownGoodState.apifyCount * 100);
        if (changePercent > FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE) checks.push(`Apify product count changed by ${changePercent.toFixed(1)}% (was ${lastKnownGoodState.apifyCount}, now ${data.apifyCount})`);
      }
      break;
    }
    case 'inventory': {
      if (data.totalProducts > 0 && data.updatesNeeded > data.totalProducts * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100)) checks.push(`Too many inventory changes: ${data.updatesNeeded} > ${Math.floor(data.totalProducts * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100))}`);
      if (typeof data.errorRate === 'number' && data.errorRate > FAILSAFE_LIMITS.MAX_ERROR_RATE) checks.push(`Error rate too high: ${data.errorRate.toFixed(1)}% > ${FAILSAFE_LIMITS.MAX_ERROR_RATE}%`);
      break;
    }
    case 'discontinued': {
      if (data.toDiscontinue > FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE) checks.push(`Too many products to discontinue: ${data.toDiscontinue} > ${FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE}`);
      break;
    }
    case 'products': {
      if (data.existingCount > 0 && data.toCreate > data.existingCount * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100)) checks.push(`Too many new products: ${data.toCreate} > ${Math.floor(data.existingCount * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100))}`);
      break;
    }
  }
  if (checks.length > 0) {
    triggerFailsafe(checks.join('; '));
    return false;
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

// API clients with timeout
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
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

async function getApifyProducts() {
  let allItems = [];
  let offset = 0;
  let pageCount = 0;
  const limit = 500;

  addLog('Starting Apify product fetch...', 'info');

  try {
    while (true) {
      const response = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=${limit}&offset=${offset}`);
      const items = response.data;
      allItems.push(...items);
      pageCount++;
      addLog(`Apify page ${pageCount}: fetched ${items.length} products (total: ${allItems.length})`, 'info');
      if (items.length < limit) break;
      offset += limit;
      await new Promise(r => setTimeout(r, 1000));
    }
  } catch (error) {
    addLog(`Apify fetch error: ${error.message}`, 'error');
    stats.errors++;
    triggerFailsafe(`Apify fetch failed: ${error.message}`);
    throw error;
  }

  addLog(`Apify fetch complete: ${allItems.length} total products`, 'info');
  if (!checkFailsafeConditions('fetch', { apifyCount: allItems.length })) throw new Error('Failsafe triggered: Apify product count anomaly');
  return allItems;
}

// onlyApifyTag=true returns only Supplier:Apify tagged products; false returns ALL Shopify products
async function getShopifyProducts({ onlyApifyTag = true } = {}) {
  let allProducts = [];
  let sinceId = null;
  let pageCount = 0;
  const limit = 250;
  const fields = 'id,handle,title,variants,tags';

  addLog('Starting Shopify product fetch...', 'info');

  try {
    while (true) {
      let url = `/products.json?limit=${limit}&fields=${fields}`;
      if (sinceId) url += `&since_id=${sinceId}`;
      const response = await shopifyClient.get(url);
      const products = response.data.products;
      allProducts.push(...products);
      pageCount++;
      addLog(`Shopify page ${pageCount}: fetched ${products.length} products (total: ${allProducts.length})`, 'info');
      if (products.length < limit) break;
      sinceId = products[products.length - 1].id;
      await new Promise(r => setTimeout(r, 500));
    }
  } catch (error) {
    addLog(`Shopify fetch error: ${error.message}`, 'error');
    stats.errors++;
    triggerFailsafe(`Shopify fetch failed: ${error.message}`);
    throw error;
  }

  const filtered = onlyApifyTag ? allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify')) : allProducts;
  addLog(`Shopify fetch complete: ${allProducts.length} total products, ${onlyApifyTag ? filtered.length + ' with Supplier:Apify tag' : 'using ALL products for matching'}`, 'info');
  if (!checkFailsafeConditions('fetch', { shopifyCount: filtered.length })) throw new Error('Failsafe triggered: Shopify product count anomaly');
  return filtered;
}

function normalizeHandle(input, index, isTitle = false) {
  let handle = input || '';
  if (!isTitle && handle && handle !== 'undefined') {
    handle = handle.replace(config.apify.urlPrefix, '')
      .replace(/\.html$/, '')
      .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
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
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
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

function generateSEODescription(product) {
  const title = product.title;
  const originalDescription = product.description || '';
  const features = [];
  if (title.toLowerCase().includes('halloween')) features.push('perfect for Halloween celebrations');
  if (title.toLowerCase().includes('kids') || title.toLowerCase().includes('children')) features.push('designed for children');
  if (title.toLowerCase().includes('game') || title.toLowerCase().includes('puzzle')) features.push('entertaining and educational');
  if (title.toLowerCase().includes('mask')) features.push('comfortable and easy to wear');
  if (title.toLowerCase().includes('decoration')) features.push('enhances any space');
  if (title.toLowerCase().includes('toy')) features.push('safe and durable construction');

  let seo = `${title} - Premium quality ${title.toLowerCase()} available at LandOfEssentials. `;
  if (originalDescription && originalDescription.length > 20) seo += `${originalDescription.substring(0, 150)}... `;
  if (features.length > 0) seo += `This product is ${features.join(', ')}. `;
  seo += `Order now for fast delivery. Shop with confidence at LandOfEssentials - your trusted online retailer for quality products.`;
  return seo;
}

function processApifyProducts(apifyData, options = { processPrice: true }) {
  return apifyData.map((item, index) => {
    const handle = extractHandleFromCanonicalUrl(item, index);
    if (!handle) {
      addLog(`Failed to generate handle for ${item.title || 'unknown'}`, 'error');
      stats.errors++;
      return null;
    }

    let cleanTitle = item.title || 'Untitled Product';
    cleanTitle = cleanTitle.replace(/\b\d{4}\b/g, '')
      .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
      .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
      .replace(/\s+/g, ' ')
      .trim();

    // Inventory normalization with status
    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20;
    const rawStatus = item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || item.variants?.[0]?.stockStatus || '';
    const normalizedStatus = String(rawStatus).trim().toLowerCase().replace(/[\s_-]/g, '');
    const isOut = ['outofstock', 'soldout', 'unavailable', 'nostock'].includes(normalizedStatus);
    const isIn = ['instock', 'available', 'instockavailable'].includes(normalizedStatus);
    if (isOut) inventory = 0;
    else if (isIn && (!inventory || inventory === 0)) inventory = 20;

    // Pricing (only when creating)
    let price = 0, finalPrice = 0, compareAtPrice = 0;
    if (options.processPrice) {
      if (item.variants?.length > 0) {
        const variant = item.variants[0];
        if (variant.price) price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
      }
      if (!price) price = parseFloat(item.price || 0);
      const originalPrice = price;
      const hasDecimals = String(price).includes('.');
      if (price > 0 && price < 10 && hasDecimals) { /* already pounds */ }
      else if (!hasDecimals && price > 50) { price = price / 100; if (index < 5) addLog(`Price converted from pence: ${originalPrice} → ${price}`, 'info'); }
      else if (price > 1000) { price = price / 100; if (index < 5) addLog(`Price converted from pence (high): ${originalPrice} → ${price}`, 'info'); }

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

      if (!finalPrice || finalPrice < 5) { addLog(`Minimum price applied for ${item.title}: ${price.toFixed(2)} → 15.00`, 'warning'); price = 5.00; finalPrice = 15.00; }
      compareAtPrice = (finalPrice * 1.2).toFixed(2);
      if (index < 5) addLog(`Price debug for ${cleanTitle}: base=${price.toFixed(2)}, final=${finalPrice.toFixed(2)}`, 'info');
    }

    const images = [];
    if (Array.isArray(item.medias)) for (let i = 0; i < Math.min(3, item.medias.length); i++) if (item.medias[i]?.url) images.push(item.medias[i].url);

    const productData = {
      handle, title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '',
      originalPrice: (price || 0).toFixed(2),
      price: (finalPrice || 0).toFixed(2),
      compareAtPrice,
      inventory, images, vendor: 'LandOfEssentials'
    };
    if (options.processPrice) productData.seoDescription = generateSEODescription(productData);
    return productData;
  }).filter(Boolean);
}

async function enableInventoryTracking(productId, variantId) {
  try {
    await shopifyClient.put(`/variants/${variantId}.json`, { variant: { id: variantId, inventory_management: 'shopify', inventory_policy: 'deny' } });
    addLog(`Enabled inventory tracking for variant ${variantId}`, 'success');
    return true;
  } catch (error) {
    addLog(`Failed to enable inventory tracking for variant ${variantId}: ${error.message}`, 'error');
    return false;
  }
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

// Jobs
async function createNewProductsJob(token, productsToCreate) {
  if (shouldAbort(token)) { addLog('Product creation skipped - paused/failsafe', 'warning'); return { created: 0, errors: 0, total: productsToCreate.length }; }

  // Compare AGAINST ALL SHOPIFY PRODUCTS to avoid false "new"
  const shopifyAll = await getShopifyProducts({ onlyApifyTag: false });
  const shopifyHandles = new Set(shopifyAll.map(p => p.handle.toLowerCase()));
  const toCreateList = productsToCreate.filter(p => !shopifyHandles.has(p.handle.toLowerCase()));

  addLog(`Planned new products: ${toCreateList.length} (after comparing against ALL Shopify)`, 'info');

  if (!checkFailsafeConditions('products', { toCreate: toCreateList.length, existingCount: shopifyAll.length })) {
    return { created: 0, errors: 0, total: toCreateList.length };
  }

  let created = 0, errors = 0;
  const batchSize = 5;

  for (let i = 0; i < toCreateList.length; i += batchSize) {
    if (shouldAbort(token)) { addLog('Aborting product creation due to pause/failsafe', 'warning'); break; }
    const batch = toCreateList.slice(i, i + batchSize);

    for (const product of batch) {
      if (shouldAbort(token)) { addLog('Aborting product creation due to pause/failsafe', 'warning'); break; }
      try {
        const shopifyProduct = {
          title: product.title,
          body_html: (product.seoDescription || '').replace(/\n/g, '<br>'),
          handle: product.handle,
          vendor: product.vendor,
          product_type: 'General',
          tags: `Supplier:Apify,Cost:${product.originalPrice},SKU:${product.sku},Auto-Sync`,
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
        addLog(`Created: ${product.title} (£${product.price}) with tracking enabled`, 'success');
        created++;
        stats.newProducts++;
        await new Promise(r => setTimeout(r, 3000));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`Failed to create ${product.title}: ${error.message}`, 'error');
        if (error.response?.status === 429) {
          addLog('Rate limit hit - waiting 30 seconds', 'warning');
          await new Promise(r => setTimeout(r, 30000));
        }
      }
    }
    if (i + batchSize < toCreateList.length) {
      addLog(`Completed batch ${Math.floor(i / batchSize) + 1}, waiting before next batch...`, 'info');
      await new Promise(r => setTimeout(r, 8000));
    }
  }

  lastRun.products = { created, errors, at: new Date().toISOString() };
  addLog(`Product creation completed: ${created} created, ${errors} errors`, created > 0 ? 'success' : 'info');
  return { created, errors, total: toCreateList.length };
}

async function handleDiscontinuedProductsJob(token) {
  let discontinued = 0, errors = 0;

  try {
    addLog('Checking for discontinued products...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    if (shouldAbort(token)) { addLog('Aborting discontinued due to pause/failsafe', 'warning'); return { discontinued: 0, errors: 0, total: 0 }; }

    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
    const shopifyMaps = buildShopifyMaps(shopifyData);

    const matchedShopifyIds = new Set();
    apifyProcessed.forEach(apifyProd => {
      const { product } = matchShopifyProduct(apifyProd, shopifyMaps);
      if (product) matchedShopifyIds.add(product.id);
    });

    const candidates = shopifyData.filter(p => !matchedShopifyIds.has(p.id));
    addLog(`Discontinued scan: ${candidates.length} unmatched Shopify products (pre-threshold)`, 'info');

    const nowMissing = [];
    for (const p of candidates) {
      if (shouldAbort(token)) { addLog('Aborting discontinued due to pause/failsafe', 'warning'); break; }
      const key = p.handle.toLowerCase();
      const count = (missingCounters.get(key) || 0) + 1;
      missingCounters.set(key, count);
      if (count >= DISCONTINUE_MISS_RUNS) nowMissing.push(p);
    }
    shopifyData.forEach(p => { if (matchedShopifyIds.has(p.id)) missingCounters.delete(p.handle.toLowerCase()); });

    addLog(`Consecutive-miss filter: ${nowMissing.length} eligible after ${DISCONTINUE_MISS_RUNS} runs`, 'info');

    if (!checkFailsafeConditions('discontinued', { toDiscontinue: nowMissing.length })) {
      return { discontinued: 0, errors: 0, total: nowMissing.length };
    }

    for (const product of nowMissing) {
      if (shouldAbort(token)) { addLog('Aborting discontinued due to pause/failsafe', 'warning'); break; }
      try {
        if (product.variants?.[0]) {
          const qty = product.variants[0].inventory_quantity || 0;
          if (qty > 0) {
            await shopifyClient.post('/inventory_levels/set.json', {
              location_id: config.shopify.locationId,
              inventory_item_id: product.variants[0].inventory_item_id,
              available: 0
            });
            addLog(`Discontinued: ${product.title} (${qty} → 0)`, 'success');
            discontinued++;
            stats.discontinued++;
          }
        }
        await new Promise(r => setTimeout(r, 400));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`Failed to discontinue ${product.title}: ${error.message}`, 'error');
      }
    }

    lastRun.discontinued = { discontinued, errors, at: new Date().toISOString() };
    addLog(`Discontinued product check completed: ${discontinued} discontinued, ${errors} errors`, discontinued > 0 ? 'success' : 'info');
    return { discontinued, errors, total: nowMissing.length };
  } catch (error) {
    addLog(`Discontinued product workflow failed: ${error.message}`, 'error');
    stats.errors++;
    return { discontinued, errors: errors + 1, total: 0 };
  }
}

async function updateInventoryJob(token) {
  if (systemPaused) {
    addLog('Inventory update skipped - system is paused', 'warning');
    return { updated: 0, created: 0, errors: 0, total: 0 };
  }

  let updated = 0, errors = 0, trackingEnabled = 0;
  mismatches = [];

  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    if (shouldAbort(token)) { addLog('Aborting inventory update due to pause/failsafe', 'warning'); return { updated, created: 0, errors, total: 0 }; }

    if (apifyData.length > FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS && shopifyData.length > FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS) {
      lastKnownGoodState = { apifyCount: apifyData.length, shopifyCount: shopifyData.length, timestamp: new Date().toISOString() };
    }

    addLog(`Data comparison: ${apifyData.length} Apify products vs ${shopifyData.length} Shopify products`, 'info');

    const processedProducts = processApifyProducts(apifyData, { processPrice: false });
    addLog(`Processed ${processedProducts.length} valid Apify products after filtering`, 'info');

    const maps = buildShopifyMaps(shopifyData);

    let matchedCount = 0;
    let matchedByHandle = 0, matchedByTitle = 0, matchedBySku = 0, matchedByPartial = 0;
    let skippedSameInventory = 0, skippedNoInventoryItemId = 0;
    const inventoryUpdates = [];
    const debugSample = [];

    processedProducts.forEach((apifyProduct, idx) => {
      const { product: shopifyProduct, matchType } = matchShopifyProduct(apifyProduct, maps);
      if (!shopifyProduct) {
        if (mismatches.length < 100) mismatches.push({ apifyTitle: apifyProduct.title, apifyHandle: apifyProduct.handle, apifyUrl: apifyProduct.url || apifyProduct.canonicalUrl || 'N/A', shopifyHandle: 'NOT FOUND' });
        if (idx < 10) debugSample.push({ apifyTitle: apifyProduct.title, apifyHandle: apifyProduct.handle, apifySku: apifyProduct.sku, matched: false, matchType: 'none', shopifyHandle: 'N/A' });
        return;
      }

      matchedCount++;
      if (matchType === 'handle') matchedByHandle++; else if (matchType === 'title') matchedByTitle++; else if (matchType === 'sku') matchedBySku++; else if (matchType === 'partial-handle') matchedByPartial++;

      if (!shopifyProduct.variants?.[0]?.inventory_item_id) { skippedNoInventoryItemId++; return; }

      const currentInventory = shopifyProduct.variants[0].inventory_quantity || 0;
      const targetInventory = apifyProduct.inventory;

      if (currentInventory === targetInventory) { skippedSameInventory++; return; }

      inventoryUpdates.push({
        handle: apifyProduct.handle,
        title: shopifyProduct.title,
        currentInventory,
        newInventory: targetInventory,
        inventoryItemId: shopifyProduct.variants[0].inventory_item_id,
        matchType,
        productId: shopifyProduct.id,
        variantId: shopifyProduct.variants[0].id
      });

      if (idx < 10) debugSample.push({ apifyTitle: apifyProduct.title, apifyHandle: apifyProduct.handle, apifySku: apifyProduct.sku, matched: true, matchType, shopifyHandle: shopifyProduct.handle });
    });

    addLog('=== MATCHING DEBUG SAMPLE ===', 'info');
    debugSample.forEach(item => addLog(`${item.matched ? '✓' : '✗'} ${item.apifyTitle} | Handle: ${item.apifyHandle} | SKU: ${item.apifySku || 'none'} | Match: ${item.matchType} | Shopify: ${item.shopifyHandle}`, item.matched ? 'info' : 'warning'));

    addLog('=== MATCHING ANALYSIS ===', 'info');
    addLog(`Total matched: ${matchedCount} (${((matchedCount / processedProducts.length) * 100).toFixed(1)}%)`, 'info');
    addLog(`- Matched by handle: ${matchedByHandle}`, 'info');
    addLog(`- Matched by SKU: ${matchedBySku}`, 'info');
    addLog(`- Matched by title: ${matchedByTitle}`, 'info');
    addLog(`- Matched by partial: ${matchedByPartial}`, 'info');
    addLog(`Skipped (no inventory_item_id): ${skippedNoInventoryItemId}`, 'info');
    addLog(`Skipped (same inventory): ${skippedSameInventory}`, 'info');
    addLog(`Updates needed: ${inventoryUpdates.length}`, 'info');
    addLog(`Mismatches: ${processedProducts.length - matchedCount}`, 'warning');

    const errorRate = errors > 0 ? (errors / Math.max(inventoryUpdates.length, 1) * 100) : 0;
    if (!checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalProducts: processedProducts.length, errorRate })) {
      return { updated: 0, created: 0, errors: 0, total: inventoryUpdates.length };
    }

    if (!config.shopify.locationId) {
      addLog('ERROR: SHOPIFY_LOCATION_ID environment variable is not set!', 'error');
      return { updated: 0, created: 0, errors: 1, total: inventoryUpdates.length };
    }

    for (const update of inventoryUpdates) {
      if (shouldAbort(token)) { addLog('Aborting inventory update due to pause/failsafe', 'warning'); break; }
      try {
        // Ensure tracking
        const variantResp = await shopifyClient.get(`/products/${update.productId}.json?fields=variants`);
        const variant = variantResp.data.product.variants.find(v => v.id === update.variantId) || variantResp.data.product.variants[0];
        if (!variant.inventory_management) {
          addLog(`Enabling inventory tracking for: ${update.title}`, 'info');
          const enabled = await enableInventoryTracking(update.productId, variant.id);
          if (!enabled) { errors++; stats.errors++; continue; }
          trackingEnabled++;
          await new Promise(r => setTimeout(r, 800));
        }

        // Ensure connected to location
        const checkResponse = await shopifyClient.get(`/inventory_levels.json?inventory_item_ids=${update.inventoryItemId}&location_ids=${config.shopify.locationId}`).catch(() => null);
        if (!checkResponse?.data?.inventory_levels?.length) {
          try {
            await shopifyClient.post('/inventory_levels/connect.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: update.inventoryItemId });
            addLog(`Connected inventory item to location for: ${update.title}`, 'info');
            await new Promise(r => setTimeout(r, 250));
          } catch { /* ignore */ }
        }

        await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: update.inventoryItemId, available: update.newInventory });
        addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory}) [${update.matchType}]`, 'success');
        updated++;
        stats.inventoryUpdates++;
        await new Promise(r => setTimeout(r, 400));
      } catch (error) {
        errors++;
        stats.errors++;
        const errData = error.response?.data;
        const errText = JSON.stringify(errData?.errors || errData?.error || error.message);
        addLog(`✗ Failed: ${update.title} - Status ${error.response?.status || 'NA'}: ${errText}`, 'error');
        if (error.response?.status === 429) {
          addLog('Rate limit hit - waiting 30 seconds', 'warning');
          await new Promise(r => setTimeout(r, 30000));
        } else {
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    }

    stats.lastSync = new Date().toISOString();
    lastRun.inventory = { updated, errors, at: stats.lastSync };
    addLog('=== INVENTORY UPDATE COMPLETE ===', 'info');
    addLog(`Result: ${updated} updated, ${trackingEnabled} tracking enabled, ${errors} errors out of ${inventoryUpdates.length} attempts`, updated > 0 ? 'success' : 'info');
    return { updated, created: 0, errors, total: inventoryUpdates.length, trackingEnabled };
  } catch (error) {
    addLog(`Inventory update workflow failed: ${error.message}`, 'error');
    stats.errors++;
    return { updated, created: 0, errors: errors + 1, total: 0 };
  }
}

// UI (permanent dark mode, non-blocking)
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
    body { background: linear-gradient(to bottom right, #1a1a2e, #16213e); }
    .gradient-bg { background: linear-gradient(135deg, #ff6e7f 0%, #bfe9ff 100%); box-shadow: 0 10px 20px rgba(255,110,127,0.4); position: relative; overflow: hidden; }
    .card-hover { transition: all .3s ease-in-out; background: rgba(31,41,55,.2); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,.1); box-shadow: 0 8px 32px rgba(0,0,0,.4); }
    .card-hover:hover { transform: translateY(-8px) scale(1.03); box-shadow: 0 12px 40px rgba(0,0,0,.3); border-color: rgba(255,110,127,.5); }
    .btn-hover { transition: all .2s ease-in-out; } .btn-hover:hover { transform: translateY(-1px); }
    .fade-in { animation: fadeIn .6s ease-in-out; } @keyframes fadeIn { from { opacity: 0; transform: translateY(30px); } to { opacity: 1; transform: translateY(0); } }
    .spinner { display: none; border: 4px solid rgba(255,255,255,.3); border-top: 4px solid #ff6e7f; border-radius: 50%; width: 20px; height: 20px; animation: spin 1s linear infinite; margin-left: 8px;}
    @keyframes spin { 0% { transform: rotate(0deg);} 100%{ transform: rotate(360deg);} }
    .log-container { background: rgba(17,24,39,.95); backdrop-filter: blur(10px); box-shadow: 0 0 30px rgba(0,0,0,.3); }
  </style>
</head>
<body class="min-h-screen font-sans">
  <div class="container mx-auto px-4 py-8">
    <div class="relative bg-gray-800 rounded-2xl shadow-lg p-8 mb-8 gradient-bg text-white fade-in">
      <h1 class="text-4xl font-extrabold tracking-tight">Shopify Sync Dashboard</h1>
      <p class="mt-2 text-lg opacity-90">Seamless product synchronization with Apify, optimized for SEO</p>
    </div>

    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-4">
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-blue-400">New Products (Total)</h3><p class="text-3xl font-bold text-gray-100" id="newProducts">${stats.newProducts}</p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-green-400">Inventory Updates (Total)</h3><p class="text-3xl font-bold text-gray-100" id="inventoryUpdates">${stats.inventoryUpdates}</p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-orange-400">Discontinued (Total)</h3><p class="text-3xl font-bold text-gray-100" id="discontinued">${stats.discontinued}</p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-lg font-semibold text-red-400">Errors (Total)</h3><p class="text-3xl font-bold text-gray-100" id="errors">${stats.errors}</p></div>
    </div>

    <div class="grid grid-cols-1 sm:grid-cols-3 gap-6 mb-8">
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-md font-semibold text-blue-300">Last Product Run</h3><p class="text-gray-300 text-sm">Created: <span id="lrCreated">${lastRun.products.created}</span>, Errors: <span id="lrPErrors">${lastRun.products.errors}</span></p><p class="text-gray-500 text-xs">At: <span id="lrPAt">${lastRun.products.at || '-'}</span></p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-md font-semibold text-green-300">Last Inventory Run</h3><p class="text-gray-300 text-sm">Updated: <span id="lrUpdated">${lastRun.inventory.updated}</span>, Errors: <span id="lrIErrors">${lastRun.inventory.errors}</span></p><p class="text-gray-500 text-xs">At: <span id="lrIAt">${lastRun.inventory.at || '-'}</span></p></div>
      <div class="rounded-2xl p-6 card-hover fade-in"><h3 class="text-md font-semibold text-orange-300">Last Discontinued Run</h3><p class="text-gray-300 text-sm">Discontinued: <span id="lrDisc">${lastRun.discontinued.discontinued}</span>, Errors: <span id="lrDErrors">${lastRun.discontinued.errors}</span></p><p class="text-gray-500 text-xs">At: <span id="lrDAt">${lastRun.discontinued.at || '-'}</span></p></div>
    </div>

    <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
      <h2 class="text-2xl font-semibold text-gray-100 mb-4">System Controls</h2>

      ${failsafeTriggered ? `
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
            <p class="text-sm ${systemPaused ? 'text-red-400' : 'text-green-400'}" id="systemStatusDesc">${systemPaused ? 'Automatic syncing is disabled' : 'Automatic syncing every 30min (inventory) & 6hrs (products)'}</p>
          </div>
          <div class="flex items-center">
            <button onclick="togglePause()" id="pauseButton" class="${systemPaused ? 'bg-green-500 hover:bg-green-600' : 'bg-red-500 hover:bg-red-600'} text-white px-4 py-2 rounded-lg btn-hover">
              ${systemPaused ? 'Resume System' : 'Pause System'}
            </button>
            <div id="pauseSpinner" class="spinner"></div>
          </div>
        </div>
      </div>

      <div class="flex flex-wrap gap-4">
        <div class="flex items-center">
          <button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover">Create New Products</button>
          <div id="productsSpinner" class="spinner"></div>
        </div>
        <div class="flex items-center">
          <button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover">Update Inventory</button>
          <div id="inventorySpinner" class="spinner"></div>
        </div>
        <div class="flex items-center">
          <button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover">Check Discontinued</button>
          <div id="discontinuedSpinner" class="spinner"></div>
        </div>
        <div class="flex items-center">
          <button onclick="fixInventoryTracking()" class="bg-purple-500 text-white px-6 py-3 rounded-lg btn-hover">Fix Inventory Tracking</button>
          <div id="fixSpinner" class="spinner"></div>
        </div>
        ${mismatches.length > 0 ? `
        <div class="flex items-center">
          <button onclick="triggerSync('mismatches')" class="bg-yellow-500 text-white px-6 py-3 rounded-lg btn-hover">Create from Mismatches</button>
          <div id="mismatchesSpinner" class="spinner"></div>
        </div>` : ''}
      </div>

      <div class="mt-4 p-4 rounded-lg bg-gray-700">
        <p class="text-sm text-gray-400"><strong>Failsafe Protection Active:</strong></p>
        <ul class="text-xs text-gray-400 mt-2">
          <li>• Auto-pauses if Apify/Shopify fetch fails</li>
          <li>• Prevents large unexpected changes (>30%)</li>
          <li>• Global abort on failsafe/pause (stops running jobs)</li>
          <li>• Discontinue only after ${DISCONTINUE_MISS_RUNS} consecutive missing runs</li>
        </ul>
      </div>
    </div>

    <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
      <h2 class="text-2xl font-semibold text-gray-100 mb-4">Mismatch Report</h2>
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left text-gray-400 mismatch-table">
          <thead class="text-xs text-gray-400 uppercase bg-gray-700">
            <tr>
              <th class="px-4 py-2" onclick="sortTable(0)">Apify Title</th>
              <th class="px-4 py-2" onclick="sortTable(1)">Apify Handle</th>
              <th class="px-4 py-2" onclick="sortTable(2)">Apify URL</th>
              <th class="px-4 py-2" onclick="sortTable(3)">Shopify Handle</th>
            </tr>
          </thead>
          <tbody>
            ${mismatches.slice(0, 20).map(m => `
              <tr class="border-b border-gray-700">
                <td class="px-4 py-2">${m.apifyTitle || ''}</td>
                <td class="px-4 py-2">${m.apifyHandle || ''}</td>
                <td class="px-4 py-2">${m.apifyUrl || ''}</td>
                <td class="px-4 py-2">${m.shopifyHandle || ''}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>

    <div class="rounded-2xl p-6 card-hover fade-in log-container">
      <h2 class="text-2xl font-semibold text-gray-100 mb-4">Activity Log</h2>
      <div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer">
        ${logs.map(log => `<div class="${log.type === 'success' ? 'text-green-400' : log.type === 'error' ? 'text-red-400' : log.type === 'warning' ? 'text-yellow-400' : 'text-gray-300'}">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`).join('')}
      </div>
    </div>
  </div>

  <script>
    let systemPaused = ${systemPaused};

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
        } else {
          addLogEntry('❌ Failed to toggle pause', 'error');
        }
      } catch (e) {
        addLogEntry('❌ Failed to toggle pause', 'error');
      } finally {
        button.disabled = false; spinner.style.display = 'none';
      }
    }

    function updateSystemStatus() {
      const statusEl = document.getElementById('systemStatus');
      const statusDescEl = document.getElementById('systemStatusDesc');
      const buttonEl = document.getElementById('pauseButton');
      const containerEl = buttonEl.closest('.rounded-lg');
      if (systemPaused) {
        statusEl.textContent = 'System Status: PAUSED';
        statusEl.className = 'font-medium text-red-300';
        statusDescEl.textContent = 'Automatic syncing is disabled';
        statusDescEl.className = 'text-sm text-red-400';
        buttonEl.textContent = 'Resume System';
        buttonEl.className = 'bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover';
        containerEl.className = 'mb-6 p-4 rounded-lg bg-red-900 border-red-700 border';
      } else {
        statusEl.textContent = 'System Status: ACTIVE';
        statusEl.className = 'font-medium text-green-300';
        statusDescEl.textContent = 'Automatic syncing every 30min (inventory) & 6hrs (products)';
        statusDescEl.className = 'text-sm text-green-400';
        buttonEl.textContent = 'Pause System';
        buttonEl.className = 'bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover';
        containerEl.className = 'mb-6 p-4 rounded-lg bg-green-900 border-green-700 border';
      }
    }

    async function clearFailsafe() {
      try {
        const res = await fetch('/api/failsafe/clear', { method: 'POST' });
        const result = await res.json();
        if (result.success) {
          addLogEntry('✅ Failsafe cleared', 'success');
          setTimeout(() => location.reload(), 1000);
        }
      } catch (e) {
        addLogEntry('❌ Failed to clear failsafe', 'error');
      }
    }

    async function fixInventoryTracking() {
      const button = event.target;
      const spinner = document.getElementById('fixSpinner');
      if (!confirm('This will enable inventory tracking for all products that don\\'t have it. Continue?')) return;
      button.disabled = true; spinner.style.display='inline-block';
      try {
        const res = await fetch('/api/fix/inventory-tracking', { method: 'POST' });
        const result = await res.json();
        if (result.success) addLogEntry('✅ ' + result.message, 'success');
        else addLogEntry('❌ Failed to fix inventory tracking', 'error');
      } catch (e) {
        addLogEntry('❌ Failed to fix inventory tracking', 'error');
      } finally {
        button.disabled = false; spinner.style.display='none';
      }
    }

    async function triggerSync(type) {
      const button = event.target;
      const spinner = document.getElementById(type + 'Spinner');
      button.disabled = true; spinner.style.display = 'inline-block';
      try {
        const res = await fetch('/api/sync/' + type, { method: 'POST' });
        const result = await res.json();
        if (result.success) addLogEntry('✅ ' + result.message, 'success');
        else addLogEntry('❌ ' + (result.message || 'Sync failed to start'), 'error');
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
      const newLog = document.createElement('div');
      newLog.className = color;
      newLog.textContent = '[' + time + '] ' + message;
      logContainer.insertBefore(newLog, logContainer.firstChild);
    }

    function sortTable(n) {
      const table = document.querySelector('.mismatch-table tbody');
      const rows = Array.from(table.getElementsByTagName('tr'));
      const isAsc = table.dataset.sortDir !== 'asc';
      table.dataset.sortDir = isAsc ? 'asc' : 'desc';
      rows.sort((a, b) => {
        const aText = a.getElementsByTagName('td')[n].textContent;
        const bText = b.getElementsByTagName('td')[n].textContent;
        return isAsc ? aText.localeCompare(bText) : bText.localeCompare(aText);
      });
      while (table.firstChild) table.removeChild(table.firstChild);
      rows.forEach(row => table.appendChild(row));
    }

    // Auto-refresh stats every 30 seconds
    setInterval(async () => {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        document.getElementById('newProducts').textContent = data.newProducts;
        document.getElementById('inventoryUpdates').textContent = data.inventoryUpdates;
        document.getElementById('discontinued').textContent = data.discontinued;
        document.getElementById('errors').textContent = data.errors;
        document.getElementById('lrCreated').textContent = data.lastRun?.products?.created ?? 0;
        document.getElementById('lrPErrors').textContent = data.lastRun?.products?.errors ?? 0;
        document.getElementById('lrPAt').textContent = data.lastRun?.products?.at ?? '-';
        document.getElementById('lrUpdated').textContent = data.lastRun?.inventory?.updated ?? 0;
        document.getElementById('lrIErrors').textContent = data.lastRun?.inventory?.errors ?? 0;
        document.getElementById('lrIAt').textContent = data.lastRun?.inventory?.at ?? '-';
        document.getElementById('lrDisc').textContent = data.lastRun?.discontinued?.discontinued ?? 0;
        document.getElementById('lrDErrors').textContent = data.lastRun?.discontinued?.errors ?? 0;
        document.getElementById('lrDAt').textContent = data.lastRun?.discontinued?.at ?? '-';
        if (data.systemPaused !== systemPaused) { systemPaused = data.systemPaused; updateSystemStatus(); }
      } catch (e) {}
    }, 30000);
  </script>
</body>
</html>
  `);
});

// API endpoints
app.get('/api/status', (req, res) => {
  res.json({
    ...stats,
    lastRun,
    systemPaused,
    failsafeTriggered,
    failsafeReason,
    logs: logs.slice(0, 50),
    mismatches: mismatches.slice(0, 50),
    uptime: process.uptime(),
    lastKnownGoodState,
    environment: {
      apifyToken: config.apify.token ? 'SET' : 'MISSING',
      shopifyToken: config.shopify.accessToken ? 'SET' : 'MISSING',
      shopifyDomain: config.shopify.domain || 'MISSING'
    }
  });
});

app.post('/api/failsafe/clear', (req, res) => {
  failsafeTriggered = false;
  failsafeReason = '';
  lastFailsafeNotified = '';
  addLog('Failsafe cleared manually', 'info');
  notifyTelegram('✅ Failsafe cleared');
  res.json({ success: true });
});

app.get('/api/mismatches', (req, res) => {
  res.json({ mismatches });
});

app.get('/api/locations', async (req, res) => {
  try {
    const response = await shopifyClient.get('/locations.json');
    const locations = response.data.locations;
    res.json({ currentLocationId: config.shopify.locationId, availableLocations: locations.map(loc => ({ id: loc.id, name: loc.name, active: loc.active })) });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/pause', (req, res) => {
  systemPaused = !systemPaused;
  abortVersion++; // abort running jobs immediately
  addLog(`System ${systemPaused ? 'paused' : 'resumed'}`, 'info');
  res.json({ success: true, paused: systemPaused });
});

// Background-trigger endpoints: return immediately
app.post('/api/sync/products', async (req, res) => {
  const started = startBackgroundJob('products', 'Product sync', async (token) => {
    try {
      const apifyData = await getApifyProducts();
      if (shouldAbort(token)) return;
      const processedProducts = processApifyProducts(apifyData, { processPrice: true });
      if (shouldAbort(token)) return;
      await createNewProductsJob(token, processedProducts);
    } catch (error) {
      addLog(`Product sync failed: ${error.message}`, 'error');
      stats.errors++;
    }
  });
  if (!started) return res.json({ success: false, message: 'Product sync already running' });
  res.json({ success: true, message: 'Product sync started in background' });
});

app.post('/api/sync/inventory', async (req, res) => {
  const started = startBackgroundJob('inventory', 'Inventory sync', async (token) => {
    try {
      const result = await updateInventoryJob(token);
      lastRun.inventory = { updated: result.updated, errors: result.errors, at: new Date().toISOString() };
    } catch (error) {
      addLog(`Inventory sync failed: ${error.message}`, 'error');
      stats.errors++;
    }
  });
  if (!started) return res.json({ success: false, message: 'Inventory sync already running' });
  res.json({ success: true, message: 'Inventory sync started in background' });
});

app.post('/api/sync/discontinued', async (req, res) => {
  const started = startBackgroundJob('discontinued', 'Discontinued check', async (token) => {
    try {
      const result = await handleDiscontinuedProductsJob(token);
      lastRun.discontinued = { discontinued: result.discontinued, errors: result.errors, at: new Date().toISOString() };
    } catch (error) {
      addLog(`Discontinued check failed: ${error.message}`, 'error');
      stats.errors++;
    }
  });
  if (!started) return res.json({ success: false, message: 'Discontinued check already running' });
  res.json({ success: true, message: 'Discontinued check started in background' });
});

app.post('/api/sync/mismatches', async (req, res) => {
  const started = startBackgroundJob('products', 'Create from mismatches', async (token) => {
    try {
      const [apifyData] = await Promise.all([getApifyProducts()]);
      if (shouldAbort(token)) return;
      const processedProducts = processApifyProducts(apifyData, { processPrice: true });
      await createNewProductsJob(token, processedProducts);
    } catch (error) {
      addLog(`Mismatch sync failed: ${error.message}`, 'error');
      stats.errors++;
    }
  });
  if (!started) return res.json({ success: false, message: 'Another product job is already running' });
  res.json({ success: true, message: 'Creating products from mismatches in background' });
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
            await shopifyClient.put(`/variants/${variant.id}.json`, { variant: { id: variant.id, inventory_management: 'shopify', inventory_policy: 'deny' } });
            if (variant.inventory_item_id) {
              await shopifyClient.post('/inventory_levels/connect.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: variant.inventory_item_id }).catch(() => {});
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

// Scheduled jobs with failsafe
cron.schedule('*/30 * * * *', async () => {
  if (systemPaused || failsafeTriggered) {
    addLog(`Scheduled inventory update skipped - system ${systemPaused ? 'paused' : 'failsafe triggered'}`, 'warning');
    return;
  }
  if (!jobLocks.inventory) startBackgroundJob('inventory', 'Scheduled inventory sync', async (token) => { await updateInventoryJob(token); });
});

cron.schedule('0 */6 * * *', async () => {
  if (systemPaused || failsafeTriggered) {
    addLog(`Scheduled product creation skipped - system ${systemPaused ? 'paused' : 'failsafe triggered'}`, 'warning');
    return;
  }
  if (!jobLocks.products) startBackgroundJob('products', 'Scheduled product sync', async (token) => {
    try {
      const apifyData = await getApifyProducts();
      if (shouldAbort(token)) return;
      const processedProducts = processApifyProducts(apifyData, { processPrice: true });
      if (shouldAbort(token)) return;
      await createNewProductsJob(token, processedProducts);
      if (shouldAbort(token)) return;
      await handleDiscontinuedProductsJob(token);
    } catch (error) {
      addLog(`Scheduled product creation failed: ${error.message}`, 'error');
      stats.errors++;
    }
  });
});

app.listen(PORT, () => {
  const requiredEnv = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'];
  const missing = requiredEnv.filter(key => !process.env[key]);
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error');
    process.exit(1);
  }
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success');
  addLog('Inventory updates: Every 30 minutes', 'info');
  addLog('Product creation: Every 6 hours', 'info');
  addLog('Failsafe protection: ACTIVE', 'info');
  if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) addLog('Telegram notifications: ENABLED', 'info');
  else addLog('Telegram notifications: DISABLED (set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)', 'warning');
});
