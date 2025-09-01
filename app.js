const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null, skusMigrated: 0, duplicatesDeleted: 0 };
let lastRun = {
  inventory: { updated: 0, errors: 0, at: null },
  products: { created: 0, errors: 0, at: null, details: [] },
  discontinued: { discontinued: 0, errors: 0, at: null, details: [] },
  skuMigration: { migrated: 0, errors: 0, skipped: 0, duplicatesDeleted: 0, at: null, details: [] }
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

// ERROR TRACKING for weekly reports
const errorTracking = {
  errors: [],
  errorCounts: new Map(),
  lastReportSent: null
};

const jobLocks = { inventory: false, products: false, discontinued: false, skuMigration: false, duplicateCleanup: false };
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
const PRICING_RULES = [
  { min: 0, max: 1, action: 'add', value: 5.5 },
  { min: 1.01, max: 2, action: 'add', value: 5.95 },
  { min: 2.01, max: 3, action: 'add', value: 6.99 },
  { min: 3.01, max: 5, action: 'multiply', value: 3.2 },
  { min: 5.01, max: 7, action: 'multiply', value: 2.5 },
  { min: 7.01, max: 9, action: 'multiply', value: 2.2 },
  { min: 9.01, max: 12, action: 'multiply', value: 2 },
  { min: 12.01, max: 20, action: 'multiply', value: 1.9 },
  { min: 20.01, max: Infinity, action: 'multiply', value: 1.8 }
];

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
  
  // Track errors for weekly report
  if (type === 'error' || type === 'warning') {
    trackError(message, type);
  }
}

// NEW: Track errors for weekly reporting
function trackError(message, type) {
  // Store the error
  errorTracking.errors.push({
    timestamp: new Date().toISOString(),
    message,
    type
  });
  
  // Keep only last 7 days of errors
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  errorTracking.errors = errorTracking.errors.filter(e => new Date(e.timestamp) > sevenDaysAgo);
  
  // Extract error pattern (simplify the message to find common patterns)
  const pattern = message
    .replace(/"[^"]+"/g, '"{ITEM}"') // Replace quoted items with placeholder
    .replace(/\d+/g, '{NUM}') // Replace numbers with placeholder
    .replace(/SKU: \S+/g, 'SKU: {SKU}') // Replace SKUs with placeholder
    .substring(0, 100); // Limit length
  
  errorTracking.errorCounts.set(pattern, (errorTracking.errorCounts.get(pattern) || 0) + 1);
}

// NEW: Generate weekly error report
async function generateWeeklyErrorReport() {
  addLog('📊 Generating weekly error report...', 'info');
  
  const report = {
    period: {
      from: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
      to: new Date().toISOString()
    },
    totalErrors: errorTracking.errors.length,
    errorsByType: {},
    topErrors: [],
    recommendations: []
  };
  
  // Count errors by type
  errorTracking.errors.forEach(e => {
    report.errorsByType[e.type] = (report.errorsByType[e.type] || 0) + 1;
  });
  
  // Get top 10 most common error patterns
  const sortedErrors = Array.from(errorTracking.errorCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
  
  report.topErrors = sortedErrors.map(([pattern, count]) => ({
    pattern,
    count,
    percentage: ((count / report.totalErrors) * 100).toFixed(1)
  }));
  
  // Generate recommendations based on common errors
  if (report.topErrors.some(e => e.pattern.includes('SKU not found'))) {
    report.recommendations.push('Many SKU mismatches detected. Consider running a full SKU migration.');
  }
  if (report.topErrors.some(e => e.pattern.includes('Rate limit'))) {
    report.recommendations.push('Frequent rate limits. Consider increasing delay between API calls.');
  }
  if (report.topErrors.some(e => e.pattern.includes('No match found'))) {
    report.recommendations.push('Many products without matches. Review product title/handle formatting.');
  }
  if (report.topErrors.some(e => e.pattern.includes('inventory'))) {
    report.recommendations.push('Inventory sync issues detected. Check if all products have proper SKUs.');
  }
  
  // Format report for display and Telegram
  let reportText = `📊 <b>Weekly Error Report</b>\n`;
  reportText += `Period: ${new Date(report.period.from).toLocaleDateString()} - ${new Date(report.period.to).toLocaleDateString()}\n\n`;
  reportText += `<b>Summary:</b>\n`;
  reportText += `Total Errors: ${report.totalErrors}\n`;
  reportText += `Warnings: ${report.errorsByType.warning || 0}\n`;
  reportText += `Errors: ${report.errorsByType.error || 0}\n\n`;
  
  if (report.topErrors.length > 0) {
    reportText += `<b>Top Issues:</b>\n`;
    report.topErrors.slice(0, 5).forEach((e, i) => {
      reportText += `${i + 1}. ${e.pattern.substring(0, 50)}... (${e.count}x - ${e.percentage}%)\n`;
    });
    reportText += '\n';
  }
  
  if (report.recommendations.length > 0) {
    reportText += `<b>Recommendations:</b>\n`;
    report.recommendations.forEach((r, i) => {
      reportText += `${i + 1}. ${r}\n`;
    });
  }
  
  // Send to Telegram
  if (report.totalErrors > 0) {
    await notifyTelegram(reportText);
  }
  
  // Clear error counts for next week
  errorTracking.errorCounts.clear();
  errorTracking.lastReportSent = new Date().toISOString();
  
  addLog('Weekly error report sent successfully', 'success');
  return report;
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
  const rule = PRICING_RULES.find(r => cost >= r.min && cost <= r.max);
  if (!rule) {
    addLog(`Warning: No pricing rule for cost £${cost}, using 1.8x multiplier`, 'warning');
    return (cost * 1.8).toFixed(2);
  }
  let finalPrice;
  if (rule.action === 'add') {
    finalPrice = cost + rule.value;
  } else if (rule.action === 'multiply') {
    finalPrice = cost * rule.value;
  }
  finalPrice = Math.max(finalPrice, cost + 0.01);
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

async function getShopifyProducts({ onlyApifyTag = true, fields = 'id,handle,title,variants,tags,status,created_at,updated_at' } = {}) {
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

// ENHANCED: Better text normalization for matching
function normalizeForMatching(text = '') {
  return String(text)
    .toLowerCase()
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ') // Remove content in parentheses
    .replace(/\s*```math
.*?```\s*/g, ' ') // Remove content in brackets
    .replace(/-(parcel|large-letter|letter)-rate$/i, '') // Remove shipping suffixes
    .replace(/-p\d+$/i, '') // Remove product codes
    .replace(/\b(a|an|the|of|in|on|at|to|for|with|by)\b/g, '') // Remove common words
    .replace(/[^a-z0-9]+/g, ' ') // Replace non-alphanumeric with space
    .replace(/\s+/g, ' ') // Multiple spaces to single
    .trim();
}

// Calculate word similarity score
function calculateSimilarity(str1, str2) {
  const norm1 = normalizeForMatching(str1);
  const norm2 = normalizeForMatching(str2);
  
  if (norm1 === norm2) return 1;
  
  const words1 = norm1.split(' ').filter(w => w.length > 2);
  const words2 = norm2.split(' ').filter(w => w.length > 2);
  
  if (words1.length === 0 || words2.length === 0) return 0;
  
  let matches = 0;
  words1.forEach(w1 => {
    if (words2.includes(w1)) matches++;
  });
  
  return matches / Math.max(words1.length, words2.length);
}

function normalizeHandle(input, index) { 
  let handle = String(input || `product-${index}`).toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, ''); 
  if (!handle || handle === '-' || handle.length < 2) handle = `product-${index}-${Date.now()}`; 
  return handle; 
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
    
    const sku = item.sku || item.variants?.[0]?.sku || `APF-${handle}`;
    
    const result = { 
      handle, 
      title: item.title, 
      inventory, 
      sku,
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

function matchProductBySku(apifyProduct, shopifySkuMap) {
  if (!apifyProduct.sku) return null;
  return shopifySkuMap.get(apifyProduct.sku.toLowerCase()) || null;
}

// FIXED: Delete NEWER duplicates, keep OLDER ones (with sales history)
async function cleanupDuplicatesJob(token) {
  addLog('🧹 Starting duplicate cleanup (keeping OLDER products with sales history)...', 'info');
  let deleted = 0, errors = 0;
  const deletedList = [];
  
  try {
    const shopifyData = await getShopifyProducts({ onlyApifyTag: true });
    
    // Group products by normalized title
    const titleGroups = new Map();
    shopifyData.forEach(product => {
      const normalizedTitle = normalizeForMatching(product.title);
      if (!titleGroups.has(normalizedTitle)) {
        titleGroups.set(normalizedTitle, []);
      }
      titleGroups.get(normalizedTitle).push(product);
    });
    
    // Find and delete NEWER duplicates (keep OLDER ones)
    for (const [title, products] of titleGroups) {
      if (products.length > 1) {
        // Sort by created_at date (OLDEST first)
        products.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
        
        const oldest = products[0];
        const duplicatesToDelete = products.slice(1);
        
        addLog(`Found ${products.length} duplicates for "${oldest.title}". Keeping oldest (created: ${oldest.created_at})`, 'warning');
        
        // Delete the NEWER duplicates
        for (const duplicate of duplicatesToDelete) {
          if (shouldAbort(token)) break;
          
          try {
            await shopifyClient.delete(`/products/${duplicate.id}.json`);
            deleted++;
            stats.duplicatesDeleted++;
            deletedList.push({
              title: duplicate.title,
              handle: duplicate.handle,
              created: duplicate.created_at
            });
            addLog(`✓ Deleted newer duplicate: "${duplicate.title}" (created: ${duplicate.created_at})`, 'success');
            await new Promise(r => setTimeout(r, 500));
          } catch (error) {
            errors++;
            addLog(`✗ Failed to delete duplicate "${duplicate.title}": ${error.message}`, 'error');
          }
        }
      }
    }
    
    addLog(`✅ Duplicate cleanup complete! Deleted ${deleted} newer duplicates, kept older products with sales history`, 'success');
    
    if (deleted > 0) {
      let message = `🧹 <b>Duplicate Cleanup Complete</b>\n\n`;
      message += `Deleted: ${deleted} newer duplicates\n`;
      message += `Kept: Older products with sales history\n`;
      message += `Errors: ${errors}\n\n`;
      if (deletedList.length > 0) {
        message += `<b>Sample of deleted duplicates:</b>\n`;
        deletedList.slice(0, 5).forEach(p => {
          message += `• ${p.title} (created: ${new Date(p.created).toLocaleDateString()})\n`;
        });
      }
      await notifyTelegram(message);
    }
    
  } catch (error) {
    addLog(`Duplicate cleanup failed: ${error.message}`, 'error');
    errors++;
  }
  
  return { deleted, errors };
}

// ENHANCED: Aggressive SKU Migration with duplicate cleanup first
async function migrateSkusJob(token) {
  addLog('🔄 Starting AGGRESSIVE SKU Migration...', 'info');
  let migrated = 0, skipped = 0, errors = 0, alreadyHasSku = 0;
  const migrationDetails = [];
  
  try {
    // First, clean up duplicates (keeping older ones)
    addLog('Step 1: Cleaning up duplicates (keeping older products with sales history)...', 'info');
    const { deleted } = await cleanupDuplicatesJob(token);
    
    if (shouldAbort(token)) return;
    
    addLog('Step 2: Fetching fresh data after cleanup...', 'info');
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(),
      getShopifyProducts({ onlyApifyTag: true })
    ]);
    
    if (shouldAbort(token)) return;
    
    const apifyProcessed = processApifyProducts(apifyData);
    
    // Build multiple indexes for Apify products
    const apifyIndexes = {
      byNormalizedTitle: new Map(),
      byWords: new Map(),
      byHandle: new Map(),
      bySku: new Map(),
      all: apifyProcessed
    };
    
    apifyProcessed.forEach(p => {
      // Index by normalized title
      const normalizedTitle = normalizeForMatching(p.title);
      apifyIndexes.byNormalizedTitle.set(normalizedTitle, p);
      
      // Index by individual words (for partial matching)
      const words = normalizedTitle.split(' ').filter(w => w.length > 3);
      words.forEach(word => {
        if (!apifyIndexes.byWords.has(word)) {
          apifyIndexes.byWords.set(word, []);
        }
        apifyIndexes.byWords.get(word).push(p);
      });
      
      // Index by handle variations
      const handle = p.handle.toLowerCase();
      apifyIndexes.byHandle.set(handle, p);
      
      // Also index stripped handle
      const strippedHandle = handle
        .replace(/-(parcel|large-letter|letter)-rate$/i, '')
        .replace(/-p\d+$/i, '')
        .replace(/-[a-z0-9]{4,8}$/i, '');
      if (strippedHandle !== handle) {
        apifyIndexes.byHandle.set(strippedHandle, p);
      }
      
      // Index by SKU
      if (p.sku) {
        apifyIndexes.bySku.set(p.sku.toLowerCase(), p);
      }
    });
    
    addLog(`Step 3: Processing ${shopifyData.length} Shopify products for SKU migration...`, 'info');
    
    const unmatched = [];
    
    for (const shopifyProduct of shopifyData) {
      if (shouldAbort(token)) break;
      
      const currentSku = shopifyProduct.variants?.[0]?.sku;
      
      // Check if already has a valid Apify SKU
      if (currentSku && apifyIndexes.bySku.has(currentSku.toLowerCase())) {
        alreadyHasSku++;
        continue;
      }
      
      // Try multiple matching strategies
      let apifyMatch = null;
      let matchMethod = '';
      
      // Strategy 1: Exact normalized title match
      const normalizedShopifyTitle = normalizeForMatching(shopifyProduct.title);
      apifyMatch = apifyIndexes.byNormalizedTitle.get(normalizedShopifyTitle);
      if (apifyMatch) matchMethod = 'exact-title';
      
      // Strategy 2: Handle match
      if (!apifyMatch) {
        const shopifyHandle = shopifyProduct.handle.toLowerCase();
        apifyMatch = apifyIndexes.byHandle.get(shopifyHandle);
        if (apifyMatch) matchMethod = 'handle';
        
        if (!apifyMatch) {
          // Try stripped handle
          const strippedShopifyHandle = shopifyHandle
            .replace(/-(parcel|large-letter|letter)-rate$/i, '')
            .replace(/-p\d+$/i, '')
            .replace(/-[a-z0-9]{4,8}$/i, '');
          apifyMatch = apifyIndexes.byHandle.get(strippedShopifyHandle);
          if (apifyMatch) matchMethod = 'handle-stripped';
        }
      }
      
      // Strategy 3: Find best similarity match
      if (!apifyMatch) {
        let bestScore = 0;
        let bestMatch = null;
        
        // Get candidate products based on shared words
        const shopifyWords = normalizedShopifyTitle.split(' ').filter(w => w.length > 3);
        const candidates = new Set();
        
        shopifyWords.forEach(word => {
          const products = apifyIndexes.byWords.get(word) || [];
          products.forEach(p => candidates.add(p));
        });
        
        // Calculate similarity for each candidate
        for (const candidate of candidates) {
          const score = calculateSimilarity(shopifyProduct.title, candidate.title);
          if (score > bestScore && score > 0.6) { // Require at least 60% similarity
            bestScore = score;
            bestMatch = candidate;
          }
        }
        
        if (bestMatch) {
          apifyMatch = bestMatch;
          matchMethod = `similarity-${Math.round(bestScore * 100)}%`;
        }
      }
      
      // Strategy 4: If still no match, try partial title match
      if (!apifyMatch) {
        const shopifyTitleWords = normalizedShopifyTitle.split(' ').filter(w => w.length > 4);
        if (shopifyTitleWords.length > 0) {
          for (const apifyProduct of apifyProcessed) {
            const apifyTitleNorm = normalizeForMatching(apifyProduct.title);
            let matchCount = 0;
            for (const word of shopifyTitleWords) {
              if (apifyTitleNorm.includes(word)) matchCount++;
            }
            if (matchCount >= Math.min(3, shopifyTitleWords.length * 0.7)) {
              apifyMatch = apifyProduct;
              matchMethod = 'partial-title';
              break;
            }
          }
        }
      }
      
      if (apifyMatch && apifyMatch.sku) {
        try {
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
            newSku: apifyMatch.sku,
            method: matchMethod
          });
          
          addLog(`✓ [${matchMethod}] Migrated SKU for "${shopifyProduct.title}": ${currentSku || 'none'} → ${apifyMatch.sku}`, 'success');
          
          await new Promise(r => setTimeout(r, 500));
        } catch (error) {
          errors++;
          addLog(`✗ Failed to update SKU for "${shopifyProduct.title}": ${error.message}`, 'error');
        }
      } else {
        unmatched.push({
          title: shopifyProduct.title,
          handle: shopifyProduct.handle,
          currentSku: currentSku || 'none'
        });
        addLog(`⚠️ No match found for "${shopifyProduct.title}" - needs manual review`, 'warning');
        skipped++;
      }
    }
    
    const totalProcessed = migrated + skipped + alreadyHasSku;
    const successRate = ((migrated + alreadyHasSku) / totalProcessed * 100).toFixed(1);
    
    addLog(`✅ AGGRESSIVE SKU Migration Complete!`, 'success');
    addLog(`   Migrated: ${migrated}`, 'success');
    addLog(`   Already had SKU: ${alreadyHasSku}`, 'success');
    addLog(`   No match found: ${skipped}`, 'warning');
    addLog(`   Errors: ${errors}`, errors > 0 ? 'error' : 'info');
    addLog(`   Success rate: ${successRate}%`, 'success');
    
    // Report unmatched products
    if (unmatched.length > 0 && unmatched.length <= 20) {
      addLog('Unmatched products requiring manual review:', 'warning');
      unmatched.forEach(p => {
        addLog(`  - "${p.title}" (handle: ${p.handle})`, 'warning');
      });
    }
    
    let telegramMessage = `✅ <b>SKU Migration Complete</b>\n\n`;
    telegramMessage += `Migrated: ${migrated}\n`;
    telegramMessage += `Already correct: ${alreadyHasSku}\n`;
    telegramMessage += `No match: ${skipped}\n`;
    telegramMessage += `Errors: ${errors}\n`;
    telegramMessage += `Success rate: ${successRate}%\n\n`;
    
    if (unmatched.length > 0) {
      telegramMessage += `⚠️ ${unmatched.length} products need manual review`;
    }
    
    await notifyTelegram(telegramMessage);
    
  } catch (error) {
    addLog(`SKU Migration failed: ${error.message}`, 'error');
    errors++;
  }
  
  lastRun.skuMigration = { 
    migrated, 
    errors, 
    skipped,
    duplicatesDeleted: stats.duplicatesDeleted,
    at: new Date().toISOString(), 
    details: migrationDetails 
  };
}

// Core job functions (updateInventoryJob, handleDiscontinuedProductsJob, createNewProductsJob remain the same)
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
      addLog(`✓ Updated: "${update.title}" (SKU: ${update.sku}): ${update.currentInventory} → ${update.newInventory}`, 'success');
    } catch (error) { 
      errors++; 
      stats.errors++; 
      addLog(`✗ Failed to update "${update.title}": ${error.message}`, 'error'); 
    }
    await new Promise(r => setTimeout(r, 500));
  }
  return { updated, errors };
}

async function updateInventoryJob(token) {
  addLog('Starting inventory sync (SKU-based)...', 'info');
  mismatches = [];
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(), 
      getShopifyProducts({ onlyApifyTag: true })
    ]);
    
    if (shouldAbort(token) || apifyData.length === 0) return addLog('Aborting sync.', 'warning');
    
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
    
    const apifySkus = new Set();
    const apifyProcessed = processApifyProducts(apifyData);
    apifyProcessed.forEach(p => {
      if (p.sku) apifySkus.add(p.sku.toLowerCase());
    });
    
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
    
    const existingSkus = new Set();
    shopifyData.forEach(p => {
      const sku = p.variants?.[0]?.sku;
      if (sku) existingSkus.add(sku.toLowerCase());
    });
    
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: true, fullData: true });
    const toCreate = [];
    
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
            sku: product.sku,
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
          cost: product.costPrice,
          at: new Date().toISOString() 
        });
        recentlyCreated.unshift({ 
          title: product.title, 
          sku: product.sku,
          price: product.sellingPrice,
          cost: product.costPrice,
          at: new Date().toISOString() 
        });
        if (recentlyCreated.length > 50) recentlyCreated.length = 50;
        
        created++;
        stats.newProducts++;
        addLog(`✓ Created: "${product.title}" (SKU: ${product.sku}) - Cost: £${product.costPrice} → Sell: £${product.sellingPrice}`, 'success');
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

// UI - Enhanced with error report viewer
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
  </style>
</head>
<body class="min-h-screen font-sans text-gray-200">
  <div class="container mx-auto px-4 py-8">
    <h1 class="text-4xl font-extrabold tracking-tight text-white mb-8">Shopify Sync Dashboard</h1>
    
    <!-- Error Report Alert -->
    <div class="mb-6 p-4 rounded-lg bg-amber-900 border-2 border-amber-500">
      <h3 class="font-bold text-amber-300 mb-2">📊 Weekly Error Reporting</h3>
      <p class="text-sm text-amber-200 mb-3">System automatically generates weekly error reports every Sunday to help identify and fix common issues.</p>
      <div class="flex items-center gap-4">
        <button onclick="generateErrorReport()" class="bg-amber-500 hover:bg-amber-600 text-white px-4 py-2 rounded-lg btn-hover">
          Generate Error Report Now
        </button>
        <span class="text-amber-300 text-sm">Last report: ${errorTracking.lastReportSent ? new Date(errorTracking.lastReportSent).toLocaleString() : 'Never'}</span>
      </div>
    </div>
    
    <!-- SKU Migration Alert -->
    <div class="mb-6 p-4 rounded-lg bg-blue-900 border-2 border-blue-500">
      <h3 class="font-bold text-blue-300 mb-2">🔄 SKU Migration & Duplicate Cleanup</h3>
      <p class="text-sm text-blue-200 mb-3">Keeps OLDER products (with sales history), removes NEWER duplicates.</p>
      <div class="flex items-center gap-4">
        <button onclick="migrateSKUs()" class="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-lg btn-hover">
          Run Full SKU Migration
        </button>
        <button onclick="cleanupDuplicates()" class="bg-purple-500 hover:bg-purple-600 text-white px-4 py-2 rounded-lg btn-hover">
          Clean Duplicates Only
        </button>
      </div>
      <div class="mt-2 text-sm">
        <span class="text-blue-300">SKUs Migrated: ${stats.skusMigrated || 0}</span>
        <span class="text-purple-300 ml-4">Duplicates Deleted: ${stats.duplicatesDeleted || 0}</span>
        ${lastRun.skuMigration?.at ? `<span class="text-gray-400 ml-4">Last run: ${formatLastRun(lastRun.skuMigration)}</span>` : ''}
      </div>
    </div>
    
    <!-- Stats -->
    <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
      <div class="rounded-xl p-4 card-hover">
        <h3 class="text-sm">New Products</h3>
        <p class="text-2xl font-bold">${stats.newProducts}</p>
      </div>
      <div class="rounded-xl p-4 card-hover">
        <h3 class="text-sm">Inventory Updates</h3>
        <p class="text-2xl font-bold">${stats.inventoryUpdates}</p>
      </div>
      <div class="rounded-xl p-4 card-hover">
        <h3 class="text-sm">Discontinued</h3>
        <p class="text-2xl font-bold">${stats.discontinued}</p>
      </div>
      <div class="rounded-xl p-4 card-hover">
        <h3 class="text-sm">Total Errors</h3>
        <p class="text-2xl font-bold text-red-400">${stats.errors}</p>
      </div>
    </div>
    
    <!-- Current Error Summary -->
    <div id="errorSummary" class="rounded-xl p-6 card-hover mb-8 hidden">
      <h2 class="text-xl font-semibold mb-4">Current Week Error Summary</h2>
      <div id="errorSummaryContent"></div>
    </div>
    
    <!-- Controls -->
    <div class="rounded-xl p-6 card-hover mb-8">
      <h2 class="text-xl font-semibold mb-4">System Controls</h2>
      <div class="flex flex-wrap gap-3">
        <button onclick="togglePause()" id="pauseButton" class="px-4 py-2 rounded-lg btn-hover">Pause</button>
        <button onclick="triggerSync('products')" class="bg-blue-500 text-white px-4 py-2 rounded-lg btn-hover">Create Products</button>
        <button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-4 py-2 rounded-lg btn-hover">Update Inventory</button>
        <button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-4 py-2 rounded-lg btn-hover">Check Discontinued</button>
      </div>
    </div>
    
    <!-- Logs -->
    <div class="rounded-xl p-6 card-hover">
      <h2 class="text-xl font-semibold mb-4">Activity Log</h2>
      <div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer"></div>
    </div>
  </div>
  
  <script>
    let systemPaused = ${systemPaused};
    function togglePause() { fetch('/api/pause', { method: 'POST' }).then(() => location.reload()); }
    function triggerSync(type) { fetch('/api/sync/' + type, { method: 'POST' }); }
    function migrateSKUs() { if(confirm('This will migrate SKUs and clean newer duplicates (keeping older products with sales history). Continue?')) fetch('/api/migrate-skus', { method: 'POST' }); }
    function cleanupDuplicates() { if(confirm('This will delete NEWER duplicate products (keeping older ones with sales history). Continue?')) fetch('/api/cleanup-duplicates', { method: 'POST' }); }
    function generateErrorReport() { fetch('/api/error-report', { method: 'POST' }); }
    
    setInterval(async () => {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        
        // Update logs
        document.getElementById('logContainer').innerHTML = data.logs.map(log => 
          \`<div class="\${log.type === 'error' ? 'text-red-400' : log.type === 'warning' ? 'text-yellow-400' : log.type === 'success' ? 'text-green-400' : 'text-gray-300'}">
            [\${new Date(log.timestamp).toLocaleTimeString()}] \${log.message}
          </div>\`
        ).join('');
        
        // Update error summary if available
        if (data.errorSummary && data.errorSummary.totalErrors > 0) {
          document.getElementById('errorSummary').classList.remove('hidden');
          let summaryHtml = '<div class="grid grid-cols-2 gap-4">';
          summaryHtml += \`<div><span class="text-gray-400">Total Errors (7 days):</span> <span class="text-red-400 font-bold">\${data.errorSummary.totalErrors}</span></div>\`;
          summaryHtml += \`<div><span class="text-gray-400">Most Common:</span> <span class="text-amber-400">\${data.errorSummary.topError || 'N/A'}</span></div>\`;
          summaryHtml += '</div>';
          document.getElementById('errorSummaryContent').innerHTML = summaryHtml;
        }
      } catch (e) {}
    }, 2000);
  </script>
</body>
</html>`;
  res.send(html);
});

app.get('/api/status', (req, res) => { 
  // Get quick error summary
  const errorSummary = {
    totalErrors: errorTracking.errors.length,
    topError: errorTracking.errorCounts.size > 0 ? 
      Array.from(errorTracking.errorCounts.entries()).sort((a,b) => b[1] - a[1])[0][0] : null
  };
  
  res.json({ 
    stats, 
    lastRun, 
    logs, 
    systemPaused, 
    recentlyCreated, 
    recentlyDiscontinued,
    errorSummary
  }); 
});

app.post('/api/pause', (req, res) => { 
  systemPaused = !systemPaused; 
  abortVersion++; 
  addLog(`System ${systemPaused ? 'paused' : 'resumed'}.`, 'warning'); 
  res.json({ success: true }); 
});

app.post('/api/migrate-skus', (req, res) => { 
  startBackgroundJob('skuMigration', 'Aggressive SKU Migration', migrateSkusJob) 
    ? res.json({ success: true }) 
    : res.status(409).json({ success: false, message: "Already running" }); 
});

app.post('/api/cleanup-duplicates', (req, res) => { 
  startBackgroundJob('duplicateCleanup', 'Duplicate Cleanup', cleanupDuplicatesJob) 
    ? res.json({ success: true }) 
    : res.status(409).json({ success: false, message: "Already running" }); 
});

app.post('/api/error-report', (req, res) => { 
  generateWeeklyErrorReport().then(() => res.json({ success: true }))
    .catch(e => res.status(500).json({ success: false, error: e.message })); 
});

app.post('/api/sync/:type', (req, res) => { 
  const { type } = req.params; 
  const jobs = { inventory: updateInventoryJob, products: createNewProductsJob, discontinued: handleDiscontinuedProductsJob }; 
  if (!jobs[type]) return res.status(400).json({success: false}); 
  startBackgroundJob(type, `Manual ${type} sync`, jobs[type]) 
    ? res.json({success: true}) 
    : res.status(409).json({success: false}); 
});

// Scheduled jobs
cron.schedule('0 1 * * *', () => { 
  if (!systemPaused && !failsafeTriggered) 
    startBackgroundJob('inventory', 'Scheduled inventory sync', updateInventoryJob); 
});
cron.schedule('0 2 * * 5', () => { 
  if (!systemPaused && !failsafeTriggered) 
    startBackgroundJob('products', 'Scheduled Product Sync', createNewProductsJob); 
});
cron.schedule('0 3 * * *', () => { 
  if (!systemPaused && !failsafeTriggered) 
    startBackgroundJob('discontinued', 'Scheduled Discontinued Check', handleDiscontinuedProductsJob); 
});

// NEW: Weekly error report - every Sunday at 9 AM
cron.schedule('0 9 * * 0', () => {
  addLog('Running scheduled weekly error report...', 'info');
  generateWeeklyErrorReport();
});

const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]);
  if (missing.length > 0) { 
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); 
    process.exit(1); 
  }
  addLog('System configured to keep OLDER products (with sales history) when cleaning duplicates', 'info');
  addLog('Weekly error reports scheduled for Sundays at 9 AM', 'info');
});

process.on('SIGTERM', () => { server.close(() => process.exit(0)); });
process.on('SIGINT', () => { server.close(() => process.exit(0)); });
