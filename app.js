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

function 
