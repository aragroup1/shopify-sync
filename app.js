const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0 };
let lastRun = { inventory: {}, products: { createdItems: [] }, discontinued: { discontinuedItems: [] }, deduplicate: {}, mapSkus: {} };
let logs = [];
let systemPaused = false;
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;
const missingCounters = new Map();
let errorSummary = new Map();

const jobLocks = { inventory: false, products: false, discontinued: false, mapSkus: false, deduplicate: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), MAX_DISCONTINUE_PERCENTAGE: Number(process.env.MAX_DISCONTINUE_PERCENTAGE || 5), MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
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
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`  // ✅ FIXED with backticks
  } 
};
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper Functions ---
function addLog(message, type = 'info', error = null) {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 200) logs.length = 200;
  console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`);

  if (type === 'error') {
    stats.errors++;
    let normalizedError = (error?.message || message).replace(/"[^"]+"/g, '"{VAR}"').replace(/\b\d{5,}\b/g, '{ID}');
    errorSummary.set(normalizedError, (errorSummary.get(normalizedError) || 0) + 1);
  }
}
async function notifyTelegram(text) { 
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return; 
  try { 
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 }); 
  } catch (e) { 
    addLog(`Telegram notify failed: ${e.message}`, 'warning', e); 
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
      addLog(`Unhandled error in ${name}: ${e.message}\n${e.stack}`, 'error', e); 
    } finally { 
      jobLocks[key] = false; 
      addLog(`${name} job finished`, 'info'); 
    } 
  }); 
  return true; 
}
function getWordOverlap(str1, str2) { 
  const words1 = new Set(str1.split(' ')); 
  const words2 = new Set(str2.split(' ')); 
  const intersection = new Set([...words1].filter(x => words2.has(x))); 
  return (intersection.size / Math.max(words1.size, words2.size)) * 100; 
}

// --- Data Fetching & Processing ---
async function getApifyProducts() { 
  let allItems = []; 
  let offset = 0; 
  addLog('Starting Apify product fetch...', 'info'); 
  try { 
    while (true) { 
      const { data } = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=1000&offset=${offset}`); 
      allItems.push(...data); 
      if (data.length < 1000) break; 
      offset += 1000; 
    } 
  } catch (error) { 
    addLog(`Apify fetch error: ${error.message}`, 'error', error); 
    throw error; 
  } 
  addLog(`Apify fetch complete: ${allItems.length} total products.`, 'info'); 
  return allItems; 
}

async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) { 
  let allProducts = []; 
  addLog(`Starting Shopify fetch...`, 'info'); 
  try { 
    let url = `/products.json?limit=250&fields=${fields}`; 
    while (url) { 
      const response = await shopifyClient.get(url); 
      allProducts.push(...response.data.products); 
      const linkHeader = response.headers.link; 
      url = null; 
      if (linkHeader) { 
        const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"')); 
        if (nextLink) { 
          const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/); 
          if (pageInfoMatch) url = `/products.json?limit=250&fields=${fields}&page_info=${pageInfoMatch[1]}`; 
        } 
      } 
      await new Promise(r => setTimeout(r, 500)); 
    } 
  } catch (error) { 
    addLog(`Shopify fetch error: ${error.message}`, 'error', error); 
    throw error; 
  } 
  addLog(`Shopify fetch complete: ${allProducts.length} total products.`, 'info'); 
  return allProducts; 
}

// --- Rest of your long script remains exactly the same except with this fixed config ---
